#!/usr/bin/env python3

# Copyright Bunting Labs, Inc. 2025

import xml.etree.ElementTree as ET
import copy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Tuple, Optional


class ChangeType(Enum):
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


@dataclass
class ProjectDifference:
    path: str
    change_type: ChangeType
    old_value: Any = None
    new_value: Any = None
    description: str = ""


@dataclass
class StructuralChange:
    element_type: str
    parent_path: str
    old_element: Optional[ET.Element] = None
    new_element: Optional[ET.Element] = None
    change_type: ChangeType = ChangeType.MODIFIED


IGNORED_ATTRIBUTES = [
    "saveDateTime",
    "saveUser",
    "saveUserFull",
    "extent",
    "DefaultViewExtent",
]


class QGSProjectDiffer:
    def __init__(self):
        self.ignored_attributes = IGNORED_ATTRIBUTES

    def parse_qgs_content(self, content):
        try:
            if content.strip().startswith("<!DOCTYPE"):
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    if line.strip().startswith("<qgis"):
                        content = "\n".join(lines[i:])
                        break
            return ET.fromstring(content)
        except ET.ParseError as e:
            raise ET.ParseError(f"Failed to parse QGS content: {e}")

    def detect_layer_changes(self, old_root: ET.Element, new_root: ET.Element) -> List[StructuralChange]:
        """Smart layer comparison by ID rather than position"""
        changes = []
        
        old_layers = old_root.findall('.//maplayer')
        new_layers = new_root.findall('.//maplayer')
        
        # Build ID mappings
        old_layer_ids = {}
        new_layer_ids = {}
        
        for layer in old_layers:
            id_elem = layer.find('id')
            if id_elem is not None and id_elem.text:
                old_layer_ids[id_elem.text] = layer
                
        for layer in new_layers:
            id_elem = layer.find('id')
            if id_elem is not None and id_elem.text:
                new_layer_ids[id_elem.text] = layer
        
        all_layer_ids = set(old_layer_ids.keys()) | set(new_layer_ids.keys())
        
        for layer_id in all_layer_ids:
            old_layer = old_layer_ids.get(layer_id)
            new_layer = new_layer_ids.get(layer_id)
            
            if old_layer is not None and new_layer is not None:
                # Layer exists in both - check if modified
                if ET.tostring(old_layer, encoding='unicode') != ET.tostring(new_layer, encoding='unicode'):
                    changes.append(StructuralChange(
                        element_type="maplayer",
                        parent_path="projectlayers",
                        old_element=old_layer,
                        new_element=new_layer,
                        change_type=ChangeType.MODIFIED
                    ))
            elif old_layer is None and new_layer is not None:
                # Layer added
                changes.append(StructuralChange(
                    element_type="maplayer",
                    parent_path="projectlayers", 
                    old_element=None,
                    new_element=new_layer,
                    change_type=ChangeType.ADDED
                ))
            elif old_layer is not None and new_layer is None:
                # Layer removed
                changes.append(StructuralChange(
                    element_type="maplayer",
                    parent_path="projectlayers",
                    old_element=old_layer,
                    new_element=None,
                    change_type=ChangeType.REMOVED
                ))
        
        return changes

    def detect_structural_changes(
        self, old_element: ET.Element, new_element: ET.Element, path: str = ""
    ) -> List[StructuralChange]:
        changes = []

        # Handle multiple children with same tag (e.g. maplayer[0], maplayer[1])
        old_children_by_tag = {}
        new_children_by_tag = {}

        for child in old_element:
            tag = child.tag
            if tag not in old_children_by_tag:
                old_children_by_tag[tag] = []
            old_children_by_tag[tag].append(child)

        for child in new_element:
            tag = child.tag
            if tag not in new_children_by_tag:
                new_children_by_tag[tag] = []
            new_children_by_tag[tag].append(child)

        all_tags = set(old_children_by_tag.keys()) | set(new_children_by_tag.keys())

        for tag in all_tags:
            old_children = old_children_by_tag.get(tag, [])
            new_children = new_children_by_tag.get(tag, [])


            if len(old_children) == 1 and len(new_children) == 1:
                # Single child with this tag
                old_child = old_children[0]
                new_child = new_children[0]
                child_path = f"{path}⧫{tag}" if path else tag

                if ET.tostring(old_child, encoding="unicode") != ET.tostring(
                    new_child, encoding="unicode"
                ):
                    changes.append(
                        StructuralChange(
                            element_type=tag,
                            parent_path=path,
                            old_element=old_child,
                            new_element=new_child,
                            change_type=ChangeType.MODIFIED,
                        )
                    )

                    # Recursively check children
                    child_changes = self.detect_structural_changes(
                        old_child, new_child, child_path
                    )
                    changes.extend(child_changes)

            elif len(old_children) > 1 or len(new_children) > 1:
                # Multiple children with same tag - compare by index
                max_count = max(len(old_children), len(new_children))
                for i in range(max_count):
                    old_child = old_children[i] if i < len(old_children) else None
                    new_child = new_children[i] if i < len(new_children) else None
                    child_path = f"{path}⧫{tag}[{i}]" if path else f"{tag}[{i}]"

                    if old_child is not None and new_child is not None:
                        if ET.tostring(old_child, encoding="unicode") != ET.tostring(
                            new_child, encoding="unicode"
                        ):
                            changes.append(
                                StructuralChange(
                                    element_type=tag,
                                    parent_path=path,
                                    old_element=old_child,
                                    new_element=new_child,
                                    change_type=ChangeType.MODIFIED,
                                )
                            )

                            # Recursively check children
                            child_changes = self.detect_structural_changes(
                                old_child, new_child, child_path
                            )
                            changes.extend(child_changes)

                    elif old_child is None and new_child is not None:
                        changes.append(
                            StructuralChange(
                                element_type=tag,
                                parent_path=path,
                                old_element=None,
                                new_element=new_child,
                                change_type=ChangeType.ADDED,
                            )
                        )
                    elif old_child is not None and new_child is None:
                        changes.append(
                            StructuralChange(
                                element_type=tag,
                                parent_path=path,
                                old_element=old_child,
                                new_element=None,
                                change_type=ChangeType.REMOVED,
                            )
                        )

            else:
                # One side has children, other doesn't
                if old_children and not new_children:
                    for old_child in old_children:
                        changes.append(
                            StructuralChange(
                                element_type=tag,
                                parent_path=path,
                                old_element=old_child,
                                new_element=None,
                                change_type=ChangeType.REMOVED,
                            )
                        )
                elif new_children and not old_children:
                    for new_child in new_children:
                        changes.append(
                            StructuralChange(
                                element_type=tag,
                                parent_path=path,
                                old_element=None,
                                new_element=new_child,
                                change_type=ChangeType.ADDED,
                            )
                        )

        return changes

    def compare_projects(
        self, old_content: str, new_content: str
    ) -> List[ProjectDifference]:
        old_root = self.parse_qgs_content(old_content)
        new_root = self.parse_qgs_content(new_content)

        # Detect layer changes using smart ID matching
        layer_changes = self.detect_layer_changes(old_root, new_root)
        
        # Detect all other structural changes recursively  
        structural_changes = self.detect_structural_changes(old_root, new_root)
        
        # Combine layer changes with other changes
        all_changes = layer_changes + structural_changes

        # Convert structural changes to project differences
        differences = []
        for change in all_changes:
            if change.parent_path:
                desc = f"{change.change_type.value.title()} {change.element_type} in {change.parent_path}"
                path = f"{change.parent_path}⧫{change.element_type}"
            else:
                desc = f"{change.change_type.value.title()} {change.element_type}"
                path = change.element_type

            differences.append(
                ProjectDifference(
                    path=path,
                    change_type=change.change_type,
                    old_value=change.old_element,
                    new_value=change.new_element,
                    description=desc,
                )
            )

        return differences

    def format_differences(self, differences: List[ProjectDifference]) -> str:
        if not differences:
            return "No differences found."

        lines = [f"Found {len(differences)} difference(s):\n"]

        for diff in differences:
            icon = (
                "+"
                if diff.change_type == ChangeType.ADDED
                else "-"
                if diff.change_type == ChangeType.REMOVED
                else "~"
            )
            lines.append(f"  {icon} {diff.description}")

        return "\n".join(lines)


def check_project_differences(
    current_content: str, database_content: str
) -> Tuple[List[ProjectDifference], str]:
    differ = QGSProjectDiffer()
    
    
    # For soft apply, we want to see what the database has that current doesn't
    differences = differ.compare_projects(current_content, database_content)
    formatted = differ.format_differences(differences)

    return differences, formatted


def apply_project_title(new_title: str):
    try:
        from qgis.core import QgsProject

        project = QgsProject.instance()
        project.setTitle(new_title)
        print(f"Set project title to '{new_title}'")

    except Exception as e:
        print(f"Error setting project title: {str(e)}")


def get_layer_position(new_maplayer_element: ET.Element, database_content: str) -> int:
    """Find the position where this layer should be inserted based on layer tree order"""
    try:
        # Get the layer ID
        id_elem = new_maplayer_element.find('id')
        if id_elem is None or not id_elem.text:
            return 0  # Default to top if no ID
            
        target_layer_id = id_elem.text
        
        # Parse database content to get layer tree order
        differ = QGSProjectDiffer()
        db_root = differ.parse_qgs_content(database_content)
        layer_tree = db_root.find('.//layer-tree-group')
        
        if layer_tree is None:
            return 0
        
        # Find position of target layer in layer tree
        for i, tree_layer in enumerate(layer_tree.findall('layer-tree-layer')):
            layer_id = tree_layer.get('id')
            if layer_id == target_layer_id:
                return i
                
        return 0  # Default to top if not found
        
    except Exception as e:
        return 0


def apply_layer_addition(new_maplayer_element: ET.Element, position: int = 0):
    try:
        from qgis.core import QgsProject, QgsVectorLayer, QgsRasterLayer, QgsReadWriteContext
        
        # Extract layer info from XML
        layer_type = new_maplayer_element.get('type', 'vector')
        
        # Get datasource, layername, and provider
        datasource_elem = new_maplayer_element.find('datasource')
        layername_elem = new_maplayer_element.find('layername')
        provider_elem = new_maplayer_element.find('provider')
        id_elem = new_maplayer_element.find('id')
        
        if datasource_elem is None or layername_elem is None or provider_elem is None:
            print("Error: Missing required elements (datasource, layername, or provider)")
            return
            
        datasource = datasource_elem.text or ""
        layername = layername_elem.text or "New Layer"
        provider = provider_elem.text or ""
        layer_id = id_elem.text if id_elem is not None else "unknown"
        
        # Skip Pointer Positions layer - don't sync between users
        if layername == "Pointer Positions":
            print(f"⏭️ Skipping Pointer Positions layer - not synced between users")
            return True
        
        # Check if layer already exists
        project = QgsProject.instance()
        existing_layer = project.mapLayer(layer_id)
        if existing_layer:
            return
        
        # Create layer based on type
        if layer_type == "vector":
            layer = QgsVectorLayer(datasource, layername, provider)
        elif layer_type == "raster":
            layer = QgsRasterLayer(datasource, layername, provider)
        else:
            print(f"Unsupported layer type: {layer_type}")
            return
            
        # Check if layer is valid
        if not layer.isValid():
            print(f"Failed to create valid layer from datasource: {datasource}")
            return
            
        # Set the layer ID to match the database BEFORE adding to project
        layer.setId(layer_id)
            
        # Add to project (without auto-adding to tree)
        project = QgsProject.instance()
        project.addMapLayer(layer, False)
        
        # Add to layer tree at correct position
        root = project.layerTreeRoot()
        root.insertLayer(position, layer)
        
        print(f"Added layer: {layername} ({layer_type}) at position {position}")
        
    except Exception as e:
        print(f"Error adding layer: {str(e)}")


def apply_symbology(layer_index: int, new_maplayer_element: ET.Element):
    try:
        from qgis.core import QgsProject, QgsReadWriteContext, QgsMapLayer
        from qgis.PyQt.QtXml import QDomDocument

        # Get the project and layer
        project = QgsProject.instance()
        layers = list(project.mapLayers().values())

        if layer_index >= len(layers):
            print(
                f"Error: Layer index {layer_index} out of range (have {len(layers)} layers)"
            )
            return

        layer = layers[layer_index]

        # Convert ElementTree to QDomDocument
        xml_string = ET.tostring(new_maplayer_element, encoding="unicode")
        dom_doc = QDomDocument()
        if not dom_doc.setContent(xml_string):
            print(f"Error: Failed to parse XML for layer {layer_index}")
            return

        maplayer_node = dom_doc.documentElement()

        # Create read context
        context = QgsReadWriteContext()
        context.setPathResolver(project.pathResolver())

        # Apply all styling: symbology, labels, rendering, etc.
        error_msg = ""
        success = layer.readSymbology(
            maplayer_node, error_msg, context, QgsMapLayer.AllStyleCategories
        )

        if success:
            layer.triggerRepaint()
            layer.emitStyleChanged()

            # Also refresh the map canvas
            try:
                from qgis.utils import iface

                if iface:
                    iface.mapCanvas().refresh()
                    iface.layerTreeView().refreshLayerSymbology(layer.id())
            except:
                pass

            print(f"Applied symbology to layer {layer_index} ({layer.name()})")
        else:
            print(f"Failed to apply symbology to layer {layer_index}: {error_msg}")

    except Exception as e:
        print(f"Error applying symbology to layer {layer_index}: {str(e)}")


def apply_changes(
    differences: List[ProjectDifference], database_content: str = None
) -> bool:
    applied = False
    applied_changes = 0
    skipped_changes = 0


    for diff in differences:
        handled = False

        # Handle layer additions
        if diff.change_type == ChangeType.ADDED and isinstance(diff.new_value, ET.Element):
            element_type = diff.path.split("⧫")[-1]
            parent_path = "⧫".join(diff.path.split("⧫")[:-1])
            
            if element_type == "maplayer" and parent_path == "projectlayers":
                position = get_layer_position(diff.new_value, database_content)
                apply_layer_addition(diff.new_value, position)
                applied_changes += 1
                applied = True
                handled = True

        # Handle structural changes
        elif diff.change_type == ChangeType.MODIFIED and isinstance(
            diff.new_value, ET.Element
        ):
            # Extract element type from path (e.g. maplayer[0]⧫renderer-v2 -> renderer-v2)
            element_type = diff.path.split("⧫")[-1]
            parent_path = "⧫".join(diff.path.split("⧫")[:-1])

            if element_type in ["renderer-v2", "labeling", "blendMode", "layerOpacity"]:
                if (
                    element_type == "renderer-v2"
                    and "maplayer[" in parent_path
                    and database_content
                ):
                    layer_index = int(parent_path.split("maplayer[")[1].split("]")[0])

                    # Get the complete maplayer element from database content
                    try:
                        differ = QGSProjectDiffer()
                        db_root = differ.parse_qgs_content(database_content)
                        maplayers = db_root.findall(".//maplayer")

                        if layer_index < len(maplayers):
                            # Get the database layer and find its ID  
                            db_layer_element = maplayers[layer_index]
                            db_layer_id_elem = db_layer_element.find('id')
                            if db_layer_id_elem is not None and db_layer_id_elem.text:
                                db_layer_id = db_layer_id_elem.text
                                
                                # Find the matching layer in current project by ID
                                from qgis.core import QgsProject
                                project = QgsProject.instance()
                                target_layer = project.mapLayer(db_layer_id)
                                if target_layer:
                                    # Get current project layers in order to find index
                                    current_layers = list(project.mapLayers().values())
                                    try:
                                        current_index = current_layers.index(target_layer)
                                        
                                        # Get the original maplayer element and replace its renderer
                                        maplayer_copy = copy.deepcopy(db_layer_element)

                                        # Remove old renderer-v2 and add new one
                                        old_renderer = maplayer_copy.find("renderer-v2")
                                        if old_renderer is not None:
                                            maplayer_copy.remove(old_renderer)

                                        maplayer_copy.append(diff.new_value)
                                        apply_symbology(current_index, maplayer_copy)
                                    except ValueError:
                                        pass
                        else:
                            print(
                                f"Error: Layer index {layer_index} not found in database content"
                            )
                    except Exception as e:
                        print(f"Error reconstructing maplayer element: {e}")
                    applied_changes += 1
                    applied = True
                    handled = True
                else:
                    print(
                        f"TODO: Handle important change {element_type} in {parent_path}"
                    )
                    applied_changes += 1
                    applied = True
                    handled = True

        # Handle project title changes
        elif diff.change_type == ChangeType.MODIFIED and (
            "title" in diff.path and isinstance(diff.new_value, ET.Element)
        ):
            # Extract title text from the new element
            title_text = diff.new_value.text if diff.new_value.text else ""
            if title_text:
                apply_project_title(title_text)
                applied_changes += 1
                applied = True
                handled = True

        if not handled:
            skipped_changes += 1

    print(f"Applied {applied_changes} change(s), skipped {skipped_changes} change(s)")
    return applied


def compare_project_files(file1: str, file2: str) -> List[ProjectDifference]:
    differ = QGSProjectDiffer()

    with open(file1, "r", encoding="utf-8") as f:
        content1 = f.read()
    with open(file2, "r", encoding="utf-8") as f:
        content2 = f.read()

    return differ.compare_projects(content1, content2)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python project_differ.py <file1.qgs> <file2.qgs>")
        sys.exit(1)

    file1, file2 = sys.argv[1], sys.argv[2]

    try:
        differences = compare_project_files(file1, file2)
        differ = QGSProjectDiffer()
        formatted = differ.format_differences(differences)
        print(formatted)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)