# Copyright Bunting Labs, Inc. 2025

import psycopg2
from PyQt5.QtWidgets import (
    QDockWidget,
    QWidget,
    QVBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QLineEdit,
    QMessageBox,
    QListWidget,
    QListWidgetItem,
    QCheckBox,
)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal
from PyQt5.QtGui import QFont
from .project_differ import check_project_differences, apply_changes


def classFactory(iface):
    return SharedProjectsPlugin(iface)


class DatabasePollingThread(QThread):
    new_projects_found = pyqtSignal(int)  # number of new projects
    
    def __init__(self, connection_string, project_id=None, poll_interval_seconds=5):
        super().__init__()
        self.connection_string = connection_string
        self.project_id = project_id
        self.poll_interval = poll_interval_seconds
        self.running = False
        self.last_known_id = 0
        
    def run(self):
        if self.project_id:
            print(f"🔄 Starting database polling for project_id '{self.project_id}' (every {self.poll_interval}s)")
        else:
            print(f"🔄 Starting database polling for all projects (every {self.poll_interval}s)")
        
        # Get initial highest ID for this project_id
        try:
            conn = psycopg2.connect(self.connection_string, connect_timeout=10)
            with conn.cursor() as cur:
                if self.project_id:
                    cur.execute("SELECT COALESCE(MAX(id), 0) FROM qgis_projects WHERE project_id = %s", (self.project_id,))
                else:
                    cur.execute("SELECT COALESCE(MAX(id), 0) FROM qgis_projects")
                self.last_known_id = cur.fetchone()[0]
                print(f"📊 Starting from project ID: {self.last_known_id}")
            conn.close()
        except Exception as e:
            print(f"❌ Error getting initial project count: {e}")
            self.last_known_id = 0
            
        self.running = True
        
        while self.running:
            try:
                # Sleep first, then check
                self.msleep(self.poll_interval * 1000)
                
                if not self.running:
                    break
                    
                # Check for new projects
                conn = psycopg2.connect(self.connection_string, connect_timeout=10)
                with conn.cursor() as cur:
                    if self.project_id:
                        cur.execute("SELECT COUNT(*) FROM qgis_projects WHERE id > %s AND project_id = %s", 
                                   (self.last_known_id, self.project_id))
                    else:
                        cur.execute("SELECT COUNT(*) FROM qgis_projects WHERE id > %s", (self.last_known_id,))
                    new_count = cur.fetchone()[0]
                    
                    if new_count > 0:
                        # Get the new highest ID
                        if self.project_id:
                            cur.execute("SELECT MAX(id) FROM qgis_projects WHERE project_id = %s", (self.project_id,))
                        else:
                            cur.execute("SELECT MAX(id) FROM qgis_projects")
                        new_highest_id = cur.fetchone()[0]
                        
                        if self.project_id:
                            print(f"📡 Found {new_count} new project(s) for '{self.project_id}'! (ID {self.last_known_id + 1} to {new_highest_id})")
                        else:
                            print(f"📡 Found {new_count} new project(s)! (ID {self.last_known_id + 1} to {new_highest_id})")
                        
                        self.last_known_id = new_highest_id
                        self.new_projects_found.emit(new_count)
                        
                conn.close()
                    
            except Exception as e:
                print(f"❌ Polling error: {e}")
                
        print(f"🔌 Database polling stopped")
                
    def stop(self):
        self.running = False


class ConnectionStatusWidget(QWidget):
    def __init__(self, iface):
        super().__init__()
        self.iface = iface
        # TODO: Configure your database connection string
        self.connection_string = "postgresql://user:password@host:port/database"
        self.polling_thread = None
        self.autosave_enabled = False
        self.autosave_timer = QTimer()
        self.autosave_timer.setSingleShot(True)  # Only fire once
        self.autosave_timer.timeout.connect(self.perform_autosave)
        self.pointer_layer = None
        self.pointer_refresh_timer = None
        self.autosave_in_progress = False  # Prevent recursive saves
        self.setup_ui()
        self.setup_timer()

    def setup_ui(self):
        layout = QVBoxLayout()

        # Connection status
        self.status_label = QLabel("Status: Disconnected")
        layout.addWidget(self.status_label)

        # Setup database button
        self.setup_db_button = QPushButton("Setup Database Schema")
        self.setup_db_button.clicked.connect(self.setup_database_schema)
        layout.addWidget(self.setup_db_button)

        # Polling checkbox
        self.polling_checkbox = QCheckBox("Auto-detect new projects (5s polling)")
        self.polling_checkbox.stateChanged.connect(self.toggle_polling)
        layout.addWidget(self.polling_checkbox)

        # Auto-save checkbox
        self.autosave_checkbox = QCheckBox("Auto-save on project changes")
        self.autosave_checkbox.stateChanged.connect(self.toggle_autosave)
        layout.addWidget(self.autosave_checkbox)

        # Project ID input
        project_label = QLabel("Project ID:")
        layout.addWidget(project_label)
        self.project_id_input = QLineEdit()
        self.project_id_input.setPlaceholderText("Enter project ID...")
        layout.addWidget(self.project_id_input)

        # Save button
        self.save_button = QPushButton("Save Project")
        self.save_button.clicked.connect(self.save_project)
        layout.addWidget(self.save_button)

        # Load button
        self.load_button = QPushButton("Load Project")
        self.load_button.clicked.connect(self.load_project)
        layout.addWidget(self.load_button)

        # Add pointer layer button
        self.pointer_layer_button = QPushButton("Add Pointer Layer")
        self.pointer_layer_button.clicked.connect(self.add_pointer_layer)
        layout.addWidget(self.pointer_layer_button)

        layout.addStretch()
        self.setLayout(layout)

    def setup_timer(self):
        # Auto-test connection every 300 seconds (5 minutes)
        self.timer = QTimer()
        self.timer.timeout.connect(self.test_connection)
        self.timer.start(300000)

        # Test immediately on startup
        self.test_connection()
        
    def load_project(self):
        """Load the latest version of the project from database"""
        try:
            project_id = self.project_id_input.text().strip()
            if not project_id:
                print("⚠️ Please enter a project ID to load")
                return

            # Get latest project from database
            conn = psycopg2.connect(
                self.connection_string,
                connect_timeout=10
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT qgs_content, created_at
                    FROM qgis_projects 
                    WHERE project_id = %s
                    ORDER BY created_at DESC 
                    LIMIT 1
                """,
                    (project_id,),
                )
                row = cur.fetchone()
            conn.close()

            if not row:
                print(f"❌ No project found with ID '{project_id}'")
                return

            # Load the project
            from qgis.core import QgsProject
            import tempfile
            import os

            qgs_content = row[0]
            last_saved = row[1].strftime("%Y-%m-%d %H:%M:%S")

            # Write content to temporary file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".qgs", delete=False, encoding="utf-8"
            ) as tmp:
                tmp.write(qgs_content)
                tmp_path = tmp.name

            # Load project in QGIS
            project = QgsProject.instance()
            project.read(tmp_path)

            # Clean up temporary file
            os.unlink(tmp_path)
            
            print(f"📁 Loaded project '{project_id}' (saved: {last_saved})")

        except Exception as e:
            print(f"❌ Error loading project: {e}")
        
    def toggle_polling(self, state):
        """Start or stop the polling thread based on checkbox state"""
        if state == Qt.Checked:
            # Start polling
            if not self.polling_thread or not self.polling_thread.isRunning():
                project_id = self.project_id_input.text().strip() or None
                self.polling_thread = DatabasePollingThread(
                    self.connection_string, 
                    project_id=project_id,
                    poll_interval_seconds=5
                )
                self.polling_thread.new_projects_found.connect(self.on_new_projects_found)
                self.polling_thread.start()
                if project_id:
                    print(f"✅ Auto-detection enabled for project '{project_id}' (polling every 5 seconds)")
                else:
                    print(f"✅ Auto-detection enabled for all projects (polling every 5 seconds)")
        else:
            # Stop polling
            if self.polling_thread:
                self.polling_thread.stop()
                self.polling_thread.wait(1000)  # Wait up to 1 second
                print(f"⏹️ Auto-detection disabled")
        
    def on_new_projects_found(self, count):
        """Handle when new projects are detected"""
        print(f"🔔 Detected {count} new project(s) in database!")
        
        # Show notification in status
        self.status_label.setText(f"Status: {count} new project(s) available! 🔔")
        self.status_label.setStyleSheet("color: orange; font-weight: bold;")
        
        # Reset status after 5 seconds
        QTimer.singleShot(5000, lambda: self.reset_status())
        
        # Auto-load if enabled and project ID is set
        project_id = self.project_id_input.text().strip()
        if self.polling_checkbox.isChecked() and project_id:
            print(f"🔄 Auto-loading updated project '{project_id}'")
            self.load_project()
        
    def reset_status(self):
        """Reset status back to connected"""
        self.status_label.setText("Status: Connected ✓")
        
    def toggle_autosave(self, state):
        """Start or stop auto-save based on checkbox state"""
        from qgis.core import QgsProject
        
        if state == Qt.Checked:
            # Enable auto-save
            self.autosave_enabled = True
            # Disconnect first to prevent duplicate connections
            try:
                QgsProject.instance().isDirtyChanged.disconnect(self.on_project_dirty)
            except:
                pass  # Connection might not exist
            QgsProject.instance().isDirtyChanged.connect(self.on_project_dirty)
            print(f"✅ Auto-save enabled - will save on project changes")
        else:
            # Disable auto-save
            self.autosave_enabled = False
            # Stop any pending timer
            self.autosave_timer.stop()
            try:
                QgsProject.instance().isDirtyChanged.disconnect(self.on_project_dirty)
            except:
                pass  # Connection might not exist
            print(f"⏹️ Auto-save disabled")
            
    def on_project_dirty(self, is_dirty):
        """Handle when project dirty state changes - debounced with timer"""
        if self.autosave_enabled and is_dirty and not self.autosave_in_progress:
            project_id = self.project_id_input.text().strip()
            if project_id:
                print(f"💾 Project became dirty - scheduling auto-save in 1 second...")
                # Restart the timer - this debounces multiple dirty signals
                self.autosave_timer.start(1000)  # 1 second delay
            else:
                print(f"⚠️ Project became dirty but no project ID set - skipping auto-save")
        elif self.autosave_in_progress:
            print(f"🔄 Ignoring dirty signal - auto-save in progress")
                
    def perform_autosave(self):
        """Actually perform the auto-save after timer delay"""
        project_id = self.project_id_input.text().strip()
        if project_id and self.autosave_enabled:
            print(f"💾 Performing auto-save to project ID '{project_id}'")
            self.autosave_in_progress = True
            try:
                self.save_project_with_id(project_id)
            finally:
                self.autosave_in_progress = False
        else:
            print(f"⚠️ Auto-save timer fired but conditions not met")
                
    def save_project_with_id(self, project_id):
        """Save project with specific ID (used by auto-save)"""
        try:
            from qgis.core import QgsProject
            import tempfile
            import os

            # Get current project content
            project = QgsProject.instance()

            # Read project as XML string
            project_xml = project.readEntry("", "")[0]
            if not project_xml:
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".qgs", delete=False
                ) as tmp:
                    project.write(tmp.name)
                    tmp_path = tmp.name

                with open(tmp_path, "r", encoding="utf-8") as f:
                    project_xml = f.read()

                os.unlink(tmp_path)

            # Save to database
            conn = psycopg2.connect(
                self.connection_string,
                connect_timeout=10
            )
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO qgis_projects (project_id, qgs_content) VALUES (%s, %s) RETURNING id",
                    (project_id, project_xml),
                )
                new_id = cur.fetchone()[0]
                conn.commit()
            conn.close()

            # Update polling thread to ignore this auto-saved entry
            if self.polling_thread and self.polling_thread.isRunning():
                self.polling_thread.last_known_id = new_id
                print(f"🔄 Updated polling thread to ignore auto-saved ID: {new_id}")

            # Mark project as clean
            project.setDirty(False)
            
            print(f"💾 Auto-saved project '{project_id}' with ID: {new_id}")

        except Exception as e:
            print(f"❌ Error auto-saving project: {e}")
        self.status_label.setStyleSheet("color: green; font-weight: bold;")

    def setup_database_schema(self):
        """One-time database schema setup - call manually if needed"""
        try:
            conn = psycopg2.connect(
                self.connection_string,
                connect_timeout=10
            )
            with conn.cursor() as cur:
                # Create table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS qgis_projects (
                        id SERIAL PRIMARY KEY,
                        qgs_content TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)

                # Add project_id column if it doesn't exist
                cur.execute("""
                    ALTER TABLE qgis_projects 
                    ADD COLUMN IF NOT EXISTS project_id VARCHAR(255)
                """)
                
                # Create notification function with debug logging
                cur.execute("""
                    CREATE OR REPLACE FUNCTION notify_project_change()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        RAISE NOTICE 'Trigger fired: operation = %, id = %', TG_OP, NEW.id;
                        
                        IF TG_OP = 'INSERT' THEN
                            RAISE NOTICE 'Sending INSERT notification for project_id = %', NEW.project_id;
                            PERFORM pg_notify('project_updates', 
                                json_build_object(
                                    'action', 'INSERT',
                                    'project_id', NEW.project_id,
                                    'id', NEW.id
                                )::text
                            );
                            RETURN NEW;
                        ELSIF TG_OP = 'UPDATE' THEN
                            RAISE NOTICE 'Sending UPDATE notification for project_id = %', NEW.project_id;
                            PERFORM pg_notify('project_updates',
                                json_build_object(
                                    'action', 'UPDATE', 
                                    'project_id', NEW.project_id,
                                    'id', NEW.id
                                )::text
                            );
                            RETURN NEW;
                        END IF;
                        RETURN NULL;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
                
                # Create trigger
                cur.execute("""
                    DROP TRIGGER IF EXISTS project_change_trigger ON qgis_projects;
                    CREATE TRIGGER project_change_trigger
                        AFTER INSERT OR UPDATE ON qgis_projects
                        FOR EACH ROW EXECUTE FUNCTION notify_project_change();
                """)
                
                conn.commit()
                print(f"✅ Database schema and notification triggers set up successfully!")
            conn.close()
        except Exception as e:
            print(f"❌ Database setup error: {e}")

    def test_connection(self):
        try:
            conn = psycopg2.connect(
                self.connection_string,
                connect_timeout=10
            )
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.close()

            self.status_label.setText("Status: Connected ✓")
            self.status_label.setStyleSheet("color: green; font-weight: bold;")

        except Exception as e:
            self.status_label.setText("Status: Disconnected ✗")
            self.status_label.setStyleSheet("color: red; font-weight: bold;")

    def save_project(self):
        try:
            from qgis.core import QgsProject

            # Get current project content
            project = QgsProject.instance()

            # Read project as XML string
            project_xml = project.readEntry("", "")[0]  # Get full project XML
            if not project_xml:
                # Alternative: write to temporary location and read
                import tempfile
                import os

                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".qgs", delete=False
                ) as tmp:
                    project.write(tmp.name)
                    tmp_path = tmp.name

                with open(tmp_path, "r", encoding="utf-8") as f:
                    project_xml = f.read()

                os.unlink(tmp_path)  # Clean up

            # Get project ID from input
            project_id = self.project_id_input.text().strip() or None

            # Save to database
            conn = psycopg2.connect(
                self.connection_string,
                connect_timeout=10
            )
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO qgis_projects (project_id, qgs_content) VALUES (%s, %s) RETURNING id",
                    (project_id, project_xml),
                )
                new_id = cur.fetchone()[0]
                conn.commit()
            conn.close()

            print(f"💾 Project saved successfully with ID: {new_id}")

        except Exception as e:
            print(f"❌ Error saving project: {e}")

    def add_pointer_layer(self):
        """Add a vector layer that shows pointer positions from the server"""
        try:
            from qgis.core import QgsVectorLayer, QgsProject
            from PyQt5.QtCore import QTimer
            
            # TODO: Configure your GeoJSON endpoint URL
            geojson_url = "https://your-server.com/coordinates"
            
            # Create vector layer from remote GeoJSON
            layer = QgsVectorLayer(geojson_url, "Pointer Positions", "ogr")
            
            if not layer.isValid():
                print("❌ Failed to create pointer layer")
                return
                
            # Add layer to project
            QgsProject.instance().addMapLayer(layer)
            self.pointer_layer = layer
            
            # TODO: Configure path to your QML style file
            qml_path = "/path/to/your/style.qml"
            try:
                layer.loadNamedStyle(qml_path)
                print("✅ Applied style to pointer layer")
            except Exception as style_error:
                print(f"⚠️ Could not load style from {qml_path}: {style_error}")
            
            # Set up auto-refresh timer
            self.pointer_refresh_timer = QTimer()
            self.pointer_refresh_timer.timeout.connect(self.refresh_pointer_layer)
            self.pointer_refresh_timer.start(2000)  # Refresh every 2 seconds
            
            print("✅ Added pointer layer successfully with auto-refresh")
            
        except Exception as e:
            print(f"❌ Error adding pointer layer: {e}")
    
    def refresh_pointer_layer(self):
        """Refresh the pointer layer data"""
        try:
            if self.pointer_layer and self.pointer_layer.isValid():
                self.pointer_layer.dataProvider().reloadData()
                self.pointer_layer.triggerRepaint()
        except Exception as e:
            print(f"❌ Error refreshing pointer layer: {e}")
            
    def cleanup(self):
        """Stop polling thread and timers when widget is destroyed"""
        if self.polling_thread:
            self.polling_thread.stop()
            self.polling_thread.wait(1000)  # Wait up to 1 second
        
        if self.pointer_refresh_timer:
            self.pointer_refresh_timer.stop()
            self.pointer_refresh_timer = None
            

class SharedProjectsPlugin:
    def __init__(self, iface):
        self.iface = iface
        self.dock_widget = None

    def initGui(self):
        # Create dock widget with connection status
        self.dock_widget = QDockWidget("Shared Projects", self.iface.mainWindow())
        self.connection_widget = ConnectionStatusWidget(self.iface)
        self.dock_widget.setWidget(self.connection_widget)

        # Add to QGIS interface
        self.iface.addDockWidget(Qt.RightDockWidgetArea, self.dock_widget)

    def unload(self):
        if self.dock_widget:
            # Cleanup notification thread
            if hasattr(self.connection_widget, 'cleanup'):
                self.connection_widget.cleanup()
            self.iface.removeDockWidget(self.dock_widget)
            self.dock_widget = None