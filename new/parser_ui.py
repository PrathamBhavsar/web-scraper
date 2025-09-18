#!/usr/bin/env python3
"""
Web Parser Control Panel GUI - Fixed Version

A comprehensive Tkinter-based GUI application for monitoring and controlling 
the main_scraper.py web parser with real-time progress tracking, log display,
and configuration management.

FIXES:
- Unicode/encoding issues resolved
- Improved subprocess handling for parser control
- Better error handling and process management
- Enhanced cross-platform compatibility

Author: AI Assistant
Version: 1.1 - Fixed encoding and process control issues
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import json
import os
import sys
import subprocess
import threading
import time
import queue
from pathlib import Path
from typing import Dict, Any, Optional
import datetime


class LogRedirector:
    """Redirects stdout/stderr to GUI text widget"""

    def __init__(self, text_widget, tag="stdout"):
        self.text_widget = text_widget
        self.tag = tag

    def write(self, text):
        if text.strip():  # Only write non-empty text
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            formatted_text = f"[{timestamp}] {text}"

            # Thread-safe GUI update
            self.text_widget.after(0, self._append_text, formatted_text)

    def _append_text(self, text):
        try:
            self.text_widget.insert(tk.END, text, self.tag)
            self.text_widget.see(tk.END)
        except Exception as e:
            print(f"Error appending text: {e}")

    def flush(self):
        pass


class ConfigManager:
    """Manages configuration file operations"""

    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.default_config = {
            "general": {
                "base_url": "https://rule34video.com",
                "download_path": "scraper_downloads",
                "max_storage_gb": 940
            },
            "processing": {
                "mode": "direct",
                "use_parallel": True,
                "parallel_batch_size": 5
            }
        }

    def load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            if not os.path.exists(self.config_file):
                return self.default_config.copy()

            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        except Exception as e:
            print(f"Error loading config: {e}")
            return self.default_config.copy()

    def save_config(self, config: Dict[str, Any]) -> bool:
        """Save configuration to JSON file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False


class ProgressTracker:
    """Tracks parser progress from progress.json"""

    def __init__(self, progress_file: str = "progress.json"):
        self.progress_file = progress_file
        self.default_progress = {
            "last_page": 1000,
            "total_downloaded": 0,
            "total_size_mb": 0.0,
            "downloaded_videos": [],
            "failed_videos": [],
            "last_updated": ""
        }

    def load_progress(self) -> Dict[str, Any]:
        """Load progress data from JSON file"""
        try:
            if not os.path.exists(self.progress_file):
                return self.default_progress.copy()

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress = json.load(f)
            return progress
        except Exception as e:
            print(f"Error loading progress: {e}")
            return self.default_progress.copy()

    def reset_progress(self) -> bool:
        """Reset progress to initial state"""
        try:
            reset_data = self.default_progress.copy()
            reset_data["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(reset_data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error resetting progress: {e}")
            return False


class ImprovedParserController:
    """Enhanced parser controller with better subprocess handling"""

    def __init__(self, gui_log_callback=None):
        self.process = None
        self.is_running = False
        self.output_thread = None
        self.should_stop_monitoring = False
        self.gui_log_callback = gui_log_callback

    def start_parser(self) -> bool:
        """Start the parser in a subprocess with proper encoding handling"""
        if self.is_running:
            self._log("Parser is already running!")
            return False

        try:
            # Check if main_scraper.py exists
            script_path = "main_scraper.py"
            if not os.path.exists(script_path):
                self._log(f"Error: {script_path} not found!")
                return False

            self._log("Starting parser subprocess...")

            # Set environment variables to handle Unicode
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'

            # Create startup info for Windows to hide console window
            startupinfo = None
            if sys.platform.startswith('win'):
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE

            # Start the parser process with proper encoding
            self.process = subprocess.Popen(
                [sys.executable, "-u", script_path],  # -u for unbuffered output
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                encoding='utf-8',
                errors='replace',  # Handle encoding errors gracefully
                bufsize=1,
                env=env,
                startupinfo=startupinfo,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform.startswith('win') else 0
            )

            self.is_running = True
            self.should_stop_monitoring = False

            # Start output monitoring thread
            self.output_thread = threading.Thread(target=self._monitor_output, daemon=True)
            self.output_thread.start()

            self._log(f"Parser started successfully with PID: {self.process.pid}")
            return True

        except Exception as e:
            self._log(f"Error starting parser: {e}")
            self.is_running = False
            return False

    def stop_parser(self) -> bool:
        """Stop the parser process gracefully"""
        if not self.is_running or not self.process:
            self._log("Parser is not running!")
            return False

        try:
            self._log("Stopping parser...")

            # Signal monitoring thread to stop
            self.should_stop_monitoring = True

            # Try graceful termination first
            try:
                self.process.terminate()

                # Wait for graceful shutdown with timeout
                try:
                    exit_code = self.process.wait(timeout=10)
                    self._log(f"Parser stopped gracefully (exit code: {exit_code})")
                except subprocess.TimeoutExpired:
                    self._log("Parser didn't stop gracefully, forcing termination...")
                    self.process.kill()
                    exit_code = self.process.wait()
                    self._log(f"Parser forcefully terminated (exit code: {exit_code})")

            except Exception as term_error:
                self._log(f"Error during termination: {term_error}")
                try:
                    self.process.kill()
                    self.process.wait()
                    self._log("Parser forcefully killed")
                except Exception as kill_error:
                    self._log(f"Error killing process: {kill_error}")

            self.is_running = False
            self.process = None

            # Wait for monitoring thread to finish
            if self.output_thread and self.output_thread.is_alive():
                self.output_thread.join(timeout=2)

            return True

        except Exception as e:
            self._log(f"Error stopping parser: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get parser status"""
        if not self.process:
            return {"running": False, "pid": None, "exit_code": None}

        # Check if process is still alive
        exit_code = self.process.poll()
        if exit_code is not None:
            # Process has terminated
            self.is_running = False
            return {"running": False, "pid": self.process.pid, "exit_code": exit_code}

        return {"running": self.is_running, "pid": self.process.pid, "exit_code": None}

    def _monitor_output(self):
        """Monitor parser output in background thread"""
        self._log("Output monitoring started")

        try:
            while self.is_running and not self.should_stop_monitoring and self.process:
                try:
                    # Check if process is still alive
                    if self.process.poll() is not None:
                        self._log("Parser process has terminated")
                        self.is_running = False
                        break

                    # Try to read a line with timeout
                    line = self.process.stdout.readline()

                    if line:
                        # Clean the line and log it
                        clean_line = line.strip()
                        if clean_line:
                            self._log(f"[PARSER] {clean_line}")

                    # Small delay to prevent busy waiting
                    time.sleep(0.1)

                except Exception as e:
                    self._log(f"Error reading parser output: {e}")
                    break

        except Exception as e:
            self._log(f"Error in output monitoring thread: {e}")
        finally:
            self._log("Output monitoring stopped")

    def _log(self, message: str):
        """Log a message to console and GUI if callback available"""
        print(message)  # Always log to console
        if self.gui_log_callback:
            try:
                self.gui_log_callback(message)
            except Exception as e:
                print(f"Error calling GUI log callback: {e}")


class WebParserGUI:
    """Main GUI application class"""

    def __init__(self):
        self.root = tk.Tk()
        self.setup_window()

        # Initialize components
        self.config_manager = ConfigManager()
        self.progress_tracker = ProgressTracker()

        # Load initial data
        self.config_data = self.config_manager.load_config()
        self.progress_data = self.progress_tracker.load_progress()

        # GUI state
        self.update_running = True
        self.output_queue = queue.Queue()

        self.setup_gui()
        self.setup_log_redirection()

        # Initialize parser controller with GUI log callback
        self.parser_controller = ImprovedParserController(gui_log_callback=self.add_log_message)

        self.setup_monitoring()

    def setup_window(self):
        """Configure main window"""
        self.root.title("Web Parser Control Panel v1.1")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)

        # Configure style
        style = ttk.Style()
        style.theme_use('clam')

        # Configure colors
        self.colors = {
            'bg': '#f0f0f0',
            'fg': '#333333',
            'success': '#28a745',
            'warning': '#ffc107',
            'danger': '#dc3545',
            'info': '#17a2b8'
        }

        self.root.configure(bg=self.colors['bg'])

    def setup_gui(self):
        """Setup the GUI layout"""
        # Main container
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Title
        title_label = ttk.Label(
            main_frame, 
            text="Web Parser Control Panel", 
            font=('Arial', 16, 'bold')
        )
        title_label.pack(pady=(0, 10))

        # Create notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Setup tabs
        self.setup_main_tab()
        self.setup_config_tab()
        self.setup_logs_tab()

    def setup_main_tab(self):
        """Setup main control tab"""
        # Main tab
        main_tab = ttk.Frame(self.notebook)
        self.notebook.add(main_tab, text="Main Control")

        # Status frame
        status_frame = ttk.LabelFrame(main_tab, text="Parser Status", padding=10)
        status_frame.pack(fill=tk.X, pady=(0, 10))

        # Status indicator
        status_inner = ttk.Frame(status_frame)
        status_inner.pack(fill=tk.X)

        ttk.Label(status_inner, text="Status:", font=('Arial', 10, 'bold')).pack(side=tk.LEFT)
        self.status_label = ttk.Label(
            status_inner, 
            text="Stopped", 
            font=('Arial', 10),
            foreground='red'
        )
        self.status_label.pack(side=tk.LEFT, padx=(5, 0))

        # PID info
        pid_frame = ttk.Frame(status_frame)
        pid_frame.pack(fill=tk.X, pady=(5, 0))

        ttk.Label(pid_frame, text="Process ID:", font=('Arial', 10, 'bold')).pack(side=tk.LEFT)
        self.pid_label = ttk.Label(pid_frame, text="N/A", font=('Arial', 10))
        self.pid_label.pack(side=tk.LEFT, padx=(5, 0))

        # Current page info
        page_frame = ttk.Frame(status_frame)
        page_frame.pack(fill=tk.X, pady=(5, 0))

        ttk.Label(page_frame, text="Current Page:", font=('Arial', 10, 'bold')).pack(side=tk.LEFT)
        self.current_page_label = ttk.Label(page_frame, text="N/A", font=('Arial', 10))
        self.current_page_label.pack(side=tk.LEFT, padx=(5, 0))

        # Progress frame
        progress_frame = ttk.LabelFrame(main_tab, text="Download Progress", padding=10)
        progress_frame.pack(fill=tk.X, pady=(0, 10))

        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            progress_frame,
            variable=self.progress_var,
            maximum=100,
            mode='determinate'
        )
        self.progress_bar.pack(fill=tk.X, pady=(0, 5))

        # Progress labels
        progress_info = ttk.Frame(progress_frame)
        progress_info.pack(fill=tk.X)

        self.progress_text = ttk.Label(
            progress_info,
            text="0.00 MB / 940.00 GB (0.00%)",
            font=('Arial', 9)
        )
        self.progress_text.pack(side=tk.LEFT)

        self.video_count_label = ttk.Label(
            progress_info,
            text="Videos: 0",
            font=('Arial', 9)
        )
        self.video_count_label.pack(side=tk.RIGHT)

        # Control buttons frame
        control_frame = ttk.LabelFrame(main_tab, text="Parser Controls", padding=10)
        control_frame.pack(fill=tk.X, pady=(0, 10))

        button_frame = ttk.Frame(control_frame)
        button_frame.pack()

        # Start button
        self.start_button = ttk.Button(
            button_frame,
            text="Start Parsing",
            command=self.start_parser
        )
        self.start_button.pack(side=tk.LEFT, padx=(0, 5))

        # Stop button
        self.stop_button = ttk.Button(
            button_frame,
            text="Stop Parsing",
            command=self.stop_parser,
            state=tk.DISABLED
        )
        self.stop_button.pack(side=tk.LEFT, padx=5)

        # Reset button
        self.reset_button = ttk.Button(
            button_frame,
            text="Reset Progress",
            command=self.reset_progress
        )
        self.reset_button.pack(side=tk.LEFT, padx=(5, 0))

        # Statistics frame
        stats_frame = ttk.LabelFrame(main_tab, text="Statistics", padding=10)
        stats_frame.pack(fill=tk.BOTH, expand=True)

        # Create statistics display
        self.setup_statistics_display(stats_frame)

    def setup_config_tab(self):
        """Setup configuration tab"""
        config_tab = ttk.Frame(self.notebook)
        self.notebook.add(config_tab, text="Configuration")

        # Scrollable frame for config
        canvas = tk.Canvas(config_tab)
        scrollbar = ttk.Scrollbar(config_tab, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # General settings
        general_frame = ttk.LabelFrame(scrollable_frame, text="General Settings", padding=10)
        general_frame.pack(fill=tk.X, padx=10, pady=5)

        # Download directory
        dir_frame = ttk.Frame(general_frame)
        dir_frame.pack(fill=tk.X, pady=2)

        ttk.Label(dir_frame, text="Download Directory:", width=20).pack(side=tk.LEFT)
        self.download_dir_var = tk.StringVar(value=self.config_data.get('general', {}).get('download_path', 'scraper_downloads'))
        self.download_dir_entry = ttk.Entry(dir_frame, textvariable=self.download_dir_var, state=tk.NORMAL)
        self.download_dir_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(5, 5))

        ttk.Button(
            dir_frame,
            text="Browse",
            command=self.browse_directory
        ).pack(side=tk.RIGHT)

        # Storage limit
        storage_frame = ttk.Frame(general_frame)
        storage_frame.pack(fill=tk.X, pady=2)

        ttk.Label(storage_frame, text="Max Storage (GB):", width=20).pack(side=tk.LEFT)
        self.max_storage_var = tk.DoubleVar(value=self.config_data.get('general', {}).get('max_storage_gb', 940))
        storage_spinbox = ttk.Spinbox(
            storage_frame,
            from_=1,
            to=10000,
            textvariable=self.max_storage_var,
            width=10
        )
        storage_spinbox.pack(side=tk.LEFT, padx=(5, 0))

        # Base URL
        url_frame = ttk.Frame(general_frame)
        url_frame.pack(fill=tk.X, pady=2)

        ttk.Label(url_frame, text="Base URL:", width=20).pack(side=tk.LEFT)
        self.base_url_var = tk.StringVar(value=self.config_data.get('general', {}).get('base_url', 'https://rule34video.com'))
        ttk.Entry(url_frame, textvariable=self.base_url_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(5, 0))

        # Processing settings
        processing_frame = ttk.LabelFrame(scrollable_frame, text="Processing Settings", padding=10)
        processing_frame.pack(fill=tk.X, padx=10, pady=5)

        # Parallel processing
        parallel_frame = ttk.Frame(processing_frame)
        parallel_frame.pack(fill=tk.X, pady=2)

        self.use_parallel_var = tk.BooleanVar(value=self.config_data.get('processing', {}).get('use_parallel', True))
        ttk.Checkbutton(
            parallel_frame,
            text="Enable Parallel Processing",
            variable=self.use_parallel_var
        ).pack(side=tk.LEFT)

        # Batch size
        batch_frame = ttk.Frame(processing_frame)
        batch_frame.pack(fill=tk.X, pady=2)

        ttk.Label(batch_frame, text="Batch Size:", width=20).pack(side=tk.LEFT)
        self.batch_size_var = tk.IntVar(value=self.config_data.get('processing', {}).get('parallel_batch_size', 5))
        ttk.Spinbox(
            batch_frame,
            from_=1,
            to=20,
            textvariable=self.batch_size_var,
            width=10
        ).pack(side=tk.LEFT, padx=(5, 0))

        # Configuration buttons
        config_button_frame = ttk.Frame(scrollable_frame)
        config_button_frame.pack(fill=tk.X, padx=10, pady=10)

        ttk.Button(
            config_button_frame,
            text="Save Configuration",
            command=self.save_config
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            config_button_frame,
            text="Reload Configuration",
            command=self.reload_config
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            config_button_frame,
            text="Reset to Defaults",
            command=self.reset_config
        ).pack(side=tk.LEFT, padx=(5, 0))

    def setup_logs_tab(self):
        """Setup logs display tab"""
        logs_tab = ttk.Frame(self.notebook)
        self.notebook.add(logs_tab, text="Logs")

        # Log controls
        log_controls = ttk.Frame(logs_tab)
        log_controls.pack(fill=tk.X, padx=5, pady=5)

        ttk.Button(
            log_controls,
            text="Clear Logs",
            command=self.clear_logs
        ).pack(side=tk.LEFT)

        ttk.Button(
            log_controls,
            text="Save Logs",
            command=self.save_logs
        ).pack(side=tk.LEFT, padx=(5, 0))

        # Auto-scroll checkbox
        self.auto_scroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            log_controls,
            text="Auto-scroll",
            variable=self.auto_scroll_var
        ).pack(side=tk.RIGHT)

        # Log display
        log_frame = ttk.Frame(logs_tab)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            wrap=tk.WORD,
            font=('Consolas', 9),
            bg='#1e1e1e',
            fg='#ffffff',
            insertbackground='white'
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Configure text tags for colored output
        self.log_text.tag_configure("stdout", foreground="#ffffff")
        self.log_text.tag_configure("stderr", foreground="#ff6b6b")
        self.log_text.tag_configure("info", foreground="#4ecdc4")
        self.log_text.tag_configure("warning", foreground="#ffe66d")
        self.log_text.tag_configure("error", foreground="#ff6b6b")

    def setup_statistics_display(self, parent):
        """Setup the statistics display"""
        # Create grid of statistics
        stats_grid = ttk.Frame(parent)
        stats_grid.pack(fill=tk.BOTH, expand=True)

        # Configure grid weights
        for i in range(3):
            stats_grid.columnconfigure(i, weight=1)
        for i in range(2):
            stats_grid.rowconfigure(i, weight=1)

        # Downloaded videos stat
        downloaded_frame = ttk.Frame(stats_grid, relief='solid', borderwidth=1)
        downloaded_frame.grid(row=0, column=0, sticky='nsew', padx=2, pady=2)

        ttk.Label(downloaded_frame, text="Downloaded", font=('Arial', 12, 'bold')).pack(pady=(10, 0))
        ttk.Label(downloaded_frame, text="Downloaded Videos", font=('Arial', 10)).pack()
        self.downloaded_count_label = ttk.Label(downloaded_frame, text="0", font=('Arial', 16))
        self.downloaded_count_label.pack(pady=(0, 10))

        # Failed videos stat
        failed_frame = ttk.Frame(stats_grid, relief='solid', borderwidth=1)
        failed_frame.grid(row=0, column=1, sticky='nsew', padx=2, pady=2)

        ttk.Label(failed_frame, text="Failed", font=('Arial', 12, 'bold')).pack(pady=(10, 0))
        ttk.Label(failed_frame, text="Failed Videos", font=('Arial', 10)).pack()
        self.failed_count_label = ttk.Label(failed_frame, text="0", font=('Arial', 16))
        self.failed_count_label.pack(pady=(0, 10))

        # Current page stat
        page_frame = ttk.Frame(stats_grid, relief='solid', borderwidth=1)
        page_frame.grid(row=0, column=2, sticky='nsew', padx=2, pady=2)

        ttk.Label(page_frame, text="Page", font=('Arial', 12, 'bold')).pack(pady=(10, 0))
        ttk.Label(page_frame, text="Current Page", font=('Arial', 10)).pack()
        self.page_stat_label = ttk.Label(page_frame, text="N/A", font=('Arial', 16))
        self.page_stat_label.pack(pady=(0, 10))

        # Total size stat
        size_frame = ttk.Frame(stats_grid, relief='solid', borderwidth=1)
        size_frame.grid(row=1, column=0, sticky='nsew', padx=2, pady=2)

        ttk.Label(size_frame, text="Size", font=('Arial', 12, 'bold')).pack(pady=(10, 0))
        ttk.Label(size_frame, text="Total Size", font=('Arial', 10)).pack()
        self.size_stat_label = ttk.Label(size_frame, text="0.00 MB", font=('Arial', 16))
        self.size_stat_label.pack(pady=(0, 10))

        # Storage usage stat
        usage_frame = ttk.Frame(stats_grid, relief='solid', borderwidth=1)
        usage_frame.grid(row=1, column=1, sticky='nsew', padx=2, pady=2)

        ttk.Label(usage_frame, text="Usage", font=('Arial', 12, 'bold')).pack(pady=(10, 0))
        ttk.Label(usage_frame, text="Storage Usage", font=('Arial', 10)).pack()
        self.usage_stat_label = ttk.Label(usage_frame, text="0.00%", font=('Arial', 16))
        self.usage_stat_label.pack(pady=(0, 10))

        # Last updated stat
        updated_frame = ttk.Frame(stats_grid, relief='solid', borderwidth=1)
        updated_frame.grid(row=1, column=2, sticky='nsew', padx=2, pady=2)

        ttk.Label(updated_frame, text="Updated", font=('Arial', 12, 'bold')).pack(pady=(10, 0))
        ttk.Label(updated_frame, text="Last Updated", font=('Arial', 10)).pack()
        self.updated_stat_label = ttk.Label(updated_frame, text="Never", font=('Arial', 12))
        self.updated_stat_label.pack(pady=(0, 10))

    def setup_log_redirection(self):
        """Setup log redirection to GUI"""
        self.log_redirector = LogRedirector(self.log_text)

        # Initial welcome message
        self.log_text.insert(tk.END, "=== Web Parser Control Panel Started ===\n", "info")
        self.log_text.insert(tk.END, f"Timestamp: {datetime.datetime.now()}\n", "info")
        self.log_text.insert(tk.END, f"Working Directory: {os.getcwd()}\n", "info")
        self.log_text.insert(tk.END, "Ready to start parser...\n\n", "info")

    def setup_monitoring(self):
        """Setup background monitoring threads"""
        # Progress monitoring thread
        self.progress_thread = threading.Thread(target=self.monitor_progress, daemon=True)
        self.progress_thread.start()

        # Parser status monitoring thread
        self.status_thread = threading.Thread(target=self.monitor_parser_status, daemon=True)
        self.status_thread.start()

    def monitor_progress(self):
        """Monitor progress.json for changes"""
        last_modified = 0

        while self.update_running:
            try:
                if os.path.exists("progress.json"):
                    current_modified = os.path.getmtime("progress.json")

                    if current_modified != last_modified:
                        last_modified = current_modified
                        self.update_progress_display()

                time.sleep(2)  # Check every 2 seconds

            except Exception as e:
                print(f"Error monitoring progress: {e}")
                time.sleep(5)

    def monitor_parser_status(self):
        """Monitor parser status and update GUI accordingly"""
        while self.update_running:
            try:
                if self.parser_controller:
                    status = self.parser_controller.get_status()

                    # Update GUI status in main thread
                    self.root.after(0, self._update_status_display, status)

                time.sleep(1)  # Check every second

            except Exception as e:
                print(f"Error monitoring parser status: {e}")
                time.sleep(5)

    def _update_status_display(self, status):
        """Update status display in main thread"""
        try:
            running = status.get('running', False)
            pid = status.get('pid')
            exit_code = status.get('exit_code')

            if running:
                self.status_label.config(text="Running", foreground='green')
                self.start_button.config(state=tk.DISABLED)
                self.stop_button.config(state=tk.NORMAL)
                self.reset_button.config(state=tk.DISABLED)
                self.download_dir_entry.config(state=tk.DISABLED)
            else:
                self.status_label.config(text="Stopped", foreground='red')
                self.start_button.config(state=tk.NORMAL)
                self.stop_button.config(state=tk.DISABLED)
                self.reset_button.config(state=tk.NORMAL)
                self.download_dir_entry.config(state=tk.NORMAL)

                # If process exited with error, show it
                if exit_code is not None and exit_code != 0:
                    self.add_log_message(f"Parser exited with error code: {exit_code}", "error")

            # Update PID display
            if pid:
                self.pid_label.config(text=str(pid))
            else:
                self.pid_label.config(text="N/A")

        except Exception as e:
            print(f"Error updating status display: {e}")

    def update_progress_display(self):
        """Update progress bar and statistics"""
        try:
            progress_data = self.progress_tracker.load_progress()
            self.progress_data = progress_data

            # Get configuration for max storage
            total_storage_gb = self.max_storage_var.get()
            total_storage_mb = total_storage_gb * 1024

            # Calculate progress
            current_mb = progress_data.get('total_size_mb', 0.0)
            percentage = (current_mb / total_storage_mb) * 100 if total_storage_mb > 0 else 0

            # Update progress bar
            self.root.after(0, self._update_progress_widgets, current_mb, total_storage_gb, percentage, progress_data)

        except Exception as e:
            print(f"Error updating progress display: {e}")

    def _update_progress_widgets(self, current_mb, total_storage_gb, percentage, progress_data):
        """Update progress widgets in main thread"""
        try:
            # Update progress bar
            self.progress_var.set(min(percentage, 100))

            # Update progress text
            if current_mb >= 1024:
                current_display = f"{current_mb/1024:.2f} GB"
            else:
                current_display = f"{current_mb:.2f} MB"

            self.progress_text.config(
                text=f"{current_display} / {total_storage_gb:.0f} GB ({percentage:.2f}%)"
            )

            # Update video count
            downloaded_count = progress_data.get('total_downloaded', 0)
            self.video_count_label.config(text=f"Videos: {downloaded_count}")

            # Update current page
            current_page = progress_data.get('last_page', 'N/A')
            self.current_page_label.config(text=str(current_page))

            # Update statistics
            self.downloaded_count_label.config(text=str(downloaded_count))
            failed_count = len(progress_data.get('failed_videos', []))
            self.failed_count_label.config(text=str(failed_count))
            self.page_stat_label.config(text=str(current_page))
            self.size_stat_label.config(text=current_display)
            self.usage_stat_label.config(text=f"{percentage:.2f}%")

            # Update last updated
            last_updated = progress_data.get('last_updated', 'Never')
            self.updated_stat_label.config(text=last_updated)

        except Exception as e:
            print(f"Error updating widgets: {e}")

    def add_log_message(self, message, tag="stdout"):
        """Add message to log display"""
        try:
            # Ensure this runs in main thread
            if threading.current_thread() != threading.main_thread():
                self.root.after(0, self.add_log_message, message, tag)
                return

            self.log_text.insert(tk.END, message + "\n", tag)

            # Auto-scroll if enabled
            if self.auto_scroll_var.get():
                self.log_text.see(tk.END)

            # Limit log size (keep last 1000 lines)
            lines = self.log_text.get("1.0", tk.END).split("\n")
            if len(lines) > 1000:
                # Delete first 200 lines
                self.log_text.delete("1.0", "200.0")

        except Exception as e:
            print(f"Error adding log message: {e}")

    def start_parser(self):
        """Start the parser"""
        try:
            self.add_log_message("Starting parser...", "info")

            if self.parser_controller.start_parser():
                self.add_log_message("Parser started successfully", "info")
            else:
                self.add_log_message("Failed to start parser", "error")
                messagebox.showerror("Error", "Failed to start parser. Check logs for details.")

        except Exception as e:
            self.add_log_message(f"Error starting parser: {e}", "error")
            messagebox.showerror("Error", f"Error starting parser: {e}")

    def stop_parser(self):
        """Stop the parser"""
        try:
            self.add_log_message("Stopping parser...", "warning")

            if self.parser_controller.stop_parser():
                self.add_log_message("Parser stopped successfully", "warning")
            else:
                self.add_log_message("Failed to stop parser", "error")

        except Exception as e:
            self.add_log_message(f"Error stopping parser: {e}", "error")

    def reset_progress(self):
        """Reset progress data"""
        if self.parser_controller.get_status().get('running', False):
            messagebox.showwarning("Warning", "Cannot reset progress while parser is running.")
            return

        # Confirm reset
        result = messagebox.askyesno(
            "Confirm Reset",
            "Are you sure you want to reset all progress?\n\nThis will:\n"
            "• Reset download progress to 0\n"
            "• Clear downloaded video list\n"
            "• Reset current page to 1000\n\n"
            "This action cannot be undone."
        )

        if result:
            try:
                if self.progress_tracker.reset_progress():
                    self.add_log_message("Progress reset successfully", "warning")
                    self.update_progress_display()
                    messagebox.showinfo("Success", "Progress has been reset successfully.")
                else:
                    self.add_log_message("Failed to reset progress", "error")
                    messagebox.showerror("Error", "Failed to reset progress.")

            except Exception as e:
                self.add_log_message(f"Error resetting progress: {e}", "error")
                messagebox.showerror("Error", f"Error resetting progress: {e}")

    def browse_directory(self):
        """Browse for download directory"""
        if self.parser_controller.get_status().get('running', False):
            messagebox.showwarning("Warning", "Cannot change directory while parser is running.")
            return

        directory = filedialog.askdirectory(
            title="Select Download Directory",
            initialdir=self.download_dir_var.get()
        )

        if directory:
            self.download_dir_var.set(directory)

    def save_config(self):
        """Save configuration changes"""
        if self.parser_controller.get_status().get('running', False):
            messagebox.showwarning("Warning", "Cannot save configuration while parser is running.")
            return

        try:
            # Update config data
            config = self.config_data.copy()

            # Update general settings
            config.setdefault('general', {})
            config['general']['download_path'] = self.download_dir_var.get()
            config['general']['max_storage_gb'] = self.max_storage_var.get()
            config['general']['base_url'] = self.base_url_var.get()

            # Update processing settings
            config.setdefault('processing', {})
            config['processing']['use_parallel'] = self.use_parallel_var.get()
            config['processing']['parallel_batch_size'] = self.batch_size_var.get()

            # Save to file
            if self.config_manager.save_config(config):
                self.config_data = config
                self.add_log_message("Configuration saved successfully", "info")
                messagebox.showinfo("Success", "Configuration saved successfully.")
            else:
                self.add_log_message("Failed to save configuration", "error")
                messagebox.showerror("Error", "Failed to save configuration.")

        except Exception as e:
            self.add_log_message(f"Error saving configuration: {e}", "error")
            messagebox.showerror("Error", f"Error saving configuration: {e}")

    def reload_config(self):
        """Reload configuration from file"""
        try:
            self.config_data = self.config_manager.load_config()

            # Update GUI fields
            self.download_dir_var.set(self.config_data.get('general', {}).get('download_path', 'scraper_downloads'))
            self.max_storage_var.set(self.config_data.get('general', {}).get('max_storage_gb', 940))
            self.base_url_var.set(self.config_data.get('general', {}).get('base_url', 'https://rule34video.com'))
            self.use_parallel_var.set(self.config_data.get('processing', {}).get('use_parallel', True))
            self.batch_size_var.set(self.config_data.get('processing', {}).get('parallel_batch_size', 5))

            self.add_log_message("Configuration reloaded", "info")
            messagebox.showinfo("Success", "Configuration reloaded successfully.")

        except Exception as e:
            self.add_log_message(f"Error reloading configuration: {e}", "error")
            messagebox.showerror("Error", f"Error reloading configuration: {e}")

    def reset_config(self):
        """Reset configuration to defaults"""
        if self.parser_controller.get_status().get('running', False):
            messagebox.showwarning("Warning", "Cannot reset configuration while parser is running.")
            return

        result = messagebox.askyesno(
            "Confirm Reset",
            "Are you sure you want to reset configuration to defaults?"
        )

        if result:
            try:
                self.config_data = self.config_manager.default_config.copy()

                # Update GUI fields
                self.download_dir_var.set('scraper_downloads')
                self.max_storage_var.set(940)
                self.base_url_var.set('https://rule34video.com')
                self.use_parallel_var.set(True)
                self.batch_size_var.set(5)

                self.add_log_message("Configuration reset to defaults", "warning")
                messagebox.showinfo("Success", "Configuration reset to defaults.")

            except Exception as e:
                self.add_log_message(f"Error resetting configuration: {e}", "error")
                messagebox.showerror("Error", f"Error resetting configuration: {e}")

    def clear_logs(self):
        """Clear the log display"""
        self.log_text.delete(1.0, tk.END)
        self.add_log_message("Logs cleared", "info")

    def save_logs(self):
        """Save logs to file"""
        filename = filedialog.asksaveasfilename(
            title="Save Logs",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialname=f"parser_logs_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

        if filename:
            try:
                logs_content = self.log_text.get(1.0, tk.END)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(logs_content)

                self.add_log_message(f"Logs saved to {filename}", "info")
                messagebox.showinfo("Success", f"Logs saved to {filename}")

            except Exception as e:
                self.add_log_message(f"Error saving logs: {e}", "error")
                messagebox.showerror("Error", f"Error saving logs: {e}")

    def on_closing(self):
        """Handle application closing"""
        if self.parser_controller.get_status().get('running', False):
            result = messagebox.askyesno(
                "Confirm Exit",
                "Parser is still running. Do you want to stop it and exit?"
            )
            if result:
                self.parser_controller.stop_parser()
                time.sleep(1)  # Give it time to stop
            else:
                return

        # Stop monitoring threads
        self.update_running = False

        # Destroy window
        self.root.destroy()

    def run(self):
        """Run the GUI application"""
        # Set up window close handler
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Initial progress update
        self.update_progress_display()

        # Start the main loop
        self.root.mainloop()


def main():
    """Main entry point"""
    print("Starting Web Parser Control Panel v1.1...")

    # Check if main_scraper.py exists
    if not os.path.exists("main_scraper.py"):
        messagebox.showerror(
            "Missing File",
            "main_scraper.py not found!\n\n"
            "Please make sure the main parser script is in the same directory as this GUI."
        )
        return 1

    try:
        # Create and run GUI
        app = WebParserGUI()
        app.run()
        return 0

    except Exception as e:
        messagebox.showerror("Error", f"Failed to start GUI application:\n\n{e}")
        return 1


if __name__ == "__main__":
    """Entry point for the GUI application"""
    exit_code = main()
    sys.exit(exit_code)
