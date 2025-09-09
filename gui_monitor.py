# gui_monitor.py - GUI Monitor showing REAL scraper logs with STOP button

import tkinter as tk
from tkinter import ttk, scrolledtext
import json
import time
import os
import logging
from pathlib import Path
from datetime import datetime
import subprocess
import threading

class VPSScraperMonitorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Video Scraper - VPS Monitor Dashboard")
        self.root.geometry("1000x700")
        self.root.configure(bg='#1e1e1e')
        
        # Initialize attributes (same logic as console version)
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.progress_file = os.path.join(self.script_dir, "progress.json")
        self.log_file = os.path.join(self.script_dir, "scraper.log")
        self.service_log_file = os.path.join(self.script_dir, "scraper_service.log")
        self.download_path = Path("C:/scraper_downloads")
        
        # GUI specific
        self.info_labels = {}
        self.is_monitoring = True
        
        # Log tracking for live updates
        self.last_log_position = 0
        self.last_service_log_position = 0
        self.displayed_logs = set()  # Track displayed logs to avoid duplicates
        
        # Create GUI elements
        self.create_widgets()
        
        # Start monitoring updates
        self.start_monitoring()
        
        # Handle window close
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
    def create_widgets(self):
        """Create all GUI widgets"""
        
        # Main container
        main_frame = tk.Frame(self.root, bg='#1e1e1e')
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Top section - Title and info
        top_frame = tk.Frame(main_frame, bg='#1e1e1e')
        top_frame.pack(fill='x', pady=(0, 10))
        
        # Title
        title_label = tk.Label(
            top_frame, 
            text="📊 Video Scraper - VPS Monitor Dashboard", 
            font=('Arial', 20, 'bold'),
            fg='#00ff41',
            bg='#1e1e1e'
        )
        title_label.pack()
        
        # Subtitle
        subtitle_label = tk.Label(
            top_frame,
            text="💻 VPS Mode: Live scraper monitoring (disconnection safe)",
            font=('Arial', 12),
            fg='#ffffff',
            bg='#1e1e1e'
        )
        subtitle_label.pack(pady=(5, 0))
        
        # Control buttons frame
        control_frame = tk.Frame(top_frame, bg='#1e1e1e')
        control_frame.pack(pady=10)
        
        # Refresh button
        self.refresh_button = tk.Button(
            control_frame,
            text="🔄 REFRESH NOW",
            font=('Arial', 14, 'bold'),
            bg='#17a2b8',
            fg='white',
            width=15,
            height=2,
            command=self.manual_update,
            relief='flat',
            cursor='hand2'
        )
        self.refresh_button.pack(side='left', padx=5)
        
        # Stop scraper button
        self.stop_button = tk.Button(
            control_frame,
            text="🛑 STOP SCRAPER",
            font=('Arial', 14, 'bold'),
            bg='#dc3545',
            fg='white',
            width=15,
            height=2,
            command=self.stop_scraper,
            relief='flat',
            cursor='hand2'
        )
        self.stop_button.pack(side='left', padx=5)
        
        # Clear logs button
        self.clear_button = tk.Button(
            control_frame,
            text="🗑️ CLEAR DISPLAY",
            font=('Arial', 14, 'bold'),
            bg='#6c757d',
            fg='white',
            width=15,
            height=2,
            command=self.clear_terminal,
            relief='flat',
            cursor='hand2'
        )
        self.clear_button.pack(side='left', padx=5)
        
        # Service control button
        self.service_button = tk.Button(
            control_frame,
            text="⚙️ SERVICE CONTROL",
            font=('Arial', 14, 'bold'),
            bg='#28a745',
            fg='white',
            width=15,
            height=2,
            command=self.open_service_control,
            relief='flat',
            cursor='hand2'
        )
        self.service_button.pack(side='left', padx=5)
        
        # Status frame
        status_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='ridge', bd=2)
        status_frame.pack(fill='x', pady=(0, 10), padx=2)
        
        # Service status
        service_status_frame = tk.Frame(status_frame, bg='#2d2d2d')
        service_status_frame.pack(fill='x', padx=10, pady=5)
        
        tk.Label(service_status_frame, text="Windows Service:", font=('Arial', 12, 'bold'), fg='#ffffff', bg='#2d2d2d').pack(side='left')
        self.service_status_label = tk.Label(
            service_status_frame,
            text="Checking...",
            font=('Arial', 12, 'bold'),
            fg='#ffc107',
            bg='#2d2d2d'
        )
        self.service_status_label.pack(side='left', padx=(10, 0))
        
        # Process status
        process_status_frame = tk.Frame(status_frame, bg='#2d2d2d')
        process_status_frame.pack(fill='x', padx=10, pady=5)
        
        tk.Label(process_status_frame, text="Python Process:", font=('Arial', 12, 'bold'), fg='#ffffff', bg='#2d2d2d').pack(side='left')
        self.process_status_label = tk.Label(
            process_status_frame,
            text="Checking...",
            font=('Arial', 12, 'bold'),
            fg='#ffc107',
            bg='#2d2d2d'
        )
        self.process_status_label.pack(side='left', padx=(10, 0))
        
        # Working directory
        dir_frame = tk.Frame(status_frame, bg='#2d2d2d')
        dir_frame.pack(fill='x', padx=10, pady=5)
        
        tk.Label(dir_frame, text="Working Directory:", font=('Arial', 12, 'bold'), fg='#ffffff', bg='#2d2d2d').pack(side='left')
        self.dir_label = tk.Label(
            dir_frame,
            text=self.script_dir,
            font=('Arial', 10),
            fg='#17a2b8',
            bg='#2d2d2d'
        )
        self.dir_label.pack(side='left', padx=(10, 0))
        
        # Stats grid
        stats_frame = tk.Frame(main_frame, bg='#1e1e1e')
        stats_frame.pack(fill='x', pady=(0, 10))
        
        # Create stats boxes
        self.create_stat_box(stats_frame, "📊 Videos Downloaded", "videos_downloaded", 0, 0)
        self.create_stat_box(stats_frame, "📄 Current Page", "current_page", 0, 1)
        self.create_stat_box(stats_frame, "💾 Downloaded Size", "downloaded_size", 0, 2)
        self.create_stat_box(stats_frame, "📁 Folder Size", "folder_size", 0, 3)
        
        # File status frame
        files_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='ridge', bd=2)
        files_frame.pack(fill='x', pady=(0, 10), padx=2)
        
        tk.Label(
            files_frame,
            text="📄 File Status:",
            font=('Arial', 12, 'bold'),
            fg='#ffffff',
            bg='#2d2d2d'
        ).pack(pady=(10, 5))
        
        file_status_frame = tk.Frame(files_frame, bg='#2d2d2d')
        file_status_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        # File status labels
        self.file_status_labels = {}
        files_to_check = [
            ("progress.json", self.progress_file),
            ("scraper.log", self.log_file),
            ("service.log", self.service_log_file)
        ]
        
        for i, (name, path) in enumerate(files_to_check):
            file_frame = tk.Frame(file_status_frame, bg='#2d2d2d')
            file_frame.pack(fill='x', pady=2)
            
            label = tk.Label(
                file_frame,
                text=f"{name}:",
                font=('Arial', 10, 'bold'),
                fg='#ffffff',
                bg='#2d2d2d'
            )
            label.pack(side='left')
            
            status_label = tk.Label(
                file_frame,
                text="Checking...",
                font=('Arial', 10),
                fg='#ffc107',
                bg='#2d2d2d'
            )
            status_label.pack(side='left', padx=(10, 0))
            
            self.file_status_labels[name] = status_label
        
        # Terminal section for REAL logs
        terminal_frame = tk.LabelFrame(
            main_frame, 
            text="📟 Live Scraper Logs (Real-time)",
            font=('Arial', 12, 'bold'),
            fg='#00ff41',
            bg='#1e1e1e',
            relief='ridge',
            bd=2
        )
        terminal_frame.pack(fill='both', expand=True)
        
        # Terminal text area
        self.terminal_text = scrolledtext.ScrolledText(
            terminal_frame,
            height=15,
            bg='#000000',
            fg='#00ff41',
            font=('Consolas', 9),
            wrap=tk.WORD,
            insertbackground='#00ff41',
            state='disabled'  # Make it read-only
        )
        self.terminal_text.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Configure terminal tags for colored output
        self.terminal_text.tag_configure("INFO", foreground="#00ff41")
        self.terminal_text.tag_configure("WARNING", foreground="#ffc107")
        self.terminal_text.tag_configure("ERROR", foreground="#dc3545")
        self.terminal_text.tag_configure("SUCCESS", foreground="#28a745")
        self.terminal_text.tag_configure("SERVICE", foreground="#17a2b8")
        self.terminal_text.tag_configure("DOWNLOAD", foreground="#17a2b8")
        
        # Add welcome message
        self.add_terminal_log("🚀 VPS Monitor Dashboard Started - Showing REAL scraper logs", "INFO")
        self.add_terminal_log("📊 Monitoring live scraper activity...", "INFO")
        self.add_terminal_log("💡 This window is safe for VPS disconnection", "INFO")
        
    def create_stat_box(self, parent, title, key, row, col):
        """Create a statistics display box"""
        box_frame = tk.Frame(parent, bg='#2d2d2d', relief='ridge', bd=2)
        box_frame.grid(row=row, column=col, padx=5, pady=5, sticky='ew')
        
        parent.grid_columnconfigure(col, weight=1)
        
        tk.Label(
            box_frame,
            text=title,
            font=('Arial', 10, 'bold'),
            fg='#ffffff',
            bg='#2d2d2d'
        ).pack(pady=(5, 2))
        
        value_label = tk.Label(
            box_frame,
            text="Loading...",
            font=('Arial', 14, 'bold'),
            fg='#00ff41',
            bg='#2d2d2d'
        )
        value_label.pack(pady=(0, 5))
        
        self.info_labels[key] = value_label
        
    def add_terminal_log(self, message, level="INFO"):
        """Add message to terminal with timestamp and color"""
        timestamp = time.strftime("%H:%M:%S")
        
        # Level indicators
        level_indicators = {
            "INFO": "ℹ️",
            "WARNING": "⚠️", 
            "ERROR": "❌",
            "SUCCESS": "✅",
            "SERVICE": "🔧",
            "DOWNLOAD": "📥"
        }
        
        indicator = level_indicators.get(level, "ℹ️")
        full_message = f"[{timestamp}] {indicator} {message}\\n"
        
        # Insert with color (make sure terminal is editable temporarily)
        self.terminal_text.config(state='normal')
        self.terminal_text.insert(tk.END, full_message, level)
        self.terminal_text.see(tk.END)  # Auto-scroll to bottom
        self.terminal_text.config(state='disabled')
        
    def clear_terminal(self):
        """Clear terminal output"""
        self.terminal_text.config(state='normal')
        self.terminal_text.delete(1.0, tk.END)
        self.terminal_text.config(state='disabled')
        self.add_terminal_log("🗑️ Display cleared", "INFO")
        
        # Reset log positions to show fresh logs
        self.last_log_position = 0
        self.last_service_log_position = 0
        self.displayed_logs.clear()
        
    def stop_scraper(self):
        """Stop the scraper service or process"""
        self.add_terminal_log("🛑 STOP SCRAPER REQUESTED", "ERROR")
        
        try:
            # Try to stop the Windows service first
            self.add_terminal_log("🔴 Attempting to stop Windows Service...", "ERROR")
            result = subprocess.run(
                ['sc', 'stop', 'VideoScraperService'],
                capture_output=True, text=True, shell=True
            )
            
            if result.returncode == 0:
                self.add_terminal_log("✅ Windows Service stopped successfully", "SUCCESS")
            else:
                self.add_terminal_log("⚠️ Service stop failed or not installed", "WARNING")
                
                # Try to kill Python processes
                self.add_terminal_log("🔴 Attempting to stop Python processes...", "ERROR")
                result = subprocess.run(
                    ['taskkill', '/F', '/IM', 'python.exe'],
                    capture_output=True, text=True, shell=True
                )
                
                if result.returncode == 0:
                    self.add_terminal_log("✅ Python processes stopped", "SUCCESS")
                else:
                    self.add_terminal_log("⚠️ No Python processes found to stop", "WARNING")
                    
        except Exception as e:
            self.add_terminal_log(f"❌ Error stopping scraper: {e}", "ERROR")
            
    def open_service_control(self):
        """Open service control panel"""
        try:
            service_control_path = os.path.join(self.script_dir, "service_control.bat")
            if os.path.exists(service_control_path):
                subprocess.Popen(service_control_path, shell=True)
                self.add_terminal_log("⚙️ Opened service control panel", "SERVICE")
            else:
                self.add_terminal_log("❌ service_control.bat not found", "ERROR")
        except Exception as e:
            self.add_terminal_log(f"❌ Error opening service control: {e}", "ERROR")
            
    def manual_update(self):
        """Manual refresh of all data"""
        self.add_terminal_log("🔄 Manual refresh triggered", "INFO")
        self.update_display()
        
    # ========== MONITORING LOGIC ==========
    
    def get_progress_info(self):
        """Get current progress information"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            return {}
        return {}
        
    def get_folder_size_gb(self, path):
        """Calculate folder size in GB"""
        try:
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
            return total_size / (1024**3)  # Convert to GB
        except:
            return 0.0
            
    def is_scraper_running(self):
        """Check if scraper process is running"""
        try:
            result = subprocess.run(
                ['tasklist', '/FI', 'IMAGENAME eq python.exe', '/FO', 'CSV'],
                capture_output=True, text=True, shell=True
            )
            python_processes = result.stdout.count('python.exe')
            return python_processes > 0
        except:
            return False
            
    def is_service_running(self):
        """Check if the Windows service is running"""
        try:
            result = subprocess.run(
                ['sc', 'query', 'VideoScraperService'],
                capture_output=True, text=True, shell=True
            )
            if result.returncode == 0:
                return "RUNNING" in result.stdout
            return False
        except:
            return False
            
    def read_new_log_entries(self, log_file_path, last_position):
        """Read new log entries from specified file starting from last position"""
        try:
            if not os.path.exists(log_file_path):
                return [], last_position
                
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Go to last position
                f.seek(last_position)
                
                # Read new content
                new_content = f.read()
                new_position = f.tell()
                
                if new_content:
                    # Split into lines and filter out empty ones
                    new_lines = [line.strip() for line in new_content.split('\\n') if line.strip()]
                    return new_lines, new_position
                    
        except Exception as e:
            pass
            
        return [], last_position
        
    def update_display(self):
        """Update all display elements"""
        try:
            # Update service status
            service_running = self.is_service_running()
            if service_running:
                self.service_status_label.configure(text="🟢 RUNNING", fg='#28a745')
            else:
                self.service_status_label.configure(text="🔴 STOPPED", fg='#dc3545')
                
            # Update process status
            process_running = self.is_scraper_running()
            if process_running:
                self.process_status_label.configure(text="🟢 RUNNING", fg='#28a745')
            else:
                self.process_status_label.configure(text="🔴 STOPPED", fg='#dc3545')
            
            # Update progress information
            progress = self.get_progress_info()
            if progress:
                # Videos downloaded
                total_downloaded = progress.get('total_downloaded', 0)
                self.info_labels["videos_downloaded"].configure(text=str(total_downloaded))
                
                # Current page
                last_page = progress.get('last_page', 'Unknown')
                self.info_labels["current_page"].configure(text=str(last_page))
                
                # Downloaded size
                total_size_mb = progress.get('total_size_mb', 0)
                total_size_gb = total_size_mb / 1024 if total_size_mb else 0
                self.info_labels["downloaded_size"].configure(text=f"{total_size_gb:.2f} GB")
            else:
                self.info_labels["videos_downloaded"].configure(text="No Data")
                self.info_labels["current_page"].configure(text="No Data")
                self.info_labels["downloaded_size"].configure(text="No Data")
            
            # Folder size
            folder_size = self.get_folder_size_gb(self.download_path)
            self.info_labels["folder_size"].configure(text=f"{folder_size:.2f} GB")
            
            # Update file status
            files_to_check = [
                ("progress.json", self.progress_file),
                ("scraper.log", self.log_file),
                ("service.log", self.service_log_file)
            ]
            
            for name, path in files_to_check:
                if os.path.exists(path):
                    size = os.path.getsize(path)
                    self.file_status_labels[name].configure(
                        text=f"✅ {size} bytes", 
                        fg='#28a745'
                    )
                else:
                    self.file_status_labels[name].configure(
                        text="❌ Not found", 
                        fg='#dc3545'
                    )
            
            # Update REAL log display
            self.update_real_log_display()
            
        except Exception as e:
            self.add_terminal_log(f"❌ Error updating display: {e}", "ERROR")
            
    def update_real_log_display(self):
        """Read and display REAL scraper logs"""
        try:
            # Read new service log entries
            service_logs, self.last_service_log_position = self.read_new_log_entries(
                self.service_log_file, self.last_service_log_position
            )
            
            for log_entry in service_logs:
                if log_entry and log_entry not in self.displayed_logs:
                    self.displayed_logs.add(log_entry)
                    self.display_log_entry("SERVICE", log_entry)
            
            # Read new scraper log entries
            scraper_logs, self.last_log_position = self.read_new_log_entries(
                self.log_file, self.last_log_position
            )
            
            for log_entry in scraper_logs:
                if log_entry and log_entry not in self.displayed_logs:
                    self.displayed_logs.add(log_entry)
                    self.display_log_entry("SCRAPER", log_entry)
                    
            # Keep displayed_logs set from growing too large
            if len(self.displayed_logs) > 1000:
                # Keep only the most recent 500
                recent_logs = list(self.displayed_logs)[-500:]
                self.displayed_logs = set(recent_logs)
                
        except Exception as e:
            pass  # Ignore log reading errors
            
    def display_log_entry(self, source, log_entry):
        """Display a single log entry with proper formatting"""
        try:
            # Determine log level from content
            if "ERROR" in log_entry:
                level = "ERROR"
            elif "WARNING" in log_entry:
                level = "WARNING"  
            elif "SUCCESS" in log_entry or "✓" in log_entry or "successfully" in log_entry:
                level = "SUCCESS"
            elif "download" in log_entry.lower() or "Video_" in log_entry:
                level = "DOWNLOAD"
            else:
                level = "INFO"
            
            # Clean up the log entry
            clean_entry = log_entry.replace("MainThread", "MAIN")
            clean_entry = clean_entry.replace("VideoWorker-", "WORKER-")
            
            # Format with source prefix
            if source == "SERVICE":
                formatted_entry = f"🔧 SVC: {clean_entry}"
            else:
                formatted_entry = f"📊 APP: {clean_entry}"
            
            # Add to terminal without timestamp (log already has it)
            self.terminal_text.config(state='normal')
            self.terminal_text.insert(tk.END, formatted_entry + "\\n", level)
            self.terminal_text.see(tk.END)
            self.terminal_text.config(state='disabled')
            
        except Exception as e:
            pass
            
    def start_monitoring(self):
        """Start the monitoring loop"""
        def monitor_thread():
            while self.is_monitoring:
                try:
                    self.root.after(0, self.update_display)  # Update GUI from main thread
                    time.sleep(3)  # Update every 3 seconds for more responsive logs
                except Exception as e:
                    time.sleep(5)
        
        # Start monitoring in background thread
        monitor_thread = threading.Thread(target=monitor_thread, daemon=True)
        monitor_thread.start()
        
        # Initial update
        self.update_display()
        
    def on_closing(self):
        """Handle window close event"""
        self.is_monitoring = False
        self.add_terminal_log("👋 VPS Monitor closing...", "INFO")
        time.sleep(1)
        self.root.destroy()

def main():
    """Main function to run the GUI monitor"""
    root = tk.Tk()
    app = VPSScraperMonitorGUI(root)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("Monitor GUI closed by user")

if __name__ == "__main__":
    main()