# gui_monitor.py - GUI Monitor for VPS (same logic as vps_monitor.py but with GUI)

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
        self.last_check_time = None
        
        # GUI specific
        self.info_labels = {}
        self.is_monitoring = True
        
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
            text="💻 VPS Mode: Background monitoring (disconnection safe)",
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
            width=18,
            height=2,
            command=self.manual_update,
            relief='flat',
            cursor='hand2'
        )
        self.refresh_button.pack(side='left', padx=5)
        
        # Clear logs button
        self.clear_button = tk.Button(
            control_frame,
            text="🗑️ CLEAR DISPLAY",
            font=('Arial', 14, 'bold'),
            bg='#6c757d',
            fg='white',
            width=18,
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
            width=18,
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
        
        # Terminal section for logs
        terminal_frame = tk.LabelFrame(
            main_frame, 
            text="📟 Recent Log Entries",
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
            insertbackground='#00ff41'
        )
        self.terminal_text.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Configure terminal tags for colored output
        self.terminal_text.tag_configure("INFO", foreground="#00ff41")
        self.terminal_text.tag_configure("WARNING", foreground="#ffc107")
        self.terminal_text.tag_configure("ERROR", foreground="#dc3545")
        self.terminal_text.tag_configure("SUCCESS", foreground="#28a745")
        self.terminal_text.tag_configure("SERVICE", foreground="#17a2b8")
        
        # Add welcome message
        self.add_terminal_log("🚀 VPS Monitor Dashboard Started", "INFO")
        self.add_terminal_log("📊 Monitoring scraper status and progress...", "INFO")
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
            "SERVICE": "🔧"
        }
        
        indicator = level_indicators.get(level, "ℹ️")
        full_message = f"[{timestamp}] {indicator} {message}\\n"
        
        # Insert with color
        self.terminal_text.insert(tk.END, full_message, level)
        self.terminal_text.see(tk.END)  # Auto-scroll to bottom
        
    def clear_terminal(self):
        """Clear terminal output"""
        self.terminal_text.delete(1.0, tk.END)
        self.add_terminal_log("🗑️ Display cleared", "INFO")
        
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
        
    # ========== MONITORING LOGIC (Same as console version) ==========
    
    def get_progress_info(self):
        """Get current progress information (same logic as console)"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            self.add_terminal_log(f"Error reading progress: {e}", "ERROR")
        return {}
        
    def get_folder_size_gb(self, path):
        """Calculate folder size in GB (same logic as console)"""
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
        """Check if scraper process is running (same logic as console)"""
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
        """Check if the Windows service is running (same logic as console)"""
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
            
    def get_latest_log_entries(self, log_file_path, num_lines=4):
        """Get latest log entries from specified file (same logic as console)"""
        try:
            if os.path.exists(log_file_path):
                with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    return lines[-num_lines:] if lines else []
        except Exception as e:
            return [f"Error reading log: {e}"]
        return []
        
    def update_display(self):
        """Update all display elements (same logic as console but for GUI)"""
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
                
                # Latest downloads info
                downloaded = progress.get('downloaded_videos', [])
                if downloaded and len(downloaded) > 0:
                    latest = ', '.join(downloaded[-3:])  # Last 3
                    self.add_terminal_log(f"📥 Latest: {latest}", "SUCCESS")
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
            
            # Update recent log entries
            self.update_log_display()
            
        except Exception as e:
            self.add_terminal_log(f"❌ Error updating display: {e}", "ERROR")
            
    def update_log_display(self):
        """Update the log display with recent entries"""
        try:
            # Get service log entries
            service_log_entries = self.get_latest_log_entries(self.service_log_file, 3)
            if service_log_entries and any(line.strip() for line in service_log_entries):
                for entry in service_log_entries:
                    entry = entry.strip()
                    if entry and entry not in getattr(self, '_last_service_logs', []):
                        if "ERROR" in entry:
                            self.add_terminal_log(f"SERVICE: {entry}", "ERROR")
                        elif "WARNING" in entry:
                            self.add_terminal_log(f"SERVICE: {entry}", "WARNING")
                        elif "SUCCESS" in entry or "successfully" in entry:
                            self.add_terminal_log(f"SERVICE: {entry}", "SUCCESS")
                        else:
                            self.add_terminal_log(f"SERVICE: {entry}", "SERVICE")
                
                self._last_service_logs = [line.strip() for line in service_log_entries if line.strip()]
            
            # Get regular scraper log entries
            regular_log_entries = self.get_latest_log_entries(self.log_file, 2)
            if regular_log_entries and any(line.strip() for line in regular_log_entries):
                for entry in regular_log_entries:
                    entry = entry.strip()
                    if entry and entry not in getattr(self, '_last_scraper_logs', []):
                        if "ERROR" in entry:
                            self.add_terminal_log(f"SCRAPER: {entry}", "ERROR")
                        elif "WARNING" in entry:
                            self.add_terminal_log(f"SCRAPER: {entry}", "WARNING")
                        elif "SUCCESS" in entry or "completed successfully" in entry:
                            self.add_terminal_log(f"SCRAPER: {entry}", "SUCCESS")
                        else:
                            self.add_terminal_log(f"SCRAPER: {entry}", "INFO")
                
                self._last_scraper_logs = [line.strip() for line in regular_log_entries if line.strip()]
                
        except Exception as e:
            pass  # Ignore log update errors
            
    def start_monitoring(self):
        """Start the monitoring loop"""
        def monitor_thread():
            while self.is_monitoring:
                try:
                    self.root.after(0, self.update_display)  # Update GUI from main thread
                    time.sleep(10)  # Update every 10 seconds (same as console)
                except Exception as e:
                    print(f"Monitor thread error: {e}")
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