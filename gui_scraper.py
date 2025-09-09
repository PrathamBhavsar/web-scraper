# gui_scraper.py - Enhanced GUI with WORKING Force Stop

import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import time
import json
import logging
import queue
import asyncio
import os
import signal
import sys
from pathlib import Path

# Import the scraper
from main_scraper import VideoScraper

class GUILogHandler(logging.Handler):
    """Custom logging handler to send logs to GUI"""
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
        
    def emit(self, record):
        log_entry = self.format(record)
        self.log_queue.put(log_entry)

class ScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Video Scraper - Advanced Terminal View")
        self.root.geometry("1000x700")
        self.root.configure(bg='#1e1e1e')
        
        # Initialize attributes
        self.scraper = None
        self.scraper_thread = None
        self.is_running = False
        self.force_stop_requested = False  # NEW: Force stop flag
        self.info_labels = {}
        self.start_time = None
        
        # Logging setup
        self.log_queue = queue.Queue()
        self.setup_logging()
        
        # Current download info
        self.current_file = ""
        self.current_progress = 0
        
        # Create GUI elements
        self.create_widgets()
        
        # Start updating display
        self.update_display()
        self.process_log_queue()
        
        # Handle window close
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
    def setup_logging(self):
        """Setup logging to capture scraper output"""
        self.gui_log_handler = GUILogHandler(self.log_queue)
        formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
        self.gui_log_handler.setFormatter(formatter)
        
    def create_widgets(self):
        """Create all GUI widgets"""
        
        # Main container
        main_frame = tk.Frame(self.root, bg='#1e1e1e')
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Top section - Title and controls
        top_frame = tk.Frame(main_frame, bg='#1e1e1e')
        top_frame.pack(fill='x', pady=(0, 10))
        
        # Title
        title_label = tk.Label(
            top_frame, 
            text="🚀 Video Scraper - Terminal View", 
            font=('Arial', 20, 'bold'),
            fg='#00ff41',
            bg='#1e1e1e'
        )
        title_label.pack()
        
        # Control buttons frame
        control_frame = tk.Frame(top_frame, bg='#1e1e1e')
        control_frame.pack(pady=10)
        
        # Start/Stop button
        self.start_button = tk.Button(
            control_frame,
            text="▶ START SCRAPER",
            font=('Arial', 14, 'bold'),
            bg='#28a745',
            fg='white',
            width=18,
            height=2,
            command=self.toggle_scraper,
            relief='flat',
            cursor='hand2'
        )
        self.start_button.pack(side='left', padx=5)
        
        # Cancel button - ENHANCED
        self.cancel_button = tk.Button(
            control_frame,
            text="❌ FORCE STOP",
            font=('Arial', 14, 'bold'),
            bg='#dc3545',
            fg='white',
            width=18,
            height=2,
            command=self.force_stop,
            relief='flat',
            cursor='hand2',
            state='disabled'
        )
        self.cancel_button.pack(side='left', padx=5)
        
        # Clear logs button
        self.clear_button = tk.Button(
            control_frame,
            text="🗑️ CLEAR LOGS",
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
        
        # Status and current file frame
        status_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='ridge', bd=2)
        status_frame.pack(fill='x', pady=(0, 10), padx=2)
        
        # Status
        status_info_frame = tk.Frame(status_frame, bg='#2d2d2d')
        status_info_frame.pack(fill='x', padx=10, pady=5)
        
        tk.Label(status_info_frame, text="Status:", font=('Arial', 12, 'bold'), fg='#ffffff', bg='#2d2d2d').pack(side='left')
        self.status_label = tk.Label(
            status_info_frame,
            text="Ready to start",
            font=('Arial', 12),
            fg='#ffc107',
            bg='#2d2d2d'
        )
        self.status_label.pack(side='left', padx=(10, 0))
        
        # Current file being processed
        current_file_frame = tk.Frame(status_frame, bg='#2d2d2d')
        current_file_frame.pack(fill='x', padx=10, pady=5)
        
        tk.Label(current_file_frame, text="Current File:", font=('Arial', 12, 'bold'), fg='#ffffff', bg='#2d2d2d').pack(side='left')
        self.current_file_label = tk.Label(
            current_file_frame,
            text="None",
            font=('Arial', 12),
            fg='#17a2b8',
            bg='#2d2d2d'
        )
        self.current_file_label.pack(side='left', padx=(10, 0))
        
        # Progress bar for current download
        progress_frame = tk.Frame(status_frame, bg='#2d2d2d')
        progress_frame.pack(fill='x', padx=10, pady=5)
        
        tk.Label(progress_frame, text="Progress:", font=('Arial', 12, 'bold'), fg='#ffffff', bg='#2d2d2d').pack(side='left')
        
        self.current_progress_bar = ttk.Progressbar(
            progress_frame,
            length=300,
            mode='indeterminate',
            style='Current.Horizontal.TProgressbar'
        )
        self.current_progress_bar.pack(side='left', padx=(10, 0))
        
        # Middle section - Stats grid
        stats_frame = tk.Frame(main_frame, bg='#1e1e1e')
        stats_frame.pack(fill='x', pady=(0, 10))
        
        # Create stats boxes
        self.create_stat_box(stats_frame, "📊 Videos", "videos_downloaded", 0, 0)
        self.create_stat_box(stats_frame, "📄 Page", "current_page", 0, 1)
        self.create_stat_box(stats_frame, "💾 Storage", "storage_used", 0, 2)
        self.create_stat_box(stats_frame, "⏱️ Time", "time_running", 0, 3)
        
        # Storage progress bar
        storage_progress_frame = tk.Frame(main_frame, bg='#1e1e1e')
        storage_progress_frame.pack(fill='x', pady=(0, 10))
        
        tk.Label(
            storage_progress_frame,
            text="💾 Storage Usage:",
            font=('Arial', 12, 'bold'),
            fg='#ffffff',
            bg='#1e1e1e'
        ).pack(side='left')
        
        self.storage_progress_bar = ttk.Progressbar(
            storage_progress_frame,
            length=400,
            mode='determinate',
            style='Storage.Horizontal.TProgressbar'
        )
        self.storage_progress_bar.pack(side='left', padx=(10, 0))
        
        # Storage percentage label
        self.storage_percent_label = tk.Label(
            storage_progress_frame,
            text="0%",
            font=('Arial', 12, 'bold'),
            fg='#28a745',
            bg='#1e1e1e'
        )
        self.storage_percent_label.pack(side='left', padx=(10, 0))
        
        # Terminal section
        terminal_frame = tk.LabelFrame(
            main_frame, 
            text="📟 Live Terminal Output",
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
        self.terminal_text.tag_configure("DOWNLOAD", foreground="#17a2b8")
        
        # Configure styles
        self.configure_styles()
        
        # Add welcome message
        self.add_terminal_log("🚀 Video Scraper Terminal Ready", "INFO")
        self.add_terminal_log("📂 Download folder: C:/scraper_downloads/", "INFO")
        self.add_terminal_log("💡 Click START SCRAPER to begin downloading", "INFO")
        
    def configure_styles(self):
        """Configure ttk styles"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Current download progress bar
        style.configure(
            'Current.Horizontal.TProgressbar',
            background='#17a2b8',
            troughcolor='#333333',
            borderwidth=1,
            lightcolor='#17a2b8',
            darkcolor='#17a2b8'
        )
        
        # Storage progress bar
        style.configure(
            'Storage.Horizontal.TProgressbar',
            background='#28a745',
            troughcolor='#333333',
            borderwidth=1,
            lightcolor='#28a745',
            darkcolor='#28a745'
        )
        
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
            text="0",
            font=('Arial', 14, 'bold'),
            fg='#00ff41',
            bg='#2d2d2d'
        )
        value_label.pack(pady=(0, 5))
        
        self.info_labels[key] = value_label
        
    def toggle_scraper(self):
        """Start or stop the scraper"""
        if not self.is_running:
            self.start_scraper()
        else:
            self.stop_scraper()
            
    def start_scraper(self):
        """Start the scraper in a separate thread"""
        self.is_running = True
        self.force_stop_requested = False  # Reset force stop flag
        self.start_time = time.time()
        
        # Update buttons
        self.start_button.configure(
            text="⏹ STOP SCRAPER",
            bg='#fd7e14'
        )
        self.cancel_button.configure(state='normal')
        
        # Update status
        self.status_label.configure(
            text="Starting scraper...",
            fg='#28a745'
        )
        
        # Start progress animation
        self.current_progress_bar.start(10)
        
        # Add terminal logs
        self.add_terminal_log("🚀 STARTING VIDEO SCRAPER", "SUCCESS")
        self.add_terminal_log("⚙️ Initializing scraper components...", "INFO")
        
        # Start scraper thread
        self.scraper_thread = threading.Thread(target=self.run_scraper, daemon=True)
        self.scraper_thread.start()
        
    def stop_scraper(self):
        """Gracefully stop the scraper"""
        self.add_terminal_log("⏹ STOPPING SCRAPER (Graceful shutdown...)", "WARNING")
        self.is_running = False
        
        # Signal the scraper to stop
        if self.scraper:
            self.scraper.force_stop_requested = True
        
        # Update status
        self.status_label.configure(
            text="Stopping...",
            fg='#ffc107'
        )
        
    def force_stop(self):
        """ENHANCED: Force stop the scraper with multiple escalation levels"""
        self.add_terminal_log("🚨 FORCE STOP INITIATED - STOPPING ALL OPERATIONS", "ERROR")
        
        # Level 1: Set all stop flags
        self.is_running = False
        self.force_stop_requested = True
        
        if self.scraper:
            self.scraper.force_stop_requested = True
            self.add_terminal_log("🔴 Level 1: Setting scraper stop flags...", "ERROR")
        
        # Level 2: Try to interrupt the scraper thread
        if self.scraper_thread and self.scraper_thread.is_alive():
            self.add_terminal_log("🔴 Level 2: Attempting to interrupt scraper thread...", "ERROR")
            
            # Give it 2 seconds to stop gracefully
            self.scraper_thread.join(timeout=2.0)
            
            if self.scraper_thread.is_alive():
                self.add_terminal_log("🔴 Level 3: Thread still alive, forcing termination...", "ERROR")
                
                # Level 3: Force close WebDriver and processes
                try:
                    if self.scraper and hasattr(self.scraper, 'web_driver_manager'):
                        self.add_terminal_log("🔴 Terminating WebDriver processes...", "ERROR")
                        self.scraper.web_driver_manager.close_driver()
                        
                    if self.scraper and hasattr(self.scraper, 'file_downloader'):
                        self.add_terminal_log("🔴 Stopping download processes...", "ERROR")
                        # Add any download stop logic here
                        
                except Exception as e:
                    self.add_terminal_log(f"⚠️ Error during process termination: {e}", "ERROR")
                
                # Level 4: Last resort - system exit after delay
                self.add_terminal_log("🔴 Level 4: EMERGENCY SHUTDOWN in 3 seconds...", "ERROR")
                self.add_terminal_log("🔴 WARNING: Application will force-close!", "ERROR")
                
                # Give user a moment to see the message
                self.root.after(3000, self.emergency_shutdown)
        
        # Update GUI immediately
        self.scraper_finished()
        
    def emergency_shutdown(self):
        """Last resort emergency shutdown"""
        self.add_terminal_log("💥 EMERGENCY SHUTDOWN - FORCE CLOSING APPLICATION", "ERROR")
        
        # Force close everything
        try:
            # Kill any remaining processes
            os._exit(1)  # Nuclear option - force exit the entire Python process
        except:
            sys.exit(1)  # Fallback
        
    def clear_terminal(self):
        """Clear terminal output"""
        self.terminal_text.delete(1.0, tk.END)
        self.add_terminal_log("🗑️ Terminal cleared", "INFO")
        
    def run_scraper(self):
        """ENHANCED: Run the scraper with stop checking"""
        try:
            # Create scraper instance
            os.makedirs("C:\\scraper_downloads", exist_ok=True)
            self.scraper = VideoScraper()
            
            # CRITICAL: Pass the force stop flag to scraper
            self.scraper.force_stop_requested = False
            self.scraper.gui_force_stop_check = lambda: self.force_stop_requested or not self.is_running
            
            # Add GUI log handler to scraper's logger
            self.scraper.logger.addHandler(self.gui_log_handler)
            self.scraper.logger.setLevel(logging.INFO)
            
            self.add_terminal_log("✅ Scraper initialized successfully", "SUCCESS")
            self.add_terminal_log("🔄 Beginning scrape process...", "INFO")
            
            # Run the scraper
            self.scraper.run()
            
        except Exception as e:
            self.add_terminal_log(f"❌ SCRAPER ERROR: {str(e)}", "ERROR")
        finally:
            # Reset when done
            self.is_running = False
            self.force_stop_requested = False
            self.root.after(0, self.scraper_finished)
            
    def scraper_finished(self):
        """Called when scraper finishes"""
        self.start_button.configure(
            text="▶ START SCRAPER",
            bg='#28a745'
        )
        
        self.cancel_button.configure(state='disabled')
        
        self.status_label.configure(
            text="Finished",
            fg='#28a745'
        )
        
        self.current_file_label.configure(text="None")
        self.current_progress_bar.stop()
        
        if self.force_stop_requested:
            self.add_terminal_log("🛑 SCRAPER FORCE STOPPED", "ERROR")
        else:
            self.add_terminal_log("✅ SCRAPER FINISHED", "SUCCESS")
        
    def add_terminal_log(self, message, level="INFO"):
        """Add message to terminal with timestamp and color"""
        timestamp = time.strftime("%H:%M:%S")
        
        # Level indicators
        level_indicators = {
            "INFO": "ℹ️",
            "WARNING": "⚠️", 
            "ERROR": "❌",
            "SUCCESS": "✅",
            "DOWNLOAD": "📥"
        }
        
        indicator = level_indicators.get(level, "ℹ️")
        full_message = f"[{timestamp}] {indicator} {message}\\n"
        
        # Insert with color
        self.terminal_text.insert(tk.END, full_message, level)
        self.terminal_text.see(tk.END)  # Auto-scroll to bottom
        
    def process_log_queue(self):
        """Process logging queue and display in terminal"""
        try:
            while True:
                log_entry = self.log_queue.get_nowait()
                
                # Parse log entry for different types
                if "ERROR" in log_entry:
                    level = "ERROR"
                elif "WARNING" in log_entry:
                    level = "WARNING"
                elif "✓" in log_entry or "processed successfully" in log_entry:
                    level = "SUCCESS"
                elif "downloading" in log_entry.lower() or "download" in log_entry.lower():
                    level = "DOWNLOAD"
                    # Extract filename if possible
                    if "Video_" in log_entry:
                        try:
                            parts = log_entry.split("Video_")
                            if len(parts) > 1:
                                video_id = parts[1].split()[0]
                                self.current_file_label.configure(text=f"Video_{video_id}")
                        except:
                            pass
                else:
                    level = "INFO"
                    
                # Clean up the log entry
                clean_entry = log_entry.replace("MainThread", "MAIN")
                clean_entry = clean_entry.replace("VideoWorker-", "WORKER-")
                
                self.terminal_text.insert(tk.END, clean_entry + "\\n", level)
                self.terminal_text.see(tk.END)
                
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self.process_log_queue)
        
    def update_display(self):
        """Update the display with current information"""
        try:
            # Update info from progress.json
            progress_file = Path("progress.json")
            if progress_file.exists():
                with open(progress_file, 'r') as f:
                    progress_data = json.load(f)
                
                # Update videos downloaded
                total_downloaded = progress_data.get("total_downloaded", 0)
                self.info_labels["videos_downloaded"].configure(text=str(total_downloaded))
                
                # Update current page
                last_page = progress_data.get("last_page", 0)
                self.info_labels["current_page"].configure(text=str(last_page))
                
                # Update storage
                total_size_mb = progress_data.get("total_size_mb", 0)
                storage_gb = total_size_mb / 1024
                self.info_labels["storage_used"].configure(text=f"{storage_gb:.1f} GB")
                
                # Update progress bar and percentage
                max_storage = 100  # GB
                progress_percent = min((storage_gb / max_storage) * 100, 100)
                self.storage_progress_bar['value'] = progress_percent
                self.storage_percent_label.configure(text=f"{progress_percent:.1f}%")
                
                # Change color based on usage
                if progress_percent > 90:
                    self.storage_percent_label.configure(fg='#dc3545')  # Red
                elif progress_percent > 70:
                    self.storage_percent_label.configure(fg='#ffc107')  # Yellow
                else:
                    self.storage_percent_label.configure(fg='#28a745')  # Green
            
            # Update time running
            if self.start_time and self.is_running:
                elapsed = time.time() - self.start_time
                hours = int(elapsed // 3600)
                minutes = int((elapsed % 3600) // 60)
                time_str = f"{hours:02d}:{minutes:02d}"
                self.info_labels["time_running"].configure(text=time_str)
            elif not self.is_running:
                self.info_labels["time_running"].configure(text="00:00")
                
        except Exception as e:
            pass  # Ignore errors in display update
            
        # Schedule next update
        self.root.after(2000, self.update_display)  # Update every 2 seconds
        
    def on_closing(self):
        """Handle window close event"""
        if self.is_running:
            self.add_terminal_log("🔴 Application closing - stopping scraper...", "WARNING")
            self.force_stop()  # Use force stop when closing
            time.sleep(2)  # Give time for cleanup
        
        self.root.destroy()

def main():
    """Main function to run the GUI"""
    root = tk.Tk()
    app = ScraperGUI(root)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("GUI closed by user")

if __name__ == "__main__":
    main()