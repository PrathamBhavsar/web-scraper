
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import threading
import json
import time
import os
from pathlib import Path
import queue
import sys

class WebParserGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Web Parser GUI")
        self.root.geometry("900x700")
        self.root.resizable(True, True)

        # Initialize variables
        self.process = None
        self.is_running = False
        self.config = {}
        self.progress = {}

        # Queue for thread-safe GUI updates
        self.log_queue = queue.Queue()

        # Load initial data
        self.load_config()
        self.load_progress()

        # Create GUI elements
        self.create_widgets()

        # Start update timer
        self.update_stats()
        self.process_log_queue()

    def load_config(self):
        """Load configuration from config.json"""
        try:
            if os.path.exists("config.json"):
                with open("config.json", 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
            else:
                self.config = {"general": {"max_storage_gb": 100}}
                messagebox.showwarning("Config Warning", "config.json not found. Using default values.")
        except Exception as e:
            messagebox.showerror("Config Error", f"Error loading config.json: {e}")
            self.config = {"general": {"max_storage_gb": 100}}

    def load_progress(self):
        """Load progress from progress.json"""
        try:
            if os.path.exists("progress.json"):
                with open("progress.json", 'r', encoding='utf-8') as f:
                    self.progress = json.load(f)
            else:
                self.progress = {
                    "total_downloaded": 0,
                    "total_size_mb": 0,
                    "downloaded_videos": [],
                    "failed_videos": [],
                    "last_page": 0
                }
        except Exception as e:
            print(f"Error loading progress.json: {e}")
            self.progress = {
                "total_downloaded": 0,
                "total_size_mb": 0,
                "downloaded_videos": [],
                "failed_videos": [],
                "last_page": 0
            }

    def create_widgets(self):
        """Create and layout GUI widgets"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(2, weight=1)

        # Title
        title_label = ttk.Label(main_frame, text="Web Parser Controller", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 20))

        # Stats frame
        stats_frame = ttk.LabelFrame(main_frame, text="Statistics", padding="10")
        stats_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        stats_frame.columnconfigure(1, weight=1)

        # Downloaded files count
        ttk.Label(stats_frame, text="Files Downloaded:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.files_label = ttk.Label(stats_frame, text="0", font=('Arial', 10, 'bold'))
        self.files_label.grid(row=0, column=1, sticky=tk.W)

        # Storage usage
        ttk.Label(stats_frame, text="Storage Usage:").grid(row=1, column=0, sticky=tk.W, padx=(0, 10), pady=(5, 0))
        self.storage_label = ttk.Label(stats_frame, text="0.0 MB / 100.0 GB (0.0%)", font=('Arial', 10, 'bold'))
        self.storage_label.grid(row=1, column=1, sticky=tk.W, pady=(5, 0))

        # Progress bar
        ttk.Label(stats_frame, text="Progress:").grid(row=2, column=0, sticky=tk.W, padx=(0, 10), pady=(5, 0))
        self.progress_bar = ttk.Progressbar(stats_frame, mode='determinate')
        self.progress_bar.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=(5, 0))

        # Last page
        ttk.Label(stats_frame, text="Last Page:").grid(row=3, column=0, sticky=tk.W, padx=(0, 10), pady=(5, 0))
        self.page_label = ttk.Label(stats_frame, text="0", font=('Arial', 10, 'bold'))
        self.page_label.grid(row=3, column=1, sticky=tk.W, pady=(5, 0))

        # Control frame
        control_frame = ttk.Frame(main_frame)
        control_frame.grid(row=3, column=0, columnspan=2, pady=(10, 0))

        # Start/Stop buttons
        self.start_button = ttk.Button(control_frame, text="▶ Start Scraper", 
                                      command=self.start_scraper, style='Success.TButton')
        self.start_button.pack(side=tk.LEFT, padx=(0, 10))

        self.stop_button = ttk.Button(control_frame, text="⏹ Stop Scraper", 
                                     command=self.stop_scraper, state='disabled')
        self.stop_button.pack(side=tk.LEFT, padx=(0, 10))

        # Clear logs button
        self.clear_button = ttk.Button(control_frame, text="🗑 Clear Logs", 
                                      command=self.clear_logs)
        self.clear_button.pack(side=tk.LEFT, padx=(0, 10))

        # Status label
        self.status_label = ttk.Label(control_frame, text="Ready to start", 
                                     font=('Arial', 10, 'italic'))
        self.status_label.pack(side=tk.LEFT, padx=(20, 0))

        # Log frame
        log_frame = ttk.LabelFrame(main_frame, text="Scraper Output", padding="5")
        log_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        # Log text area with scrollbar
        self.log_text = scrolledtext.ScrolledText(log_frame, height=20, width=100,
                                                 font=('Consolas', 9), wrap=tk.WORD)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure text tags for colored output
        self.log_text.tag_configure("error", foreground="red")
        self.log_text.tag_configure("warning", foreground="orange")
        self.log_text.tag_configure("success", foreground="green")
        self.log_text.tag_configure("info", foreground="blue")

        # Add welcome message
        self.add_log_message("=== Web Parser GUI Started ===\n", "info")
        self.add_log_message(f"Config loaded: max_storage = {self.config.get('general', {}).get('max_storage_gb', 100)} GB\n")
        self.add_log_message(f"Ready to start scraping...\n")

    def add_log_message(self, message, tag=None):
        """Add message to log with optional color tag"""
        self.log_text.insert(tk.END, message)
        if tag:
            start_pos = f"{self.log_text.index(tk.END)}-{len(message)}c"
            self.log_text.tag_add(tag, start_pos, tk.END)

        # Auto-scroll to bottom
        self.log_text.see(tk.END)

        # Limit log size to prevent memory issues
        lines = self.log_text.get("1.0", tk.END).split("\n")
        if len(lines) > 1000:
            self.log_text.delete("1.0", f"{len(lines)-800}.0")

    def start_scraper(self):
        """Start the scraper process"""
        if self.is_running:
            return

        try:
            # Check if main_scraper.py exists
            if not os.path.exists("main_scraper.py"):
                messagebox.showerror("Error", "main_scraper.py not found!\n\nMake sure the script is in the same directory as this GUI.")
                return

            self.is_running = True
            self.start_button.config(state='disabled')
            self.stop_button.config(state='normal')
            self.status_label.config(text="Starting...")

            self.add_log_message("\n=== Starting Web Scraper ===\n", "info")
            self.add_log_message(f"Command: python main_scraper.py\n")

            # Start scraper process
            self.process = subprocess.Popen(
                [sys.executable, "main_scraper.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )

            # Start thread to read output
            self.output_thread = threading.Thread(target=self.read_output, daemon=True)
            self.output_thread.start()

            self.status_label.config(text="Running...")

        except Exception as e:
            self.add_log_message(f"Error starting scraper: {e}\n", "error")
            self.is_running = False
            self.start_button.config(state='normal')
            self.stop_button.config(state='disabled')
            self.status_label.config(text="Error")

    def stop_scraper(self):
        """Stop the scraper process"""
        if not self.is_running:
            return

        try:
            self.add_log_message("\n=== Stopping Web Scraper ===\n", "warning")

            if self.process and self.process.poll() is None:
                # Try graceful termination first
                self.process.terminate()

                # Wait a moment for graceful shutdown
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if needed
                    self.process.kill()
                    self.add_log_message("Force stopped scraper process.\n", "warning")

            self.is_running = False
            self.start_button.config(state='normal')
            self.stop_button.config(state='disabled')
            self.status_label.config(text="Stopped")

            self.add_log_message("Scraper stopped.\n", "info")

        except Exception as e:
            self.add_log_message(f"Error stopping scraper: {e}\n", "error")

    def read_output(self):
        """Read output from scraper process"""
        try:
            while self.process and self.process.poll() is None:
                line = self.process.stdout.readline()
                if line:
                    self.log_queue.put(('output', line.strip()))
                time.sleep(0.1)

            # Process finished
            if self.process:
                return_code = self.process.poll()
                self.log_queue.put(('finished', return_code))

        except Exception as e:
            self.log_queue.put(('error', str(e)))

    def process_log_queue(self):
        """Process messages from the log queue"""
        try:
            while True:
                msg_type, msg = self.log_queue.get_nowait()

                if msg_type == 'output':
                    # Determine message type for coloring
                    tag = None
                    if 'ERROR' in msg.upper() or 'FAILED' in msg.upper():
                        tag = 'error'
                    elif 'WARNING' in msg.upper() or 'WARN' in msg.upper():
                        tag = 'warning'
                    elif 'SUCCESS' in msg.upper() or 'COMPLETE' in msg.upper():
                        tag = 'success'
                    elif 'INFO' in msg.upper():
                        tag = 'info'

                    self.add_log_message(f"{msg}\n", tag)

                elif msg_type == 'finished':
                    self.add_log_message(f"\n=== Scraper Finished (exit code: {msg}) ===\n", "info")
                    self.is_running = False
                    self.start_button.config(state='normal')
                    self.stop_button.config(state='disabled')
                    self.status_label.config(text="Finished")

                elif msg_type == 'error':
                    self.add_log_message(f"Output reading error: {msg}\n", "error")

        except queue.Empty:
            pass

        # Schedule next check
        self.root.after(100, self.process_log_queue)

    def clear_logs(self):
        """Clear the log text area"""
        self.log_text.delete(1.0, tk.END)
        self.add_log_message("=== Logs Cleared ===\n", "info")

    def update_stats(self):
        """Update statistics from progress.json"""
        try:
            # Reload progress data
            self.load_progress()

            # Update file count
            total_files = self.progress.get("total_downloaded", 0)
            self.files_label.config(text=str(total_files))

            # Update storage usage
            total_mb = self.progress.get("total_size_mb", 0)
            max_gb = self.config.get("general", {}).get("max_storage_gb", 100)
            max_mb = max_gb * 1024  # Convert GB to MB

            if max_mb > 0:
                percentage = (total_mb / max_mb) * 100
                self.progress_bar['value'] = min(percentage, 100)
            else:
                percentage = 0
                self.progress_bar['value'] = 0

            # Update storage label
            storage_text = f"{total_mb:.1f} MB / {max_gb:.1f} GB ({percentage:.1f}%)"
            self.storage_label.config(text=storage_text)

            # Update last page
            last_page = self.progress.get("last_page", 0)
            self.page_label.config(text=str(last_page))

            # Color code progress bar based on usage
            if percentage > 90:
                self.progress_bar.config(style='Red.Horizontal.TProgressbar')
            elif percentage > 75:
                self.progress_bar.config(style='Yellow.Horizontal.TProgressbar')
            else:
                self.progress_bar.config(style='Green.Horizontal.TProgressbar')

        except Exception as e:
            print(f"Error updating stats: {e}")

        # Schedule next update
        self.root.after(2000, self.update_stats)  # Update every 2 seconds

    def on_closing(self):
        """Handle window closing"""
        if self.is_running:
            if messagebox.askokcancel("Quit", "Scraper is still running. Stop and quit?"):
                self.stop_scraper()
                self.root.after(1000, self.root.destroy)  # Give time to stop
        else:
            self.root.destroy()

def main():
    root = tk.Tk()
    app = WebParserGUI(root)

    # Handle window close
    root.protocol("WM_DELETE_WINDOW", app.on_closing)

    # Configure styles
    style = ttk.Style()

    # Configure colored progress bar styles if available
    try:
        style.configure('Green.Horizontal.TProgressbar', background='green')
        style.configure('Yellow.Horizontal.TProgressbar', background='orange')  
        style.configure('Red.Horizontal.TProgressbar', background='red')
    except:
        pass

    try:
        # Set icon and theme
        root.iconbitmap(default="")  # Remove if no icon
        style.theme_use('clam')  # Modern theme
    except:
        pass

    root.mainloop()

if __name__ == "__main__":
    main()
