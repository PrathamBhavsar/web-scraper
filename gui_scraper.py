# gui_scraper.py - Simple GUI for Video Scraper - FIXED VERSION

import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import time
import json
from pathlib import Path
import sys
import os

# Import the scraper
from main_scraper import VideoScraper

class ScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Video Scraper - Easy Download Tool")
        self.root.geometry("800x600")
        self.root.configure(bg='#2b2b2b')
        
        # Initialize ALL attributes FIRST
        self.scraper = None
        self.scraper_thread = None
        self.is_running = False
        self.info_labels = {}  # INITIALIZE HERE
        self.start_time = None # INITIALIZE HERE
        
        # Create GUI elements
        self.create_widgets()
        
        # Start updating display
        self.update_display()
        
    def create_widgets(self):
        """Create all GUI widgets"""
        
        # Title
        title_frame = tk.Frame(self.root, bg='#2b2b2b')
        title_frame.pack(fill='x', padx=20, pady=20)
        
        title_label = tk.Label(
            title_frame, 
            text="🚀 Video Scraper Tool", 
            font=('Arial', 24, 'bold'),
            fg='#4CAF50',
            bg='#2b2b2b'
        )
        title_label.pack()
        
        subtitle_label = tk.Label(
            title_frame,
            text="Click START to begin downloading videos automatically",
            font=('Arial', 12),
            fg='#ffffff',
            bg='#2b2b2b'
        )
        subtitle_label.pack(pady=(5, 0))
        
        # Main control frame
        control_frame = tk.Frame(self.root, bg='#2b2b2b')
        control_frame.pack(fill='x', padx=20, pady=10)
        
        # Start/Stop button
        self.start_button = tk.Button(
            control_frame,
            text="▶ START SCRAPER",
            font=('Arial', 16, 'bold'),
            bg='#4CAF50',
            fg='white',
            width=20,
            height=2,
            command=self.toggle_scraper,
            relief='flat',
            cursor='hand2'
        )
        self.start_button.pack(pady=10)
        
        # Status frame
        status_frame = tk.Frame(self.root, bg='#2b2b2b')
        status_frame.pack(fill='x', padx=20, pady=10)
        
        # Status label
        self.status_label = tk.Label(
            status_frame,
            text="Status: Ready to start",
            font=('Arial', 14, 'bold'),
            fg='#FFC107',
            bg='#2b2b2b'
        )
        self.status_label.pack()
        
        # Info grid
        info_frame = tk.Frame(self.root, bg='#2b2b2b')
        info_frame.pack(fill='x', padx=20, pady=20)
        
        # Create info boxes
        self.create_info_box(info_frame, "📊 Videos Downloaded", "videos_downloaded", 0, 0)
        self.create_info_box(info_frame, "📄 Current Page", "current_page", 0, 1)
        self.create_info_box(info_frame, "💾 Storage Used", "storage_used", 1, 0)
        self.create_info_box(info_frame, "⏱️ Time Running", "time_running", 1, 1)
        
        # Progress bar
        progress_frame = tk.Frame(self.root, bg='#2b2b2b')
        progress_frame.pack(fill='x', padx=20, pady=20)
        
        tk.Label(
            progress_frame,
            text="Storage Usage:",
            font=('Arial', 12),
            fg='#ffffff',
            bg='#2b2b2b'
        ).pack()
        
        self.progress_bar = ttk.Progressbar(
            progress_frame,
            length=400,
            mode='determinate',
            style='Custom.Horizontal.TProgressbar'
        )
        self.progress_bar.pack(pady=10)
        
        # Configure progress bar style
        style = ttk.Style()
        style.theme_use('clam')
        style.configure(
            'Custom.Horizontal.TProgressbar',
            background='#4CAF50',
            troughcolor='#404040',
            borderwidth=1,
            lightcolor='#4CAF50',
            darkcolor='#4CAF50'
        )
        
        # Log area
        log_frame = tk.Frame(self.root, bg='#2b2b2b')
        log_frame.pack(fill='both', expand=True, padx=20, pady=(0, 20))
        
        tk.Label(
            log_frame,
            text="📝 Recent Activity:",
            font=('Arial', 12, 'bold'),
            fg='#ffffff',
            bg='#2b2b2b'
        ).pack(anchor='w')
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=8,
            bg='#1e1e1e',
            fg='#ffffff',
            font=('Consolas', 10),
            wrap=tk.WORD
        )
        self.log_text.pack(fill='both', expand=True, pady=(5, 0))
        
    def create_info_box(self, parent, title, key, row, col):
        """Create an info display box"""
        box_frame = tk.Frame(parent, bg='#404040', relief='raised', bd=1)
        box_frame.grid(row=row, column=col, padx=10, pady=10, sticky='ew')
        
        parent.grid_columnconfigure(col, weight=1)
        
        tk.Label(
            box_frame,
            text=title,
            font=('Arial', 11, 'bold'),
            fg='#ffffff',
            bg='#404040'
        ).pack(pady=(10, 5))
        
        value_label = tk.Label(
            box_frame,
            text="0",
            font=('Arial', 16, 'bold'),
            fg='#4CAF50',
            bg='#404040'
        )
        value_label.pack(pady=(0, 10))
        
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
        self.start_time = time.time()
        
        # Update button
        self.start_button.configure(
            text="⏹ STOP SCRAPER",
            bg='#f44336'
        )
        
        # Update status
        self.status_label.configure(
            text="Status: Starting scraper...",
            fg='#4CAF50'
        )
        
        # Add log message
        self.add_log("🚀 Starting video scraper...")
        self.add_log("📂 Download folder: C:/scraper_downloads/")
        
        # Start scraper thread
        self.scraper_thread = threading.Thread(target=self.run_scraper, daemon=True)
        self.scraper_thread.start()
        
    def stop_scraper(self):
        """Stop the scraper"""
        self.is_running = False
        
        # Update button
        self.start_button.configure(
            text="▶ START SCRAPER",
            bg='#4CAF50'
        )
        
        # Update status
        self.status_label.configure(
            text="Status: Stopping...",
            fg='#FFC107'
        )
        
        self.add_log("⏹ Stopping scraper...")
        
    def run_scraper(self):
        """Run the scraper (called in separate thread)"""
        try:
            # Create scraper instance
            os.makedirs("C:\\scraper_downloads", exist_ok=True)
            self.scraper = VideoScraper()
            
            # Run the scraper
            self.scraper.run()
            
        except Exception as e:
            self.add_log(f"❌ Error: {str(e)}")
        finally:
            # Reset when done
            self.is_running = False
            self.root.after(0, self.scraper_finished)
            
    def scraper_finished(self):
        """Called when scraper finishes"""
        self.start_button.configure(
            text="▶ START SCRAPER",
            bg='#4CAF50'
        )
        
        self.status_label.configure(
            text="Status: Finished",
            fg='#4CAF50'
        )
        
        self.add_log("✅ Scraper finished!")
        
    def add_log(self, message):
        """Add message to log area"""
        timestamp = time.strftime("%H:%M:%S")
        full_message = f"[{timestamp}] {message}\n"
        
        self.log_text.insert(tk.END, full_message)
        self.log_text.see(tk.END)  # Scroll to bottom
        
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
                
                # Update progress bar (assuming 100GB max)
                max_storage = 100  # GB
                progress_percent = min((storage_gb / max_storage) * 100, 100)
                self.progress_bar['value'] = progress_percent
            
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
