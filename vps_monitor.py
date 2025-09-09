# vps_monitor.py - Monitor scraper progress without GUI (FIXED PATHS)

import json
import time
import os
import logging
from pathlib import Path
from datetime import datetime
import subprocess
import sys

class VPSScraperMonitor:
    def __init__(self):
        # Get the directory where this script is located
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Use absolute paths for all files
        self.progress_file = os.path.join(self.script_dir, "progress.json")
        self.log_file = os.path.join(self.script_dir, "scraper.log")
        self.service_log_file = os.path.join(self.script_dir, "scraper_service.log")
        self.download_path = Path("C:/scraper_downloads")
        self.last_check_time = None
        
    def clear_screen(self):
        """Clear the console screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
    def get_progress_info(self):
        """Get current progress information"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error reading progress: {e}")
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
            # Check for python processes
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
            
    def get_latest_log_entries(self, log_file_path, num_lines=8):
        """Get latest log entries from specified file"""
        try:
            if os.path.exists(log_file_path):
                with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    return lines[-num_lines:] if lines else []
        except Exception as e:
            return [f"Error reading log: {e}"]
        return []
        
    def display_status(self):
        """Display current status"""
        self.clear_screen()
        
        print("=" * 80)
        print("🚀 VIDEO SCRAPER - VPS MONITOR (FIXED VERSION)")
        print("=" * 80)
        print(f"⏰ Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💻 VPS Mode: Background monitoring (disconnection safe)")
        print(f"📁 Working Directory: {self.script_dir}")
        print("=" * 80)
        
        # Service Status
        service_running = self.is_service_running()
        service_status_color = "🟢" if service_running else "🔴"
        service_status_text = "RUNNING" if service_running else "STOPPED"
        print(f"{service_status_color} Windows Service: {service_status_text}")
        
        # Process Status
        process_running = self.is_scraper_running()
        process_status_color = "🟢" if process_running else "🔴"
        process_status_text = "RUNNING" if process_running else "STOPPED"
        print(f"{process_status_color} Python Process: {process_status_text}")
        
        # Progress Information
        progress = self.get_progress_info()
        if progress:
            print(f"📊 Videos Downloaded: {progress.get('total_downloaded', 0)}")
            print(f"📄 Current Page: {progress.get('last_page', 'Unknown')}")
            
            total_size_mb = progress.get('total_size_mb', 0)
            total_size_gb = total_size_mb / 1024 if total_size_mb else 0
            print(f"💾 Downloaded Size: {total_size_gb:.2f} GB ({total_size_mb:.1f} MB)")
            
            # Storage usage
            folder_size = self.get_folder_size_gb(self.download_path)
            print(f"📁 Folder Size: {folder_size:.2f} GB")
            
            # Downloaded videos list (last 5)
            downloaded = progress.get('downloaded_videos', [])
            if downloaded:
                print(f"📥 Latest Downloads: {', '.join(downloaded[-5:])}")
        else:
            print("📊 No progress data available")
            
        print("=" * 80)
        
        # File Status
        print("📄 FILE STATUS:")
        files_to_check = [
            ("progress.json", self.progress_file),
            ("scraper.log", self.log_file),
            ("service.log", self.service_log_file)
        ]
        
        for name, path in files_to_check:
            if os.path.exists(path):
                size = os.path.getsize(path)
                print(f"✅ {name}: {size} bytes")
            else:
                print(f"❌ {name}: Not found")
        
        print("-" * 40)
        
        # Recent Log Entries - Try service log first, then regular log
        print("📟 RECENT LOG ENTRIES:")
        
        # Try service log first
        service_log_entries = self.get_latest_log_entries(self.service_log_file, 4)
        if service_log_entries and any(line.strip() for line in service_log_entries):
            print("🔧 SERVICE LOG:")
            for entry in service_log_entries:
                entry = entry.strip()
                if entry:
                    if "ERROR" in entry:
                        print(f"❌ {entry}")
                    elif "WARNING" in entry:
                        print(f"⚠️  {entry}")
                    elif "SUCCESS" in entry or "successfully" in entry:
                        print(f"✅ {entry}")
                    else:
                        print(f"ℹ️  {entry}")
        
        # Then regular scraper log
        regular_log_entries = self.get_latest_log_entries(self.log_file, 4)
        if regular_log_entries and any(line.strip() for line in regular_log_entries):
            print("📊 SCRAPER LOG:")
            for entry in regular_log_entries:
                entry = entry.strip()
                if entry:
                    if "ERROR" in entry:
                        print(f"❌ {entry}")
                    elif "WARNING" in entry:
                        print(f"⚠️  {entry}")
                    elif "SUCCESS" in entry or "completed successfully" in entry:
                        print(f"✅ {entry}")
                    else:
                        print(f"ℹ️  {entry}")
        
        if not service_log_entries and not regular_log_entries:
            print("No recent log entries found")
            
        print("=" * 80)
        print("🔧 CONTROLS:")
        print("   Press Ctrl+C to exit monitor")
        print("   Service control: service_control.bat")
        print("   To stop: python scraper_service.py stop")
        print("   To start: python scraper_service.py start")
        print("=" * 80)
        
    def run(self):
        """Run the monitor"""
        print("Starting VPS Scraper Monitor (FIXED VERSION)...")
        print("This monitor is safe for VPS disconnection!")
        print(f"Working directory: {self.script_dir}")
        time.sleep(2)
        
        try:
            while True:
                self.display_status()
                time.sleep(10)  # Update every 10 seconds
                
        except KeyboardInterrupt:
            print("\\n\\n👋 VPS Monitor stopped by user")
            print("Note: Scraper service may still be running in background")
            
        except Exception as e:
            print(f"\\n❌ Monitor error: {e}")
            print("Restarting monitor in 5 seconds...")
            time.sleep(5)
            self.run()  # Restart on error

if __name__ == "__main__":
    monitor = VPSScraperMonitor()
    monitor.run()