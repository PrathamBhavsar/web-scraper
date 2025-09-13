# idm_downloader.py

import subprocess
import os
import time
import logging
from pathlib import Path
from typing import Optional
import winreg

class IDMDownloader:
    """IDM (Internet Download Manager) integration for Windows - FIXED VERSION"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('Rule34Scraper')
        self.idm_path = self.config.get("download", {}).get("idm_path",
                                                           "C:\\Program Files (x86)\\Internet Download Manager\\idman.exe")
        
        # Configure IDM registry for auto-start on initialization
        configured = self.configure_idm_auto_start()
        if configured:
            self.logger.info("IDM auto-start registry settings configured successfully.")
        else:
            self.logger.warning("Failed to configure IDM auto-start registry settings.")

    def is_idm_available(self) -> bool:
        """Check if IDM is installed and accessible"""
        try:
            return os.path.isfile(self.idm_path) and os.access(self.idm_path, os.X_OK)
        except Exception as e:
            self.logger.error(f"Error checking IDM availability: {e}")
            return False

    def add_to_queue_and_start(self, url: str, filepath: Path) -> bool:
        """
        FIXED: Add file to IDM queue AND start download immediately
        """
        try:
            if not self.is_idm_available():
                self.logger.error("IDM is not available or not installed")
                return False

            filepath = Path(filepath)
            download_dir = str(filepath.parent)
            filename = filepath.name

            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Add user agent
            user_agent = self.config.get("general", {}).get("user_agent")

            # FIXED: Use /d flag to download immediately (not just add to queue)
            command = [
                self.idm_path,
                '/d', url,  # Download immediately (not /a for queue only)
                '/p', download_dir,
                '/f', filename,
                '/s'  # Start download silently
            ]

            if user_agent:
                command.extend(['/h', f'User-Agent: {user_agent}'])

            self.logger.info(f"Adding to IDM and starting download: {filename}")
            self.logger.info(f"IDM Command: {' '.join(command)}")

            result = subprocess.run(command, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                self.logger.info(f"IDM download started successfully: {filename}")
                return True
            else:
                self.logger.error(f"Failed to start IDM download: {filename}")
                self.logger.error(f"IDM Error: {result.stderr}")
                self.logger.error(f"IDM Output: {result.stdout}")
                return False

        except Exception as e:
            self.logger.error(f"Exception starting IDM download: {e}")
            return False

    def add_to_queue(self, url: str, filepath: Path) -> bool:
        """Add file to IDM queue without starting download immediately"""
        try:
            if not self.is_idm_available():
                self.logger.error("IDM is not available or not installed")
                return False

            filepath = Path(filepath)
            download_dir = str(filepath.parent)
            filename = filepath.name

            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Add user agent
            user_agent = self.config.get("general", {}).get("user_agent")

            command = [
                self.idm_path,
                '/a', url,  # Add file to queue
                '/p', download_dir,
                '/f', filename,
                '/n'  # No start
            ]

            if user_agent:
                command.extend(['/h', f'User-Agent: {user_agent}'])

            self.logger.info(f"Adding to IDM queue: {filename}")

            result = subprocess.run(command, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                self.logger.info(f"Added to IDM queue: {filename}")
                return True
            else:
                self.logger.error(f"Failed to add to IDM queue: {filename}, Error: {result.stderr}")
                return False

        except Exception as e:
            self.logger.error(f"Exception adding to IDM queue: {e}")
            return False

    def download_single_file(self, video_url: str, video_path: Path) -> bool:
        """
        FIXED: Download single file via IDM - starts immediately and waits for completion
        """
        try:
            if not self.is_idm_available():
                self.logger.error("IDM is not available or not installed")
                return False

            self.logger.info(f"Starting IDM download for: {video_path.name}")

            # Method 1: Try direct download command first
            if self.add_to_queue_and_start(video_url, video_path):
                self.logger.info("IDM download started, waiting for completion...")
                
                if self._wait_for_idm_completion(video_path):
                    self.logger.info(f"IDM download completed successfully: {video_path.name}")
                    return True
                else:
                    self.logger.warning(f"IDM download failed or timed out: {video_path.name}")

            # Method 2: Fallback - Add to queue then start queue
            self.logger.info("Trying fallback method: Add to queue then start")
            
            if self.add_to_queue(video_url, video_path):
                self.logger.info("Added to queue, now starting queue...")
                
                if self.start_idm_queue():
                    self.logger.info("Queue started, waiting for completion...")
                    
                    if self._wait_for_idm_completion(video_path):
                        self.logger.info(f"IDM download completed successfully: {video_path.name}")
                        return True

            self.logger.error(f"All IDM download methods failed for: {video_path.name}")
            return False

        except Exception as e:
            self.logger.error(f"Error in download_single_file: {e}")
            return False

    def start_idm_queue(self) -> bool:
        """
        FIXED: Start the IDM queue with multiple methods
        """
        try:
            self.logger.info("Starting IDM queue...")

            # Method 1: Start queue command
            start_commands = [
                [self.idm_path, '/s'],  # Start downloads
                [self.idm_path, '/startqueue'],  # Start queue
                [self.idm_path, '/q'],  # Queue start alternative
            ]

            for i, command in enumerate(start_commands, 1):
                try:
                    self.logger.info(f"Trying start method {i}: {' '.join(command)}")
                    result = subprocess.run(command, capture_output=True, text=True, timeout=15)
                    
                    if result.returncode == 0:
                        self.logger.info(f"IDM queue started successfully with method {i}")
                        return True
                    else:
                        self.logger.warning(f"Method {i} failed: {result.stderr}")
                        
                except Exception as e:
                    self.logger.warning(f"Method {i} exception: {e}")
                    continue

            # Method 2: Try to open IDM main window and start
            try:
                self.logger.info("Trying to open IDM main window...")
                open_command = [self.idm_path]
                subprocess.Popen(open_command)
                time.sleep(2)
                
                # Send start command after opening
                start_result = subprocess.run([self.idm_path, '/s'], capture_output=True, text=True, timeout=10)
                if start_result.returncode == 0:
                    self.logger.info("IDM started via main window method")
                    return True
                    
            except Exception as e:
                self.logger.warning(f"Main window method failed: {e}")

            self.logger.error("All queue start methods failed")
            return False

        except Exception as e:
            self.logger.error(f"Error starting IDM queue: {e}")
            return False

    def configure_idm_auto_start(self):
        """Configure IDM registry settings for auto-start"""
        try:
            key_path = r"SOFTWARE\DownloadManager"
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as key:
                # Configure IDM to start downloads automatically
                winreg.SetValueEx(key, "StartDlgShow", 0, winreg.REG_DWORD, 0)  # Don't show start dialog
                winreg.SetValueEx(key, "StartDownload", 0, winreg.REG_DWORD, 1)  # Start downloads automatically
                winreg.SetValueEx(key, "AutoStartQueue", 0, winreg.REG_DWORD, 1)  # Auto start queue
                winreg.SetValueEx(key, "ShowAddDialog", 0, winreg.REG_DWORD, 0)  # Don't show add dialog
                
            self.logger.info("IDM configured for automatic downloads")
            return True
            
        except Exception as e:
            self.logger.warning(f"Could not configure IDM registry: {e}")
            return False

    def _wait_for_idm_completion(self, filepath: Path, max_wait_time: int = 300) -> bool:
        """
        IMPROVED: Wait for IDM to complete download with better detection
        """
        start_time = time.time()
        last_size = 0
        stable_count = 0
        
        # Possible temp file patterns IDM uses
        temp_patterns = [
            filepath.with_suffix(filepath.suffix + ".!ut"),  # Standard IDM temp
            filepath.with_suffix(filepath.suffix + ".tmp"),  # Alternative temp
            filepath.with_suffix(".!ut"),  # Extension replacement
            filepath.parent / (filepath.name + ".!ut"),  # Name append
        ]

        self.logger.info(f"Waiting for IDM to complete download: {filepath.name}")
        
        # Check if file already exists (edge case)
        if filepath.exists() and filepath.stat().st_size > 1024:
            self.logger.info(f"File already exists: {filepath.name}")
            return True

        while time.time() - start_time < max_wait_time:
            try:
                # Check if final file exists and no temp files
                if filepath.exists():
                    file_size = filepath.stat().st_size
                    
                    # Check if any temp files still exist
                    temp_exists = any(temp_file.exists() for temp_file in temp_patterns)
                    
                    if not temp_exists and file_size > 0:
                        self.logger.info(f"IDM download completed: {filepath.name} ({file_size/1024/1024:.2f} MB)")
                        return True
                
                # Check temp file progress
                active_temp = None
                for temp_file in temp_patterns:
                    if temp_file.exists():
                        active_temp = temp_file
                        break
                
                if active_temp:
                    current_size = active_temp.stat().st_size
                    
                    if current_size == last_size:
                        stable_count += 1
                        if stable_count > 12:  # 2 minutes of no progress
                            self.logger.warning(f"IDM download stalled for {filepath.name}")
                            return False
                    else:
                        stable_count = 0
                        
                    last_size = current_size
                    progress_mb = current_size / (1024 * 1024)
                    self.logger.info(f"IDM downloading: {filepath.name} ({progress_mb:.2f} MB)")
                
                time.sleep(10)  # Check every 10 seconds

            except Exception as e:
                self.logger.debug(f"Error monitoring IDM download: {e}")
                time.sleep(10)

        self.logger.error(f"IDM download timed out after {max_wait_time} seconds for {filepath.name}")
        return False

    def get_idm_version(self) -> Optional[str]:
        try:
            if not self.is_idm_available():
                return None

            result = subprocess.run(
                [self.idm_path, '/?'],
                capture_output=True,
                text=True,
                timeout=10
            )

            for line in result.stdout.split('\n'):
                if 'Internet Download Manager' in line:
                    return line.strip()

            return "IDM Available"

        except Exception:
            return None
