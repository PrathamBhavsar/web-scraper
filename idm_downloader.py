# idm_downloader.py
import subprocess
import os
import time
import logging
from pathlib import Path
from typing import Optional
import winreg

class IDMDownloader:
    """IDM (Internet Download Manager) integration for Windows"""

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
                '/a', url,     # Add file to queue
                '/p', download_dir,
                '/f', filename,
                '/n'           # No start
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
        Download single file via IDM queue - FIXED to not wait for completion
        """
        try:
            if not self.is_idm_available():
                self.logger.error("IDM is not available or not installed")
                return False

            # Add file to queue
            self.logger.info("Adding file to IDM queue")
            if not self.add_to_queue(video_url, video_path):
                self.logger.error(f"Failed to add {video_path.name} to IDM queue")
                return False

            # Start the queue automatically
            self.logger.info("Starting IDM queue automatically...")
            self.start_idm_queue()

            # Wait for THIS specific file to complete, then return
            self.logger.info(f"Waiting for IDM to download: {video_path.name}")
            if self._wait_for_idm_completion(video_path):
                self.logger.info(f"IDM download completed successfully: {video_path.name}")
                return True
            else:
                self.logger.error(f"IDM download failed for: {video_path.name}")
                return False

        except Exception as e:
            self.logger.error(f"Error in download_single_file: {e}")
            return False

    def start_idm_queue(self):
        """Start the IDM main synchronization queue automatically"""
        try:
            # Use /startqueue command to start the queue
            start_command = [self.idm_path, '/startqueue']
            result = subprocess.run(start_command, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.logger.info("IDM main synchronization queue started automatically")
            else:
                self.logger.warning(f"Failed to start IDM queue: {result.stderr}")
                
                # Try alternative command
                alt_command = [self.idm_path, '/s']
                alt_result = subprocess.run(alt_command, capture_output=True, text=True, timeout=10)
                if alt_result.returncode == 0:
                    self.logger.info("IDM queue started using alternative command")
                else:
                    self.logger.error("Failed to start IDM queue with both methods")
                    
        except Exception as e:
            self.logger.warning(f"Error starting IDM queue: {e}")

    def configure_idm_auto_start(self):
        """Configure IDM registry settings for auto-start"""
        try:
            key_path = r"SOFTWARE\DownloadManager"
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE) as key:
                winreg.SetValueEx(key, "StartDlgShow", 0, winreg.REG_DWORD, 0)
                winreg.SetValueEx(key, "StartDownload", 0, winreg.REG_DWORD, 1)
                winreg.SetValueEx(key, "AutoStartQueue", 0, winreg.REG_DWORD, 1)
            self.logger.info("IDM configured for automatic downloads")
            return True
        except Exception as e:
            self.logger.warning(f"Could not configure IDM registry: {e}")
            return False

    def _wait_for_idm_completion(self, filepath: Path, max_wait_time: int = 300) -> bool:
        """
        Wait for IDM to complete download - REDUCED timeout to prevent hanging
        """
        start_time = time.time()
        last_size = 0
        stable_count = 0
        temp_file = filepath.with_suffix(filepath.suffix + ".!ut")
        
        self.logger.info(f"Waiting for IDM to complete download: {filepath.name}")
        
        # Check if file already exists (edge case)
        if filepath.exists() and filepath.stat().st_size > 1024:
            self.logger.info(f"File already exists: {filepath.name}")
            return True
        
        while time.time() - start_time < max_wait_time:
            try:
                # Check if final file exists and temp file is gone
                if filepath.exists() and not temp_file.exists():
                    file_size = filepath.stat().st_size
                    if file_size > 0:
                        self.logger.info(f"IDM download completed: {filepath.name} ({file_size/1024/1024:.2f} MB)")
                        return True
                        
                # Check if temp file exists (IDM is still downloading)
                if temp_file.exists():
                    current_size = temp_file.stat().st_size
                    if current_size == last_size:
                        stable_count += 1
                        if stable_count > 6:  # Reduced from 30 - if no progress for 1 minute, fail
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
