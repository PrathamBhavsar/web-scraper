import os
import time
import logging
import subprocess
import requests
from pathlib import Path
from urllib.parse import urlparse

class FileDownloader:
    def __init__(self, config):
        self.config = config
        self.base_url = "https://rule34video.com"
        self.logger = logging.getLogger('Rule34Scraper')

    def download_file(self, url, filepath):
        """Download file with hybrid method: IDM first, fallback to direct HTTP download"""
        download_method = self.config.get('download', {}).get('download_method', 'hybrid')
        
        if download_method == 'idm':
            return self._download_with_idm(url, filepath)
        elif download_method == 'direct':
            return self._download_direct(url, filepath)
        elif download_method == 'hybrid':
            # Try IDM first, fallback to direct
            if self._download_with_idm(url, filepath):
                return True
            else:
                self.logger.warning(f'IDM download failed. Falling back to direct download for {filepath.name}')
                return self._download_direct(url, filepath)
        else:
            self.logger.warning(f'Unknown download method {download_method}. Using direct HTTP download.')
            return self._download_direct(url, filepath)

    def _download_with_idm(self, url, filepath):
        """Download file using Internet Download Manager (IDM)"""
        idm_path = self.config.get('download', {}).get('idm_path', 'C:\\Program Files (x86)\\Internet Download Manager\\idman.exe')
        
        # Check if IDM exists
        if not os.path.exists(idm_path):
            self.logger.warning(f'IDM not found at: {idm_path}')
            return False

        self.logger.info(f'Starting IDM download: {filepath}')
        try:
            # Ensure directory exists
            os.makedirs(filepath.parent, exist_ok=True)

            # IDM command: /d URL /p PATH /f FILENAME /n /q /s
            # /d = URL to download
            # /p = path to save
            # /f = filename  
            # /n = start download immediately
            # /q = quiet mode (no dialogs)
            # /s = start IDM if not running
            cmd = [
                idm_path, 
                "/d", url, 
                "/p", str(filepath.parent), 
                "/f", filepath.name, 
                "/n", "/q", "/s"
            ]

            self.logger.debug(f'IDM command: {" ".join(cmd)}')
            
            # Run IDM command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            # Wait a moment for IDM to process
            time.sleep(3)
            
            # Check if file was downloaded successfully
            if filepath.exists() and filepath.stat().st_size > 0:
                if self.verify_download_integrity(filepath):
                    self.logger.info(f'IDM download succeeded: {filepath} ({self._format_file_size(filepath)})')
                    return True
                else:
                    self.logger.error(f'IDM download corrupted: {filepath}')
                    if filepath.exists():
                        os.remove(filepath)
                    return False
            else:
                self.logger.error(f'IDM download failed: file missing or empty')
                return False

        except subprocess.TimeoutExpired:
            self.logger.error(f'IDM download timed out for: {filepath}')
            return False
        except Exception as e:
            self.logger.error(f'Error running IDM download: {e}')
            return False

    def _download_direct(self, url, filepath):
        """Download file directly using HTTP requests with retry and validation"""
        download_config = self.config.get("download", {})
        max_retries = download_config.get("max_retries", 3)
        timeout = download_config.get("timeout_seconds", 60)
        chunk_size = download_config.get("chunk_size", 8192)

        for attempt in range(max_retries):
            try:
                self.logger.info(f'Direct download attempt {attempt + 1}/{max_retries}: {filepath}')
                
                headers = {
                    'User-Agent': self.config.get("general", {}).get("user_agent", "Mozilla/5.0"),
                    'Referer': self.base_url,
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                }

                # Start the download
                response = requests.get(url, stream=True, timeout=timeout, headers=headers)
                response.raise_for_status()

                # Get content length for progress tracking
                total_size = int(response.headers.get('content-length', 0))
                
                # Ensure directory exists
                os.makedirs(filepath.parent, exist_ok=True)

                # Download with progress tracking
                downloaded_size = 0
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Log progress every 10MB
                            if downloaded_size % (10 * 1024 * 1024) == 0 or downloaded_size == total_size:
                                if total_size > 0:
                                    progress = (downloaded_size / total_size) * 100
                                    self.logger.debug(f'Download progress: {progress:.1f}% ({self._format_bytes(downloaded_size)}/{self._format_bytes(total_size)})')

                # Verify download
                if self.verify_download_integrity(filepath):
                    self.logger.info(f"Direct download succeeded: {filepath} ({self._format_file_size(filepath)})")
                    return True
                else:
                    self.logger.warning(f"Direct download integrity check failed: {filepath}")

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"HTTP error on attempt {attempt + 1}: {e}")
            except Exception as e:
                self.logger.warning(f"Download attempt {attempt + 1} failed: {e}")
            
            # Clean up failed download
            if os.path.exists(filepath):
                os.remove(filepath)
                
            # Wait before retry
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                self.logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

        self.logger.error(f"All download attempts failed for: {filepath}")
        return False

    def download_video(self, video_url, video_file_path):
        """Specifically handle video file downloads"""
        self.logger.info(f"Starting video download: {video_file_path}")
        return self.download_file(video_url, video_file_path)

    def download_thumbnail(self, thumbnail_url, video_id, video_dir):
        """Download and validate thumbnail images"""
        if not thumbnail_url:
            self.logger.warning(f"No thumbnail URL provided for video {video_id}")
            return False

        # Get proper file extension
        ext = self.get_file_extension(thumbnail_url)
        thumbnail_path = video_dir / f"{video_id}{ext}"

        if self.download_file(thumbnail_url, thumbnail_path):
            # Validate thumbnail size
            validation_config = self.config.get("validation", {})
            min_thumb_size = validation_config.get("min_thumbnail_size_bytes", 100)
            
            if thumbnail_path.stat().st_size >= min_thumb_size:
                self.logger.info(f"Thumbnail downloaded and validated: {thumbnail_path}")
                return True
            else:
                self.logger.warning(f"Thumbnail too small ({thumbnail_path.stat().st_size} bytes), removing: {thumbnail_path}")
                thumbnail_path.unlink()
                return False
        else:
            self.logger.warning(f"Failed to download thumbnail for {video_id}")
            return False

    def get_file_extension(self, url):
        """Determine proper file extension from URL"""
        parsed_url = urlparse(url)
        url_ext = os.path.splitext(parsed_url.path)[1].lower()
        
        if url_ext in ['.jpg', '.jpeg', '.png', '.webp']:
            return url_ext
        else:
            return '.jpg'  # Default extension

    def verify_download_integrity(self, filepath):
        """Check downloaded file is not corrupted and meets minimum size requirements"""
        try:
            if not os.path.exists(filepath):
                self.logger.error(f"File does not exist: {filepath}")
                return False

            file_size = os.path.getsize(filepath)
            validation_config = self.config.get("validation", {})

            # Check minimum size based on file type
            if filepath.suffix.lower() == '.mp4':
                min_size = validation_config.get("min_video_size_bytes", 1024)
            else:
                min_size = validation_config.get("min_thumbnail_size_bytes", 100)

            if file_size < min_size:
                self.logger.warning(f"File too small: {filepath} ({file_size} bytes, minimum {min_size})")
                return False

            # For video files, do basic MP4 header check
            if filepath.suffix.lower() == '.mp4':
                return self._verify_mp4_header_lenient(filepath)

            self.logger.debug(f"File integrity verified: {filepath} ({file_size} bytes)")
            return True

        except Exception as e:
            self.logger.error(f"Error verifying download integrity {filepath}: {e}")
            return False

    def _verify_mp4_header_lenient(self, filepath):
        """Very lenient MP4 file header verification"""
        try:
            with open(filepath, 'rb') as f:
                header = f.read(32)

            # Basic check - ensure file isn't empty and has some content
            if len(header) < 8:
                self.logger.warning(f"Video file too small to have valid header: {filepath}")
                return False

            # Look for common video file indicators
            video_indicators = [
                b'ftyp',           # Standard MP4
                b'mp4',            # MP4 string anywhere
                b'MP4',            # MP4 uppercase
                b'\x00\x00\x00',  # Common MP4 header start
                b'mdat',           # MP4 media data
                b'moov'            # MP4 movie header
            ]

            for indicator in video_indicators:
                if indicator in header:
                    self.logger.debug(f"Valid video indicator found in {filepath}: {indicator}")
                    return True

            # If no indicators found, still pass if file is reasonably sized
            file_size = os.path.getsize(filepath)
            if file_size > 10240:  # If file is larger than 10KB, probably valid
                self.logger.info(f"Video file large enough to be valid: {filepath} ({file_size} bytes)")
                return True

            self.logger.warning(f"Video file may not be valid MP4: {filepath}")
            return False

        except Exception as e:
            self.logger.error(f"Error verifying MP4 header: {e}")
            return False

    def _format_file_size(self, filepath):
        """Format file size in human readable format"""
        try:
            size = filepath.stat().st_size
            return self._format_bytes(size)
        except:
            return "unknown size"

    def _format_bytes(self, bytes_size):
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f}{unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f}TB"

    def get_download_stats(self, filepath):
        """Get file size in MB for statistics"""
        try:
            if os.path.exists(filepath):
                return os.path.getsize(filepath) / (1024 * 1024)
            return 0
        except Exception:
            return 0
