import os
import time
import logging
import requests
from pathlib import Path
from urllib.parse import urlparse

class FileDownloader:
    def __init__(self, config):
        self.config = config
        self.base_url = "https://rule34video.com"
        self.logger = logging.getLogger('Rule34Scraper')

    def download_file(self, url, filepath):
        """Download any file with retry logic and validation"""
        # Safe config access with defaults
        download_config = self.config.get("download", {})
        max_retries = download_config.get("max_retries", 3)
        timeout = download_config.get("timeout_seconds", 30)
        chunk_size = download_config.get("chunk_size", 8192)
        
        for attempt in range(max_retries):
            try:
                headers = {
                    'User-Agent': self.config.get("general", {}).get("user_agent", "Mozilla/5.0"),
                    'Referer': self.base_url
                }

                response = requests.get(url, stream=True, timeout=timeout, headers=headers)
                response.raise_for_status()

                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)

                # Verify file was downloaded
                if self.verify_download_integrity(filepath):
                    self.logger.info(f"Successfully downloaded: {filepath}")
                    return True

            except Exception as e:
                self.logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if os.path.exists(filepath):
                    os.remove(filepath)
                time.sleep(2)

        return False

    def download_video(self, video_url, video_file_path):
        """Specifically handle video file downloads"""
        self.logger.info(f"Starting video download: {video_file_path}")
        return self.download_file(video_url, video_file_path)

    def download_thumbnail(self, thumbnail_url, video_id, video_dir):
        """Download and validate thumbnail images"""
        if not thumbnail_url:
            return False

        # Get proper file extension
        ext = self.get_file_extension(thumbnail_url)
        thumbnail_path = video_dir / f"{video_id}{ext}"

        if self.download_file(thumbnail_url, thumbnail_path):
            # Safe config access with default
            validation_config = self.config.get("validation", {})
            min_thumb_size = validation_config.get("min_thumbnail_size_bytes", 100)
            
            # Validate thumbnail size
            if thumbnail_path.stat().st_size >= min_thumb_size:
                self.logger.info(f"Thumbnail downloaded and validated: {thumbnail_path}")
                return True
            else:
                self.logger.warning(f"Thumbnail too small, removing: {thumbnail_path}")
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
            
            # Safe config access with defaults
            validation_config = self.config.get("validation", {})
            
            # Check minimum size based on file type - very lenient defaults
            if filepath.suffix.lower() == '.mp4':
                min_size = validation_config.get("min_video_size_bytes", 1024)  # 1KB minimum
            else:
                min_size = validation_config.get("min_thumbnail_size_bytes", 100)  # 100 bytes minimum

            if file_size < min_size:
                self.logger.warning(f"File too small: {filepath} ({file_size} bytes, minimum {min_size})")
                return False

            # For video files, do basic MP4 header check - very lenient
            if filepath.suffix.lower() == '.mp4':
                return self._verify_mp4_header_lenient(filepath)

            self.logger.info(f"File integrity verified: {filepath} ({file_size} bytes)")
            return True

        except Exception as e:
            self.logger.error(f"Error verifying download integrity {filepath}: {e}")
            return False

    def _verify_mp4_header_lenient(self, filepath):
        """Very lenient MP4 file header verification"""
        try:
            with open(filepath, 'rb') as f:
                header = f.read(32)
                
            # Very basic check - just ensure file isn't empty and has some content
            if len(header) < 8:
                self.logger.warning(f"Video file too small to have valid header: {filepath}")
                return False
                
            # Look for any common video file indicators - very lenient
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
            # This is very lenient to avoid false negatives
            file_size = os.path.getsize(filepath)
            if file_size > 10240:  # If file is larger than 10KB, probably valid
                self.logger.info(f"Video file large enough to be valid: {filepath} ({file_size} bytes)")
                return True
            
            self.logger.warning(f"Video file may not be valid MP4: {filepath}")
            return False

        except Exception as e:
            self.logger.error(f"Error verifying MP4 header: {e}")
            return False

    def get_download_stats(self, filepath):
        """Get file size in MB for statistics"""
        try:
            if os.path.exists(filepath):
                return os.path.getsize(filepath) / (1024 * 1024)
            return 0
        except Exception:
            return 0
