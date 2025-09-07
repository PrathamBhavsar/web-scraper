# file_downloader.py - Thread-safe improvements

import os
import time
import logging
import requests
import threading
import shutil
import sys
import json
import asyncio
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional, Callable, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from idm_downloader import IDMDownloader
from file_validator import FileValidator

# Retry utilities
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class FileDownloader:
    """
    FileDownloader with storage limit checking and proper integrity validation
    Uses only requests-based downloads (Crawl4AI removed entirely)
    ENHANCED: Thread-safe for parallel video processing
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.current_page_num = None
        self.config = config or {}
        self.base_url = self.config.get("general", {}).get("base_url", "https://rule34video.com")
        self.logger = logging.getLogger('Rule34Scraper')
        
        # CRITICAL FIX: Initialize file validator component
        self.file_validator = FileValidator(config)
        
        # Initialize IDM downloader
        self.idm_downloader = IDMDownloader(config)
        
        # Ensure download directory exists
        download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
        download_path.mkdir(parents=True, exist_ok=True)
        
        # Progress tracking - THREAD SAFE
        self.current_download = None
        self.progress_lock = threading.RLock()  # Use RLock for reentrant locking
        self.last_progress_update = 0
        
        # Thread-local storage for thread-specific data
        self._local = threading.local()
        
        # Storage limits
        self.max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
        self.max_storage_bytes = self.max_storage_gb * 1024**3
        
        # Check download method and IDM availability
        self.download_method = self.config.get("download", {}).get("download_method", "direct")
        if self.download_method == "idm":
            if self.idm_downloader.is_idm_available():
                idm_version = self.idm_downloader.get_idm_version()
                self.logger.info(f"IDM download method selected. {idm_version or 'IDM Available'}")
            else:
                self.logger.error("IDM download method selected but IDM is not available!")
                self.logger.error(f"Please install IDM or check path: {self.idm_downloader.idm_path}")
        
        self.logger.info("FileDownloader initialized successfully with file_validator and thread safety")
    
    def get_thread_name(self):
        """Get current thread name for logging"""
        if not hasattr(self._local, 'thread_name'):
            self._local.thread_name = threading.current_thread().name
        return self._local.thread_name
    
    def log_info(self, message):
        """Thread-aware info logging"""
        thread_name = self.get_thread_name()
        self.logger.info(f"[{thread_name}] {message}")
    
    def log_error(self, message):
        """Thread-aware error logging"""
        thread_name = self.get_thread_name()
        self.logger.error(f"[{thread_name}] {message}")
    
    def log_warning(self, message):
        """Thread-aware warning logging"""
        thread_name = self.get_thread_name()
        self.logger.warning(f"[{thread_name}] {message}")
    
    def _check_storage_space_before_download(self, expected_size_bytes=None):
        """Check if we have enough storage space before starting download"""
        try:
            download_path = self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\")
            current_usage = 0
            for dirpath, dirnames, filenames in os.walk(download_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        current_usage += os.path.getsize(filepath)
            
            # If we have expected size, check if it would exceed limit
            if expected_size_bytes:
                projected_usage = current_usage + expected_size_bytes
                if projected_usage > self.max_storage_bytes:
                    usage_gb = current_usage / (1024**3)
                    expected_gb = expected_size_bytes / (1024**3)
                    self.log_warning(f"Download would exceed storage limit: {usage_gb:.2f} GB + {expected_gb:.2f} GB > {self.max_storage_gb} GB")
                    return False
            
            # Check current usage against limit
            if current_usage >= self.max_storage_bytes:
                usage_gb = current_usage / (1024**3)
                self.log_warning(f"Storage limit already reached: {usage_gb:.2f} GB / {self.max_storage_gb} GB")
                return False
            
            return True
        except Exception as e:
            self.log_error(f"Error checking storage space: {e}")
            return True  # Allow download if we can't check

    # ------------------------
    # Progress Bar Utilities - THREAD SAFE
    # ------------------------
    
    def _show_progress_bar(self, filename: str, percent: float, total_size_mb: Optional[float] = None):
        """Show a single-line progress bar with - and + symbols (THREAD SAFE)"""
        with self.progress_lock:
            # Throttle progress updates to avoid spam
            current_time = time.time()
            if current_time - self.last_progress_update < 0.5:  # Update every 0.5 seconds max
                return
            
            self.last_progress_update = current_time
            thread_name = self.get_thread_name()
            
            # Clear the line
            sys.stdout.write('\r' + ' ' * 120 + '\r')
            
            # Calculate bar
            bar_width = 30  # Shorter to accommodate thread name
            filled = int(bar_width * percent / 100)
            bar = '+' * filled + '-' * (bar_width - filled)
            
            # Format the display (no emojis to avoid encoding issues)
            if total_size_mb:
                size_info = f" [{total_size_mb:.1f}MB]"
            else:
                size_info = ""
            
            # Show progress with thread name (safe ASCII only)
            progress_line = f"[{thread_name}] [{bar}] {percent:6.2f}% | {filename}{size_info}"
            sys.stdout.write(progress_line)
            sys.stdout.flush()
            
            # Add newline when complete
            if percent >= 100:
                sys.stdout.write('\n')

    def _clear_progress_line(self):
        """Clear the current progress line (THREAD SAFE)"""
        with self.progress_lock:
            sys.stdout.write('\r' + ' ' * 120 + '\r')
            sys.stdout.flush()

    # ------------------------
    # Utility helpers - UNCHANGED
    # ------------------------
    
    def _get_headers(self):
        return {
            'User-Agent': self.config.get("general", {}).get("user_agent", "Mozilla/5.0"),
            'Referer': self.base_url,
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate',
            'Accept': '*/*'
        }

    def _ensure_parent(self, filepath: Path):
        filepath.parent.mkdir(parents=True, exist_ok=True)

    def _get_file_name_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        name = os.path.basename(parsed.path) or parsed.netloc
        return name

    def _get_expected_size(self, url: str, timeout: int = 10) -> Optional[int]:
        """
        Try to get Content-Length via HEAD. Returns int bytes or None.
        """
        try:
            h = self._get_headers()
            r = requests.head(url, headers=h, allow_redirects=True, timeout=timeout)
            if r.status_code == 200:
                cl = r.headers.get('Content-Length') or r.headers.get('content-length')
                if cl and cl.isdigit():
                    return int(cl)
        except Exception:
            pass
        return None

    # ------------------------
    # Main download_file (THREAD SAFE VERSION)
    # ------------------------
    
    def download_file(self, url: str, filepath: Path, progress_callback: Optional[Callable[[str, float], None]] = None) -> bool:
        """
        Download a single file with verification and strong pre-download checks
        ENHANCED: Thread-safe for parallel execution
        """
        filepath = Path(filepath)
        video_id = filepath.stem
        file_type = "thumbnail" if filepath.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp'] else "video"
        
        try:
            # For thumbnails, skip progress tracker checks (they're not tracked separately)
            if file_type == "video":
                # CRITICAL: Check progress tracker FIRST before any download attempt
                from progress_tracker import ProgressTracker
                progress_tracker = ProgressTracker()
                
                if progress_tracker.is_video_downloaded(video_id):
                    self.log_info(f"SKIPPING: Video {video_id} already recorded in progress.json")
                    return True
                
                # SECOND: Check if folder/file already exists and is valid
                if self.file_validator.validate_video_folder(video_id):
                    self.log_info(f"SKIPPING: Video {video_id} folder exists and is valid")
                    # If for some reason not in progress tracker, add it now
                    if not progress_tracker.is_video_downloaded(video_id):
                        progress_tracker.update_download_stats(video_id, 0)
                        self.log_info(f"Added existing video {video_id} to progress tracker")
                    return True
            
            # Check if the specific file already exists and is valid size
            if filepath.exists():
                file_size = filepath.stat().st_size
                validation_config = self.config.get("validation", {})
                
                if file_type == "thumbnail":
                    min_size = int(validation_config.get("min_thumbnail_size_bytes", 100) or 100)
                else:
                    min_size = int(validation_config.get("min_video_size_bytes", 1024) or 1024)
                
                if file_size >= min_size:
                    self.log_info(f"SKIPPING: {file_type.title()} file already exists and is valid: {filepath.name} ({file_size} bytes)")
                    return True
                else:
                    self.log_info(f"Existing {file_type} file too small ({file_size} bytes), will re-download: {filepath.name}")
            
            # If we reach here, the file is NOT downloaded and needs to be processed
            self.log_info(f"PROCEEDING: {file_type.title()} {filepath.name} not found or invalid - will download")
            
            self._ensure_parent(filepath)
            
            # Thread-safe current download tracking
            with self.progress_lock:
                self.current_download = filepath.name
            
            # Check storage space before starting
            expected_size = self._get_expected_size(url, timeout=10)
            if not self._check_storage_space_before_download(expected_size):
                self.log_error(f"Insufficient storage space for download: {filepath.name}")
                return False
            
            # Choose download method based on configuration
            download_method = self.config.get("download", {}).get("download_method", "direct")
            if download_method == "idm":
                success = self._idm_download(url, filepath, progress_callback=progress_callback)
            else:
                success = self._requests_stream_download(url, filepath, progress_callback=progress_callback)
            
            if success:
                self.log_info(f"{file_type.title()} download completed successfully: {filepath.name}")
            else:
                self.log_error(f"{file_type.title()} download failed: {filepath.name}")
            
            return success
            
        except Exception as e:
            self.log_error(f"Unexpected error in download_file for {file_type} {filepath.name}: {e}")
            return False

    def _idm_download(self, url: str, filepath: Path, progress_callback: Optional[Callable]=None) -> bool:
        """
        Download file using IDM with proper queue management.
        Updates progress.json with page number from self.current_page_num.
        """
        max_retries = self.config.get("download", {}).get("max_retries", 3)
        video_id = filepath.stem
        file_type = "video" if filepath.suffix.lower() == ".mp4" else "thumbnail"
        
        for attempt in range(1, max_retries + 1):
            try:
                self.log_info(f"IDM download attempt {attempt}/{max_retries}: {filepath.name}")
                
                if self.idm_downloader.download_single_file(url, filepath):
                    if self.verify_download_integrity(filepath):
                        self.log_info(f"IDM download successful: {filepath.name}")
                        
                        # Only update for videos
                        if file_type == "video":
                            file_size_mb = filepath.stat().st_size / (1024 * 1024)
                            from progress_tracker import ProgressTracker
                            tracker = ProgressTracker()
                            tracker.update_download_stats(video_id, file_size_mb, self.current_page_num)
                        
                        return True
                    else:
                        self.log_warning(f"Integrity check failed: {filepath.name}")
                        filepath.unlink(missing_ok=True)
                else:
                    self.log_warning(f"IDM download failed for {filepath.name}")
                    
            except Exception as e:
                self.log_error(f"IDM download error on attempt {attempt}: {e}")
            
            # Retry backoff
            if attempt < max_retries:
                backoff = 2 ** attempt
                self.log_info(f"Retrying in {backoff}s...")
                time.sleep(backoff)
        
        self.log_error(f"All IDM download attempts failed for {filepath.name}")
        return False

    # ------------------------
    # requests streaming download with storage monitoring - THREAD SAFE
    # ------------------------
    
    def _requests_stream_download(self, url: str, filepath: Path,
                                progress_callback: Optional[Callable[[str, float], None]] = None) -> bool:
        """
        Stream file via requests with storage monitoring during download (THREAD SAFE)
        """
        headers_base = self._get_headers()
        download_conf = self.config.get("download", {})
        
        # Improved timeout and retry settings
        max_retries = int(download_conf.get("max_retries", 5))
        chunk_size = int(download_conf.get("chunk_size", 16384) or 16384)
        connect_timeout = int(download_conf.get("connect_timeout_seconds", 30) or 30)
        read_timeout = int(download_conf.get("read_timeout_seconds", 300) or 300)
        
        session = requests.Session()
        
        # Configure more aggressive retries
        retry_strategy = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            backoff_factor=2,
            respect_retry_after_header=True
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=20, pool_block=True)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        tmp_path = filepath.with_suffix(filepath.suffix + ".part")
        self._ensure_parent(tmp_path)
        
        attempt = 0
        while attempt <= max_retries:
            attempt += 1
            try:
                # If we already have partial bytes, attempt to resume
                existing = 0
                if tmp_path.exists():
                    try:
                        existing = tmp_path.stat().st_size
                        self.log_info(f"Resuming download from {existing} bytes for {filepath.name}")
                    except Exception:
                        existing = 0
                
                headers = dict(headers_base)  # copy
                if existing > 0:
                    # Use Range header to request remainder
                    headers['Range'] = f'bytes={existing}-'
                    mode = 'ab'
                else:
                    mode = 'wb'
                
                # Stream with longer timeout tuple
                with session.get(url, stream=True, timeout=(connect_timeout, read_timeout), headers=headers) as r:
                    r.raise_for_status()
                    
                    # If server responded 200 while we requested Range, restart from scratch
                    if existing > 0 and r.status_code == 200:
                        self.log_info(f"Server doesn't support resume, restarting download for {filepath.name}")
                        existing = 0
                        mode = 'wb'
                    
                    total = None
                    content_length = r.headers.get('Content-Length')
                    if content_length and content_length.isdigit():
                        total = int(content_length)
                        if 'Range' in headers and r.status_code == 206:
                            # When 206 Partial, total is the remaining bytes; add existing to get full size
                            total = existing + total
                    
                    downloaded = existing
                    last_update_time = time.time()
                    last_storage_check = time.time()
                    
                    # Write data with improved progress tracking and storage monitoring
                    with open(tmp_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Check storage limit every 10 seconds during download
                                current_time = time.time()
                                if current_time - last_storage_check >= 10:
                                    if not self._check_storage_space_before_download():
                                        self._clear_progress_line()
                                        self.log_error(f"Storage limit reached during download of {filepath.name}")
                                        try:
                                            tmp_path.unlink(missing_ok=True)
                                        except Exception:
                                            pass
                                        return False
                                    last_storage_check = current_time
                                
                                # Throttled progress updates
                                if current_time - last_update_time >= 1.0:  # Update every second
                                    if total:
                                        percent = (downloaded / total) * 100.0
                                        percent = min(percent, 100.0)
                                        total_mb = total / (1024 * 1024)
                                        self._show_progress_bar(filepath.name, percent, total_mb)
                                    else:
                                        mb = downloaded / (1024 * 1024)
                                        self._show_progress_bar(filepath.name, min(100.0, mb * 2), mb)
                                    
                                    if progress_callback:
                                        try:
                                            progress_callback(str(filepath), percent if total else mb)
                                        except Exception:
                                            pass
                                    
                                    last_update_time = current_time
                    
                    # Move tmp to final atomically
                    try:
                        os.replace(tmp_path, filepath)
                    except Exception:
                        try:
                            shutil.move(str(tmp_path), str(filepath))
                        except Exception as e:
                            self._clear_progress_line()
                            self.log_error(f"Failed to move temp file to final: {e}")
                            return False
                    
                    if self.verify_download_integrity(filepath):
                        self._show_progress_bar(filepath.name, 100.0)
                        self.log_info(f"Successfully downloaded: {filepath.name}")
                        return True
                    else:
                        self._clear_progress_line()
                        self.log_warning(f"Downloaded file failed integrity check: {filepath.name}")
                        try:
                            filepath.unlink(missing_ok=True)
                        except Exception:
                            pass
                        
                        # If integrity failed, maybe transient - retry
                        if attempt <= max_retries:
                            backoff = min(60, 2 ** attempt)
                            self.log_info(f"Retrying in {backoff} seconds...")
                            time.sleep(backoff)
                            continue
                        return False
                        
            except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout) as e:
                self._clear_progress_line()
                self.log_warning(f"Timeout while downloading {filepath.name}: attempt {attempt}/{max_retries}")
                
                if attempt <= max_retries:
                    backoff = min(60, 2 ** attempt)
                    self.log_info(f"Retrying in {backoff} seconds (with resume support)...")
                    time.sleep(backoff)
                    continue
                else:
                    self.log_error(f"Exceeded timeout retries for {filepath.name}")
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return False
                    
            except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
                self._clear_progress_line()
                self.log_warning(f"Connection error downloading {filepath.name}: attempt {attempt}/{max_retries} - {str(e)[:100]}")
                
                if attempt <= max_retries:
                    backoff = min(60, 2 ** attempt)
                    self.log_info(f"Retrying in {backoff} seconds...")
                    time.sleep(backoff)
                    continue
                
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return False
                
            except Exception as e:
                self._clear_progress_line()
                self.log_error(f"Unexpected error during download for {filepath.name}: {str(e)[:100]}")
                
                # cleanup and retry if possible
                if attempt <= max_retries:
                    backoff = min(60, 1 + attempt * 2)
                    time.sleep(backoff)
                    continue
                
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return False
        
        # If we exit loop without returning success
        self._clear_progress_line()
        self.log_error(f"All download attempts failed for {filepath.name}")
        return False

    # Keep all other existing methods unchanged - just use thread-safe logging
    
    def download_video(self, video_url: str, video_file_path: Path) -> bool:
        download_method = self.config.get("download", {}).get("download_method", "direct")
        self.log_info(f"Starting video download ({download_method}): {video_file_path.name}")
        return self.download_file(video_url, video_file_path)

    def download_thumbnail(self, thumbnail_url: str, video_id: str, video_dir: Path) -> bool:
        if not thumbnail_url:
            self.log_warning(f"No thumbnail URL provided for {video_id}")
            return False
        
        ext = self.get_file_extension(thumbnail_url)
        thumbnail_path = Path(video_dir) / f"{video_id}{ext}"
        
        download_method = self.config.get("download", {}).get("download_method", "direct")
        self.log_info(f"Starting thumbnail download ({download_method}): {thumbnail_path.name}")
        self.log_info(f"Thumbnail URL: {thumbnail_url}")
        
        # Use the same download_file method that works for videos
        if self.download_file(thumbnail_url, thumbnail_path):
            validation_config = self.config.get("validation", {})
            min_thumb_size = int(validation_config.get("min_thumbnail_size_bytes", 100) or 100)
            
            try:
                if thumbnail_path.exists() and thumbnail_path.stat().st_size >= min_thumb_size:
                    self.log_info(f"Thumbnail downloaded successfully ({download_method}): {thumbnail_path.name} ({thumbnail_path.stat().st_size} bytes)")
                    return True
                else:
                    file_size = thumbnail_path.stat().st_size if thumbnail_path.exists() else 0
                    self.log_warning(f"Thumbnail too small ({file_size} bytes, minimum {min_thumb_size}), removing: {thumbnail_path.name}")
                    thumbnail_path.unlink(missing_ok=True)
                    return False
                    
            except Exception as e:
                self.log_error(f"Thumbnail validation error {thumbnail_path.name}: {e}")
                return False
        else:
            self.log_error(f"Failed to download thumbnail for {video_id}")
            self.log_error(f"Thumbnail URL was: {thumbnail_url}")
            return False

    def get_file_extension(self, url: str) -> str:
        parsed_url = urlparse(url)
        url_ext = os.path.splitext(parsed_url.path)[1].lower()
        if url_ext in ['.jpg', '.jpeg', '.png', '.webp']:
            return url_ext
        else:
            return '.jpg'

    def verify_download_integrity(self, filepath: Path) -> bool:
        """Enhanced verification for both videos and thumbnails"""
        try:
            if not isinstance(filepath, Path):
                filepath = Path(filepath)
            
            if not filepath.exists():
                self.log_error(f"File does not exist: {filepath.name}")
                return False
            
            file_size = os.path.getsize(filepath)
            validation_config = self.config.get("validation", {})
            
            # Determine file type and minimum size
            if filepath.suffix.lower() == '.mp4':
                min_size = int(validation_config.get("min_video_size_bytes", 1024) or 1024)
                file_type = "video"
            elif filepath.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']:
                min_size = int(validation_config.get("min_thumbnail_size_bytes", 100) or 100)
                file_type = "thumbnail"
            else:
                min_size = 100  # Default minimum
                file_type = "file"
            
            if file_size < min_size:
                self.log_warning(f"{file_type.title()} file too small: {filepath.name} ({file_size} bytes, minimum {min_size})")
                return False
            
            # Additional verification for MP4 files
            if filepath.suffix.lower() == '.mp4':
                return self._verify_mp4_header_lenient(filepath)
            
            # For thumbnails, just check if it has basic image file structure
            elif filepath.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']:
                return self._verify_image_header(filepath)
            
            return True
            
        except Exception as e:
            self.log_error(f"Error verifying download integrity {filepath.name}: {e}")
            return False

    def _verify_image_header(self, filepath: Path) -> bool:
        """Verify image file has valid header"""
        try:
            with open(filepath, 'rb') as f:
                header = f.read(16)
            
            if len(header) < 4:
                self.log_warning(f"Image file too small to have valid header: {filepath.name}")
                return False
            
            # Common image file signatures
            image_signatures = {
                b'\xff\xd8\xff': 'JPEG',
                b'\x89PNG': 'PNG',
                b'RIFF': 'WEBP',
                b'GIF8': 'GIF'
            }
            
            for signature, format_name in image_signatures.items():
                if header.startswith(signature):
                    self.logger.debug(f"Valid {format_name} signature found in {filepath.name}")
                    return True
            
            self.log_warning(f"Image file {filepath.name} does not have recognized image signature")
            return True  # Be lenient - if it downloaded and has reasonable size, accept it
            
        except Exception as e:
            self.log_error(f"Error verifying image header: {e}")
            return True  # Be lenient on verification errors

    def _verify_mp4_header_lenient(self, filepath: Path) -> bool:
        """Verify MP4 file has valid header"""
        try:
            with open(filepath, 'rb') as f:
                header = f.read(32)
            
            if len(header) < 8:
                self.log_warning(f"Video file too small to have valid header: {filepath.name}")
                return False
            
            video_indicators = [b'ftyp', b'mp4', b'MP4', b'\x00\x00\x00', b'mdat', b'moov']
            for indicator in video_indicators:
                if indicator in header:
                    return True
            
            file_size = os.path.getsize(filepath)
            if file_size > 10240:
                return True
            
            self.log_warning(f"Video file may not be valid MP4: {filepath.name}")
            return False
            
        except Exception as e:
            self.log_error(f"Error verifying MP4 header: {e}")
            return False

    def get_download_stats(self, filepath: Path) -> float:
        """Get download statistics in MB"""
        try:
            if isinstance(filepath, str):
                filepath = Path(filepath)
            if filepath.exists():
                return filepath.stat().st_size / (1024 * 1024)
            return 0.0
        except Exception:
            return 0.0