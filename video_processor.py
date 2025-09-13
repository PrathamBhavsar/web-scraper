# video_processor.py - ENHANCED with Smart Error Classification and Retry Logic

import os
import json
import time
import logging
import shutil
import threading
import requests
from pathlib import Path

class VideoProcessor:
    def __init__(self, config, file_validator, file_downloader, progress_tracker):
        self.config = config
        self.file_validator = file_validator
        self.file_downloader = file_downloader
        self.progress_tracker = progress_tracker
        self.logger = logging.getLogger('Rule34Scraper')
        self._local = threading.local()

    def get_thread_logger(self):
        """Get a thread-specific logger prefix"""
        if not hasattr(self._local, 'thread_name'):
            self._local.thread_name = threading.current_thread().name
        return self._local.thread_name

    def log_info(self, message):
        """Thread-aware logging"""
        thread_name = self.get_thread_logger()
        self.logger.info(f"[{thread_name}] {message}")

    def log_error(self, message):
        """Thread-aware error logging"""
        thread_name = self.get_thread_logger()
        self.logger.error(f"[{thread_name}] {message}")

    def log_warning(self, message):
        """Thread-aware warning logging"""
        thread_name = self.get_thread_logger()
        self.logger.warning(f"[{thread_name}] {message}")

    def process_video_parallel_safe(self, video_info, max_retries=None, page_num=None):
        """
        Process and download a single video - ENHANCED with Smart Retry Logic
        NO MORE PERMANENT FAILURES for temporary errors!
        """
        if not video_info or not video_info.get("video_src"):
            self.log_error("No video info or video source provided")
            return False

        video_id = video_info["video_id"]
        thread_name = self.get_thread_logger()
        
        self.log_info(f"Starting SMART RETRY processing for video {video_id}")

        # CRITICAL CHECK: Verify if already downloaded/failed before any processing
        if self.progress_tracker.is_video_downloaded(video_id):
            self.log_info(f"SKIPPING: Video {video_id} already downloaded")
            return True

        # NEW: Smart retry check instead of permanent skip
        should_retry, retry_reason = self.progress_tracker.should_retry_video(video_id)
        if not should_retry:
            self.log_info(f"SKIPPING: Video {video_id} - {retry_reason}")
            return False

        if retry_reason != "never_failed":
            self.log_info(f"RETRYING: Video {video_id} - {retry_reason}")

        if self.file_validator.validate_video_folder(video_id):
            self.log_info(f"SKIPPING: Video {video_id} folder exists and is valid")
            if not self.progress_tracker.is_video_downloaded(video_id):
                self.progress_tracker.update_download_stats(video_id, 0, page_num)
            return True

        self.log_info(f"PROCESSING: Video {video_id} - folder not found, will download")

        # Set page context for downloader
        self.file_downloader.current_page_num = page_num
        download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
        video_dir = download_path / video_id

        # Clean up any existing incomplete folder before starting
        if video_dir.exists():
            self.cleanup_incomplete_folder(video_id)

        # NEW: Determine retry count based on error history
        if max_retries is None:
            max_retries = self._get_smart_retry_count(video_id)

        # Enhanced retry logic with error classification
        return self.smart_retry_video_processing(video_info, max_retries, page_num)

    def _get_smart_retry_count(self, video_id):
        """Determine retry count based on video's failure history"""
        failure_record = self.progress_tracker.progress.get("video_failures", {}).get(video_id)
        
        if not failure_record:
            return 5  # Default for new videos
        
        error_type = failure_record.get("error_type", "temporary")
        
        # Different retry counts based on error type
        retry_counts = {
            "network": 8,      # Network issues - retry more
            "temporary": 6,    # General temporary issues  
            "rate_limit": 4,   # Rate limiting - fewer retries with longer delays
            "validation": 3,   # Data validation issues
            "permanent": 2     # Permanent errors - minimal retries
        }
        
        return retry_counts.get(error_type, 5)

    def smart_retry_video_processing(self, video_info, max_retries, page_num):
        """
        ENHANCED: Smart retry logic with error classification and exponential backoff
        No more permanent failures for temporary issues!
        """
        video_id = video_info["video_id"]
        
        for retry in range(max_retries):
            try:
                self.log_info(f"Processing video {video_id} (attempt {retry + 1}/{max_retries})")
                
                # Validate video info before proceeding
                validation_result = self._validate_video_info_before_processing(video_info, video_id, retry, max_retries)
                if not validation_result["success"]:
                    error_type = self._classify_error(validation_result["error"])
                    self._handle_retry_delay(error_type, retry)
                    continue

                # Create video directory
                directory_result = self._create_video_directory(video_info, video_id, retry, max_retries)
                if not directory_result["success"]:
                    error_type = self._classify_error(directory_result["error"])
                    self._handle_retry_delay(error_type, retry)
                    continue
                    
                video_dir = directory_result["video_dir"]

                # Download and validate video file
                download_result = self._download_video_with_validation_parallel(video_info, video_dir, video_id, retry, max_retries)
                if not download_result["success"]:
                    error_type = self._classify_error(download_result["error"])
                    self._handle_retry_delay(error_type, retry)
                    continue

                # Download thumbnail (optional - don't fail if this doesn't work)
                self._download_thumbnail_optional(video_info, video_id, video_dir)

                # Save metadata JSON
                metadata_result = self._save_metadata_json(video_info, video_dir, video_id, retry, max_retries)
                if not metadata_result["success"]:
                    error_type = self._classify_error(metadata_result["error"])
                    self._handle_retry_delay(error_type, retry)
                    continue

                # Final validation
                final_result = self._final_validation(video_info, video_dir, video_id, retry, max_retries)
                if not final_result["success"]:
                    error_type = self._classify_error(final_result["error"])
                    self._handle_retry_delay(error_type, retry)
                    continue

                # Success! Update progress and return
                self._update_success_progress_parallel(video_info, video_dir, page_num)
                self.log_info(f"✓ Video {video_id} processed successfully after {retry + 1} attempts")
                return True

            except Exception as e:
                error_message = str(e)
                error_type = self._classify_error(error_message)
                
                self.log_error(f"Unexpected error processing video {video_id} (attempt {retry + 1}): {e}")
                import traceback
                self.log_error(f"Traceback: {traceback.format_exc()}")
                
                self.cleanup_incomplete_folder(video_id)
                
                # Handle retry delay
                if retry < max_retries - 1:
                    self._handle_retry_delay(error_type, retry)
                else:
                    # Record failure with classification
                    self.progress_tracker.record_video_failure(video_id, error_type, error_message, retry + 1)

        # All retries exhausted - record failure but DON'T mark as permanently failed
        final_error_type = "temporary"  # Default to temporary for smart retry
        self.progress_tracker.record_video_failure(video_id, final_error_type, f"All {max_retries} retries exhausted", max_retries)
        self.log_error(f"All {max_retries} retry attempts failed for {video_id} - marked as {final_error_type} failure")
        
        return False

    def _classify_error(self, error_message):
        """
        NEW: Classify errors into categories for smart retry logic
        
        Returns: 'network', 'rate_limit', 'temporary', 'validation', 'permanent'
        """
        error_lower = str(error_message).lower()
        
        # Network-related errors (retry with backoff)
        network_indicators = [
            'connection', 'timeout', 'network', 'socket', 'dns', 'ssl',
            'certificate', 'handshake', 'reset', 'refused', 'unreachable'
        ]
        if any(indicator in error_lower for indicator in network_indicators):
            return 'network'
        
        # Rate limiting (longer delays)
        rate_limit_indicators = [
            '429', 'rate limit', 'too many requests', '503', 'service unavailable',
            'temporarily unavailable', 'quota', 'throttle'
        ]
        if any(indicator in error_lower for indicator in rate_limit_indicators):
            return 'rate_limit'
        
        # Permanent errors (minimal retries)
        permanent_indicators = [
            '404', 'not found', '403', 'forbidden', '401', 'unauthorized',
            'deleted', 'removed', 'invalid video', 'malformed', 'corrupted'
        ]
        if any(indicator in error_lower for indicator in permanent_indicators):
            return 'permanent'
        
        # Validation errors (moderate retries)
        validation_indicators = [
            'validation failed', 'invalid', 'missing field', 'parse error',
            'json', 'format', 'encoding'
        ]
        if any(indicator in error_lower for indicator in validation_indicators):
            return 'validation'
        
        # Default to temporary (allows retries)
        return 'temporary'

    def _handle_retry_delay(self, error_type, attempt_number):
        """
        NEW: Handle delays between retry attempts with exponential backoff
        """
        base_delays = {
            'network': 2,       # Start with 2 seconds for network issues
            'temporary': 3,     # 3 seconds for general issues
            'validation': 1,    # 1 second for validation issues  
            'rate_limit': 30,   # 30 seconds for rate limiting
            'permanent': 5      # 5 seconds for permanent (minimal retries)
        }
        
        base_delay = base_delays.get(error_type, 3)
        
        # Exponential backoff: base_delay * (2 ^ attempt_number)
        delay = min(base_delay * (2 ** attempt_number), 300)  # Max 5 minutes
        
        self.log_info(f"Retry delay for {error_type} error: {delay} seconds (attempt {attempt_number + 1})")
        time.sleep(delay)

    # UPDATED: Modified existing methods to return structured results

    def _validate_video_info_before_processing(self, video_info, video_id, retry, max_retries):
        """Validate video info with structured error reporting"""
        try:
            is_info_valid, info_errors = self.file_validator.validate_video_info(video_info)
            if not is_info_valid:
                error_msg = f"Video info validation failed: {info_errors}"
                self.log_error(error_msg)
                return {"success": False, "error": error_msg}
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": f"Info validation error: {e}"}

    def _create_video_directory(self, video_info, video_id, retry, max_retries):
        """Create video directory with structured error reporting"""
        try:
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            download_path.mkdir(parents=True, exist_ok=True)
            video_dir = download_path / video_id
            video_dir.mkdir(parents=True, exist_ok=True)
            self.log_info(f"Created directory: {video_dir}")
            return {"success": True, "video_dir": video_dir}
        except Exception as e:
            self.cleanup_incomplete_folder(video_id)
            return {"success": False, "error": f"Directory creation failed: {e}"}

    def _download_video_with_validation_parallel(self, video_info, video_dir, video_id, retry, max_retries):
        """Download video with structured error reporting"""
        try:
            video_file_path = video_dir / f"{video_id}.mp4"
            self.log_info(f"Starting video download for {video_id}")

            if not self.file_downloader.download_file(video_info["video_src"], video_file_path):
                error_msg = f"Download failed for {video_id}"
                self.log_error(error_msg)
                self.cleanup_incomplete_folder(video_id)
                return {"success": False, "error": error_msg}

            if not self.file_validator.verify_video_file(video_file_path):
                error_msg = f"Video verification failed for {video_id}"
                self.log_error(error_msg)
                self.cleanup_incomplete_folder(video_id)
                return {"success": False, "error": error_msg}

            return {"success": True}
        except Exception as e:
            return {"success": False, "error": f"Download error: {e}"}

    def _save_metadata_json(self, video_info, video_dir, video_id, retry, max_retries):
        """Save metadata with structured error reporting"""
        try:
            json_path = video_dir / f"{video_id}.json"
            self._ensure_complete_video_info(video_info)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=2, ensure_ascii=False)
            
            self.log_info(f"Metadata saved: {json_path}")
            return {"success": True}
        except Exception as e:
            self.cleanup_incomplete_folder(video_id)
            return {"success": False, "error": f"Metadata save error: {e}"}

    def _final_validation(self, video_info, video_dir, video_id, retry, max_retries):
        """Final validation with structured error reporting"""
        try:
            is_valid, validation_errors = self.file_validator.validate_complete_download(video_info, video_dir)
            if not is_valid:
                error_msg = f"Final validation failed: {validation_errors}"
                self.log_error(error_msg)
                self.cleanup_incomplete_folder(video_id)
                return {"success": False, "error": error_msg}
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": f"Final validation error: {e}"}

    # KEEP ALL OTHER EXISTING METHODS UNCHANGED
    def cleanup_incomplete_folder(self, video_id):
        """Remove incomplete video folders - Thread safe"""
        try:
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            video_dir = download_path / video_id
            if video_dir.exists():
                shutil.rmtree(video_dir)
                self.log_info(f"Removed incomplete folder: {video_dir}")
        except Exception as e:
            self.log_error(f"Error removing folder {video_id}: {e}")

    def _ensure_complete_video_info(self, video_info):
        """Ensure video_info has all required fields with valid values"""
        try:
            required_fields = {
                "video_id": video_info.get("video_id", ""),
                "url": video_info.get("url", ""),
                "title": video_info.get("title", f"Video_{video_info.get('video_id', 'unknown')}"),
                "duration": video_info.get("duration", "00:30"),
                "views": str(video_info.get("views", "0")),
                "uploader": video_info.get("uploader", "Unknown"),
                "upload_date": video_info.get("upload_date", int(time.time() * 1000)),
                "tags": video_info.get("tags", ["untagged"]),
                "video_src": video_info.get("video_src", ""),
                "thumbnail_src": video_info.get("thumbnail_src", "")
            }

            for field, value in required_fields.items():
                if not video_info.get(field) or (isinstance(value, str) and video_info.get(field).strip() == ""):
                    video_info[field] = value

            if isinstance(video_info.get("upload_date"), str):
                try:
                    from datetime import datetime
                    video_info["upload_date"] = int(datetime.now().timestamp() * 1000)
                except:
                    video_info["upload_date"] = int(time.time() * 1000)

            if not isinstance(video_info.get("tags"), list):
                video_info["tags"] = ["untagged"]

        except Exception as e:
            self.log_error(f"Error ensuring complete video info: {e}")

    def _download_thumbnail_optional(self, video_info, video_id, video_dir):
        """Download thumbnail (optional - don't fail if this doesn't work)"""
        try:
            if video_info.get("thumbnail_src"):
                thumbnail_downloaded = self.file_downloader.download_thumbnail(
                    video_info["thumbnail_src"], video_id, video_dir
                )
                if thumbnail_downloaded:
                    self.log_info(f"Thumbnail downloaded for {video_id}")
                else:
                    self.log_warning(f"Thumbnail download failed for {video_id} - continuing anyway")
        except Exception as e:
            self.log_warning(f"Thumbnail download failed for {video_id}: {e} - continuing anyway")

    def _update_success_progress_parallel(self, video_info, video_dir, page_num):
        """Update progress tracking after successful processing - Thread-safe"""
        try:
            video_id = video_info["video_id"]
            video_file_path = video_dir / f"{video_id}.mp4"
            file_size_mb = 0
            if video_file_path.exists():
                file_size_mb = video_file_path.stat().st_size / (1024 * 1024)

            self.progress_tracker.update_download_stats(video_id, file_size_mb, page_num)

            if video_id not in self.progress_tracker.get_downloaded_videos():
                self.log_error(f"CRITICAL: Video {video_id} not found in downloaded list after update!")
                with self.progress_tracker._lock:
                    self.progress_tracker.progress["downloaded_videos"].append(video_id)
                    self.progress_tracker.save_progress()

            self.log_info(f"Successfully processed video: {video_id} ({file_size_mb:.2f} MB)")
            self.log_info(f"Video {video_id} marked as downloaded - cleared from failure list")

        except Exception as e:
            self.log_error(f"Error updating progress: {e}")

    # Keep other existing methods unchanged...
    def batch_process_videos_parallel(self, video_info_list, max_workers=3):
        """NEW: Process multiple videos in parallel using ThreadPoolExecutor"""
        import concurrent.futures

        if not video_info_list:
            self.log_info("No videos to process")
            return {"successful": 0, "failed": 0, "skipped": 0}

        self.log_info(f"Starting batch parallel processing of {len(video_info_list)} videos (max_workers={max_workers})")
        results = {"successful": 0, "failed": 0, "skipped": 0}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="VideoProcessor") as executor:
            future_to_video = {
                executor.submit(self.process_video_parallel_safe, video_info): video_info["video_id"]
                for video_info in video_info_list
            }

            for future in concurrent.futures.as_completed(future_to_video):
                video_id = future_to_video[future]
                try:
                    success = future.result(timeout=600)  # 10 minute timeout per video
                    if success:
                        results["successful"] += 1
                        self.log_info(f"✓ Video {video_id} processed successfully")
                    else:
                        results["failed"] += 1
                        self.log_error(f"✗ Video {video_id} processing failed")
                except concurrent.futures.TimeoutError:
                    results["failed"] += 1
                    self.log_error(f"✗ Video {video_id} processing timed out")
                except Exception as e:
                    results["failed"] += 1
                    self.log_error(f"✗ Video {video_id} processing failed with exception: {e}")

        self.log_info(f"Batch processing completed: {results['successful']} successful, {results['failed']} failed")
        return results

    # Legacy method for backward compatibility
    def process_video(self, video_info, max_retries=3):
        """Legacy method - redirects to parallel-safe version"""
        return self.process_video_parallel_safe(video_info, max_retries)
