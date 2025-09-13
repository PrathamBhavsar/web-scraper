# progress_tracker.py - ENHANCED with Smart Error Classification and Retry Logic

import json
import logging
import threading
import time
from pathlib import Path

class ProgressTracker:
    def __init__(self):
        self.progress_path = Path("progress.json")
        self.progress = self.load_progress()
        self.logger = logging.getLogger('Rule34Scraper')
        # Thread safety lock for concurrent access
        self._lock = threading.RLock()
        
        # NEW: Ensure failure tracking structure exists
        if "failed_videos" not in self.progress:
            self.progress["failed_videos"] = []
        if "video_failures" not in self.progress:
            self.progress["video_failures"] = {}

    def load_progress(self):
        """Load scraping progress from progress.json"""
        if self.progress_path.exists():
            try:
                return json.loads(self.progress_path.read_text())
            except (json.JSONDecodeError, FileNotFoundError):
                self.logger.warning("Corrupted progress file, resetting.")
        
        return {
            "last_video_id": None,
            "last_page": None,
            "total_downloaded": 0,
            "total_size_mb": 0,
            "downloaded_videos": [],
            "failed_videos": [],  # Legacy - kept for compatibility
            "video_failures": {}   # NEW: Enhanced failure tracking
        }

    def save_progress(self):
        """Save current scraping progress to file (thread-safe)"""
        with self._lock:
            try:
                self.progress_path.write_text(json.dumps(self.progress, indent=2))
            except Exception as e:
                self.logger.error(f"Error saving progress: {e}")

    # EXISTING METHODS - Keep unchanged
    def update_download_stats(self, video_id, file_size_mb, page_num=None):
        """Update downloaded videos and record last_page when given (thread-safe)."""
        with self._lock:
            if video_id not in self.progress["downloaded_videos"]:
                self.progress["downloaded_videos"].append(video_id)
                self.progress["last_video_id"] = video_id
                self.progress["total_downloaded"] += 1
                self.progress["total_size_mb"] = self.progress.get("total_size_mb", 0) + file_size_mb
            
            if page_num is not None:
                self.progress["last_page"] = page_num
            
            # NEW: Clear failure record on successful download
            self.clear_video_failure(video_id)
            
            self.save_progress()
            self.logger.debug(f"Updated progress for {video_id} (thread-safe)")

    def update_last_processed_page(self, page_num):
        """Update the last processed page number (thread-safe)"""
        with self._lock:
            self.progress["last_page"] = page_num
            self.save_progress()

    def get_last_processed_page(self):
        """Get the last processed page number (thread-safe)"""
        with self._lock:
            return self.progress.get("last_page")

    def get_downloaded_videos(self):
        """Get list of all downloaded videos (thread-safe)"""
        with self._lock:
            return self.progress.get("downloaded_videos", []).copy()

    def is_video_downloaded(self, video_id):
        """Check if video has already been downloaded (thread-safe)"""
        with self._lock:
            return video_id in self.progress.get("downloaded_videos", [])

    def get_stats(self):
        """Get current download statistics (thread-safe)"""
        with self._lock:
            return {
                "total_downloaded": self.progress.get("total_downloaded", 0),
                "total_size_mb": self.progress.get("total_size_mb", 0),
                "last_video_id": self.progress.get("last_video_id"),
                "last_page": self.progress.get("last_page")
            }

    # NEW: ENHANCED FAILURE TRACKING METHODS

    def record_video_failure(self, video_id, error_type, error_message, attempt_count=1):
        """
        Record a video failure with detailed error information
        
        Args:
            video_id: Video identifier
            error_type: 'temporary', 'permanent', 'rate_limit', 'network', 'validation'
            error_message: Detailed error description
            attempt_count: Current attempt number
        """
        with self._lock:
            current_time = int(time.time())
            
            if video_id not in self.progress["video_failures"]:
                self.progress["video_failures"][video_id] = {
                    "first_failed": current_time,
                    "last_attempt": current_time,
                    "attempt_count": 0,
                    "error_type": error_type,
                    "last_error": error_message,
                    "retry_after": None  # Cooldown timestamp
                }
            
            # Update failure record
            failure_record = self.progress["video_failures"][video_id]
            failure_record["last_attempt"] = current_time
            failure_record["attempt_count"] = attempt_count
            failure_record["error_type"] = error_type
            failure_record["last_error"] = error_message
            
            # Set retry cooldown based on error type
            if error_type == "rate_limit":
                failure_record["retry_after"] = current_time + (3600 * 2)  # 2 hours
            elif error_type == "network":
                failure_record["retry_after"] = current_time + (1800)  # 30 minutes  
            elif error_type == "temporary":
                failure_record["retry_after"] = current_time + (900)  # 15 minutes
            elif error_type == "permanent":
                failure_record["retry_after"] = current_time + (86400 * 7)  # 1 week
            
            self.save_progress()
            
            self.logger.info(f"Recorded {error_type} failure for {video_id}: attempt {attempt_count}")

    def should_retry_video(self, video_id):
        """
        Determine if a previously failed video should be retried
        
        Returns: (should_retry: bool, reason: str)
        """
        with self._lock:
            # If never failed, should try
            if video_id not in self.progress.get("video_failures", {}):
                return True, "never_failed"
            
            failure_record = self.progress["video_failures"][video_id]
            current_time = int(time.time())
            
            # Check if cooldown period has passed
            retry_after = failure_record.get("retry_after", 0)
            if current_time < retry_after:
                remaining = retry_after - current_time
                return False, f"cooldown_active_{remaining}s"
            
            # Check error type for retry decision
            error_type = failure_record.get("error_type", "unknown")
            attempt_count = failure_record.get("attempt_count", 0)
            
            # Permanent errors - retry after long cooldown only
            if error_type == "permanent":
                if attempt_count >= 3:
                    # Only retry permanent failures once per week
                    last_attempt = failure_record.get("last_attempt", 0)
                    if current_time - last_attempt < (86400 * 7):  # 1 week
                        return False, f"permanent_error_recent"
                return True, "permanent_error_retry"
            
            # Temporary/Network/Rate limit errors - always retry after cooldown
            return True, f"retry_after_cooldown"

    def clear_video_failure(self, video_id):
        """Clear failure record when video succeeds (thread-safe)"""
        with self._lock:
            if video_id in self.progress.get("video_failures", {}):
                del self.progress["video_failures"][video_id]
                self.logger.info(f"Cleared failure record for {video_id}")

    def get_failure_stats(self):
        """Get failure statistics for debugging"""
        with self._lock:
            failures = self.progress.get("video_failures", {})
            
            stats = {
                "total_failed_videos": len(failures),
                "by_error_type": {},
                "retry_ready_count": 0
            }
            
            current_time = int(time.time())
            
            for video_id, failure_record in failures.items():
                error_type = failure_record.get("error_type", "unknown")
                stats["by_error_type"][error_type] = stats["by_error_type"].get(error_type, 0) + 1
                
                # Check if ready for retry
                retry_after = failure_record.get("retry_after", 0)
                if current_time >= retry_after:
                    stats["retry_ready_count"] += 1
            
            return stats

    # LEGACY METHODS - Modified for backward compatibility
    
    def add_failed_video(self, video_id):
        """
        LEGACY METHOD - Modified to use new failure tracking
        Only called for truly permanent failures now
        """
        self.record_video_failure(video_id, "permanent", "Legacy permanent failure", 999)
        self.logger.warning(f"Marked {video_id} as permanently failed (legacy method)")

    def is_video_failed(self, video_id):
        """
        LEGACY METHOD - Modified to use smart retry logic
        Now returns False if video should be retried
        """
        should_retry, reason = self.should_retry_video(video_id)
        if should_retry:
            if video_id in self.progress.get("video_failures", {}):
                self.logger.info(f"Video {video_id} failed before but ready for retry: {reason}")
            return False  # Allow retry
        else:
            self.logger.info(f"Video {video_id} should not be retried: {reason}")
            return True   # Skip video
