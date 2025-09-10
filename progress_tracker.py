# progress_tracker.py - Thread-safe version

import json
import logging
import threading
from pathlib import Path
import time

class ProgressTracker:
    def __init__(self):
        self.progress_path = Path("progress.json")
        self.progress = self.load_progress()
        self.logger = logging.getLogger('Rule34Scraper')
        # Thread safety lock for concurrent access
        self._lock = threading.RLock()
    
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
            "downloaded_videos": []
        }
    
    def save_progress(self):
        """Save current scraping progress to file (thread-safe)"""
        with self._lock:
            try:
                self.progress_path.write_text(json.dumps(self.progress, indent=2))
            except Exception as e:
                self.logger.error(f"Error saving progress: {e}")
    
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
                
                self.save_progress()
                self.logger.debug(f"Updated progress for {video_id} (thread-safe)")
    
    def update_last_processed_page(self, page_num):
        """Update the last processed page number (thread-safe)"""
        with self._lock:
            self.progress["last_page"] = page_num
            self.save_progress()
    
    def update_page_progress(self, page_num):
        """Update the last processed page number (legacy method, thread-safe)"""
        self.update_last_processed_page(page_num)
    
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
    
    def get_last_downloaded_video(self):
        """Get the ID of the last downloaded video (thread-safe)"""
        with self._lock:
            return self.progress.get("last_video_id")
    
    def get_last_processed(self):
        """Get the last processed video ID and page number (thread-safe)"""
        with self._lock:
            return self.progress.get("last_video_id"), self.progress.get("last_page")
    
    def get_stats(self):
        """Get current download statistics (thread-safe)"""
        with self._lock:
            return {
                "total_downloaded": self.progress.get("total_downloaded", 0),
                "total_size_mb": self.progress.get("total_size_mb", 0),
                "last_video_id": self.progress.get("last_video_id"),
                "last_page": self.progress.get("last_page")
            }
    
    def add_failed_video(self, video_id):
        """Mark a video as failed to avoid retrying (thread-safe)"""
        with self._lock:
            failed_videos = self.progress.get("failed_videos", [])
            if video_id not in failed_videos:
                failed_videos.append(video_id)
                self.progress["failed_videos"] = failed_videos
                self.save_progress()
    
    def is_video_failed(self, video_id):
        """Check if video has previously failed (thread-safe)"""
        with self._lock:
            return video_id in self.progress.get("failed_videos", [])
        

    def update_download_stats(self, video_id, file_size_mb, page_num=None):
        """
        Enhanced update with storage limit checking
        Returns tuple (success: bool, storage_warning: bool, storage_limit_reached: bool)
        """
        with self._lock:
            # Check if this update would exceed storage limits
            current_total_mb = self.progress.get("total_size_mb", 0)
            new_total_mb = current_total_mb + file_size_mb
            
            # Get max storage from config (need to access it somehow - will be passed from main scraper)
            # For now, we'll update this when called from main scraper
            
            # Update the progress data
            if video_id not in self.progress["downloaded_videos"]:
                self.progress["downloaded_videos"].append(video_id)
                self.progress["last_video_id"] = video_id
                self.progress["total_downloaded"] += 1
                self.progress["total_size_mb"] = new_total_mb

                if page_num is not None:
                    self.progress["last_page"] = page_num

                self.save_progress()
                self.logger.debug(f"Updated progress for {video_id}: +{file_size_mb:.2f}MB = {new_total_mb:.2f}MB total")
                
                return True, False, False
            else:
                # Video already exists, don't add size again
                self.logger.debug(f"Video {video_id} already in progress, not updating size")
                return True, False, False

    def check_storage_limit_before_update(self, file_size_mb, max_storage_gb):
        """
        Check if adding this file size would exceed storage limits
        Returns tuple (can_add: bool, current_gb: float, projected_gb: float, warning: bool)
        """
        with self._lock:
            current_mb = self.progress.get("total_size_mb", 0)
            projected_mb = current_mb + file_size_mb
            
            current_gb = current_mb / 1024
            projected_gb = projected_mb / 1024
            
            # Check if projected size exceeds limit
            if projected_gb > max_storage_gb:
                return False, current_gb, projected_gb, False
            
            # Check if projected size is close to limit (95%)
            warning = projected_gb >= (max_storage_gb * 0.95)
            
            return True, current_gb, projected_gb, warning

    def get_current_storage_usage_gb(self):
        """Get current storage usage in GB from progress data"""
        with self._lock:
            mb = self.progress.get("total_size_mb", 0)
            return mb / 1024

    def get_storage_usage_percentage(self, max_storage_gb):
        """Get current storage usage as percentage of max limit"""
        current_gb = self.get_current_storage_usage_gb()
        return (current_gb / max_storage_gb) * 100 if max_storage_gb > 0 else 0

    def is_storage_limit_reached(self, max_storage_gb, threshold=1.0):
        """
        Check if storage limit is reached based on threshold
        threshold=1.0 means exactly at limit, 0.95 means 95% of limit
        """
        current_gb = self.get_current_storage_usage_gb()
        limit_gb = max_storage_gb * threshold
        return current_gb >= limit_gb

    def reset_progress_for_storage_cleanup(self):
        """
        Reset progress tracking (use with caution - only for storage cleanup)
        This method should only be used when manually cleaning up files
        """
        with self._lock:
            self.logger.warning("  RESETTING PROGRESS TRACKING - This should only be done during storage cleanup")
            
            # Keep basic structure but reset counters
            self.progress = {
                "last_video_id": None,
                "last_page": self.progress.get("last_page", None),  # Keep last page
                "total_downloaded": 0,
                "total_size_mb": 0,
                "downloaded_videos": [],
                "failed_videos": self.progress.get("failed_videos", [])  # Keep failed list
            }
            
            self.save_progress()
            self.logger.warning("Progress tracking has been reset")

    def add_storage_limit_reached_flag(self):
        """Add a flag to indicate storage limit was reached"""
        with self._lock:
            self.progress["storage_limit_reached"] = True
            self.progress["storage_limit_reached_at"] = time.time()
            self.save_progress()

    def clear_storage_limit_flag(self):
        """Clear the storage limit reached flag"""
        with self._lock:
            if "storage_limit_reached" in self.progress:
                del self.progress["storage_limit_reached"]
            if "storage_limit_reached_at" in self.progress:
                del self.progress["storage_limit_reached_at"]
            self.save_progress()

    def was_storage_limit_reached(self):
        """Check if storage limit was previously reached"""
        with self._lock:
            return self.progress.get("storage_limit_reached", False)