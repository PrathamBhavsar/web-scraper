# progress_tracker.py - Thread-safe version

import json
import logging
import threading
from pathlib import Path

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