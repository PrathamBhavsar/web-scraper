import json
import logging
from pathlib import Path

class ProgressTracker:
    def __init__(self):
        self.progress_path = Path("progress.json")
        self.progress = self.load_progress()
        self.logger = logging.getLogger('Rule34Scraper')

    def load_progress(self):
        """Load scraping progress from progress.json"""
        if self.progress_path.exists():
            try:
                with open(self.progress_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                self.logger.warning("Corrupted progress file, creating new one")
        
        return {
            "last_video_id": None,
            "last_page": None,
            "total_downloaded": 0,
            "total_size_mb": 0,
            "downloaded_videos": []
        }

    def save_progress(self):
        """Save current scraping progress to file"""
        with open(self.progress_path, 'w') as f:
            json.dump(self.progress, f, indent=2)

    def update_download_stats(self, video_id, file_size_mb, page_num=None):
        """Update total downloaded count, size, and optionally last page."""
        if video_id not in self.progress["downloaded_videos"]:
            self.progress["downloaded_videos"].append(video_id)
            self.progress["last_video_id"] = video_id
            if page_num is not None:
                self.progress["last_page"] = page_num
            self.progress["total_downloaded"] += 1
            self.progress["total_size_mb"] = self.progress.get("total_size_mb", 0) + file_size_mb
            self.save_progress()
            
    def update_last_processed_page(self, page_num):
        """Update the last processed page number"""
        self.progress["last_page"] = page_num
        self.save_progress()

    def get_last_processed_page(self):
        """Get the last processed page number"""
        return self.progress.get("last_page")

    def get_downloaded_videos(self):
        """Get list of all downloaded videos"""
        return self.progress.get("downloaded_videos", [])

    def get_last_downloaded_video(self):
        """Get the ID of the last downloaded video"""
        return self.progress.get("last_video_id")

    def get_last_processed(self):
        """Get the last processed video ID and page number"""
        return self.progress.get("last_video_id"), self.progress.get("last_page")

    def update_page_progress(self, page_num):
        """Update the last processed page number (legacy method)"""
        self.update_last_processed_page(page_num)

    def is_video_downloaded(self, video_id):
        """Check if video has already been downloaded"""
        return video_id in self.progress["downloaded_videos"]

    def get_stats(self):
        """Get current download statistics"""
        return {
            "total_downloaded": self.progress.get("total_downloaded", 0),
            "total_size_mb": self.progress.get("total_size_mb", 0),
            "last_video_id": self.progress.get("last_video_id"),
            "last_page": self.progress.get("last_page")
        }
