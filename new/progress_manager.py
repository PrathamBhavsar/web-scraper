#!/usr/bin/env python3
"""
Progress Manager Module

Manages progress.json operations with file locking and retry tracking.
Handles failed videos with attempt counters and permanent failure tracking.

Author: AI Assistant  
Version: 1.0
"""

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import threading

logger = logging.getLogger(__name__)


class ProgressManager:
    """Manages progress.json with thread-safe operations and retry tracking."""

    def __init__(self, progress_file: str = "progress.json"):
        """
        Initialize progress manager.

        Args:
            progress_file: Path to progress.json file
        """
        self.progress_file = Path(progress_file)
        self._lock = threading.RLock()

    def _get_default_progress(self) -> Dict[str, Any]:
        """Get default progress structure."""
        return {
            "downloaded_size": 0.0,
            "failed_videos": [],
            "permanent_failed_pages": [],
            "current_page": 1000,
            "last_page": 1000,
            "total_downloaded": 0,
            "total_size_mb": 0.0,
            "downloaded_videos": [],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def load_progress(self) -> Dict[str, Any]:
        """
        Load progress data with thread safety.

        Returns:
            Progress data dictionary
        """
        with self._lock:
            try:
                if not self.progress_file.exists():
                    logger.info("Progress file not found, creating default")
                    return self._get_default_progress()

                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # Ensure all required fields exist
                default = self._get_default_progress()
                for key, value in default.items():
                    if key not in data:
                        data[key] = value

                logger.debug(f"Progress loaded: {len(data.get('downloaded_videos', []))} videos")
                return data

            except Exception as e:
                logger.error(f"Error loading progress: {e}")
                return self._get_default_progress()

    def save_progress(self, progress_data: Dict[str, Any]) -> bool:
        """
        Save progress data using atomic write.

        Args:
            progress_data: Progress data to save

        Returns:
            True if saved successfully
        """
        with self._lock:
            try:
                # Update timestamp
                progress_data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Create parent directory if needed
                self.progress_file.parent.mkdir(parents=True, exist_ok=True)

                # Write to temporary file first
                with tempfile.NamedTemporaryFile(
                    mode='w',
                    suffix='.json',
                    prefix=f'{self.progress_file.name}.tmp.',
                    dir=self.progress_file.parent,
                    delete=False,
                    encoding='utf-8'
                ) as tmp_file:
                    json.dump(progress_data, tmp_file, indent=2, ensure_ascii=False)
                    tmp_path = tmp_file.name

                # Atomic rename
                os.replace(tmp_path, self.progress_file)
                logger.debug(f"Progress saved to {self.progress_file}")
                return True

            except Exception as e:
                logger.error(f"Error saving progress: {e}")
                # Clean up temporary file if it exists
                try:
                    if 'tmp_path' in locals():
                        os.unlink(tmp_path)
                except:
                    pass
                return False

    def get_failed_videos_for_page(self, page: int) -> List[Dict[str, Any]]:
        """
        Get failed videos for a specific page.

        Args:
            page: Page number

        Returns:
            List of failed video entries for the page
        """
        progress = self.load_progress()
        failed_videos = progress.get("failed_videos", [])

        page_failures = [
            video for video in failed_videos 
            if video.get("page") == page
        ]

        logger.debug(f"Found {len(page_failures)} failed videos for page {page}")
        return page_failures

    def record_failed_videos(self, page: int, video_ids: List[str], 
                           base_folder: str) -> bool:
        """
        Record failed videos for a page with attempt tracking.

        Args:
            page: Page number
            video_ids: List of video IDs that failed
            base_folder: Base folder path for downloads

        Returns:
            True if recorded successfully
        """
        progress = self.load_progress()
        failed_videos = progress.get("failed_videos", [])
        current_time = datetime.now().isoformat()

        # Update existing entries or create new ones
        updated_ids = set()
        for i, existing_video in enumerate(failed_videos):
            if existing_video.get("video_id") in video_ids and existing_video.get("page") == page:
                failed_videos[i]["attempts"] = existing_video.get("attempts", 0) + 1
                failed_videos[i]["last_attempt_ts"] = current_time
                updated_ids.add(existing_video["video_id"])

        # Add new failed videos
        for video_id in video_ids:
            if video_id not in updated_ids:
                failed_entry = {
                    "video_id": video_id,
                    "page": page,
                    "attempts": 1,
                    "last_attempt_ts": current_time,
                    "folder": f"{base_folder}/page_{page}/{video_id}"
                }
                failed_videos.append(failed_entry)

        progress["failed_videos"] = failed_videos
        success = self.save_progress(progress)

        if success:
            logger.info(f"Recorded {len(video_ids)} failed videos for page {page}")

        return success

    def increment_attempt(self, video_id: str, page: int) -> bool:
        """
        Increment attempt count for a specific video.

        Args:
            video_id: Video ID
            page: Page number

        Returns:
            True if incremented successfully
        """
        progress = self.load_progress()
        failed_videos = progress.get("failed_videos", [])

        for video in failed_videos:
            if video.get("video_id") == video_id and video.get("page") == page:
                video["attempts"] = video.get("attempts", 0) + 1
                video["last_attempt_ts"] = datetime.now().isoformat()
                break
        else:
            # Video not found in failed list, add it
            failed_videos.append({
                "video_id": video_id,
                "page": page,
                "attempts": 1,
                "last_attempt_ts": datetime.now().isoformat(),
                "folder": f"downloads/page_{page}/{video_id}"
            })

        progress["failed_videos"] = failed_videos
        return self.save_progress(progress)

    def mark_page_permanent_failed(self, page: int) -> bool:
        """
        Mark a page as permanently failed and clean up its failed videos.

        Args:
            page: Page number to mark as permanently failed

        Returns:
            True if marked successfully
        """
        progress = self.load_progress()

        # Add to permanent failed pages if not already there
        permanent_failed = progress.get("permanent_failed_pages", [])
        if page not in permanent_failed:
            permanent_failed.append(page)
            progress["permanent_failed_pages"] = permanent_failed

        # Remove failed videos for this page
        failed_videos = progress.get("failed_videos", [])
        progress["failed_videos"] = [
            video for video in failed_videos 
            if video.get("page") != page
        ]

        success = self.save_progress(progress)

        if success:
            logger.info(f"Marked page {page} as permanently failed")

        return success

    def remove_failed_videos_for_page(self, page: int) -> bool:
        """
        Remove failed video entries for a specific page.

        Args:
            page: Page number

        Returns:
            True if removed successfully
        """
        progress = self.load_progress()
        original_count = len(progress.get("failed_videos", []))

        progress["failed_videos"] = [
            video for video in progress.get("failed_videos", [])
            if video.get("page") != page
        ]

        removed_count = original_count - len(progress["failed_videos"])
        success = self.save_progress(progress)

        if success and removed_count > 0:
            logger.info(f"Removed {removed_count} failed videos for page {page}")

        return success

    def get_video_attempt_count(self, video_id: str, page: int) -> int:
        """
        Get attempt count for a specific video.

        Args:
            video_id: Video ID
            page: Page number

        Returns:
            Number of attempts (0 if not found)
        """
        progress = self.load_progress()
        failed_videos = progress.get("failed_videos", [])

        for video in failed_videos:
            if video.get("video_id") == video_id and video.get("page") == page:
                return video.get("attempts", 0)

        return 0

    def is_page_permanently_failed(self, page: int) -> bool:
        """
        Check if a page is marked as permanently failed.

        Args:
            page: Page number

        Returns:
            True if page is permanently failed
        """
        progress = self.load_progress()
        permanent_failed = progress.get("permanent_failed_pages", [])
        return page in permanent_failed

    def update_page_progress(self, new_page: int) -> bool:
        """
        Update the current page progress.

        Args:
            new_page: New page number

        Returns:
            True if updated successfully
        """
        progress = self.load_progress()
        progress["current_page"] = new_page
        progress["last_page"] = new_page  # Maintain backward compatibility

        return self.save_progress(progress)

    def add_completed_video(self, video_id: str, size_mb: float = 0.0) -> bool:
        """
        Add a completed video to progress.

        Args:
            video_id: Video ID that completed
            size_mb: Size of the video in MB

        Returns:
            True if added successfully
        """
        progress = self.load_progress()
        downloaded_videos = progress.get("downloaded_videos", [])

        if video_id not in downloaded_videos:
            downloaded_videos.append(video_id)
            progress["downloaded_videos"] = downloaded_videos
            progress["total_downloaded"] = len(downloaded_videos)
            progress["total_size_mb"] = progress.get("total_size_mb", 0.0) + size_mb
            progress["downloaded_size"] = progress["total_size_mb"]  # Compatibility

            # Remove from failed videos if it was there
            progress["failed_videos"] = [
                video for video in progress.get("failed_videos", [])
                if video.get("video_id") != video_id
            ]

            return self.save_progress(progress)

        return True

    def get_progress_stats(self) -> Dict[str, Any]:
        """
        Get progress statistics.

        Returns:
            Dictionary with progress statistics
        """
        progress = self.load_progress()
        failed_videos = progress.get("failed_videos", [])

        return {
            "current_page": progress.get("current_page", 1000),
            "total_downloaded": progress.get("total_downloaded", 0),
            "total_size_mb": progress.get("total_size_mb", 0.0),
            "failed_video_count": len(failed_videos),
            "permanent_failed_pages": len(progress.get("permanent_failed_pages", [])),
            "last_updated": progress.get("last_updated", "Never")
        }


if __name__ == "__main__":
    # Demo usage
    import logging
    logging.basicConfig(level=logging.INFO)

    manager = ProgressManager()
    stats = manager.get_progress_stats()
    print("Progress stats:", stats)

    # Test failed video tracking
    manager.record_failed_videos(42, ["video1", "video2"], "downloads")
    failed = manager.get_failed_videos_for_page(42)
    print(f"Failed videos for page 42: {len(failed)}")
