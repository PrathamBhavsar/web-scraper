#!/usr/bin/env python3

"""
Duplicate Detection System for Rule34Video Scraper

This module provides video ID duplicate detection that works ONLY on the first page
of each scraping session to avoid re-parsing already downloaded videos.

Features:
- Checks first 100 video IDs from progress.json downloaded_videos
- Only applies on the first page being processed
- Linear dependency structure with separate classes
- Integration with existing continuous scraper

Author: AI Assistant
Version: 1.0 - Duplicate detection system
"""

import json
import os
from pathlib import Path
from typing import Set, List, Dict, Any, Optional
from datetime import datetime


class ProgressReader:
    """
    Handles reading and parsing progress.json data.

    Dependency: None (base class)
    """

    def __init__(self, progress_file: str = "progress.json"):
        """
        Initialize progress reader.

        Args:
            progress_file: Path to progress.json file
        """
        self.progress_file = Path(progress_file)

    def read_progress_data(self) -> Dict[str, Any]:
        """
        Read and parse progress.json file.

        Returns:
            Progress data dictionary
        """
        try:
            if not self.progress_file.exists():
                print(f"📄 Progress file not found: {self.progress_file}")
                return {
                    "last_page": 1000,
                    "downloaded_videos": [],
                    "total_downloaded": 0,
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Ensure required fields exist
            if "downloaded_videos" not in data:
                data["downloaded_videos"] = []

            print(f"✅ Progress data loaded: {len(data.get('downloaded_videos', []))} downloaded videos")
            return data

        except (json.JSONDecodeError, Exception) as e:
            print(f"❌ Error reading progress file: {e}")
            return {
                "last_page": 1000,
                "downloaded_videos": [],
                "total_downloaded": 0,
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

    def get_downloaded_video_ids(self) -> List[str]:
        """
        Get list of already downloaded video IDs.

        Returns:
            List of video IDs that have been downloaded
        """
        progress_data = self.read_progress_data()
        downloaded_videos = progress_data.get("downloaded_videos", [])

        print(f"📊 Total downloaded video IDs: {len(downloaded_videos)}")
        return downloaded_videos

    def get_last_processed_page(self) -> int:
        """
        Get the last processed page from progress.json.

        Returns:
            Last processed page number
        """
        progress_data = self.read_progress_data()
        return progress_data.get("last_page", 1000)


class DuplicateChecker:
    """
    Handles duplicate detection logic using progress data.

    Dependency: ProgressReader
    """

    def __init__(self, progress_reader: ProgressReader, check_limit: int = 100):
        """
        Initialize duplicate checker.

        Args:
            progress_reader: ProgressReader instance
            check_limit: Maximum number of recent video IDs to check (default: 100)
        """
        self.progress_reader = progress_reader
        self.check_limit = check_limit
        self.duplicate_check_set: Set[str] = set()
        self._load_duplicate_check_set()

    def _load_duplicate_check_set(self):
        """
        Load the first N video IDs into a set for fast duplicate checking.
        """
        downloaded_videos = self.progress_reader.get_downloaded_video_ids()

        # Take only the first check_limit entries (most recent downloads)
        recent_videos = downloaded_videos[:self.check_limit]
        self.duplicate_check_set = set(recent_videos)

        print(f"🔍 Duplicate check set loaded: {len(self.duplicate_check_set)} video IDs")
        print(f"   📝 Checking against first {self.check_limit} entries from progress.json")

        if self.duplicate_check_set:
            print(f"   🎬 Sample IDs: {list(self.duplicate_check_set)[:5]}...")

    def is_duplicate(self, video_id: str) -> bool:
        """
        Check if a video ID is already downloaded.

        Args:
            video_id: Video ID to check

        Returns:
            True if video is already downloaded, False otherwise
        """
        is_dup = video_id in self.duplicate_check_set
        if is_dup:
            print(f"   🚫 DUPLICATE: Video ID {video_id} already downloaded, skipping...")

        return is_dup

    def get_check_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the duplicate checking system.

        Returns:
            Dictionary with duplicate checker statistics
        """
        return {
            "check_limit": self.check_limit,
            "loaded_ids": len(self.duplicate_check_set),
            "sample_ids": list(self.duplicate_check_set)[:10]
        }


class FirstPageDetector:
    """
    Detects if the current page is the first page being processed in this session.

    Dependency: ProgressReader
    """

    def __init__(self, progress_reader: ProgressReader):
        """
        Initialize first page detector.

        Args:
            progress_reader: ProgressReader instance
        """
        self.progress_reader = progress_reader
        self.session_start_page: Optional[int] = None
        self.first_page_processed = False

    def initialize_session(self):
        """
        Initialize the scraping session by recording the starting page.
        """
        self.session_start_page = self.progress_reader.get_last_processed_page()
        self.first_page_processed = False

        print(f"📍 Session initialized - Starting page: {self.session_start_page}")

    def is_first_page(self, current_page: int) -> bool:
        """
        Check if the current page is the first page of this session.

        Args:
            current_page: Current page being processed

        Returns:
            True if this is the first page, False otherwise
        """
        if self.session_start_page is None:
            self.initialize_session()

        # First page is the one we started with AND we haven't processed any page yet
        is_first = (current_page == self.session_start_page) and (not self.first_page_processed)

        if is_first:
            print(f"🎯 FIRST PAGE DETECTED: Page {current_page}")
            print(f"   📋 Duplicate checking will be ENABLED for this page")
            self.first_page_processed = True
        else:
            print(f"📄 Regular page: Page {current_page}")
            print(f"   📋 Duplicate checking will be DISABLED for this page")

        return is_first


class VideoFilterProcessor:
    """
    Processes and filters video data based on duplicate detection.

    Dependencies: DuplicateChecker, FirstPageDetector
    """

    def __init__(self, duplicate_checker: DuplicateChecker, first_page_detector: FirstPageDetector):
        """
        Initialize video filter processor.

        Args:
            duplicate_checker: DuplicateChecker instance
            first_page_detector: FirstPageDetector instance
        """
        self.duplicate_checker = duplicate_checker
        self.first_page_detector = first_page_detector
        self.stats = {
            "videos_processed": 0,
            "videos_filtered": 0,
            "videos_allowed": 0,
            "duplicate_checks_enabled": 0,
            "duplicate_checks_disabled": 0
        }

    def should_process_video(self, video_id: str, current_page: int) -> bool:
        """
        Determine if a video should be processed based on duplicate detection rules.

        Args:
            video_id: Video ID to check
            current_page: Current page being processed

        Returns:
            True if video should be processed, False if it should be skipped
        """
        self.stats["videos_processed"] += 1

        # Check if this is the first page
        is_first_page = self.first_page_detector.is_first_page(current_page)

        if is_first_page:
            # Apply duplicate checking only on first page
            self.stats["duplicate_checks_enabled"] += 1

            if self.duplicate_checker.is_duplicate(video_id):
                self.stats["videos_filtered"] += 1
                print(f"   ⏭️  Skipping duplicate video ID: {video_id}")
                return False
            else:
                self.stats["videos_allowed"] += 1
                print(f"   ✅ Processing new video ID: {video_id}")
                return True
        else:
            # No duplicate checking on subsequent pages
            self.stats["duplicate_checks_disabled"] += 1
            self.stats["videos_allowed"] += 1
            print(f"   ✅ Processing video ID (no dup check): {video_id}")
            return True

    def filter_video_list(self, video_data_list: List[Dict[str, Any]], current_page: int) -> List[Dict[str, Any]]:
        """
        Filter a list of video data based on duplicate detection rules.

        Args:
            video_data_list: List of video data dictionaries
            current_page: Current page being processed

        Returns:
            Filtered list of video data
        """
        print(f"\n🔍 Filtering {len(video_data_list)} videos for page {current_page}")
        print("-" * 60)

        filtered_videos = []

        for video_data in video_data_list:
            video_id = video_data.get("video_id", "unknown")

            if self.should_process_video(video_id, current_page):
                filtered_videos.append(video_data)

        original_count = len(video_data_list)
        filtered_count = len(filtered_videos)
        skipped_count = original_count - filtered_count

        print("-" * 60)
        print(f"📊 Filtering Results:")
        print(f"   📥 Original videos: {original_count}")
        print(f"   ✅ Videos to process: {filtered_count}")
        print(f"   ⏭️  Videos skipped: {skipped_count}")
        print(f"   📈 Skip percentage: {(skipped_count/original_count*100):.1f}%" if original_count > 0 else "   📈 Skip percentage: 0%")

        return filtered_videos

    def get_session_stats(self) -> Dict[str, Any]:
        """
        Get processing statistics for the current session.

        Returns:
            Dictionary with processing statistics
        """
        return {
            **self.stats,
            "duplicate_checker_stats": self.duplicate_checker.get_check_stats(),
            "session_start_page": self.first_page_detector.session_start_page,
            "first_page_processed": self.first_page_detector.first_page_processed
        }

    def reset_stats(self):
        """Reset processing statistics."""
        self.stats = {
            "videos_processed": 0,
            "videos_filtered": 0,
            "videos_allowed": 0,
            "duplicate_checks_enabled": 0,
            "duplicate_checks_disabled": 0
        }


class DuplicateDetectionManager:
    """
    Main manager class that coordinates all duplicate detection components.

    Dependencies: All above classes
    """

    def __init__(self, progress_file: str = "progress.json", check_limit: int = 100):
        """
        Initialize duplicate detection manager.

        Args:
            progress_file: Path to progress.json file
            check_limit: Maximum number of recent video IDs to check
        """
        print(f"🔧 Initializing Duplicate Detection Manager")
        print(f"   📄 Progress file: {progress_file}")
        print(f"   🔍 Check limit: {check_limit} video IDs")

        # Initialize components in dependency order
        self.progress_reader = ProgressReader(progress_file)
        self.duplicate_checker = DuplicateChecker(self.progress_reader, check_limit)
        self.first_page_detector = FirstPageDetector(self.progress_reader)
        self.video_filter = VideoFilterProcessor(self.duplicate_checker, self.first_page_detector)

        print(f"✅ Duplicate Detection Manager initialized")

    def initialize_session(self):
        """
        Initialize a new scraping session.
        """
        print("\n🚀 Initializing new scraping session...")
        self.first_page_detector.initialize_session()
        self.video_filter.reset_stats()
        print("✅ Session initialized")

    def filter_videos_for_page(self, video_data_list: List[Dict[str, Any]], current_page: int) -> List[Dict[str, Any]]:
        """
        Filter videos for a specific page.

        Args:
            video_data_list: List of video data dictionaries
            current_page: Current page being processed

        Returns:
            Filtered list of video data
        """
        return self.video_filter.filter_video_list(video_data_list, current_page)

    def should_process_video(self, video_id: str, current_page: int) -> bool:
        """
        Check if a single video should be processed.

        Args:
            video_id: Video ID to check
            current_page: Current page being processed

        Returns:
            True if video should be processed
        """
        return self.video_filter.should_process_video(video_id, current_page)

    def get_session_summary(self) -> Dict[str, Any]:
        """
        Get summary of the current session.

        Returns:
            Session summary dictionary
        """
        return self.video_filter.get_session_stats()

    def print_session_summary(self):
        """
        Print a formatted session summary.
        """
        stats = self.get_session_summary()

        print("\n" + "="*70)
        print("📋 DUPLICATE DETECTION SESSION SUMMARY")
        print("="*70)
        print(f"📄 Session start page: {stats['session_start_page']}")
        print(f"🎯 First page processed: {stats['first_page_processed']}")
        print(f"📊 Videos processed: {stats['videos_processed']}")
        print(f"✅ Videos allowed: {stats['videos_allowed']}")
        print(f"⏭️  Videos filtered: {stats['videos_filtered']}")
        print(f"🔍 Pages with duplicate checking: {stats['duplicate_checks_enabled']}")
        print(f"📄 Pages without duplicate checking: {stats['duplicate_checks_disabled']}")

        dup_stats = stats['duplicate_checker_stats']
        print(f"\n🎬 Duplicate Checker:")
        print(f"   📝 Check limit: {dup_stats['check_limit']}")
        print(f"   💾 Loaded IDs: {dup_stats['loaded_ids']}")
        print(f"   🎯 Sample IDs: {dup_stats['sample_ids'][:5]}")
        print("="*70)


# Example usage and testing
def test_duplicate_detection():
    """
    Test function to demonstrate the duplicate detection system.
    """
    print("🧪 Testing Duplicate Detection System")
    print("="*60)

    # Initialize manager
    manager = DuplicateDetectionManager()
    manager.initialize_session()

    # Sample video data
    sample_videos = [
        {"video_id": "23443", "title": "Video 1"},  # This should be filtered (in progress.json)
        {"video_id": "99999", "title": "Video 2"},  # This should pass
        {"video_id": "88888", "title": "Video 3"},  # This should pass
    ]

    # Test first page (should apply duplicate checking)
    print("\n🧪 Testing first page (997)...")
    filtered_first = manager.filter_videos_for_page(sample_videos, 997)

    # Test second page (should NOT apply duplicate checking)
    print("\n🧪 Testing second page (996)...")
    filtered_second = manager.filter_videos_for_page(sample_videos, 996)

    # Print summary
    manager.print_session_summary()


if __name__ == "__main__":
    test_duplicate_detection()
