
"""
Progress Tracker Module

Handles reading and writing to progress.json and config.json files.
Manages downloading progress, storage limits, and starting strategy.

Author: AI Assistant 
Date: September 2025
"""

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class ProgressTracker:
    """
    Tracks and manages scraping progress, storage limits, and downloaded videos.
    """

    def __init__(self, progress_file: str = "progress.json", config_file: str = "config.json"):
        """
        Initialize the progress tracker.

        Args:
            progress_file: Path to progress.json file
            config_file: Path to config.json file
        """
        self.progress_file = Path(progress_file)
        self.config_file = Path(config_file)
        self.progress_data = {}
        self.config_data = {}

        # Load existing data
        self.load_progress()
        self.load_config()

        # Initialize default progress structure if needed
        self._ensure_progress_structure()

        print(f"📊 Progress Tracker initialized")
        print(f"📁 Progress file: {self.progress_file}")
        print(f"⚙️ Config file: {self.config_file}")

    def _ensure_progress_structure(self):
        """Ensure progress.json has all required fields."""
        default_structure = {
            "last_video_id": "",
            "last_page": 0,
            "total_downloaded": 0,
            "total_size_mb": 0,
            "downloaded_videos": [],
            "failed_videos": [],
            "video_failures": {}
        }

        for key, value in default_structure.items():
            if key not in self.progress_data:
                self.progress_data[key] = value

    def load_progress(self) -> Dict:
        """Load progress data from progress.json."""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    self.progress_data = json.load(f)
                print(f"✅ Loaded progress data: {len(self.progress_data.get('downloaded_videos', []))} downloaded videos")
            else:
                self.progress_data = {}
                print("📝 Creating new progress file")
        except Exception as e:
            print(f"❌ Error loading progress: {e}")
            self.progress_data = {}

        return self.progress_data

    def load_config(self) -> Dict:
        """Load configuration data from config.json."""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config_data = json.load(f)
                print(f"✅ Loaded config data")
            else:
                print("❌ Config file not found")
                self.config_data = {}
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            self.config_data = {}

        return self.config_data

    def save_progress(self):
        """Save current progress data to progress.json."""
        try:
            # Create backup of existing file
            if self.progress_file.exists():
                backup_file = self.progress_file.with_suffix('.json.backup')
                import shutil
                shutil.copy2(self.progress_file, backup_file)

            # Save new data
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False)

            # Print current status
            self._print_storage_status()

        except Exception as e:
            print(f"❌ Error saving progress: {e}")

    def save_config(self):
        """Save current config data to config.json."""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config_data, f, indent=2, ensure_ascii=False)
            print("✅ Config saved successfully")
        except Exception as e:
            print(f"❌ Error saving config: {e}")

    def _print_storage_status(self):
        """Print current storage status after each write."""
        current_size_mb = self.progress_data.get('total_size_mb', 0)
        max_size_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)
        max_size_mb = max_size_gb * 1024  # Convert GB to MB

        current_size_gb = current_size_mb / 1024
        usage_percentage = (current_size_mb / max_size_mb * 100) if max_size_mb > 0 else 0

        print(f"💾 Progress saved!")
        print(f"📊 Current size: {current_size_gb:.2f} GB ({current_size_mb:.0f} MB)")
        print(f"📈 Max size: {max_size_gb:.0f} GB ({max_size_mb:.0f} MB)")
        print(f"📉 Usage: {usage_percentage:.1f}%")
        print(f"🎯 Downloaded videos: {len(self.progress_data.get('downloaded_videos', []))}")

        # Warning messages
        if usage_percentage >= 90:
            print(f"⚠️ WARNING: Storage usage is at {usage_percentage:.1f}%!")
        elif usage_percentage >= 75:
            print(f"🟡 Notice: Storage usage is at {usage_percentage:.1f}%")

    def add_downloaded_video(self, video_id: str, size_mb: float = 0):
        """
        Add a video to the downloaded list and update size.

        Args:
            video_id: Video ID to add
            size_mb: Size of downloaded video in MB
        """
        if video_id not in self.progress_data.get('downloaded_videos', []):
            if 'downloaded_videos' not in self.progress_data:
                self.progress_data['downloaded_videos'] = []

            self.progress_data['downloaded_videos'].append(video_id)
            self.progress_data['total_downloaded'] = len(self.progress_data['downloaded_videos'])
            self.progress_data['total_size_mb'] = self.progress_data.get('total_size_mb', 0) + size_mb
            self.progress_data['last_video_id'] = video_id

            # Save after each addition
            self.save_progress()

            print(f"✅ Added video {video_id} ({size_mb:.1f} MB)")

    def is_video_downloaded(self, video_id: str) -> bool:
        """Check if a video is already downloaded."""
        return video_id in self.progress_data.get('downloaded_videos', [])

    def get_downloaded_videos(self) -> List[str]:
        """Get list of all downloaded video IDs."""
        return self.progress_data.get('downloaded_videos', [])

    def get_last_page(self) -> int:
        """Get the last processed page number."""
        return self.progress_data.get('last_page', 0)

    def update_last_page(self, page_num: int):
        """Update the last processed page number."""
        self.progress_data['last_page'] = page_num
        self.save_progress()

    def determine_starting_page(self) -> Tuple[int, str]:
        """
        Determine which page to start scraping from.

        Returns:
            Tuple of (page_number, strategy_description)
        """
        downloaded_videos = self.get_downloaded_videos()
        last_page = self.get_last_page()

        print("🔍 Determining starting strategy...")
        print("=" * 60)

        if not downloaded_videos or len(downloaded_videos) == 0:
            # No downloads found - start fresh from last page
            strategy = "FRESH_START"
            # For now, we'll use a default last page since we can't access the website
            # In real implementation, you'd get this from website
            starting_page = 9586  # Based on progress.json, increment by 1

            print(f"📋 Status: No downloads found in progress.json")
            print(f"🆕 Strategy: {strategy}")
            print(f"📄 Starting page: {starting_page} (will go backwards)")
            print(f"💡 Reason: Starting fresh scrape from the last page")
        else:
            # Downloads exist - resume from last page
            strategy = "RESUME"
            starting_page = last_page if last_page > 0 else 9585

            print(f"📋 Status: Found {len(downloaded_videos)} existing downloads")
            print(f"🔄 Strategy: {strategy}")
            print(f"📄 Starting page: {starting_page}")
            print(f"💡 Reason: Resuming from last processed page")

        print("=" * 60)
        return starting_page, strategy

    def check_storage_limit(self) -> Tuple[bool, float, float]:
        """
        Check if storage limit has been exceeded.

        Returns:
            Tuple of (limit_exceeded, current_size_gb, max_size_gb)
        """
        current_size_mb = self.progress_data.get('total_size_mb', 0)
        current_size_gb = current_size_mb / 1024

        max_size_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)
        max_size_mb = max_size_gb * 1024

        limit_exceeded = current_size_mb >= max_size_mb

        if limit_exceeded:
            print(f"🚫 STORAGE LIMIT EXCEEDED!")
            print(f"📊 Current: {current_size_gb:.2f} GB")
            print(f"📈 Maximum: {max_size_gb:.2f} GB")
            print(f"⛔ Parser should be stopped!")

        return limit_exceeded, current_size_gb, max_size_gb

    def add_failed_video(self, video_id: str, error_reason: str = "Unknown error"):
        """Add a video to the failed list."""
        if 'failed_videos' not in self.progress_data:
            self.progress_data['failed_videos'] = []
        if 'video_failures' not in self.progress_data:
            self.progress_data['video_failures'] = {}

        if video_id not in self.progress_data['failed_videos']:
            self.progress_data['failed_videos'].append(video_id)

        self.progress_data['video_failures'][video_id] = {
            'error': error_reason,
            'timestamp': int(time.time())
        }

        self.save_progress()
        print(f"❌ Added failed video {video_id}: {error_reason}")

    def get_config_value(self, *keys, default=None):
        """
        Get a nested configuration value.

        Args:
            *keys: Nested keys to traverse (e.g., 'general', 'max_storage_gb')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        value = self.config_data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def get_download_path(self) -> str:
        """Get the configured download path."""
        return self.get_config_value('general', 'download_path', default='C:\\scraper_downloads\\')

    def get_max_storage_gb(self) -> float:
        """Get the maximum storage limit in GB."""
        return self.get_config_value('general', 'max_storage_gb', default=940)

    def print_summary(self):
        """Print a summary of current progress and configuration."""
        print("\n" + "=" * 80)
        print("📊 PROGRESS TRACKER SUMMARY")
        print("=" * 80)

        # Progress info
        downloaded_count = len(self.get_downloaded_videos())
        failed_count = len(self.progress_data.get('failed_videos', []))
        last_page = self.get_last_page()
        current_size_mb = self.progress_data.get('total_size_mb', 0)

        print(f"📈 Downloaded videos: {downloaded_count}")
        print(f"❌ Failed videos: {failed_count}")
        print(f"📄 Last processed page: {last_page}")
        print(f"💾 Total size: {current_size_mb / 1024:.2f} GB ({current_size_mb:.0f} MB)")

        # Config info
        max_storage = self.get_max_storage_gb()
        download_path = self.get_download_path()

        print(f"📂 Download path: {download_path}")
        print(f"📊 Max storage limit: {max_storage:.0f} GB")

        # Storage status
        usage_percentage = (current_size_mb / (max_storage * 1024) * 100) if max_storage > 0 else 0
        print(f"📉 Storage usage: {usage_percentage:.1f}%")

        # Determine starting strategy
        starting_page, strategy = self.determine_starting_page()
        print(f"🚀 Starting strategy: {strategy} from page {starting_page}")

        print("=" * 80)


# Test the implementation
if __name__ == "__main__":
    print("🧪 Testing Progress Tracker...")

    # Create tracker instance
    tracker = ProgressTracker()

    # Show current summary
    tracker.print_summary()

    # Test storage check
    limit_exceeded, current_gb, max_gb = tracker.check_storage_limit()

    if limit_exceeded:
        print("🚫 Storage limit exceeded - would stop parser")
    else:
        print("✅ Storage limit OK - can continue parsing")

    print("\n🎯 Progress Tracker test completed!")
