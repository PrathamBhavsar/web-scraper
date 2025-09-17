#!/usr/bin/env python3

"""
Enhanced Progress Tracking System

This system monitors the downloads folder, tracks completed videos,
and updates progress.json with accurate information about:
- Completed video downloads
- Total download folder size
- Downloaded video IDs
- Failed downloads

Key Features:
- Monitors downloads folder for completed videos
- Updates progress.json ONLY after videos are actually downloaded
- Calculates actual folder sizes
- Tracks completion status accurately
- Does NOT update before IDM queue addition

Author: AI Assistant
Version: 1.0 - Progress tracking with download monitoring
"""

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
import hashlib


class DownloadFolderMonitor:
    """
    Monitors the downloads folder to track completed video downloads.
    """

    def __init__(self, downloads_dir: str = "downloads"):
        """
        Initialize download folder monitor.

        Args:
            downloads_dir: Path to downloads directory
        """
        self.downloads_dir = Path(downloads_dir)
        self.required_files = [".json", ".mp4", ".jpg"]  # Required files for completion

        print(f"🔍 Download Folder Monitor Initialized")
        print(f"   📁 Monitoring directory: {self.downloads_dir}")
        print(f"   📋 Required files for completion: {self.required_files}")

    def get_folder_size_mb(self) -> float:
        """
        Calculate total size of downloads folder in MB.

        Returns:
            Total folder size in megabytes
        """
        try:
            if not self.downloads_dir.exists():
                return 0.0

            total_size = 0
            for root, dirs, files in os.walk(self.downloads_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        total_size += os.path.getsize(file_path)
                    except (OSError, FileNotFoundError):
                        continue

            size_mb = total_size / (1024 * 1024)
            print(f"📊 Calculated folder size: {size_mb:.2f} MB ({total_size:,} bytes)")
            return size_mb

        except Exception as e:
            print(f"❌ Error calculating folder size: {e}")
            return 0.0

    def get_video_folders(self) -> List[Dict[str, Any]]:
        """
        Get all video folders in downloads directory.

        Returns:
            List of video folder information
        """
        video_folders = []

        try:
            if not self.downloads_dir.exists():
                print(f"📁 Downloads directory does not exist: {self.downloads_dir}")
                return video_folders

            for item in self.downloads_dir.iterdir():
                if item.is_dir():
                    folder_info = self._analyze_video_folder(item)
                    if folder_info:
                        video_folders.append(folder_info)

            print(f"📁 Found {len(video_folders)} video folders")
            return video_folders

        except Exception as e:
            print(f"❌ Error getting video folders: {e}")
            return video_folders

    def _analyze_video_folder(self, folder_path: Path) -> Optional[Dict[str, Any]]:
        """
        Analyze a video folder to determine completion status.

        Args:
            folder_path: Path to video folder

        Returns:
            Video folder information or None if invalid
        """
        try:
            folder_name = folder_path.name
            files_in_folder = list(folder_path.glob("*"))

            # Check for required file types
            has_json = any(f.suffix.lower() == ".json" for f in files_in_folder)
            has_video = any(f.suffix.lower() == ".mp4" for f in files_in_folder)
            has_thumbnail = any(f.suffix.lower() == ".jpg" for f in files_in_folder)

            # Determine completion status
            is_complete = has_json and has_video and has_thumbnail

            # Calculate folder size
            folder_size = 0
            for file_path in files_in_folder:
                try:
                    if file_path.is_file():
                        folder_size += file_path.stat().st_size
                except:
                    continue

            folder_size_mb = folder_size / (1024 * 1024)

            # Extract video ID from folder name (assuming folder name is video ID)
            video_id = folder_name

            # Read metadata if available
            title = "Unknown"
            json_files = [f for f in files_in_folder if f.suffix.lower() == ".json"]
            if json_files:
                try:
                    with open(json_files[0], 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                        title = metadata.get("title", "Unknown")
                        video_id = metadata.get("video_id", folder_name)
                except:
                    pass

            return {
                "video_id": video_id,
                "folder_name": folder_name,
                "folder_path": str(folder_path),
                "title": title,
                "is_complete": is_complete,
                "has_json": has_json,
                "has_video": has_video,
                "has_thumbnail": has_thumbnail,
                "file_count": len(files_in_folder),
                "folder_size_mb": folder_size_mb,
                "files": [f.name for f in files_in_folder]
            }

        except Exception as e:
            print(f"❌ Error analyzing folder {folder_path}: {e}")
            return None

    def get_completed_video_ids(self) -> List[str]:
        """
        Get list of video IDs that have completed downloads.

        Returns:
            List of completed video IDs
        """
        video_folders = self.get_video_folders()
        completed_ids = [folder["video_id"] for folder in video_folders if folder["is_complete"]]

        print(f"✅ Found {len(completed_ids)} completed downloads")
        if completed_ids:
            print(f"   📝 Sample completed IDs: {completed_ids[:5]}...")

        return completed_ids

    def get_failed_video_ids(self) -> List[str]:
        """
        Get list of video IDs that have failed or incomplete downloads.

        Returns:
            List of failed/incomplete video IDs
        """
        video_folders = self.get_video_folders()
        failed_ids = [folder["video_id"] for folder in video_folders if not folder["is_complete"]]

        print(f"❌ Found {len(failed_ids)} failed/incomplete downloads")
        if failed_ids:
            print(f"   📝 Sample failed IDs: {failed_ids[:5]}...")

        return failed_ids

    def get_download_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive download statistics.

        Returns:
            Dictionary with download statistics
        """
        video_folders = self.get_video_folders()
        total_folders = len(video_folders)
        completed_folders = len([f for f in video_folders if f["is_complete"]])
        failed_folders = total_folders - completed_folders
        total_size_mb = self.get_folder_size_mb()

        return {
            "total_folders": total_folders,
            "completed_folders": completed_folders,
            "failed_folders": failed_folders,
            "completion_rate": (completed_folders / total_folders * 100) if total_folders > 0 else 0,
            "total_size_mb": total_size_mb,
            "video_folders": video_folders
        }


class ProgressUpdater:
    """
    Updates progress.json with accurate download information.
    """

    def __init__(self, progress_file: str = "progress.json", downloads_dir: str = "downloads"):
        """
        Initialize progress updater.

        Args:
            progress_file: Path to progress.json file
            downloads_dir: Path to downloads directory
        """
        self.progress_file = Path(progress_file)
        self.monitor = DownloadFolderMonitor(downloads_dir)

        print(f"💾 Progress Updater Initialized")
        print(f"   📄 Progress file: {self.progress_file}")
        print(f"   📁 Downloads directory: {self.monitor.downloads_dir}")

    def read_current_progress(self) -> Dict[str, Any]:
        """
        Read current progress.json data.

        Returns:
            Current progress data
        """
        try:
            if not self.progress_file.exists():
                return self._create_default_progress()

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Ensure required fields exist
            required_fields = {
                "last_page": 1000,
                "total_downloaded": 0,
                "total_size_mb": 0.0,
                "downloaded_videos": [],
                "failed_videos": [],
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            for key, default_value in required_fields.items():
                if key not in data:
                    data[key] = default_value

            return data

        except Exception as e:
            print(f"❌ Error reading progress file: {e}")
            return self._create_default_progress()

    def _create_default_progress(self) -> Dict[str, Any]:
        """Create default progress data."""
        return {
            "last_page": 1000,
            "total_downloaded": 0,
            "total_size_mb": 0.0,
            "downloaded_videos": [],
            "failed_videos": [],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def update_progress_from_downloads(self) -> Dict[str, Any]:
        """
        Update progress.json based on actual download folder contents.

        Returns:
            Updated progress data
        """
        print("\n🔄 Updating progress from actual download folder...")
        print("-" * 60)

        # Read current progress
        current_progress = self.read_current_progress()

        # Get download statistics
        download_stats = self.monitor.get_download_statistics()
        completed_video_ids = self.monitor.get_completed_video_ids()
        failed_video_ids = self.monitor.get_failed_video_ids()

        # Update progress with actual data
        updated_progress = current_progress.copy()
        updated_progress.update({
            "total_downloaded": download_stats["completed_folders"],
            "total_size_mb": download_stats["total_size_mb"],
            "downloaded_videos": completed_video_ids,
            "failed_videos": failed_video_ids,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Save updated progress
        self.save_progress(updated_progress)

        # Show update summary
        print("📊 Progress Update Summary:")
        print(f"   📁 Total folders: {download_stats['total_folders']}")
        print(f"   ✅ Completed downloads: {download_stats['completed_folders']}")
        print(f"   ❌ Failed downloads: {download_stats['failed_folders']}")
        print(f"   📈 Completion rate: {download_stats['completion_rate']:.1f}%")
        print(f"   💾 Total size: {download_stats['total_size_mb']:.2f} MB")
        print(f"   🎬 Video IDs updated: {len(completed_video_ids)}")
        print("-" * 60)

        return updated_progress

    def save_progress(self, progress_data: Dict[str, Any]) -> bool:
        """
        Save progress data to progress.json.

        Args:
            progress_data: Progress data to save

        Returns:
            True if saved successfully
        """
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)

            print(f"💾 Progress saved to {self.progress_file}")
            return True

        except Exception as e:
            print(f"❌ Error saving progress: {e}")
            return False

    def add_video_to_progress(self, video_id: str, force_check: bool = True) -> bool:
        """
        Add a specific video ID to progress if it's actually downloaded.

        Args:
            video_id: Video ID to add
            force_check: Whether to verify the download exists

        Returns:
            True if added successfully
        """
        if force_check:
            completed_ids = self.monitor.get_completed_video_ids()
            if video_id not in completed_ids:
                print(f"⚠️  Video {video_id} not found in completed downloads")
                return False

        current_progress = self.read_current_progress()

        if video_id not in current_progress["downloaded_videos"]:
            current_progress["downloaded_videos"].append(video_id)
            current_progress["total_downloaded"] = len(current_progress["downloaded_videos"])
            current_progress["total_size_mb"] = self.monitor.get_folder_size_mb()
            current_progress["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.save_progress(current_progress)
            print(f"✅ Added video {video_id} to progress")
            return True
        else:
            print(f"ℹ️  Video {video_id} already in progress")
            return True

    def update_page_progress(self, new_page: int) -> bool:
        """
        Update the last_page in progress.json.

        Args:
            new_page: New page number

        Returns:
            True if updated successfully
        """
        current_progress = self.read_current_progress()
        current_progress["last_page"] = new_page
        current_progress["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return self.save_progress(current_progress)


class EnhancedProgressTracker:
    """
    Main progress tracking coordinator that integrates folder monitoring
    with progress.json updates.
    """

    def __init__(self, progress_file: str = "progress.json", downloads_dir: str = "downloads"):
        """
        Initialize enhanced progress tracker.

        Args:
            progress_file: Path to progress.json file
            downloads_dir: Path to downloads directory
        """
        self.progress_file = progress_file
        self.downloads_dir = downloads_dir
        self.updater = ProgressUpdater(progress_file, downloads_dir)

        print(f"🚀 Enhanced Progress Tracker Initialized")
        print(f"   📄 Progress file: {progress_file}")
        print(f"   📁 Downloads directory: {downloads_dir}")

    def sync_progress_with_downloads(self) -> Dict[str, Any]:
        """
        Synchronize progress.json with actual download folder contents.

        Returns:
            Updated progress data
        """
        print("\n🔄 SYNCHRONIZING PROGRESS WITH DOWNLOADS")
        print("=" * 70)

        updated_progress = self.updater.update_progress_from_downloads()

        print("✅ Progress synchronization completed")
        print("=" * 70)

        return updated_progress

    def update_after_page_completion(self, page: int) -> Dict[str, Any]:
        """
        Update progress after completing a page (monitors downloads and updates).

        Args:
            page: Page number that was completed

        Returns:
            Updated progress data
        """
        print(f"\n📄 Updating progress after page {page} completion...")

        # First sync with actual downloads
        updated_progress = self.sync_progress_with_downloads()

        # Then update page
        self.updater.update_page_progress(page - 1)  # Move to next page (backwards)

        return updated_progress

    def get_progress_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive progress summary.

        Returns:
            Progress summary with download statistics
        """
        current_progress = self.updater.read_current_progress()
        download_stats = self.updater.monitor.get_download_statistics()

        return {
            "progress_file_data": current_progress,
            "download_folder_stats": download_stats,
            "sync_needed": len(current_progress["downloaded_videos"]) != download_stats["completed_folders"]
        }

    def verify_and_fix_progress(self) -> Dict[str, Any]:
        """
        Verify progress.json accuracy and fix any discrepancies.

        Returns:
            Verification and fix results
        """
        print("\n🔍 VERIFYING PROGRESS ACCURACY")
        print("=" * 50)

        current_progress = self.updater.read_current_progress()
        download_stats = self.updater.monitor.get_download_statistics()

        # Check for discrepancies
        progress_video_count = len(current_progress["downloaded_videos"])
        actual_completed_count = download_stats["completed_folders"]
        progress_size = current_progress["total_size_mb"]
        actual_size = download_stats["total_size_mb"]

        print(f"📊 Verification Results:")
        print(f"   📄 Progress file videos: {progress_video_count}")
        print(f"   📁 Actual completed videos: {actual_completed_count}")
        print(f"   📄 Progress file size: {progress_size:.2f} MB")
        print(f"   📁 Actual folder size: {actual_size:.2f} MB")

        needs_fix = (
            progress_video_count != actual_completed_count or
            abs(progress_size - actual_size) > 0.1  # Allow small rounding differences
        )

        if needs_fix:
            print("⚠️  Discrepancies found - fixing progress...")
            fixed_progress = self.sync_progress_with_downloads()
            print("✅ Progress fixed and synchronized")

            return {
                "verification_passed": False,
                "discrepancies_found": True,
                "fixed": True,
                "updated_progress": fixed_progress
            }
        else:
            print("✅ Progress file is accurate")

            return {
                "verification_passed": True,
                "discrepancies_found": False,
                "fixed": False,
                "current_progress": current_progress
            }


def main():
    """
    Demonstration of the enhanced progress tracking system.
    """
    print("🚀 Enhanced Progress Tracking System")
    print("=" * 60)
    print("🔧 Features:")
    print("   - Monitors downloads folder for completed videos")
    print("   - Updates progress.json with actual download data")
    print("   - Calculates real folder sizes")
    print("   - Tracks completion status accurately")
    print("   - Verifies and fixes progress discrepancies")
    print("=" * 60)

    # Initialize tracker
    tracker = EnhancedProgressTracker()

    # Show current progress summary
    print("\n📊 Current Progress Summary:")
    summary = tracker.get_progress_summary()

    progress_data = summary["progress_file_data"]
    download_stats = summary["download_folder_stats"]

    print(f"📄 Progress File:")
    print(f"   Last page: {progress_data['last_page']}")
    print(f"   Total downloaded: {progress_data['total_downloaded']}")
    print(f"   Total size: {progress_data['total_size_mb']:.2f} MB")
    print(f"   Downloaded videos: {len(progress_data['downloaded_videos'])}")

    print(f"\n📁 Download Folder:")
    print(f"   Total folders: {download_stats['total_folders']}")
    print(f"   Completed folders: {download_stats['completed_folders']}")
    print(f"   Failed folders: {download_stats['failed_folders']}")
    print(f"   Actual size: {download_stats['total_size_mb']:.2f} MB")

    if summary["sync_needed"]:
        print("\n⚠️  Synchronization needed!")
        choice = input("\n🤔 Synchronize progress with downloads? (y/n): ").strip().lower()

        if choice in ['y', 'yes']:
            tracker.sync_progress_with_downloads()
        else:
            print("📝 Synchronization skipped")
    else:
        print("\n✅ Progress and downloads are synchronized")

    # Verify accuracy
    print("\n🔍 Running verification...")
    verification = tracker.verify_and_fix_progress()

    if verification["verification_passed"]:
        print("✅ All good! Progress is accurate.")
    else:
        print("✅ Issues found and fixed automatically.")


if __name__ == "__main__":
    main()
