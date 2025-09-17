#!/usr/bin/env python3

"""
Updated Progress Handler with Enhanced Progress Tracking

This version integrates with the progress tracking IDM manager to ensure
progress.json is updated accurately ONLY after downloads are completed.

Features:
- Uses ProgressTrackingVideoIDMProcessor for accurate progress updates
- Updates progress.json ONLY after download verification
- Monitors downloads folder for completion status
- Provides manual progress sync capabilities
- Enhanced logging and verification

Author: AI Assistant
Version: 3.0 - Enhanced progress tracking integration
"""

import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
import asyncio
import time
from progress_tracking import EnhancedProgressTracker


class UpdatedProgressHandler:
    """
    Updated Progress Handler that integrates with enhanced progress tracking.

    This version ensures progress.json is updated accurately based on actual
    download completion rather than just IDM queue additions.
    """

    def __init__(self, progress_file: str = "progress.json", downloads_dir: str = "downloads"):
        """
        Initialize Updated Progress Handler.

        Args:
            progress_file: Path to the progress.json file
            downloads_dir: Path to downloads directory
        """
        self.progress_file = Path(progress_file)
        self.downloads_dir = downloads_dir
        self.base_url = "https://rule34video.com/latest-updates/"

        # Initialize enhanced progress tracker
        self.progress_tracker = EnhancedProgressTracker(str(progress_file), downloads_dir)

        print(f"✅ Updated Progress Handler Initialized")
        print(f"📄 Progress file: {self.progress_file}")
        print(f"📁 Downloads directory: {self.downloads_dir}")
        print(f"🌐 Base URL: {self.base_url}")
        print(f"📊 Progress tracking: Enhanced with download verification")

    def read_progress(self) -> Optional[Dict[str, Any]]:
        """
        Read progress data from progress.json file.

        Returns:
            Dictionary containing progress data or None if file doesn't exist/is invalid
        """
        try:
            progress_data = self.progress_tracker.updater.read_current_progress()

            print("✅ Progress file loaded successfully")
            print(f"📄 Last processed page: {progress_data.get('last_page', 1000)}")
            print(f"📥 Total downloaded: {progress_data.get('total_downloaded', 0)}")
            print(f"💾 Current size: {progress_data.get('total_size_mb', 0.0):.2f} MB")

            downloaded_videos = progress_data.get('downloaded_videos', [])
            print(f"🎬 Downloaded video IDs: {len(downloaded_videos)}")

            if downloaded_videos:
                print(f"   📝 Recent IDs: {downloaded_videos[:5]}...")

            return progress_data

        except Exception as e:
            print(f"❌ Error reading progress file: {e}")
            print("🔄 Starting from page 1000...")
            return {
                "last_page": 1000, 
                "total_size_mb": 0.0, 
                "total_downloaded": 0,
                "downloaded_videos": []
            }

    def get_last_page(self) -> int:
        """
        Get the last processed page number from progress.json.

        Returns:
            Last processed page number (defaults to 1000 if not found)
        """
        progress_data = self.read_progress()
        if progress_data:
            last_page = progress_data.get('last_page', 1000)
            print(f"🔍 Retrieved last page: {last_page}")
            return last_page
        else:
            print("🔄 No progress found, starting from page 1000")
            return 1000

    def construct_url(self, page: Optional[int] = None) -> str:
        """
        Construct the full URL with page number.

        Args:
            page: Specific page number (uses last_page from progress if None)

        Returns:
            Full URL with page number
        """
        if page is None:
            page = self.get_last_page()

        url = f"{self.base_url}{page}"
        print(f"🌐 Constructed URL: {url}")
        return url

    def get_progress_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive progress summary including download verification.

        Returns:
            Dictionary containing progress summary with verification data
        """
        # Get progress tracker summary
        tracker_summary = self.progress_tracker.get_progress_summary()

        progress_data = tracker_summary["progress_file_data"]
        download_stats = tracker_summary["download_folder_stats"]
        sync_needed = tracker_summary["sync_needed"]

        return {
            "last_page": progress_data.get('last_page', 1000),
            "total_downloaded": progress_data.get('total_downloaded', 0),
            "total_size_mb": progress_data.get('total_size_mb', 0.0),
            "downloaded_videos": len(progress_data.get('downloaded_videos', [])),
            "failed_videos": len(progress_data.get('failed_videos', [])),
            "last_updated": progress_data.get('last_updated', "Unknown"),
            "actual_completed_folders": download_stats["completed_folders"],
            "actual_failed_folders": download_stats["failed_folders"],
            "actual_total_size_mb": download_stats["total_size_mb"],
            "sync_needed": sync_needed,
            "completion_rate": download_stats["completion_rate"],
            "status": "Progress loaded with verification"
        }

    def sync_progress_with_downloads(self) -> Dict[str, Any]:
        """
        Synchronize progress.json with actual download folder contents.

        Returns:
            Synchronization results
        """
        print("\n🔄 Synchronizing progress with downloads...")
        print("-" * 60)

        sync_results = self.progress_tracker.sync_progress_with_downloads()

        print("✅ Progress synchronization completed")
        print("-" * 60)

        return sync_results

    def verify_progress_accuracy(self) -> Dict[str, Any]:
        """
        Verify progress accuracy and fix any discrepancies.

        Returns:
            Verification results
        """
        print("\n🔍 Verifying progress accuracy...")
        verification_results = self.progress_tracker.verify_and_fix_progress()

        if verification_results["verification_passed"]:
            print("✅ Progress verification passed")
        else:
            print("⚠️  Progress issues found and fixed")

        return verification_results

    def process_single_page(self, page: int, download_dir: str = "downloads", idm_path: str = None,
                           enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                           wait_for_completion: bool = True, completion_wait_time: int = 30) -> Dict[str, Any]:
        """
        Process a single specific page with enhanced progress tracking.

        Args:
            page: Page number to process
            download_dir: Directory for downloads
            idm_path: Path to IDM executable
            enable_duplicate_detection: Enable duplicate detection for first page
            duplicate_check_limit: Max number of recent downloads to check against
            wait_for_completion: Whether to wait and update progress after completion
            completion_wait_time: Time to wait before checking completion

        Returns:
            Results from processing this specific page
        """
        print(f"\n📄 Processing single page with enhanced progress tracking: {page}")
        print("-" * 70)

        try:
            # Construct URL for specific page
            page_url = f"{self.base_url}{page}"
            print(f"🌐 Page URL: {page_url}")

            # Import and initialize the Progress Tracking IDM processor
            from idm_manager import ProgressTrackingVideoIDMProcessor

            processor = ProgressTrackingVideoIDMProcessor(
                base_url=page_url,
                download_dir=download_dir,
                idm_path=idm_path,
                enable_duplicate_detection=enable_duplicate_detection,
                duplicate_check_limit=duplicate_check_limit,
                progress_file=str(self.progress_file)
            )

            print(f"✅ Progress Tracking IDM Processor initialized for page {page}")
            print(f"   🔍 Duplicate detection: {'Enabled' if enable_duplicate_detection else 'Disabled'}")
            print(f"   📝 Check limit: {duplicate_check_limit} recent video IDs")
            print(f"   ⏳ Wait for completion: {'Yes' if wait_for_completion else 'No'}")
            print(f"   🕒 Completion wait time: {completion_wait_time} seconds")

            # Process this specific page
            print(f"\n🚀 Starting enhanced processing workflow for page {page}...")
            results = asyncio.run(processor.process_all_videos(
                wait_for_completion=wait_for_completion,
                completion_wait_time=completion_wait_time
            ))

            print(f"✅ Page {page} processing completed")

            # Enhanced result reporting
            if results.get("success"):
                idm_results = results.get("idm_results", {})
                duplicates_filtered = idm_results.get("videos_filtered_by_duplicates", 0)
                videos_processed = idm_results.get("videos_passed_duplicate_check", 0)
                progress_updated = idm_results.get("progress_update_results") is not None

                print(f"📊 Page {page} Results:")
                print(f"   🎬 Videos found: {results.get('videos_parsed', 0)}")
                print(f"   ✅ Videos processed: {videos_processed}")
                print(f"   🚫 Duplicates filtered: {duplicates_filtered}")
                print(f"   📥 IDM queue additions: {idm_results.get('successful_additions', 0)}")
                print(f"   📊 Progress updated: {'Yes' if progress_updated else 'No'}")

                # Show progress update results if available
                progress_update_results = idm_results.get("progress_update_results")
                if progress_update_results:
                    verification = progress_update_results.get("verification_results", {})
                    print(f"   ✅ Verification passed: {verification.get('verification_passed', False)}")
                    updated_progress = progress_update_results.get("updated_progress", {})
                    print(f"   💾 New total size: {updated_progress.get('total_size_mb', 0):.2f} MB")
                    print(f"   🎬 New video count: {updated_progress.get('total_downloaded', 0)}")

            return {
                "success": True,
                "page_processed": page,
                "url_used": page_url,
                "processing_results": results,
                "duplicate_detection_applied": enable_duplicate_detection,
                "progress_tracking_enabled": True,
                "wait_for_completion": wait_for_completion
            }

        except ImportError as e:
            print(f"❌ Could not import Progress Tracking IDM manager: {e}")
            print("💡 Make sure idm_manager.py is in the same directory")
            return {
                "success": False,
                "error": f"Import error: {e}",
                "page_processed": page,
                "url_used": f"{self.base_url}{page}"
            }
        except Exception as e:
            print(f"❌ Error processing page {page}: {e}")
            return {
                "success": False,
                "error": str(e),
                "page_processed": page,
                "url_used": f"{self.base_url}{page}"
            }

    def start_idm_process(self, download_dir: str = "downloads", idm_path: str = None,
                         enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                         wait_for_completion: bool = True, completion_wait_time: int = 30) -> Dict[str, Any]:
        """
        Start the IDM manager process with enhanced progress tracking.

        This method processes the current page from progress.json with progress tracking.

        Args:
            download_dir: Directory for downloads
            idm_path: Path to IDM executable
            enable_duplicate_detection: Enable duplicate detection for first page
            duplicate_check_limit: Max number of recent downloads to check against
            wait_for_completion: Whether to wait and update progress after completion
            completion_wait_time: Time to wait before checking completion

        Returns:
            Results from the IDM processing
        """
        print("=" * 80)
        print("🚀 STARTING ENHANCED IDM PROCESS WITH PROGRESS TRACKING")
        print("=" * 80)

        # Get the current page from progress
        summary = self.get_progress_summary()
        current_page = summary['last_page']

        # Display comprehensive progress summary
        print("📊 COMPREHENSIVE PROGRESS SUMMARY:")
        print(f"   📄 Current page: {summary['last_page']}")
        print(f"   📥 Reported downloaded: {summary['total_downloaded']}")
        print(f"   📁 Actual completed folders: {summary['actual_completed_folders']}")
        print(f"   💾 Reported size: {summary['total_size_mb']:.2f} MB")
        print(f"   💾 Actual folder size: {summary['actual_total_size_mb']:.2f} MB")
        print(f"   🎬 Downloaded videos: {summary['downloaded_videos']}")
        print(f"   ❌ Failed videos: {summary['failed_videos']}")
        print(f"   📈 Completion rate: {summary['completion_rate']:.1f}%")
        print(f"   🕒 Last updated: {summary['last_updated']}")
        print(f"   ⚠️  Sync needed: {'Yes' if summary['sync_needed'] else 'No'}")
        print(f"   🔍 Duplicate detection: {'Enabled' if enable_duplicate_detection else 'Disabled'}")
        print(f"   📊 Progress tracking: Enhanced with download verification")
        print("=" * 80)

        # Sync progress if needed
        if summary['sync_needed']:
            print("⚠️  Progress out of sync - performing automatic sync...")
            self.sync_progress_with_downloads()
            print("✅ Progress synchronized")

        # Process the current page with enhanced progress tracking
        results = self.process_single_page(
            current_page, 
            download_dir, 
            idm_path,
            enable_duplicate_detection,
            duplicate_check_limit,
            wait_for_completion,
            completion_wait_time
        )

        print("=" * 80)
        print("✅ ENHANCED IDM PROCESS WITH PROGRESS TRACKING COMPLETED")
        print("=" * 80)

        return results

    def update_page_progress(self, new_page: int) -> bool:
        """
        Update the last_page in progress.json.

        Args:
            new_page: New page number

        Returns:
            True if updated successfully
        """
        return self.progress_tracker.updater.update_page_progress(new_page)


def main():
    """
    Main function to demonstrate usage of Updated Progress Handler.
    """
    print("🎬 Updated Rule34Video Progress Handler with Enhanced Progress Tracking")
    print("=" * 80)
    print("🆕 ENHANCED FEATURES:")
    print("   - Updates progress.json ONLY after download verification")
    print("   - Monitors downloads folder for actual completion")
    print("   - Provides progress synchronization and verification")
    print("   - Enhanced duplicate detection with first page only")
    print("   - Real-time folder size calculation")
    print("   - Automatic progress fixing and correction")
    print("=" * 80)

    # Initialize progress handler
    handler = UpdatedProgressHandler()

    # Show comprehensive progress status
    print("\n📊 Comprehensive Progress Status:")
    summary = handler.get_progress_summary()
    for key, value in summary.items():
        print(f"   {key}: {value}")

    # Get the URL that will be used
    url = handler.construct_url()
    print(f"\n🎯 Current page URL: {url}")

    # Check if sync is needed
    if summary["sync_needed"]:
        print("\n⚠️  Progress synchronization recommended!")
        choice = input("\n🤔 Synchronize progress with downloads? (y/n): ").strip().lower()

        if choice in ['y', 'yes']:
            handler.sync_progress_with_downloads()
        else:
            print("📝 Synchronization skipped")

    print("\n💡 Usage examples:")
    print("   # Process current page with progress tracking:")
    print("   handler.start_idm_process(wait_for_completion=True)")
    print("   ")
    print("   # Process specific page with progress tracking:")
    print("   handler.process_single_page(997, wait_for_completion=True)")
    print("   ")
    print("   # Sync progress manually:")
    print("   handler.sync_progress_with_downloads()")
    print("   ")
    print("   # Verify progress accuracy:")
    print("   handler.verify_progress_accuracy()")

    print("\n🔧 Enhanced Progress Tracking Behavior:")
    print("   - First page: Duplicate detection + progress tracking")
    print("   - Subsequent pages: Progress tracking only")
    print("   - Progress updated ONLY after download verification")
    print("   - Automatic folder monitoring and size calculation")
    print("   - Verification and correction of progress discrepancies")

    print("\n📋 Dependencies:")
    print("   ✅ idm_manager.py (progress tracking IDM integration)")
    print("   ✅ progress_tracking.py (progress tracking system)")
    print("   ✅ duplicate_detection.py (duplicate detection system)")
    print("   ✅ video_data_parser.py (video parsing)")
    print("   ✅ progress.json (progress tracking with verification)")


if __name__ == "__main__":
    main()
