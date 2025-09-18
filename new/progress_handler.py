#!/usr/bin/env python3

"""
Improved Progress Handler with Enhanced Monitoring

This version integrates with the improved IDM manager to provide better
download completion detection and reduce failed videos.

Key Improvements:
- Uses dynamic completion monitoring instead of fixed wait times
- Better integration with improved IDM manager
- More robust progress tracking
- Enhanced validation and error handling

Author: AI Assistant
Version: 3.1 - Improved with dynamic monitoring
"""

import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
import asyncio
import time

from progress_tracking import EnhancedProgressTracker

class ImprovedProgressHandler:
    """
    Improved Progress Handler that uses dynamic completion monitoring
    to reduce failed videos and improve download success rates.
    """

    def __init__(self, progress_file: str = "progress.json", downloads_dir: str = "downloads"):
        """
        Initialize Improved Progress Handler.

        Args:
            progress_file: Path to the progress.json file
            downloads_dir: Path to downloads directory
        """
        self.progress_file = Path(progress_file)
        self.downloads_dir = downloads_dir
        self.base_url = "https://rule34video.com/latest-updates/"

        # Initialize enhanced progress tracker
        self.progress_tracker = EnhancedProgressTracker(str(progress_file), downloads_dir)

        print(f"✅ Improved Progress Handler Initialized")
        print(f"📄 Progress file: {self.progress_file}")
        print(f"📁 Downloads directory: {self.downloads_dir}")
        print(f"🌐 Base URL: {self.base_url}")
        print(f"📊 Progress tracking: Enhanced with dynamic monitoring")

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
                print(f" 📝 Recent IDs: {downloaded_videos[:5]}...")

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
            print("⚠️ Progress issues found and fixed")

        return verification_results

    async def process_single_page(self, page: int, download_dir: str = "downloads", idm_path: str = None,
                           enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                           use_dynamic_monitoring: bool = True) -> Dict[str, Any]:
        """
        Process a single specific page with improved monitoring.

        Args:
            page: Page number to process
            download_dir: Directory for downloads
            idm_path: Path to IDM executable
            enable_duplicate_detection: Enable duplicate detection for first page
            duplicate_check_limit: Max number of recent downloads to check against
            use_dynamic_monitoring: Whether to use dynamic completion monitoring

        Returns:
            Results from processing this specific page
        """
        print(f"\n📄 Processing single page with improved monitoring: {page}")
        print("-" * 70)

        try:
            # Construct URL for specific page
            page_url = f"{self.base_url}{page}"
            print(f"🌐 Page URL: {page_url}")

            # Import and initialize the Improved IDM processor
            from idm_manager import ImprovedIDMManager

            # Create IDM manager with improved settings
            idm_manager = ImprovedIDMManager(
                base_download_dir=download_dir,
                idm_path=idm_path,
                enable_duplicate_detection=enable_duplicate_detection,
                duplicate_check_limit=duplicate_check_limit,
                progress_file=str(self.progress_file)
            )

            print(f"✅ Improved IDM Manager initialized for page {page}")
            print(f" 🔍 Duplicate detection: {'Enabled' if enable_duplicate_detection else 'Disabled'}")
            print(f" 📝 Check limit: {duplicate_check_limit} recent video IDs")
            print(f" 🔄 Dynamic monitoring: {'Enabled' if use_dynamic_monitoring else 'Disabled'}")

            # Import video parser
            from video_data_parser import OptimizedVideoDataParser
            parser = OptimizedVideoDataParser(page_url)

            # Process videos with improved workflow
            print(f"\n🚀 Starting improved processing workflow for page {page}...")

            # Step 1: Extract video URLs
            video_urls = await parser.extract_video_urls() if asyncio.iscoroutinefunction(parser.extract_video_urls) else parser.extract_video_urls()

            if not video_urls:
                return {"success": False, "error": "No video URLs found"}

            # Step 2: Parse video metadata
            videos_data = await parser.parse_all_videos() if asyncio.iscoroutinefunction(parser.parse_all_videos) else parser.parse_all_videos()

            if not videos_data:
                return {"success": False, "error": "No video metadata could be parsed"}

            # Step 3: Process with improved IDM manager
            results = idm_manager.process_all_videos(
                videos_data,
                start_queue=True,
                current_page=page,
                use_dynamic_monitoring=use_dynamic_monitoring
            )

            print(f"✅ Page {page} processing completed")

            # Enhanced result reporting
            if results.get("success"):
                print(f"📊 Page {page} Results:")
                print(f" 🎬 Videos found: {len(videos_data)}")
                print(f" ✅ Videos processed: {results.get('videos_passed_duplicate_check', 0)}")
                print(f" 🚫 Duplicates filtered: {results.get('videos_filtered_by_duplicates', 0)}")
                print(f" 📥 IDM queue additions: {results.get('successful_additions', 0)}")
                print(f" 🔄 Dynamic monitoring used: {results.get('dynamic_monitoring_used', False)}")

                # Show monitoring results if available
                monitoring_results = results.get("monitoring_results")
                if monitoring_results:
                    print(f" ⏱️  Monitoring time: {monitoring_results['monitoring_time_minutes']:.1f} minutes")
                    print(f" 📈 Final completion rate: {monitoring_results['completion_rate']:.1f}%")

                # Show progress update results if available
                progress_update_results = results.get("progress_update_results")
                if progress_update_results:
                    verification = progress_update_results.get("verification_results", {})
                    print(f" ✅ Verification passed: {verification.get('verification_passed', False)}")

                    updated_progress = progress_update_results.get("updated_progress", {})
                    print(f" 💾 New total size: {updated_progress.get('total_size_mb', 0):.2f} MB")
                    print(f" 🎬 New video count: {updated_progress.get('total_downloaded', 0)}")

            return {
                "success": True,
                "page_processed": page,
                "url_used": page_url,
                "processing_results": results,
                "duplicate_detection_applied": enable_duplicate_detection,
                "dynamic_monitoring_used": use_dynamic_monitoring
            }

        except ImportError as e:
            print(f"❌ Could not import required modules: {e}")
            print("💡 Make sure all improved files are in the same directory")
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

    async def process_single_page_async(self, page: int, download_dir: str = "downloads", idm_path: str = None,
                                       enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                                       use_dynamic_monitoring: bool = True) -> Dict[str, Any]:
        """
        Async version of process_single_page for better integration with async video parser.

        Args:
            page: Page number to process
            download_dir: Directory for downloads
            idm_path: Path to IDM executable
            enable_duplicate_detection: Enable duplicate detection for first page
            duplicate_check_limit: Max number of recent downloads to check against
            use_dynamic_monitoring: Whether to use dynamic completion monitoring

        Returns:
            Results from processing this specific page
        """
        print(f"\n📄 Processing single page (async) with improved monitoring: {page}")
        print("-" * 70)

        try:
            # Construct URL for specific page
            page_url = f"{self.base_url}{page}"
            print(f"🌐 Page URL: {page_url}")

            # Import and initialize the Improved IDM processor
            from idm_manager import ImprovedIDMManager
            from video_data_parser import OptimizedVideoDataParser

            # Create IDM manager with improved settings
            idm_manager = ImprovedIDMManager(
                base_download_dir=download_dir,
                idm_path=idm_path,
                enable_duplicate_detection=enable_duplicate_detection,
                duplicate_check_limit=duplicate_check_limit,
                progress_file=str(self.progress_file)
            )

            parser = OptimizedVideoDataParser(page_url)

            print(f"✅ Improved components initialized for page {page}")

            # Process videos with improved workflow (async)
            print(f"\n🚀 Starting async improved processing workflow for page {page}...")

            # Step 1: Extract video URLs
            video_urls = await parser.extract_video_urls()
            if not video_urls:
                return {"success": False, "error": "No video URLs found"}

            # Step 2: Parse video metadata
            videos_data = await parser.parse_all_videos()
            if not videos_data:
                return {"success": False, "error": "No video metadata could be parsed"}

            # Step 3: Process with improved IDM manager
            results = idm_manager.process_all_videos(
                videos_data,
                start_queue=True,
                current_page=page,
                use_dynamic_monitoring=use_dynamic_monitoring
            )

            print(f"✅ Page {page} async processing completed")

            return {
                "success": True,
                "page_processed": page,
                "url_used": page_url,
                "processing_results": results,
                "duplicate_detection_applied": enable_duplicate_detection,
                "dynamic_monitoring_used": use_dynamic_monitoring
            }

        except Exception as e:
            print(f"❌ Error in async processing for page {page}: {e}")
            return {
                "success": False,
                "error": str(e),
                "page_processed": page,
                "url_used": f"{self.base_url}{page}"
            }

    def start_idm_process(self, download_dir: str = "downloads", idm_path: str = None,
                         enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                         use_dynamic_monitoring: bool = True) -> Dict[str, Any]:
        """
        Start the improved IDM manager process with dynamic monitoring.

        Args:
            download_dir: Directory for downloads
            idm_path: Path to IDM executable
            enable_duplicate_detection: Enable duplicate detection for first page
            duplicate_check_limit: Max number of recent downloads to check against
            use_dynamic_monitoring: Whether to use dynamic completion monitoring

        Returns:
            Results from the IDM processing
        """
        print("=" * 80)
        print("🚀 STARTING IMPROVED IDM PROCESS WITH DYNAMIC MONITORING")
        print("=" * 80)

        # Get the current page from progress
        summary = self.get_progress_summary()
        current_page = summary['last_page']

        # Display comprehensive progress summary
        print("📊 COMPREHENSIVE PROGRESS SUMMARY:")
        print(f" 📄 Current page: {summary['last_page']}")
        print(f" 📥 Reported downloaded: {summary['total_downloaded']}")
        print(f" 📁 Actual completed folders: {summary['actual_completed_folders']}")
        print(f" 💾 Reported size: {summary['total_size_mb']:.2f} MB")
        print(f" 💾 Actual folder size: {summary['actual_total_size_mb']:.2f} MB")
        print(f" 🎬 Downloaded videos: {summary['downloaded_videos']}")
        print(f" ❌ Failed videos: {summary['failed_videos']}")
        print(f" 📈 Completion rate: {summary['completion_rate']:.1f}%")
        print(f" 🕒 Last updated: {summary['last_updated']}")
        print(f" ⚠️ Sync needed: {'Yes' if summary['sync_needed'] else 'No'}")
        print(f" 🔍 Duplicate detection: {'Enabled' if enable_duplicate_detection else 'Disabled'}")
        print(f" 🔄 Dynamic monitoring: {'Enabled' if use_dynamic_monitoring else 'Disabled'}")
        print("=" * 80)

        # Sync progress if needed
        if summary['sync_needed']:
            print("⚠️ Progress out of sync - performing automatic sync...")
            self.sync_progress_with_downloads()
            print("✅ Progress synchronized")

        # Process the current page with improved monitoring
        results = asyncio.run(self.process_single_page_async(
            current_page,
            download_dir,
            idm_path,
            enable_duplicate_detection,
            duplicate_check_limit,
            use_dynamic_monitoring
        ))

        print("=" * 80)
        print("✅ IMPROVED IDM PROCESS WITH DYNAMIC MONITORING COMPLETED")
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
    Main function to demonstrate usage of Improved Progress Handler.
    """
    print("🎬 Improved Rule34Video Progress Handler with Dynamic Monitoring")
    print("=" * 80)
    print("🆕 IMPROVED FEATURES:")
    print(" - Dynamic completion monitoring (waits until downloads actually finish)")
    print(" - Reduced failed videos through better validation")
    print(" - Adaptive wait times based on download progress")
    print(" - Real-time IDM queue status monitoring")
    print(" - Enhanced folder validation with stability checks")
    print(" - Progress updates ONLY after verified completion")
    print("=" * 80)

    # Initialize progress handler
    handler = ImprovedProgressHandler()

    # Show comprehensive progress status
    print("\n📊 Comprehensive Progress Status:")
    summary = handler.get_progress_summary()
    for key, value in summary.items():
        print(f" {key}: {value}")

    # Get the URL that will be used
    url = handler.construct_url()
    print(f"\n🎯 Current page URL: {url}")

    # Check if sync is needed
    if summary["sync_needed"]:
        print("\n⚠️ Progress synchronization recommended!")
        choice = input("\n🤔 Synchronize progress with downloads? (y/n): ").strip().lower()
        if choice in ['y', 'yes']:
            handler.sync_progress_with_downloads()
        else:
            print("📝 Synchronization skipped")

    print("\n💡 Usage examples:")
    print(" # Process current page with dynamic monitoring:")
    print(" handler.start_idm_process(use_dynamic_monitoring=True)")
    print(" ")
    print(" # Process specific page with dynamic monitoring:")
    print(" asyncio.run(handler.process_single_page_async(997, use_dynamic_monitoring=True))")
    print(" ")
    print(" # Sync progress manually:")
    print(" handler.sync_progress_with_downloads()")

    print("\n🔧 Improved Dynamic Monitoring Behavior:")
    print(" - Waits minimum 2 minutes before starting checks")
    print(" - Monitors download progress every 15 seconds")
    print(" - Waits for downloads to be stable for 60 seconds")
    print(" - Maximum wait time: 30 minutes")
    print(" - Updates progress ONLY after verified completion")

    print("\n📋 Dependencies:")
    print(" ✅ idm_manager.py (dynamic monitoring IDM integration)")
    print(" ✅ progress_tracking.py (progress tracking system)")
    print(" ✅ duplicate_detection.py (duplicate detection system)")
    print(" ✅ video_data_parser.py (video parsing)")
    print(" ✅ progress.json (progress tracking with verification)")

if __name__ == "__main__":
    main()
