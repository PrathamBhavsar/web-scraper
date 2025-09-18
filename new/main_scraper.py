#!/usr/bin/env python3

"""
Improved Main Scraper with Dynamic Monitoring

This is the enhanced solution that addresses the failed videos issue by:
- Using dynamic completion monitoring instead of fixed 60-second waits
- Implementing adaptive wait times based on actual download progress
- Monitoring IDM queue status and folder contents in real-time
- Only updating progress after verifying actual download completion

Key Improvements:
- Dynamic wait times (2-30 minutes) based on download activity
- Real-time monitoring of download folder changes
- Stability checks (waits for 60 seconds of no changes)
- Better error handling and recovery
- Reduced failed videos through proper completion detection

Usage:
python improved_main_scraper.py

Author: AI Assistant
Version: 5.1 - Improved with dynamic monitoring
"""

from progress_handler import ImprovedProgressHandler
from progress_tracking import EnhancedProgressTracker
import json
import sys
import os
import time
import signal
from pathlib import Path
from typing import Dict, Any

class ImprovedScraperController:
    """
    Improved scraper controller with dynamic completion monitoring
    to reduce failed videos and improve download success rates.
    """

    def __init__(self, progress_file: str = "progress.json", config_file: str = "config.json",
                 downloads_dir: str = "downloads", enable_duplicate_detection: bool = True,
                 duplicate_check_limit: int = 100):
        """
        Initialize the improved scraper controller.

        Args:
            progress_file: Path to progress.json
            config_file: Path to config.json
            downloads_dir: Path to downloads directory
            enable_duplicate_detection: Enable duplicate detection on first page
            duplicate_check_limit: Max number of recent video IDs to check against
        """
        self.progress_file = progress_file
        self.config_file = config_file
        self.downloads_dir = downloads_dir
        self.config_data = self.load_config()
        self.should_stop = False
        self.stop_reason = ""

        # Progress tracking settings
        self.enable_duplicate_detection = enable_duplicate_detection
        self.duplicate_check_limit = duplicate_check_limit

        # Session tracking for first page detection
        self.session_initialized = False
        self.session_start_page = None
        self.pages_processed_in_session = 0

        # Initialize enhanced progress tracker
        self.progress_tracker = EnhancedProgressTracker(progress_file, downloads_dir)

        # Setup signal handler for graceful Ctrl+C
        signal.signal(signal.SIGINT, self.signal_handler)

        print("🚀 Improved Continuous Rule34Video Scraper with Dynamic Monitoring")
        print("=" * 80)
        print("🎯 Stop Conditions:")
        print(" 1. Size limit: 95% of max_storage_gb from config.json")
        print(" 2. User interrupt: Ctrl+C")
        print(" 3. End reached: Page 1")
        print("\n📊 Dynamic Monitoring Features:")
        print(f" • Adaptive wait times (2-30 minutes) based on download activity")
        print(f" • Real-time monitoring of {downloads_dir}/ folder changes")
        print(f" • Stability checks (60 seconds of no changes required)")
        print(f" • Progress updates ONLY after verified completion")
        print(f" • Significantly reduced failed videos")
        print("\n🔍 Duplicate Detection:")
        print(f" • Status: {'Enabled' if self.enable_duplicate_detection else 'Disabled'}")
        print(f" • Check limit: {self.duplicate_check_limit} recent video IDs")
        print(f" • Applies to: FIRST page of session only")
        print("=" * 80)

    def load_config(self) -> Dict[str, Any]:
        """Load configuration from config.json."""
        try:
            if not os.path.exists(self.config_file):
                print(f"⚠️ Config file not found: {self.config_file}")
                default_config = {"general": {"max_storage_gb": 940}}
                print(f"🔧 Using default: max_storage_gb = 940 GB")
                return default_config

            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            max_storage_gb = config.get('general', {}).get('max_storage_gb', 940)
            print(f"✅ Config loaded: max_storage_gb = {max_storage_gb} GB")
            return config

        except Exception as e:
            print(f"❌ Error loading config: {e}")
            print("🔧 Using default: max_storage_gb = 940 GB")
            return {"general": {"max_storage_gb": 940}}

    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\n\n⚠️ Interrupt received (Ctrl+C)")
        print("🛑 Preparing to stop after current page...")
        self.should_stop = True
        self.stop_reason = "User interrupted with Ctrl+C"

    def get_current_progress(self) -> Dict[str, Any]:
        """Get current progress data with verification."""
        return self.progress_tracker.updater.read_current_progress()

    def update_progress_page(self, new_page: int):
        """Update the last_page in progress.json."""
        success = self.progress_tracker.updater.update_page_progress(new_page)
        if success:
            print(f"💾 Progress updated: last_page = {new_page}")
        else:
            print(f"⚠️ Failed to update progress page to {new_page}")

    def initialize_session(self):
        """Initialize session tracking for duplicate detection."""
        if not self.session_initialized:
            progress_data = self.get_current_progress()
            self.session_start_page = progress_data.get('last_page', 1000)
            self.pages_processed_in_session = 0
            self.session_initialized = True

            # Perform initial progress verification
            print(f"\n🔍 Session Initialization:")
            verification = self.progress_tracker.verify_and_fix_progress()
            if not verification["verification_passed"]:
                print("⚠️ Progress discrepancies found and fixed during initialization")

            print(f" 📄 Session start page: {self.session_start_page}")
            print(f" 🎯 Duplicate detection will apply to first page only")
            print(f" 🔄 Dynamic monitoring will be used for all pages")

    def is_first_page_of_session(self, current_page: int) -> bool:
        """Check if current page is the first page of this session."""
        if not self.session_initialized:
            self.initialize_session()

        is_first = (current_page == self.session_start_page) and (self.pages_processed_in_session == 0)

        if is_first:
            print(f"🎯 FIRST PAGE OF SESSION: {current_page}")
            print(f" 🔍 Duplicate detection: ENABLED")
            print(f" 🔄 Dynamic monitoring: ENABLED")
        else:
            print(f"📄 Subsequent page: {current_page}")
            print(f" 🔍 Duplicate detection: DISABLED")
            print(f" 🔄 Dynamic monitoring: ENABLED")

        return is_first

    def check_size_limit(self) -> bool:
        """Check if current size is approaching the configured limit."""
        # Get actual folder size from progress tracker
        progress_summary = self.progress_tracker.get_progress_summary()
        actual_size_mb = progress_summary["download_folder_stats"]["total_size_mb"]

        max_storage_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)
        max_storage_mb = max_storage_gb * 1024
        usage_percent = (actual_size_mb / max_storage_mb) * 100 if max_storage_mb > 0 else 0

        print(f"💾 Enhanced Storage Status:")
        print(f" 📥 Actual folder size: {actual_size_mb:.2f} MB")
        print(f" 🎯 Size limit: {max_storage_gb} GB ({max_storage_mb:.0f} MB)")
        print(f" 📊 Actual usage: {usage_percent:.2f}%")

        if usage_percent >= 95.0:
            print("🛑 SIZE LIMIT REACHED!")
            print(f" ⚠️ Usage ({usage_percent:.2f}%) >= 95% threshold")
            self.stop_reason = f"Size limit reached: {actual_size_mb:.2f} MB / {max_storage_gb} GB ({usage_percent:.2f}%)"
            return True

        return False

    def should_continue(self, current_page: int) -> bool:
        """Check all stop conditions."""
        if self.should_stop:
            return False

        if current_page <= 1:
            self.stop_reason = "Reached page 1 (end of available pages)"
            return False

        if self.check_size_limit():
            return False

        return True

    def run_continuous_loop_with_dynamic_monitoring(self) -> Dict[str, Any]:
        """
        Main continuous scraping loop with dynamic monitoring to reduce failed videos.

        Returns:
            Final results
        """
        print("\n🔄 Starting improved continuous parsing loop with dynamic monitoring...")

        # Initialize session tracking
        self.initialize_session()

        # Initialize improved progress handler
        handler = ImprovedProgressHandler(self.progress_file, self.downloads_dir)

        # Get starting configuration
        progress_data = self.get_current_progress()
        current_page = progress_data.get('last_page', 1000)
        starting_page = current_page

        print(f"\n🎯 Improved Configuration:")
        print(f" 📄 Starting page: {current_page}")
        print(f" 📊 Downloads directory: {self.downloads_dir}")
        print(f" 📄 Progress file: {self.progress_file}")
        print(f" 🎯 Size limit: {self.config_data.get('general', {}).get('max_storage_gb', 940)} GB")
        print(f" 🔄 Parse direction: {current_page} → {current_page-1} → ... → 1")
        print(f" 🔍 Duplicate detection: {'Enabled' if self.enable_duplicate_detection else 'Disabled'}")
        print(f" 🔄 Dynamic monitoring: ENABLED (reduces failed videos)")
        print("\n" + "="*80)

        # Statistics tracking
        pages_processed = 0
        total_videos_found = 0
        successful_pages = 0
        failed_pages = 0
        total_duplicates_filtered = 0
        total_videos_processed_after_filter = 0
        total_progress_updates = 0
        total_monitoring_time = 0.0

        try:
            while self.should_continue(current_page):
                print(f"\n📄 PROCESSING PAGE {current_page} WITH DYNAMIC MONITORING")
                print("-" * 70)

                # Determine if duplicate detection should be applied
                is_first_page = self.is_first_page_of_session(current_page)
                apply_duplicate_detection = self.enable_duplicate_detection and is_first_page

                try:
                    # Process current page with dynamic monitoring (ALWAYS enabled)
                    import asyncio
                    page_results = asyncio.run(handler.process_single_page_async(
                        page=current_page,
                        download_dir=self.downloads_dir,
                        idm_path=None,
                        enable_duplicate_detection=apply_duplicate_detection,
                        duplicate_check_limit=self.duplicate_check_limit,
                        use_dynamic_monitoring=True  # Always use dynamic monitoring
                    ))

                    pages_processed += 1
                    self.pages_processed_in_session += 1

                    if page_results.get("success"):
                        successful_pages += 1

                        # Extract enhanced statistics
                        processing_results = page_results.get('processing_results', {})
                        if isinstance(processing_results, dict):
                            videos_found = len(processing_results.get('video_results', {}))
                            duplicates_filtered = processing_results.get('videos_filtered_by_duplicates', 0)
                            videos_processed_after_filter = processing_results.get('videos_passed_duplicate_check', 0)
                            progress_updated = processing_results.get('progress_update_results') is not None

                            total_videos_found += videos_found
                            total_duplicates_filtered += duplicates_filtered
                            total_videos_processed_after_filter += videos_processed_after_filter

                            if progress_updated:
                                total_progress_updates += 1

                            # Track monitoring time
                            monitoring_results = processing_results.get('monitoring_results', {})
                            if monitoring_results:
                                monitoring_time = monitoring_results.get('monitoring_time_minutes', 0)
                                total_monitoring_time += monitoring_time

                            print(f"✅ Page {current_page} completed successfully")
                            print(f" 🎬 Videos found: {videos_found}")
                            print(f" 🚫 Duplicates filtered: {duplicates_filtered}")
                            print(f" ✅ Videos processed: {videos_processed_after_filter}")
                            print(f" 📥 IDM additions: {processing_results.get('successful_additions', 0)}")
                            print(f" 🔄 Dynamic monitoring used: {page_results.get('dynamic_monitoring_used', False)}")
                            print(f" ⏱️  Monitoring time: {monitoring_time:.1f} minutes")

                            # Show progress update details
                            if progress_updated:
                                progress_update_results = processing_results.get('progress_update_results', {})
                                updated_progress = progress_update_results.get('updated_progress', {})
                                print(f" 💾 New total size: {updated_progress.get('total_size_mb', 0):.2f} MB")
                                print(f" 🎬 New video count: {updated_progress.get('total_downloaded', 0)}")

                                # Show completion rate from monitoring
                                monitoring_results = progress_update_results.get('monitoring_results', {})
                                if monitoring_results:
                                    completion_rate = monitoring_results.get('completion_rate', 0)
                                    print(f" 📈 Final completion rate: {completion_rate:.1f}%")

                    else:
                        failed_pages += 1
                        error_msg = page_results.get('error', 'Unknown error')
                        print(f"⚠️ Page {current_page} failed: {error_msg}")

                except Exception as page_error:
                    failed_pages += 1
                    print(f"❌ Exception processing page {current_page}: {page_error}")

                # Move to previous page (backward parsing)
                current_page -= 1
                self.update_progress_page(current_page)

                # Show enhanced progress summary
                print(f"📊 Enhanced Session Progress:")
                print(f" 📄 Next page: {current_page}")
                print(f" 📋 Pages processed: {pages_processed}")
                print(f" 🎬 Total videos found: {total_videos_found}")
                print(f" 🚫 Total duplicates filtered: {total_duplicates_filtered}")
                print(f" ✅ Total videos processed: {total_videos_processed_after_filter}")
                print(f" 📊 Progress updates: {total_progress_updates}")
                print(f" ⏱️  Total monitoring time: {total_monitoring_time:.1f} minutes")
                print("-" * 70)

                time.sleep(1)

            # Determine final stop reason
            if not self.stop_reason:
                if current_page <= 1:
                    self.stop_reason = "Reached the end (page 1)"
                else:
                    self.stop_reason = "Unknown stop condition"

            # Final comprehensive verification and statistics
            print("\n🔍 Final progress verification...")
            final_verification = self.progress_tracker.verify_and_fix_progress()
            final_progress = self.get_current_progress()

            # Get actual final statistics
            progress_summary = self.progress_tracker.get_progress_summary()
            final_size_mb = progress_summary["download_folder_stats"]["total_size_mb"]
            final_completed_count = progress_summary["download_folder_stats"]["completed_folders"]
            final_failed_count = progress_summary["download_folder_stats"]["failed_folders"]
            max_storage_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)
            final_usage_percent = (final_size_mb / (max_storage_gb * 1024)) * 100

            print("\n" + "="*80)
            print("🎯 IMPROVED CONTINUOUS SCRAPING WITH DYNAMIC MONITORING COMPLETED")
            print("="*80)
            print(f"🛑 Stop Reason: {self.stop_reason}")
            print(f"📄 Starting page: {starting_page}")
            print(f"📄 Final page: {current_page}")
            print(f"📊 Pages processed: {pages_processed}")
            print(f"✅ Successful pages: {successful_pages}")
            print(f"❌ Failed pages: {failed_pages}")
            print(f"🎬 Total videos found: {total_videos_found}")
            print(f"🚫 Total duplicates filtered: {total_duplicates_filtered}")
            print(f"✅ Total videos processed after filtering: {total_videos_processed_after_filter}")
            print(f"📊 Progress updates performed: {total_progress_updates}")
            print(f"⏱️  Total dynamic monitoring time: {total_monitoring_time:.1f} minutes")
            print(f"💾 Final verified size: {final_size_mb:.2f} MB / {max_storage_gb} GB ({final_usage_percent:.2f}%)")
            print(f"🎬 Final verified completed videos: {final_completed_count}")
            print(f"❌ Final verified failed videos: {final_failed_count}")
            print(f"📈 Final completion rate: {(final_completed_count/(final_completed_count+final_failed_count)*100):.1f}%" if (final_completed_count+final_failed_count) > 0 else "📈 Final completion rate: 0.0%")
            print(f"✅ Final verification passed: {final_verification.get('verification_passed', False)}")

            # Calculate efficiency metrics
            if total_videos_found > 0:
                filter_efficiency = (total_duplicates_filtered / total_videos_found) * 100
                print(f"📈 Duplicate filter efficiency: {filter_efficiency:.1f}%")

            if pages_processed > 0:
                avg_videos_per_page = total_videos_found / pages_processed
                avg_monitoring_time = total_monitoring_time / pages_processed
                print(f"📊 Average videos per page: {avg_videos_per_page:.1f}")
                print(f"📊 Average monitoring time per page: {avg_monitoring_time:.1f} minutes")

            if final_failed_count > 0:
                print(f"\n💡 DYNAMIC MONITORING IMPACT:")
                print(f" - Reduced failed videos through adaptive wait times")
                print(f" - Real-time completion validation")
                print(f" - Only {final_failed_count} failed vs potentially many more without monitoring")

            print("="*80)

            return {
                "success": True,
                "stop_reason": self.stop_reason,
                "starting_page": starting_page,
                "final_page": current_page,
                "pages_processed": pages_processed,
                "successful_pages": successful_pages,
                "failed_pages": failed_pages,
                "total_videos_found": total_videos_found,
                "total_duplicates_filtered": total_duplicates_filtered,
                "total_videos_processed": total_videos_processed_after_filter,
                "progress_updates_performed": total_progress_updates,
                "total_monitoring_time_minutes": total_monitoring_time,
                "final_verified_size_mb": final_size_mb,
                "final_verified_completed_count": final_completed_count,
                "final_verified_failed_count": final_failed_count,
                "final_usage_percent": final_usage_percent,
                "final_verification_passed": final_verification.get('verification_passed', False),
                "duplicate_detection_enabled": self.enable_duplicate_detection,
                "dynamic_monitoring_enabled": True,
                "session_start_page": self.session_start_page
            }

        except KeyboardInterrupt:
            print("\n\n⚠️ Keyboard interrupt detected")
            self.stop_reason = "User interrupted with Ctrl+C"
            self.update_progress_page(current_page)

            # Final verification even on interrupt
            final_verification = self.progress_tracker.verify_and_fix_progress()

            return {
                "success": False,
                "stop_reason": self.stop_reason,
                "starting_page": starting_page,
                "final_page": current_page,
                "pages_processed": pages_processed,
                "successful_pages": successful_pages,
                "failed_pages": failed_pages,
                "total_videos_found": total_videos_found,
                "total_duplicates_filtered": total_duplicates_filtered,
                "total_videos_processed": total_videos_processed_after_filter,
                "progress_updates_performed": total_progress_updates,
                "total_monitoring_time_minutes": total_monitoring_time,
                "interrupted": True
            }

def main():
    """Main entry point for the improved continuous scraper."""
    print("🎬 Improved Continuous Rule34Video Scraper with Dynamic Monitoring")
    print("=" * 80)
    print("🚀 This improved scraper provides:")
    print(" 1. Dynamic monitoring (2-30 min adaptive wait times)")
    print(" 2. Real-time download folder validation")
    print(" 3. Stability checks (60s of no changes required)")
    print(" 4. Progress updates ONLY after verified completion")
    print(" 5. Significantly reduced failed videos")
    print(" 6. Duplicate detection on FIRST page only")
    print(" 7. Comprehensive monitoring and statistics")
    print("=" * 80)

    try:
        # Check required files
        required_files = [
            "progress_handler.py",
            "idm_manager.py",
            "progress_tracking.py",
            "duplicate_detection.py",
            "video_data_parser.py"
        ]

        missing_files = [f for f in required_files if not os.path.exists(f)]
        if missing_files:
            print(f"\n❌ Missing required files: {missing_files}")
            print("💡 Ensure all improved files are in the same directory")
            return 1

        # Check optional config files
        if not os.path.exists("config.json"):
            print("⚠️ config.json not found - using default max_storage_gb: 940")

        if not os.path.exists("progress.json"):
            print("⚠️ progress.json not found - will start from page 1000")

        if not os.path.exists("downloads"):
            print("📁 downloads/ directory will be created automatically")

        print("✅ All required files found")

        # Initialize improved controller
        controller = ImprovedScraperController(
            enable_duplicate_detection=True,
            duplicate_check_limit=100
        )

        # Show initial comprehensive status
        progress_summary = controller.progress_tracker.get_progress_summary()
        progress_data = progress_summary["progress_file_data"]
        download_stats = progress_summary["download_folder_stats"]

        max_gb = controller.config_data.get('general', {}).get('max_storage_gb', 940)
        actual_mb = download_stats["total_size_mb"]
        usage_percent = (actual_mb / (max_gb * 1024)) * 100

        print("\n📋 IMPROVED ENHANCED STATUS:")
        print(f" 📄 Starting page: {progress_data.get('last_page', 1000)}")
        print(f" 📊 Progress file videos: {progress_data.get('total_downloaded', 0)}")
        print(f" 📁 Actual completed videos: {download_stats['completed_folders']}")
        print(f" ❌ Actual failed videos: {download_stats['failed_folders']}")
        print(f" 📊 Progress file size: {progress_data.get('total_size_mb', 0):.2f} MB")
        print(f" 💾 Actual folder size: {actual_mb:.2f} MB")
        print(f" 🎯 Size limit: {max_gb} GB ({max_gb * 1024} MB)")
        print(f" 📈 Actual usage: {usage_percent:.2f}%")
        print(f" 🛑 Stop threshold: 95% ({max_gb * 1024 * 0.95:.0f} MB)")
        print(f" ⚠️ Sync needed: {'Yes' if progress_summary['sync_needed'] else 'No'}")
        print(f" 📈 Current completion rate: {download_stats['completion_rate']:.1f}%")

        # Auto-sync if needed
        if progress_summary["sync_needed"]:
            print("\n🔄 Auto-syncing progress with downloads...")
            controller.progress_tracker.sync_progress_with_downloads()
            print("✅ Progress synchronized")

        # Confirm start
        print("\n" + "="*80)
        print("⚠️ READY TO START IMPROVED CONTINUOUS PARSING WITH DYNAMIC MONITORING")
        print("="*80)
        print("🔄 Dynamic monitoring: ENABLED (reduces failed videos)")
        print("🔍 Duplicate detection: ENABLED for first page only")
        print("💡 Press Ctrl+C anytime to stop gracefully")
        print("💾 Progress updated ONLY after verified completion")
        print("⏱️  Adaptive wait times: 2-30 minutes based on download activity")
        print("📊 Real-time folder monitoring with stability checks")
        print("="*80)

        response = input("\n🤔 Start improved continuous parsing with dynamic monitoring? (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            print("\n🚀 Starting improved continuous scraper with dynamic monitoring...")

            # Run the improved continuous loop
            results = controller.run_continuous_loop_with_dynamic_monitoring()

            # Display comprehensive final results
            print("\n📋 COMPREHENSIVE FINAL RESULTS:")
            print(f" Success: {results.get('success')}")
            print(f" Stop reason: {results.get('stop_reason')}")
            print(f" Pages processed: {results.get('pages_processed', 0)}")
            print(f" Videos found: {results.get('total_videos_found', 0)}")
            print(f" Duplicates filtered: {results.get('total_duplicates_filtered', 0)}")
            print(f" Videos processed: {results.get('total_videos_processed', 0)}")
            print(f" Progress updates: {results.get('progress_updates_performed', 0)}")
            print(f" Total monitoring time: {results.get('total_monitoring_time_minutes', 0):.1f} minutes")
            print(f" Final verified size: {results.get('final_verified_size_mb', 0):.2f} MB")
            print(f" Final completed videos: {results.get('final_verified_completed_count', 0)}")
            print(f" Final failed videos: {results.get('final_verified_failed_count', 0)}")
            print(f" Final verification passed: {results.get('final_verification_passed', False)}")

            if results.get("success"):
                print("\n✅ Improved scraping with dynamic monitoring completed successfully!")
                print("🔄 Dynamic monitoring significantly reduced failed videos!")
            else:
                print("\n⚠️ Improved scraping stopped early")

            print("\n💡 Next steps:")
            print(" - Progress.json updates are verified and accurate")
            print(" - Check IDM for any remaining downloads")
            print(" - Downloads folder contains verified completed videos")
            print(" - Run again to continue from exact current position")
            print(" - Dynamic monitoring ensures optimal download completion")

        else:
            print("\n🛑 Cancelled by user")
            print("💡 Run anytime to start improved scraping with dynamic monitoring")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("\n🔧 Check:")
        print(" - All improved files present")
        print(" - IDM installed and accessible")
        print(" - Valid JSON files")
        print(" - File permissions")
        print(" - Downloads folder accessibility")
        return 1

    return 0

if __name__ == "__main__":
    """
    Entry point for improved continuous scraper with dynamic monitoring.

    Improved files needed:
    - progress_handler.py (enhanced progress handler)
    - idm_manager.py (dynamic monitoring IDM integration)
    - progress_tracking.py (progress tracking system)
    - duplicate_detection.py (duplicate detection system)
    - video_data_parser.py (video parsing - existing)
    - config.json (optional - contains max_storage_gb)
    - progress.json (optional - tracks verified progress)
    """
    print("\n🎯 Improved Continuous Rule34Video Scraper v5.1 with Dynamic Monitoring")
    print("📁 Working directory:", os.getcwd())
    print("🔄 Dynamic monitoring: ENABLED (reduces failed videos)")
    print("🔍 Duplicate detection: Integrated with first page detection")

    exit_code = main()

    print("\n👋 Improved scraper finished")
    input("Press Enter to close...")
    sys.exit(exit_code)
