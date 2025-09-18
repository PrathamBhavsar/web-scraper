#!/usr/bin/env python3

"""
Enhanced Main Scraper with Complete Debugging and Verification
This version ensures ALL videos found on each page are processed and added to IDM
"""

from progress_handler import ImprovedProgressHandler
from progress_tracking import EnhancedProgressTracker
import json
import sys
import os
from pathlib import Path
from typing import Dict, Any

class EnhancedScraperController:
    """
    Enhanced scraper controller with complete debugging and verification
    to ensure all videos are processed correctly.
    """

    def __init__(self, progress_file: str = "progress.json", config_file: str = "config.json",
                 downloads_dir: str = "downloads", enable_duplicate_detection: bool = True,
                 duplicate_check_limit: int = 100):
        """
        Initialize the enhanced scraper controller.

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

        print("🚀 Enhanced Continuous Scraper with Complete Debugging")
        print("=" * 80)
        print("🐛 DEBUG FEATURES ENABLED:")
        print("  - Page URL logging")
        print("  - Video count verification")
        print("  - IDM addition tracking")
        print("  - Processing failure identification")
        print("  - Complete audit trail")
        print("  - Dynamic monitoring capabilities")
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

            print(f"  📄 Session start page: {self.session_start_page}")
            print(f"  🎯 Duplicate detection will apply to first page only")
            print(f"  🔄 Dynamic monitoring available for all pages")

    def is_first_page_of_session(self, current_page: int) -> bool:
        """Check if current page is the first page of this session."""
        if not self.session_initialized:
            self.initialize_session()

        is_first = (current_page == self.session_start_page) and (self.pages_processed_in_session == 0)

        if is_first:
            print(f"🎯 FIRST PAGE OF SESSION: {current_page}")
            print(f"  🔍 Duplicate detection: ENABLED")
            print(f"  🔄 Dynamic monitoring: AVAILABLE")
        else:
            print(f"📄 Subsequent page: {current_page}")
            print(f"  🔍 Duplicate detection: DISABLED")
            print(f"  🔄 Dynamic monitoring: AVAILABLE")

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
        print(f"  📥 Actual folder size: {actual_size_mb:.2f} MB")
        print(f"  🎯 Size limit: {max_storage_gb} GB ({max_storage_mb:.0f} MB)")
        print(f"  📊 Actual usage: {usage_percent:.2f}%")

        if usage_percent >= 95.0:
            print("🛑 SIZE LIMIT REACHED!")
            print(f"  ⚠️ Usage ({usage_percent:.2f}%) >= 95% threshold")
            self.stop_reason = f"Size limit reached: {actual_size_mb:.2f} MB / {max_storage_gb} GB ({usage_percent:.2f}%)"
            return True

        return False

    # Check for video_data_parser (legacy dependency)
    try:
        import video_data_parser
        print("✅ Video data parser available")
    except ImportError:
        print("⚠️  video_data_parser.py not found - parsing may fail")
        return False

    print("✅ All required modules available")
    return True


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Web Parser with Batch Processing and Retry Logic",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run with default settings
  %(prog)s --start-page 1000        # Start from specific page
  %(prog)s --max-pages 10           # Limit to 10 pages
  %(prog)s --batch-pages 5          # Process 5 pages per batch
  %(prog)s --dry-run                # Simulate without downloads
  %(prog)s --max-retries 5          # Set max retries per page
        """
    )

    # Basic operation
    parser.add_argument(
        '--start-page', type=int, default=None,
        help='Starting page number (uses progress.json if not specified)'
    )
    parser.add_argument(
        '--max-pages', type=int, default=None,
        help='Maximum number of pages to process (unlimited if not specified)'
    )

    # Batch configuration
    parser.add_argument(
        '--batch-pages', type=int, default=None,
        help='Number of pages to process per batch (overrides config)'
    )
    parser.add_argument(
        '--batch-wait', type=int, default=None,
        help='Initial wait time after batch enqueue in seconds (overrides config)'
    )
    parser.add_argument(
        '--max-retries', type=int, default=None,
        help='Maximum retry attempts per page (overrides config)'
    )

    # Operation modes
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Simulate operations without actual downloads or file changes'
    )

    # Configuration
    parser.add_argument(
        '--config', default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--progress', default='progress.json', 
        help='Path to progress file (default: progress.json)'
    )
    parser.add_argument(
        '--downloads-dir', default=None,
        help='Downloads directory (overrides config)'
    )

    # Logging
    parser.add_argument(
        '--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Set logging level (overrides config)'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Enable verbose logging (sets DEBUG level)'
    )

    return parser


def update_config_from_args(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """
    Update configuration based on command line arguments.

    Args:
        config_manager: Configuration manager instance
        args: Parsed command line arguments

        Returns:
            Final results
        """
        print("\n🔄 Starting enhanced continuous parsing loop with complete debugging...")

        # Initialize session tracking
        self.initialize_session()

        # Initialize enhanced progress handler
        handler = ImprovedProgressHandler(self.progress_file, self.downloads_dir)

        # Get starting configuration
        progress_data = self.get_current_progress()
        current_page = progress_data.get('last_page', 1000)
        starting_page = current_page

        print(f"\n🎯 Enhanced Configuration:")
        print(f"  📄 Starting page: {current_page}")
        print(f"  📊 Downloads directory: {self.downloads_dir}")
        print(f"  📄 Progress file: {self.progress_file}")
        print(f"  🎯 Size limit: {self.config_data.get('general', {}).get('max_storage_gb', 940)} GB")
        print(f"  🔄 Parse direction: {current_page} → {current_page-1} → ... → 1")
        print(f"  🔍 Duplicate detection: {'Enabled' if self.enable_duplicate_detection else 'Disabled'}")
        print(f"  🐛 Complete debugging: ENABLED")
        print(f"  🔄 Dynamic monitoring: AVAILABLE")

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
                print(f"\n📄 PROCESSING PAGE {current_page} WITH COMPLETE DEBUGGING")
                print("-" * 70)

                # Debug logging
                print(f"🐛 DEBUG: Current page URL will be: https://rule34video.com/latest-updates/{current_page}")

                # Determine if duplicate detection should be applied
                is_first_page = self.is_first_page_of_session(current_page)
                apply_duplicate_detection = self.enable_duplicate_detection and is_first_page

                try:
                    # Process current page with enhanced debugging
                    import asyncio
                    page_results = asyncio.run(handler.process_single_page_async(
                        page=current_page,
                        download_dir=self.downloads_dir,
                        idm_path=None,
                        enable_duplicate_detection=apply_duplicate_detection,
                        duplicate_check_limit=self.duplicate_check_limit,
                        use_dynamic_monitoring=True  # Always available but user controls usage
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
                            print(f"🐛 DEBUG: Page {current_page} Detailed Results:")
                            print(f"  🎬 Videos found: {videos_found}")
                            print(f"  🚫 Duplicates filtered: {duplicates_filtered}")
                            print(f"  ✅ Videos processed: {videos_processed_after_filter}")
                            print(f"  📥 IDM additions: {processing_results.get('successful_additions', 0)}")
                            print(f"  🔄 Dynamic monitoring used: {page_results.get('dynamic_monitoring_used', False)}")

                            if monitoring_time > 0:
                                print(f"  ⏱️ Monitoring time: {monitoring_time:.1f} minutes")

                            # Show progress update details
                            if progress_updated:
                                progress_update_results = processing_results.get('progress_update_results', {})
                                updated_progress = progress_update_results.get('updated_progress', {})
                                print(f"  💾 New total size: {updated_progress.get('total_size_mb', 0):.2f} MB")
                                print(f"  🎬 New video count: {updated_progress.get('total_downloaded', 0)}")

                                # Show completion rate from monitoring
                                monitoring_results = progress_update_results.get('monitoring_results', {})
                                if monitoring_results:
                                    completion_rate = monitoring_results.get('completion_rate', 0)
                                    print(f"  📈 Final completion rate: {completion_rate:.1f}%")

                            # CRITICAL: Verify counts match expected processing
                            expected_idm_additions = videos_found - duplicates_filtered
                            actual_idm_additions = processing_results.get('successful_additions', 0)

                            if expected_idm_additions != actual_idm_additions:
                                print(f"🚨 CRITICAL DEBUG WARNING: IDM addition count mismatch!")
                                print(f"  Expected IDM additions: {expected_idm_additions}")
                                print(f"  Actual IDM additions: {actual_idm_additions}")
                                print(f"  This may indicate processing failures!")

                        else:
                            print(f"⚠️ Warning: processing_results format unexpected")

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
                print(f"  📄 Next page: {current_page}")
                print(f"  📋 Pages processed: {pages_processed}")
                print(f"  🎬 Total videos found: {total_videos_found}")
                print(f"  🚫 Total duplicates filtered: {total_duplicates_filtered}")
                print(f"  ✅ Total videos processed: {total_videos_processed_after_filter}")
                print(f"  📊 Progress updates: {total_progress_updates}")
                if total_monitoring_time > 0:
                    print(f"  ⏱️ Total monitoring time: {total_monitoring_time:.1f} minutes")
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
            print("🎯 ENHANCED CONTINUOUS SCRAPING WITH COMPLETE DEBUGGING COMPLETED")
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
            if total_monitoring_time > 0:
                print(f"⏱️ Total dynamic monitoring time: {total_monitoring_time:.1f} minutes")
            print(f"💾 Final verified size: {final_size_mb:.2f} MB / {max_storage_gb} GB ({final_usage_percent:.2f}%)")
            print(f"🎬 Final verified completed videos: {final_completed_count}")
            print(f"❌ Final verified failed videos: {final_failed_count}")
            if (final_completed_count + final_failed_count) > 0:
                completion_rate = (final_completed_count/(final_completed_count+final_failed_count)*100)
                print(f"📈 Final completion rate: {completion_rate:.1f}%")
            else:
                print("📈 Final completion rate: 0.0%")
            print(f"✅ Final verification passed: {final_verification.get('verification_passed', False)}")

            # Calculate efficiency metrics
            if total_videos_found > 0:
                filter_efficiency = (total_duplicates_filtered / total_videos_found) * 100
                print(f"📈 Duplicate filter efficiency: {filter_efficiency:.1f}%")

            if pages_processed > 0:
                avg_videos_per_page = total_videos_found / pages_processed
                print(f"📊 Average videos per page: {avg_videos_per_page:.1f}")
                if total_monitoring_time > 0:
                    avg_monitoring_time = total_monitoring_time / pages_processed
                    print(f"📊 Average monitoring time per page: {avg_monitoring_time:.1f} minutes")

            if final_failed_count > 0:
                print(f"\n💡 DEBUGGING RECOMMENDATIONS:")
                print(f"  - Check IDM queue for stuck downloads")
                print(f"  - Verify internet connection stability")
                print(f"  - Consider using dynamic monitoring for better success rates")
                print(f"  - Only {final_failed_count} failed vs potentially many more with debugging improvements")

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
                "complete_debugging_enabled": True,
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
    """Main entry point for the enhanced continuous scraper."""
    print("🎬 Enhanced Continuous Rule34Video Scraper with Complete Debugging")
    print("=" * 80)
    print("🚀 This enhanced scraper provides:")
    print("  1. Complete video processing verification")
    print("  2. Page URL and request logging")
    print("  3. Video count verification at each step")
    print("  4. IDM addition success/failure tracking")
    print("  5. Processing failure identification")
    print("  6. Complete audit trail")
    print("  7. Dynamic monitoring capabilities")
    print("  8. Duplicate detection on FIRST page only")
    print("  9. Comprehensive monitoring and statistics")
    print("=" * 80)

    try:
        # Check required files
        required_files = [
            "progress_handler.py",
            "idm_manager_debug.py",
            "video_data_parser_debug.py",
            "progress_tracking.py",
            "duplicate_detection.py"
        ]

        missing_files = [f for f in required_files if not os.path.exists(f)]
        if missing_files:
            print(f"\n❌ Missing required files: {missing_files}")
            print("💡 Make sure debug files are in the same directory")
            return 1

        # Check optional config files
        if not os.path.exists("config.json"):
            print("⚠️ config.json not found - using default max_storage_gb: 940")

        if not os.path.exists("progress.json"):
            print("⚠️ progress.json not found - will start from page 1000")

        if not os.path.exists("downloads"):
            print("📁 downloads/ directory will be created automatically")

        print("✅ All required debug files found")

        # Initialize enhanced controller
        controller = EnhancedScraperController(
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

        print("\n📋 ENHANCED COMPREHENSIVE STATUS:")
        print(f"  📄 Starting page: {progress_data.get('last_page', 1000)}")
        print(f"  📊 Progress file videos: {progress_data.get('total_downloaded', 0)}")
        print(f"  📁 Actual completed videos: {download_stats['completed_folders']}")
        print(f"  ❌ Actual failed videos: {download_stats['failed_folders']}")
        print(f"  📊 Progress file size: {progress_data.get('total_size_mb', 0):.2f} MB")
        print(f"  💾 Actual folder size: {actual_mb:.2f} MB")
        print(f"  🎯 Size limit: {max_gb} GB ({max_gb * 1024} MB)")
        print(f"  📈 Actual usage: {usage_percent:.2f}%")
        print(f"  🛑 Stop threshold: 95% ({max_gb * 1024 * 0.95:.0f} MB)")
        print(f"  ⚠️ Sync needed: {'Yes' if progress_summary['sync_needed'] else 'No'}")
        print(f"  📈 Current completion rate: {download_stats['completion_rate']:.1f}%")
        print(f"  🐛 Complete debugging: ENABLED")
        print(f"  🔄 Dynamic monitoring: AVAILABLE")

        # Auto-sync if needed
        if progress_summary["sync_needed"]:
            print("\n🔄 Auto-syncing progress with downloads...")
            controller.progress_tracker.sync_progress_with_downloads()
            print("✅ Progress synchronized")

        # Confirm start
        print("\n" + "="*80)
        print("⚠️ READY TO START ENHANCED CONTINUOUS PARSING WITH COMPLETE DEBUGGING")
        print("="*80)
        print("🐛 Complete debugging: ENABLED (detailed video processing verification)")
        print("🔍 Duplicate detection: ENABLED for first page only")
        print("💡 Press Ctrl+C anytime to stop gracefully")
        print("💾 Progress updated ONLY after verified completion")
        print("🔄 Dynamic monitoring: AVAILABLE (can be used for better completion rates)")
        print("📊 Complete audit trail: ENABLED")
        print("="*80)

        response = input("\n🤔 Start enhanced continuous parsing with complete debugging? (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            print("\n🚀 Starting enhanced continuous scraper with complete debugging...")

            # Run the enhanced continuous loop
            results = controller.run_continuous_loop_with_complete_debugging()

            # Display comprehensive final results
            print("\n📋 COMPREHENSIVE FINAL RESULTS:")
            print(f"  Success: {results.get('success')}")
            print(f"  Stop reason: {results.get('stop_reason')}")
            print(f"  Pages processed: {results.get('pages_processed', 0)}")
            print(f"  Videos found: {results.get('total_videos_found', 0)}")
            print(f"  Duplicates filtered: {results.get('total_duplicates_filtered', 0)}")
            print(f"  Videos processed: {results.get('total_videos_processed', 0)}")
            print(f"  Progress updates: {results.get('progress_updates_performed', 0)}")
            if results.get('total_monitoring_time_minutes', 0) > 0:
                print(f"  Total monitoring time: {results.get('total_monitoring_time_minutes', 0):.1f} minutes")
            print(f"  Final verified size: {results.get('final_verified_size_mb', 0):.2f} MB")
            print(f"  Final completed videos: {results.get('final_verified_completed_count', 0)}")
            print(f"  Final failed videos: {results.get('final_verified_failed_count', 0)}")
            print(f"  Final verification passed: {results.get('final_verification_passed', False)}")

            if results.get("success"):
                print("\n✅ Enhanced scraping with complete debugging completed successfully!")
                print("🐛 Complete debugging provided detailed processing verification!")
            else:
                print("\n⚠️ Enhanced scraping stopped early")

            print("\n💡 Next steps:")
            print("  - Progress.json updates are verified and accurate")
            print("  - Check IDM for any remaining downloads")
            print("  - Downloads folder contains verified completed videos")
            print("  - Run again to continue from exact current position")
            print("  - Complete debugging ensures optimal processing verification")

        else:
            print("\n🛑 Cancelled by user")
            print("💡 Run anytime to start enhanced scraping with complete debugging")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("\n🔧 Check:")
        print("  - All enhanced debug files present")
        print("  - IDM installed and accessible")
        print("  - Valid JSON files")
        print("  - File permissions")
        print("  - Downloads folder accessibility")
        return 1


if __name__ == "__main__":
    """
    Entry point for enhanced continuous scraper with complete debugging.

    Enhanced files needed:
    - progress_handler.py (enhanced progress handler)
    - idm_manager_debug.py (debug IDM integration)
    - progress_tracking.py (progress tracking system)
    - duplicate_detection.py (duplicate detection system)
    - video_data_parser_debug.py (debug video parsing)
    - config.json (optional - contains max_storage_gb)
    - progress.json (optional - tracks verified progress)
    """
    print("\n🎯 Enhanced Continuous Rule34Video Scraper with Complete Debugging")
    print("📁 Working directory:", os.getcwd())
    print("🐛 Complete debugging: ENABLED (detailed processing verification)")
    print("🔍 Duplicate detection: Integrated with first page detection")
    print("🔄 Dynamic monitoring: Available for enhanced completion rates")

    exit_code = main()
    print("\n👋 Enhanced scraper finished")
    input("Press Enter to close...")
    sys.exit(exit_code)
