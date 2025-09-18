#!/usr/bin/env python3
# Enhanced Main Scraper with Complete Debugging and Verification
# This version ensures ALL videos found on each page are processed and added to IDM

from progress_handler import ImprovedProgressHandler
from progress_tracking import EnhancedProgressTracker
import json
import sys
import os
import time
import signal
from pathlib import Path
from typing import Dict, Any

class EnhancedScraperController:
    def __init__(self, progress_file: str = "progress.json", config_file: str = "config.json",
                 downloads_dir: str = "downloads", enable_duplicate_detection: bool = True,
                 duplicate_check_limit: int = 100):

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
        print("=" * 80)

    def load_config(self) -> Dict[str, Any]:
        try:
            if not os.path.exists(self.config_file):
                print(f"⚠️ Config file not found: {self.config_file}")
                default_config = {"general": {"max_storage_gb": 940}}
                return default_config

            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                max_storage_gb = config.get('general', {}).get('max_storage_gb', 940)
                print(f"✅ Config loaded: max_storage_gb = {max_storage_gb} GB")
                return config
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            return {"general": {"max_storage_gb": 940}}

    def signal_handler(self, signum, frame):
        print("\n\n⚠️ Interrupt received (Ctrl+C)")
        print("🛑 Preparing to stop after current page...")
        self.should_stop = True
        self.stop_reason = "User interrupted with Ctrl+C"

    def get_current_progress(self) -> Dict[str, Any]:
        return self.progress_tracker.updater.read_current_progress()

    def update_progress_page(self, new_page: int):
        success = self.progress_tracker.updater.update_page_progress(new_page)
        if success:
            print(f"💾 Progress updated: last_page = {new_page}")
        else:
            print(f"⚠️ Failed to update progress page to {new_page}")

    def initialize_session(self):
        if not self.session_initialized:
            progress_data = self.get_current_progress()
            self.session_start_page = progress_data.get('last_page', 1000)
            self.pages_processed_in_session = 0
            self.session_initialized = True

            print(f"\n🔍 Session Initialization:")
            verification = self.progress_tracker.verify_and_fix_progress()
            if not verification["verification_passed"]:
                print("⚠️ Progress discrepancies found and fixed during initialization")
            print(f" 📄 Session start page: {self.session_start_page}")

    def is_first_page_of_session(self, current_page: int) -> bool:
        if not self.session_initialized:
            self.initialize_session()

        is_first = (current_page == self.session_start_page) and (self.pages_processed_in_session == 0)

        if is_first:
            print(f"🎯 FIRST PAGE OF SESSION: {current_page}")
            print(f" 🔍 Duplicate detection: ENABLED")
        else:
            print(f"📄 Subsequent page: {current_page}")
            print(f" 🔍 Duplicate detection: DISABLED")

        return is_first

    def check_size_limit(self) -> bool:
        progress_summary = self.progress_tracker.get_progress_summary()
        actual_size_mb = progress_summary["download_folder_stats"]["total_size_mb"]
        max_storage_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)
        max_storage_mb = max_storage_gb * 1024
        usage_percent = (actual_size_mb / max_storage_mb) * 100 if max_storage_mb > 0 else 0

        print(f"💾 Storage Status:")
        print(f" 📥 Actual folder size: {actual_size_mb:.2f} MB")
        print(f" 🎯 Size limit: {max_storage_gb} GB ({max_storage_mb:.0f} MB)")
        print(f" 📊 Usage: {usage_percent:.2f}%")

        if usage_percent >= 95.0:
            print("🛑 SIZE LIMIT REACHED!")
            self.stop_reason = f"Size limit reached: {actual_size_mb:.2f} MB / {max_storage_gb} GB"
            return True

        return False

    def should_continue(self, current_page: int) -> bool:
        if self.should_stop:
            return False
        if current_page <= 1:
            self.stop_reason = "Reached page 1 (end of available pages)"
            return False
        if self.check_size_limit():
            return False
        return True

    def run_continuous_loop_with_complete_debugging(self) -> Dict[str, Any]:
        print("\n🔄 Starting enhanced continuous parsing loop...")

        # Initialize session tracking
        self.initialize_session()

        # Initialize enhanced progress handler that uses debug versions
        handler = EnhancedProgressHandler(self.progress_file, self.downloads_dir)

        # Get starting configuration
        progress_data = self.get_current_progress()
        current_page = progress_data.get('last_page', 1000)
        starting_page = current_page

        print(f"\n🎯 Enhanced Configuration:")
        print(f" 📄 Starting page: {current_page}")
        print(f" 📊 Downloads directory: {self.downloads_dir}")
        print(f" 📄 Progress file: {self.progress_file}")
        print(f" 🐛 Complete debugging: ENABLED")
        print("\n" + "="*80)

        # Statistics tracking
        pages_processed = 0
        total_videos_found = 0
        total_videos_processed = 0
        total_videos_added_to_idm = 0
        successful_pages = 0
        failed_pages = 0
        total_duplicates_filtered = 0

        try:
            while self.should_continue(current_page):
                print(f"\n📄 PROCESSING PAGE {current_page} WITH COMPLETE DEBUGGING")
                print("=" * 80)
                print(f"🐛 DEBUG: Current page URL will be: https://rule34video.com/latest-updates/{current_page}")

                # Determine if duplicate detection should be applied
                is_first_page = self.is_first_page_of_session(current_page)
                apply_duplicate_detection = self.enable_duplicate_detection and is_first_page

                try:
                    # Process current page with debug versions
                    import asyncio
                    page_results = asyncio.run(handler.process_single_page_async_debug(
                        page=current_page,
                        download_dir=self.downloads_dir,
                        idm_path=None,
                        enable_duplicate_detection=apply_duplicate_detection,
                        duplicate_check_limit=self.duplicate_check_limit,
                        use_dynamic_monitoring=True
                    ))

                    pages_processed += 1
                    self.pages_processed_in_session += 1

                    if page_results.get("success"):
                        successful_pages += 1

                        # Extract enhanced statistics with debugging
                        processing_results = page_results.get('processing_results', {})
                        if isinstance(processing_results, dict):
                            videos_found = processing_results.get('videos_found_count', 0)
                            videos_processed = processing_results.get('videos_processed_count', 0)
                            videos_added_to_idm = processing_results.get('videos_added_to_idm_count', 0)
                            duplicates_filtered = processing_results.get('videos_filtered_by_duplicates', 0)

                            total_videos_found += videos_found
                            total_videos_processed += videos_processed
                            total_videos_added_to_idm += videos_added_to_idm
                            total_duplicates_filtered += duplicates_filtered

                            print(f"✅ Page {current_page} completed successfully")
                            print(f"🐛 DEBUG: Page {current_page} Results:")
                            print(f"  🎬 Videos found: {videos_found}")
                            print(f"  🔄 Videos processed: {videos_processed}")
                            print(f"  📥 Videos added to IDM: {videos_added_to_idm}")
                            print(f"  🚫 Duplicates filtered: {duplicates_filtered}")

                            # CRITICAL: Verify counts match
                            expected_processed = videos_found - duplicates_filtered
                            if videos_processed != expected_processed:
                                print(f"🚨 CRITICAL WARNING: Processing count mismatch!")
                                print(f"  Expected to process: {expected_processed}")
                                print(f"  Actually processed: {videos_processed}")

                            if videos_added_to_idm == 0 and videos_processed > 0:
                                print(f"🚨 CRITICAL WARNING: Videos processed but NONE added to IDM!")

                            verification_passed = processing_results.get('verification_passed', False)
                            print(f"  ✅ Verification passed: {verification_passed}")

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
                print(f"\n📊 Session Progress Summary:")
                print(f" 📄 Next page: {current_page}")
                print(f" 📋 Pages processed: {pages_processed}")
                print(f" 🎬 Total videos found: {total_videos_found}")
                print(f" 🔄 Total videos processed: {total_videos_processed}")
                print(f" 📥 Total videos added to IDM: {total_videos_added_to_idm}")
                print(f" 🚫 Total duplicates filtered: {total_duplicates_filtered}")
                print("=" * 80)

                time.sleep(1)

            # Final results
            print(f"\n🎯 ENHANCED SCRAPING COMPLETED")
            print("=" * 80)
            print(f"🛑 Stop Reason: {self.stop_reason}")
            print(f"📄 Starting page: {starting_page}")
            print(f"📄 Final page: {current_page}")
            print(f"📊 Pages processed: {pages_processed}")
            print(f"✅ Successful pages: {successful_pages}")
            print(f"❌ Failed pages: {failed_pages}")
            print(f"🎬 Total videos found: {total_videos_found}")
            print(f"🔄 Total videos processed: {total_videos_processed}")
            print(f"📥 Total videos added to IDM: {total_videos_added_to_idm}")
            print(f"🚫 Total duplicates filtered: {total_duplicates_filtered}")

            if total_videos_found > 0:
                success_rate = (total_videos_added_to_idm / total_videos_found) * 100
                print(f"📈 Overall success rate: {success_rate:.1f}%")

            print("=" * 80)

            return {
                "success": True,
                "stop_reason": self.stop_reason,
                "starting_page": starting_page,
                "final_page": current_page,
                "pages_processed": pages_processed,
                "successful_pages": successful_pages,
                "failed_pages": failed_pages,
                "total_videos_found": total_videos_found,
                "total_videos_processed": total_videos_processed,
                "total_videos_added_to_idm": total_videos_added_to_idm,
                "total_duplicates_filtered": total_duplicates_filtered
            }

        except KeyboardInterrupt:
            print("\n\n⚠️ Keyboard interrupt detected")
            self.stop_reason = "User interrupted with Ctrl+C"
            self.update_progress_page(current_page)
            return {
                "success": False,
                "stop_reason": self.stop_reason,
                "interrupted": True
            }

class EnhancedProgressHandler(ImprovedProgressHandler):
    # Enhanced progress handler that uses debug versions

    async def process_single_page_async_debug(self, page: int, download_dir: str = "downloads", 
                                            idm_path: str = None, enable_duplicate_detection: bool = True, 
                                            duplicate_check_limit: int = 100, use_dynamic_monitoring: bool = True) -> Dict[str, Any]:

        print(f"\n📄 Processing page with COMPLETE DEBUGGING: {page}")
        print("-" * 70)

        try:
            # Construct URL for specific page
            page_url = f"{self.base_url}{page}"
            print(f"🌐 Page URL: {page_url}")
            print(f"🐛 DEBUG: About to visit URL: {page_url}")

            # Import and initialize the DEBUG IDM processor
            from idm_manager_debug import ImprovedIDMManager
            from video_data_parser_debug import OptimizedVideoDataParser

            # Create IDM manager with debug settings
            idm_manager = ImprovedIDMManager(
                base_download_dir=download_dir,
                idm_path=idm_path,
                enable_duplicate_detection=enable_duplicate_detection,
                duplicate_check_limit=duplicate_check_limit,
                progress_file=str(self.progress_file)
            )

            parser = OptimizedVideoDataParser(page_url)
            print(f"✅ Debug components initialized for page {page}")

            # Process videos with debug workflow
            print(f"\n🚀 Starting DEBUG processing workflow for page {page}...")

            # Step 1: Extract video URLs with debugging
            print(f"🐛 DEBUG: Step 1 - Extracting video URLs from {page_url}")
            video_urls = await parser.extract_video_urls()

            print(f"🐛 DEBUG: Video URL extraction completed")
            print(f"🐛 DEBUG: Found {len(video_urls)} video URLs")

            if not video_urls:
                return {"success": False, "error": "No video URLs found"}

            # Step 2: Parse video metadata with debugging  
            print(f"🐛 DEBUG: Step 2 - Parsing video metadata for {len(video_urls)} videos")
            videos_data = await parser.parse_all_videos()

            print(f"🐛 DEBUG: Video parsing completed")
            print(f"🐛 DEBUG: Parsed {len(videos_data)} videos")

            if not videos_data:
                return {"success": False, "error": "No video metadata could be parsed"}

            # Step 3: Process with debug IDM manager
            print(f"🐛 DEBUG: Step 3 - Processing {len(videos_data)} videos with IDM manager")
            results = idm_manager.process_all_videos(
                videos_data,
                start_queue=True,
                current_page=page,
                use_dynamic_monitoring=use_dynamic_monitoring
            )

            print(f"✅ Page {page} debug processing completed")
            print(f"🐛 DEBUG: Final results summary:")
            print(f"  Videos found: {results.get('videos_found_count', 0)}")
            print(f"  Videos processed: {results.get('videos_processed_count', 0)}")
            print(f"  Videos added to IDM: {results.get('videos_added_to_idm_count', 0)}")

            return {
                "success": True,
                "page_processed": page,
                "url_used": page_url,
                "processing_results": results,
                "duplicate_detection_applied": enable_duplicate_detection,
                "dynamic_monitoring_used": use_dynamic_monitoring
            }

        except Exception as e:
            print(f"❌ Error in debug processing for page {page}: {e}")
            return {
                "success": False,
                "error": str(e),
                "page_processed": page,
                "url_used": f"{self.base_url}{page}"
            }

def main():
    print("🎬 Enhanced Continuous Scraper with Complete Debugging")
    print("=" * 80)
    print("🐛 This enhanced scraper provides:")
    print("  1. Complete video processing verification")
    print("  2. Page URL and request logging")
    print("  3. Video count verification at each step")
    print("  4. IDM addition success/failure tracking")
    print("  5. Processing failure identification")
    print("  6. Complete audit trail")
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

        print("✅ All required debug files found")

        # Initialize enhanced controller
        controller = EnhancedScraperController(
            enable_duplicate_detection=True,
            duplicate_check_limit=100
        )

        # Show initial status
        progress_summary = controller.progress_tracker.get_progress_summary()
        progress_data = progress_summary["progress_file_data"]

        print(f"\n📋 ENHANCED STATUS:")
        print(f" 📄 Starting page: {progress_data.get('last_page', 1000)}")
        print(f" 🐛 Complete debugging: ENABLED")
        print(f" 🔍 Video processing verification: ENABLED")
        print(f" 📊 Page URL logging: ENABLED")

        # Confirm start
        print("\n" + "="*80)
        print("⚠️ READY TO START ENHANCED SCRAPING WITH COMPLETE DEBUGGING")
        print("="*80)

        response = input("\n🤔 Start enhanced scraping with complete debugging? (y/n): ").strip().lower()

        if response in ['y', 'yes']:
            print("\n🚀 Starting enhanced scraper...")

            # Run the enhanced continuous loop
            results = controller.run_continuous_loop_with_complete_debugging()

            print("\n📋 FINAL RESULTS:")
            for key, value in results.items():
                print(f" {key}: {value}")

            if results.get("success"):
                print("\n✅ Enhanced scraping completed successfully!")
            else:
                print("\n⚠️ Enhanced scraping stopped early")
        else:
            print("\n🛑 Cancelled by user")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit_code = main()
    print("\n👋 Enhanced scraper finished")
    input("Press Enter to close...")
    sys.exit(exit_code)
