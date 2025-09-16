
# integrated_main_scraper.py - FINAL VERSION with Enhanced IDM Manager
import os
from pathlib import Path
import re
import time
import logging
import traceback
import json

# Import existing components we keep
from config_manager import ConfigManager
from progress_tracker import ProgressTracker
from web_driver_manager import WebDriverManager
from date_parser import DateParser
from page_navigator import PageNavigator
from file_validator import FileValidator

# Import the Enhanced IDM Manager (handles both parsing and downloading)
from enhanced_idm_manager import EnhancedIDMManager

class IntegratedVideoScraper:
    """
    FINAL INTEGRATED VIDEO SCRAPER

    Uses Enhanced IDM Manager for ALL parsing and downloading.
    Keeps existing progress tracking, storage management, and page navigation.

    Workflow:
    1. Check progress.json for last downloaded page
    2. Navigate to starting page  
    3. Loop through pages backwards
    4. For each page: Enhanced IDM Manager handles everything
    5. Update progress tracking
    6. Monitor storage limits
    """

    def __init__(self):
        # Initialize existing components we keep
        self.config_manager = ConfigManager()
        self.config = self.config_manager.get_config()
        self.setup_logging()

        # Initialize components we keep
        self.progress_tracker = ProgressTracker()
        self.web_driver_manager = WebDriverManager(self.config)
        self.date_parser = DateParser()
        self.page_navigator = PageNavigator(self.config, self.web_driver_manager)
        self.file_validator = FileValidator(self.config)

        # Initialize Enhanced IDM Manager (replaces video_info_extractor, idm_downloader, video_processor)
        download_path = self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\")
        idm_path = self.config.get("download", {}).get("idm_path", None)

        # Pass web driver manager to Enhanced IDM Manager for parsing
        self.enhanced_idm_manager = EnhancedIDMManager(
            base_download_dir=download_path,
            idm_path=idm_path,
            web_driver_manager=self.web_driver_manager
        )

        # Storage management (keep existing)
        self.max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
        self.warning_threshold = 0.9
        self.last_storage_check = 0
        self.storage_check_interval = 60

        # Force stop support
        self.force_stop_requested = False

        self.logger.info("🎬 FINAL INTEGRATED VIDEO SCRAPER INITIALIZED")
        self.logger.info("🔧 Enhanced IDM Manager handles ALL parsing and downloading")
        self.logger.info(f"📁 Download directory: {download_path}")

    def setup_logging(self):
        """Configure logging system"""
        log_level = getattr(logging, self.config["logging"]["log_level"].upper())
        formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

        self.logger = logging.getLogger('IntegratedScraper')
        self.logger.setLevel(log_level)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        if self.config["logging"]["log_to_file"]:
            file_handler = logging.FileHandler(self.config["logging"]["log_file_path"])
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def get_download_folder_size(self):
        """Calculate total size of download folder in bytes"""
        try:
            download_path = self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\")
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(download_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
            return total_size
        except Exception as e:
            self.logger.error(f"Error calculating folder size: {e}")
            return 0

    def check_storage_limits(self, force_check=False):
        """Check storage limits and return tuple (is_limit_reached, current_usage_gb)"""
        current_time = time.time()
        if not force_check and (current_time - self.last_storage_check) < self.storage_check_interval:
            return False, 0.0

        self.last_storage_check = current_time
        try:
            current_usage_bytes = self.get_download_folder_size()
            current_usage_gb = current_usage_bytes / (1024**3)
            max_storage_bytes = self.max_storage_gb * 1024**3
            usage_percentage = (current_usage_gb / self.max_storage_gb) * 100

            self.logger.info(f"Storage usage: {current_usage_gb:.2f} GB / {self.max_storage_gb} GB ({usage_percentage:.1f}%)")

            # Check for 90% warning threshold
            if current_usage_gb >= (self.max_storage_gb * self.warning_threshold):
                if not hasattr(self, '_warning_logged') or not self._warning_logged:
                    self.logger.warning(f"WARNING: Storage usage is above 90%! Current: {current_usage_gb:.2f} GB ({usage_percentage:.1f}%)")
                    self._warning_logged = True

            # Check if limit is reached
            if current_usage_bytes >= max_storage_bytes:
                self.logger.error(f"STORAGE LIMIT REACHED: {current_usage_gb:.2f} GB / {self.max_storage_gb} GB")
                return True, current_usage_gb

            return False, current_usage_gb
        except Exception as e:
            self.logger.error(f"Error checking storage limits: {e}")
            return False, 0.0

    def check_force_stop(self):
        """Check if force stop was requested"""
        if hasattr(self, 'gui_force_stop_check') and callable(self.gui_force_stop_check):
            if self.gui_force_stop_check():
                self.force_stop_requested = True

        if self.force_stop_requested:
            self.logger.warning(" FORCE STOP DETECTED - Aborting current operation")
            return True
        return False

    def determine_start_strategy(self):
        """Determine starting page and navigate to it"""
        self.logger.info("=" * 80)
        self.logger.info("DETERMINING START STRATEGY")
        self.logger.info("=" * 80)

        # Step 1: Check progress.json for downloaded videos
        self.logger.info("Step 1: Checking progress.json for existing downloads...")
        downloaded_videos = self.progress_tracker.get_downloaded_videos()

        if not downloaded_videos or len(downloaded_videos) == 0:
            self.logger.info("RESULT: No downloads found in progress.json")
            self.logger.info("DECISION: Starting FRESH SCRAPE - will find ACTUAL last page from website")

            # Get actual last page from website with retries
            self.logger.info("Step 2: Fetching ACTUAL highest page number from website...")
            max_retries = 3
            for retry in range(max_retries):
                try:
                    self.logger.info(f" Attempt {retry + 1}/{max_retries} to find last page...")
                    last_page = self.page_navigator.get_last_page_number()

                    if last_page and last_page >= 1000:  # Reasonable validation
                        self.logger.info(f" SUCCESS: Found actual last page = {last_page}")
                        self.logger.info(f" NAVIGATING to the last page {last_page} to start scraping from there...")

                        if self.page_navigator.handle_page_navigation(last_page):
                            self.logger.info(f" Successfully navigated to page {last_page}")
                            self.logger.info(f"STRATEGY: Will scrape backwards from {last_page} -> {last_page-1} -> ... -> 1")
                            self.logger.info("=" * 80)
                            return last_page
                        else:
                            fallback_page = last_page - 1
                            if self.page_navigator.handle_page_navigation(fallback_page):
                                self.logger.info(f" Fallback successful: Starting from page {fallback_page}")
                                return fallback_page

                    if retry < max_retries - 1:
                        time.sleep(10)
                        continue

                except Exception as e:
                    self.logger.error(f" Error on attempt {retry + 1}: {e}")
                    if retry < max_retries - 1:
                        time.sleep(10)
                        continue

            # Final fallback
            fallback_page = 1000
            self.logger.error(" CRITICAL: Could not determine actual last page after all attempts!")
            self.logger.warning(f" FALLBACK: Trying to start from page {fallback_page}")
            if self.page_navigator.handle_page_navigation(fallback_page):
                self.logger.info(f" Emergency fallback successful: Starting from page {fallback_page}")
            return fallback_page

        else:
            self.logger.info(f"RESULT: Found {len(downloaded_videos)} existing downloads in progress.json")

            # Step 2: Check last processed page for resume
            last_processed_page = self.progress_tracker.get_last_processed_page()
            if last_processed_page and last_processed_page >= 1:
                self.logger.info(f" FOUND: Last processed page was {last_processed_page}")
                self.logger.info(f" NAVIGATING to resume page {last_processed_page}...")

                if self.page_navigator.handle_page_navigation(last_processed_page):
                    self.logger.info(f" Successfully navigated to resume page {last_processed_page}")
                    self.logger.info(f"DECISION: Resuming from page {last_processed_page} going backwards")
                    return last_processed_page
                else:
                    self.logger.error(f" Could not navigate to resume page {last_processed_page}")

            # Fallback to getting actual last page
            try:
                last_page = self.page_navigator.get_last_page_number()
                if last_page and self.page_navigator.handle_page_navigation(last_page):
                    return last_page
            except Exception as e:
                self.logger.error(f"Error getting fallback page: {e}")

            return 1000

    def process_page_with_enhanced_idm_manager(self, page_num):
        """
        🎯 CORE INTEGRATION METHOD

        Process a single page using Enhanced IDM Manager for ALL parsing and downloading.
        This completely replaces the old workflow with IDM Manager handling everything.
        """
        try:
            self.logger.info(f"🎬 Processing page {page_num} with Enhanced IDM Manager")
            self.logger.info("🔧 IDM Manager will handle: URL extraction -> Video parsing -> IDM downloading")

            # Check for force stop
            if self.check_force_stop():
                self.logger.warning(" FORCE STOP REQUESTED - Skipping page processing")
                return False

            # Step 1: Get video URLs from the page (using existing page navigator)
            self.logger.info(f"📋 Step 1: Getting video URLs from page {page_num}...")
            video_urls = self.page_navigator.get_video_links_from_page(page_num)
            if not video_urls:
                self.logger.error(f"No video links found on page {page_num}")
                return False

            self.logger.info(f"Found {len(video_urls)} video URLs on page {page_num}")

            # Step 2: Filter out already downloaded videos (using existing progress tracker)
            self.logger.info(f"📋 Step 2: Filtering out already downloaded videos...")
            new_video_urls = []
            for video_url in video_urls:
                try:
                    video_id = self.enhanced_idm_manager.extract_video_id_from_url(video_url)
                    if self.progress_tracker.is_video_downloaded(video_id):
                        self.logger.debug(f"Skipping {video_id}: already downloaded")
                        continue

                    # Check if folder already exists and is valid
                    if self.file_validator.validate_video_folder(video_id):
                        self.logger.debug(f"Skipping {video_id}: valid folder exists")
                        continue

                    new_video_urls.append(video_url)

                except Exception as e:
                    self.logger.warning(f"Error checking video {video_url}: {e}")
                    continue

            if not new_video_urls:
                self.logger.info("No new videos to process on this page - all already downloaded")
                return True

            self.logger.info(f"Found {len(new_video_urls)} new videos to process")

            # Check for force stop before processing
            if self.check_force_stop():
                self.logger.warning(" FORCE STOP during video URL processing")
                return False

            # Step 3: 🎯 ENHANCED IDM MANAGER TAKES COMPLETE CONTROL
            self.logger.info(f"📋 Step 3: Enhanced IDM Manager processing {len(new_video_urls)} videos...")
            self.logger.info("🔧 This includes: Parsing video data + Adding to IDM queue + Starting downloads")

            try:
                # This single call handles EVERYTHING:
                # - Parse video data from each URL
                # - Create directory structure
                # - Save metadata JSON files  
                # - Add MP4 and JPG to IDM queue
                # - Start IDM downloads
                results = self.enhanced_idm_manager.process_video_urls(new_video_urls, start_queue=True)

                if results.get("success"):
                    successful_count = results.get("successful_additions", 0)
                    failed_count = results.get("failed_additions", 0)
                    parsed_count = results.get("parsed_videos", 0)

                    self.logger.info(f"✅ Enhanced IDM Manager Results for page {page_num}:")
                    self.logger.info(f"  🔍 Videos parsed: {parsed_count}/{len(new_video_urls)}")
                    self.logger.info(f"  ✅ Successfully added to IDM: {successful_count}")
                    self.logger.info(f"  ❌ Failed to add to IDM: {failed_count}")

                    # Step 4: Update progress tracker with successful downloads
                    self.logger.info(f"📋 Step 4: Updating progress tracker...")
                    for video_url in new_video_urls:
                        try:
                            video_id = self.enhanced_idm_manager.extract_video_id_from_url(video_url)

                            # Check if this video was successfully processed
                            video_results = results.get('video_results', {})
                            if video_id in video_results:
                                video_result = video_results[video_id]
                                # If video was successfully added to IDM queue, mark as downloaded
                                if video_result.get('video', False):
                                    self.progress_tracker.update_download_stats(video_id, 0, page_num)
                                    self.logger.debug(f"Marked {video_id} as downloaded in progress tracker")

                        except Exception as e:
                            self.logger.warning(f"Error updating progress for video: {e}")

                    self.logger.info(f"✅ Page {page_num} processing completed successfully")
                    return True

                else:
                    error_msg = results.get("error", "Unknown error")
                    self.logger.error(f"❌ Enhanced IDM Manager failed to process page {page_num}: {error_msg}")
                    return False

            except Exception as e:
                self.logger.error(f"❌ Error with Enhanced IDM Manager processing page {page_num}: {e}")
                traceback.print_exc()
                return False

        except Exception as e:
            self.logger.error(f"❌ Error processing page {page_num}: {e}")
            traceback.print_exc()
            return False

    def run_backwards_scrape(self, start_page):
        """Run scrape from high page numbers going backwards to page 1"""
        current_page = start_page
        self.logger.info(f"🚀 STARTING BACKWARDS SCRAPE WITH ENHANCED IDM MANAGER")
        self.logger.info(f"📋 Begin page: {start_page}")
        self.logger.info(f"📋 Direction: BACKWARDS (high to low)")
        self.logger.info(f"🔧 Enhanced IDM Manager handles ALL parsing and downloading per page")

        while current_page >= 1:
            # Check for force stop before each page
            if self.check_force_stop():
                self.logger.warning(" FORCE STOP REQUESTED - Stopping backwards scrape")
                break

            # Check storage before each page
            limit_reached, usage_gb = self.check_storage_limits()
            if limit_reached:
                self.logger.info("Storage limit reached, stopping scrape")
                break

            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"🎬 PROCESSING PAGE {current_page} (ENHANCED IDM MANAGER)")
            self.logger.info(f"📊 Remaining pages to process: {current_page} pages")
            self.logger.info(f"{'='*80}")

            try:
                # 🎯 CORE INTEGRATION: Process page with Enhanced IDM Manager
                page_processed = self.process_page_with_enhanced_idm_manager(current_page)

                if page_processed:
                    # Update last processed page (existing progress tracking)
                    self.progress_tracker.update_last_processed_page(current_page)
                    self.logger.info(f"✅ Page {current_page} completed successfully")
                else:
                    self.logger.warning(f"⚠️ Page {current_page} processing failed")

                # Move to previous page (backwards)
                current_page -= 1
                self.logger.info(f"⬅️ Moving backwards: Next page will be {current_page if current_page >= 1 else 'COMPLETE'}")

                # Wait between pages (with force stop checking)
                if current_page >= 1:
                    self.wait_between_pages()

            except Exception as e:
                if self.check_force_stop():
                    self.logger.warning(" FORCE STOP during page processing")
                    break
                self.logger.error(f"❌ Error processing page {current_page}: {e}")
                traceback.print_exc()
                current_page -= 1
                continue

        # Final summary
        final_page = current_page + 1
        if self.force_stop_requested:
            self.logger.info(f"🛑 BACKWARDS SCRAPE FORCE STOPPED")
            self.logger.info(f"📋 Last completed page: {final_page}")
        else:
            self.logger.info(f"🎉 BACKWARDS SCRAPE COMPLETED")
            self.logger.info(f"📋 Last page processed: {final_page}")
            self.logger.info(f"📋 Total pages processed: {start_page - final_page + 1}")

        # Print final statistics
        self.enhanced_idm_manager.print_stats()

    def wait_between_pages(self):
        """Wait between pages with force stop checking"""
        page_delay = self.config.get("general", {}).get("delay_between_pages", 5000) / 1000
        self.logger.info(f"⏱️ Waiting {page_delay} seconds before next page...")

        wait_increment = 0.5  # Check every 0.5 seconds
        total_waited = 0
        while total_waited < page_delay:
            if self.check_force_stop():
                self.logger.warning(" FORCE STOP REQUESTED - Interrupting page delay")
                break
            time.sleep(min(wait_increment, page_delay - total_waited))
            total_waited += wait_increment

    def run(self):
        """🎯 MAIN EXECUTION LOOP - FINAL INTEGRATED VERSION"""
        try:
            self.logger.info("🚀 STARTING FINAL INTEGRATED VIDEO SCRAPER")
            self.logger.info("🔧 Enhanced IDM Manager handles ALL parsing and downloading")
            self.logger.info("📋 Existing components handle progress tracking, storage, navigation")

            # Initialize force stop flag
            self.force_stop_requested = False

            # Initial storage check
            limit_reached, usage_gb = self.check_storage_limits(force_check=True)
            if limit_reached:
                self.logger.error("Storage limit already reached. Cannot start scraping.")
                return

            # Setup web driver
            self.web_driver_manager.setup_driver()

            # Determine starting strategy (existing logic)
            start_page = self.determine_start_strategy()
            self.logger.info(f"📋 Starting scrape from page {start_page} working backwards")

            # 🎯 Run backwards scrape with Enhanced IDM Manager integration
            self.run_backwards_scrape(start_page)

        except KeyboardInterrupt:
            self.logger.info("⚠️ Scraping interrupted by user (Ctrl+C)")
            current_usage = self.get_download_folder_size()
            self.logger.info(f"📊 Usage at interruption: {current_usage / (1024**3):.2f} GB")
        except Exception as e:
            if self.force_stop_requested:
                self.logger.info("🛑 Scraper stopped due to force stop request")
            else:
                self.logger.error(f"❌ Unexpected error in main loop: {e}")
                traceback.print_exc()
        finally:
            try:
                self.web_driver_manager.close_driver()
            except Exception as e:
                self.logger.error(f"❌ Error during cleanup: {e}")

            if self.force_stop_requested:
                self.logger.info("🛑 Scraper force stopped by user")
            else:
                self.logger.info("🎉 Scraper finished normally")


# 🎯 USAGE - FINAL WORKING PRODUCT
if __name__ == "__main__":
    print("🎬 FINAL INTEGRATED VIDEO SCRAPER")
    print("=" * 70)
    print("🔧 WHAT THIS DOES:")
    print("   ✅ Check starting method from progress.json")
    print("   ✅ Goes to last page if downloaded_videos list is empty")  
    print("   ✅ Continues from last page if not empty")
    print("   ✅ Parse and download videos entirely from Enhanced IDM Manager")
    print("   ✅ Loop Enhanced IDM Manager for each page")
    print("   ✅ Keep everything else the same (progress, storage, validation)")
    print("=" * 70)
    print("🎯 INTEGRATION POINTS:")
    print("   📋 Progress tracking: KEPT (progress.json)")
    print("   📊 Storage monitoring: KEPT (max size limits)")  
    print("   🌐 Page navigation: KEPT (backwards scraping)")
    print("   🔍 Video parsing: REPLACED with Enhanced IDM Manager")
    print("   📥 Video downloading: REPLACED with Enhanced IDM Manager")
    print("   ✅ File validation: KEPT (folder validation)")
    print("=" * 70)

    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)

    # 🚀 RUN THE INTEGRATED SCRAPER
    scraper = IntegratedVideoScraper()
    scraper.run()

    print("\n✅ Integration complete!")
    print("🎯 Enhanced IDM Manager now handles all parsing and downloads")
    print("📋 All other functionality remains unchanged")
