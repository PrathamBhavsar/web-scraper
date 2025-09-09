# main_scraper.py - PARALLEL PROCESSING VERSION

import os
from pathlib import Path
import re
import time
import logging
import traceback
import asyncio
import json
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

from config_manager import ConfigManager
from progress_tracker import ProgressTracker
from web_driver_manager import WebDriverManager
from date_parser import DateParser
from page_navigator import PageNavigator
from video_info_extractor import VideoInfoExtractor
from file_validator import FileValidator
from file_downloader import FileDownloader
from video_processor import VideoProcessor
from smart_retry_extractor import SmartRetryExtractor

class VideoScraper:
    def __init__(self):
        # Initialize all component managers
        self.config_manager = ConfigManager()
        self.config = self.config_manager.get_config()
        self.setup_logging()
        
        # Initialize all components
        self.progress_tracker = ProgressTracker()
        self.web_driver_manager = WebDriverManager(self.config)
        self.date_parser = DateParser()
        self.page_navigator = PageNavigator(self.config, self.web_driver_manager)
        self.file_validator = FileValidator(self.config)
        self.file_downloader = FileDownloader(self.config)
        self.video_info_extractor = VideoInfoExtractor(
            self.config, self.web_driver_manager, self.date_parser
        )
        self.video_processor = VideoProcessor(
            self.config, self.file_validator, self.file_downloader, self.progress_tracker
        )
        self.smart_retry_extractor = SmartRetryExtractor(
            self.config, self.web_driver_manager, self.video_info_extractor, self.file_validator
        )
        
        # PARALLEL PROCESSING CONFIGURATION
        self.max_concurrent_videos = self.config.get("processing", {}).get("max_concurrent_videos", 3)
        self.use_parallel_video_processing = self.config.get("processing", {}).get("use_parallel_video_processing", True)
        
        # Processing mode configuration
        self.processing_mode = self.config.get("processing", {}).get("mode", "hybrid")
        self.parallel_batch_size = self.config.get("processing", {}).get("parallel_batch_size", 5)
        self.use_parallel_processing = self.config.get("processing", {}).get("use_parallel", True)
        
        # Storage management
        self.max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
        self.warning_threshold = 0.9 # 90% threshold for warning
        self.last_storage_check = 0
        self.storage_check_interval = 60 # Check storage every 60 seconds
        
        # Log parallel processing configuration
        self.logger.info(f"PARALLEL PROCESSING ENABLED: Processing up to {self.max_concurrent_videos} videos simultaneously")
        
        # Log download method status and IDM availability
        download_method = self.config.get("download", {}).get("download_method", "direct")
        self.logger.info(f"Download method configured: {download_method}")
        
        if download_method == "idm":
            if self.file_downloader.idm_downloader.is_idm_available():
                idm_version = self.file_downloader.idm_downloader.get_idm_version()
                self.logger.info(f"IDM status: Available - {idm_version}")
            else:
                self.logger.warning("IDM method selected but IDM not available - downloads will fail!")
                self.logger.warning(f"Please check IDM installation at: {self.file_downloader.idm_downloader.idm_path}")
        else:
            self.logger.info(f"Using direct download method")

    def setup_logging(self):
        """Configure logging system"""
        log_level = getattr(logging, self.config["logging"]["log_level"].upper())
        
        # Create formatter with thread name
        formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
        
        # Setup logger
        self.logger = logging.getLogger('Rule34Scraper')
        self.logger.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler if enabled
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
        """
        Check storage limits and return tuple (is_limit_reached, current_usage_gb)
        Also logs warning when 90% threshold is reached
        """
        current_time = time.time()
        # Skip frequent checks unless forced
        if not force_check and (current_time - self.last_storage_check) < self.storage_check_interval:
            return False, 0.0
        
        self.last_storage_check = current_time
        
        try:
            current_usage_bytes = self.get_download_folder_size()
            current_usage_gb = current_usage_bytes / (1024**3)
            max_storage_bytes = self.max_storage_gb * 1024**3
            usage_percentage = (current_usage_gb / self.max_storage_gb) * 100
            
            # Log current usage periodically
            self.logger.info(f"Storage usage: {current_usage_gb:.2f} GB / {self.max_storage_gb} GB ({usage_percentage:.1f}%)")
            
            # Check for 90% warning threshold
            if current_usage_gb >= (self.max_storage_gb * self.warning_threshold):
                if not hasattr(self, '_warning_logged') or not self._warning_logged:
                    self.logger.warning(f"WARNING: Storage usage is above 90%! Current: {current_usage_gb:.2f} GB ({usage_percentage:.1f}%)")
                    self.logger.warning(f"Approaching storage limit of {self.max_storage_gb} GB. Consider increasing max_storage_gb in config.json")
                    self._warning_logged = True
            
            # Check if limit is reached
            if current_usage_bytes >= max_storage_bytes:
                self.logger.error(f"STORAGE LIMIT REACHED: {current_usage_gb:.2f} GB / {self.max_storage_gb} GB")
                self.logger.error("Stopping scraper to prevent exceeding storage limit")
                return True, current_usage_gb
            
            return False, current_usage_gb
            
        except Exception as e:
            self.logger.error(f"Error checking storage limits: {e}")
            return False, 0.0

    def run(self):
        """Main execution loop - smart resume with backwards-only scraping + FORCE STOP SUPPORT"""
        try:
            # Initialize force stop flag for GUI integration
            if not hasattr(self, 'force_stop_requested'):
                self.force_stop_requested = False
            
            # Initial storage check
            limit_reached, usage_gb = self.check_storage_limits(force_check=True)
            if limit_reached:
                self.logger.error("Storage limit already reached. Cannot start scraping.")
                return

            # Setup driver
            self.web_driver_manager.setup_driver()

            # Determine starting strategy (always backwards now)
            start_page = self.determine_start_strategy()
            self.logger.info(f"Starting scrape from page {start_page} working backwards")

            asyncio.run(self.run_backwards_scrape(start_page))

        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user (Ctrl+C)")
            current_usage = self.get_download_folder_size()
            self.logger.info(f"Usage at interruption: {current_usage / (1024**3):.2f} GB")
        except Exception as e:
            if self.force_stop_requested:
                self.logger.info("Scraper stopped due to force stop request")
            else:
                self.logger.error(f"Unexpected error in main loop: {e}")
                traceback.print_exc()
        finally:
            try:
                self.web_driver_manager.close_driver()
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
            
            if self.force_stop_requested:
                self.logger.info("Scraper force stopped by user")
            else:
                self.logger.info("Scraper finished normally")
    def check_force_stop(self):
        """Check if force stop was requested (for GUI integration)"""
        # Check GUI force stop if available
        if hasattr(self, 'gui_force_stop_check') and callable(self.gui_force_stop_check):
            if self.gui_force_stop_check():
                self.force_stop_requested = True
                
        # Check internal force stop flag
        if hasattr(self, 'force_stop_requested') and self.force_stop_requested:
            self.logger.warning("🛑 FORCE STOP DETECTED - Aborting current operation")
            return True
            
        return False

    def determine_start_strategy(self):
        """
        CORRECTED: Determine starting page and NAVIGATE to it properly
        """
        self.logger.info("=" * 80)
        self.logger.info("DETERMINING START STRATEGY")
        self.logger.info("=" * 80)

        # Step 1: Check progress.json for downloaded videos
        self.logger.info("Step 1: Checking progress.json for existing downloads...")
        downloaded_videos = self.progress_tracker.get_downloaded_videos()
        
        if not downloaded_videos or len(downloaded_videos) == 0:
            self.logger.info("RESULT: No downloads found in progress.json")
            self.logger.info("DECISION: Starting FRESH SCRAPE - will find ACTUAL last page from website")
            
            # ENHANCED: Get actual last page from website with retries
            self.logger.info("Step 2: Fetching ACTUAL highest page number from website...")
            
            max_retries = 3
            for retry in range(max_retries):
                try:
                    self.logger.info(f" Attempt {retry + 1}/{max_retries} to find last page...")
                    
                    # CORRECTED: This now uses the proper XPath and container ID
                    last_page = self.page_navigator.get_last_page_number()
                    
                    if last_page and last_page >= 1000:  # Reasonable validation (expecting high page numbers like 9547)
                        self.logger.info(f" SUCCESS: Found actual last page = {last_page}")
                        
                        # CRITICAL: Navigate to the actual last page to verify it exists and start from there
                        self.logger.info(f" NAVIGATING to the last page {last_page} to start scraping from there...")
                        
                        if self.page_navigator.handle_page_navigation(last_page):
                            self.logger.info(f" Successfully navigated to page {last_page}")
                            self.logger.info(f"STRATEGY: Will scrape backwards from {last_page} -> {last_page-1} -> ... -> 1")
                            self.logger.info("=" * 80)
                            return last_page
                        else:
                            self.logger.error(f" Failed to navigate to page {last_page}, trying fallback...")
                            # Try a slightly lower page number
                            fallback_page = last_page - 1
                            if self.page_navigator.handle_page_navigation(fallback_page):
                                self.logger.info(f" Fallback successful: Starting from page {fallback_page}")
                                self.logger.info("=" * 80)
                                return fallback_page
                        
                    elif last_page and last_page > 100:  # Accept reasonable page numbers
                        self.logger.warning(f"Found page {last_page} - lower than expected but will use it")
                        
                        # Navigate to verify
                        if self.page_navigator.handle_page_navigation(last_page):
                            self.logger.info(f" Successfully navigated to page {last_page}")
                            self.logger.info(f"STRATEGY: Will scrape backwards from {last_page} -> {last_page-1} -> ... -> 1")
                            self.logger.info("=" * 80)
                            return last_page
                        
                    else:
                        self.logger.error(f" Invalid last page number: {last_page}")
                        if retry < max_retries - 1:
                            self.logger.info(f" Waiting 10 seconds before retry...")
                            time.sleep(10)
                            continue
                        
                except Exception as e:
                    self.logger.error(f" Error on attempt {retry + 1}: {e}")
                    if retry < max_retries - 1:
                        self.logger.info(f" Waiting 10 seconds before retry...")
                        time.sleep(10)
                        continue
            
            # Final fallback - but try to navigate to it
            fallback_page = 1000
            self.logger.error(" CRITICAL: Could not determine actual last page after all attempts!")
            self.logger.warning(f" FALLBACK: Trying to start from page {fallback_page}")
            
            if self.page_navigator.handle_page_navigation(fallback_page):
                self.logger.info(f" Emergency fallback successful: Starting from page {fallback_page}")
            else:
                self.logger.error(" Even fallback navigation failed!")
                
            self.logger.info("=" * 80)
            return fallback_page
            
        else:
            self.logger.info(f"RESULT: Found {len(downloaded_videos)} existing downloads in progress.json")
            
            # Step 2: Check last processed page  
            self.logger.info("Step 2: Checking last processed page for resume...")
            last_processed_page = self.progress_tracker.get_last_processed_page()
            
            if last_processed_page and last_processed_page >= 1:
                self.logger.info(f" FOUND: Last processed page was {last_processed_page}")
                
                # CRITICAL: Navigate to the resume page to verify it exists
                self.logger.info(f" NAVIGATING to resume page {last_processed_page}...")
                if self.page_navigator.handle_page_navigation(last_processed_page):
                    self.logger.info(f" Successfully navigated to resume page {last_processed_page}")
                    self.logger.info(f"DECISION: Resuming from page {last_processed_page} going backwards")
                    self.logger.info(f"STRATEGY: Will scrape backwards from {last_processed_page} -> {last_processed_page-1} -> ... -> 1")
                    self.logger.info("REASON: This ensures we capture any new content added since last run")
                    self.logger.info("=" * 80)
                    return last_processed_page
                else:
                    self.logger.error(f" Could not navigate to resume page {last_processed_page}")
                    self.logger.info("DECISION: Resume page invalid, finding actual last page instead")
            else:
                self.logger.warning(f" Invalid or missing last processed page: {last_processed_page}")
                self.logger.info("DECISION: Downloads exist but no valid last page - finding actual last page")
            
            # Get actual last page as fallback
            self.logger.info("Step 3: Fetching actual last page as fallback...")
            try:
                last_page = self.page_navigator.get_last_page_number()
                if last_page and last_page >= 1:
                    # Navigate to verify
                    if self.page_navigator.handle_page_navigation(last_page):
                        self.logger.info(f" FALLBACK SUCCESS: Starting from page {last_page}")
                        self.logger.info(f"STRATEGY: Will scrape backwards from {last_page} -> {last_page-1} -> ... -> 1")
                        self.logger.info("=" * 80)
                        return last_page
                    else:
                        self.logger.error(f" Invalid fallback page: {last_page}")
                        
            except Exception as e:
                self.logger.error(f" Error getting fallback page: {e}")
            
            # Final emergency fallback
            emergency_page = 1000
            self.logger.error(f" EMERGENCY FALLBACK: Trying page {emergency_page}")
            if self.page_navigator.handle_page_navigation(emergency_page):
                self.logger.info(f" Emergency navigation successful")
            else:
                self.logger.error(" Emergency navigation failed!")
                
            self.logger.info("=" * 80)
            return emergency_page
            
    async def run_backwards_scrape(self, start_page):
        """Run scrape from high page numbers going backwards to page 1 + FORCE STOP SUPPORT"""
        current_page = start_page
        self.logger.info(f"STARTING BACKWARDS SCRAPE")
        self.logger.info(f"Begin page: {start_page}")
        self.logger.info(f"Direction: BACKWARDS (high to low)")
        self.logger.info(f"Page sequence example: {start_page} -> {max(1, start_page-1)} -> {max(1, start_page-2)} -> ... -> 1")

        while current_page >= 1:
            # CRITICAL: Check for force stop before each page
            if self.check_force_stop():
                self.logger.warning("🛑 FORCE STOP REQUESTED - Stopping backwards scrape")
                break
                
            # Check storage before each page
            limit_reached, usage_gb = self.check_storage_limits()
            if limit_reached:
                self.logger.info("Storage limit reached, stopping scrape")
                break

            self.logger.info(f"\\n{'='*80}")
            self.logger.info(f"PROCESSING PAGE {current_page} (BACKWARDS SCRAPE)")
            self.logger.info(f"Remaining pages to process: {current_page} pages")
            self.logger.info(f"{'='*80}")

            try:
                # Check force stop before processing page
                if self.check_force_stop():
                    self.logger.warning("🛑 FORCE STOP REQUESTED - Stopping before page processing")
                    break
                
                # NEW: Use parallel processing for this page
                page_processed = await self._process_single_page_parallel(current_page)

                if page_processed:
                    self.progress_tracker.update_last_processed_page(current_page)
                    self.logger.info(f"Page {current_page} completed successfully")
                else:
                    self.logger.warning(f"Page {current_page} processing failed")

                # Move to previous page (backwards)
                current_page -= 1
                self.logger.info(f"Moving backwards: Next page will be {current_page if current_page >= 1 else 'COMPLETE'}")

                # Wait between pages (with force stop checking)
                if current_page >= 1:
                    await self._wait_between_pages()

            except Exception as e:
                if self.check_force_stop():
                    self.logger.warning("🛑 FORCE STOP during page processing")
                    break
                    
                self.logger.error(f"Error processing page {current_page}: {e}")
                traceback.print_exc()
                current_page -= 1
                continue

        final_page = current_page + 1
        
        if self.force_stop_requested:
            self.logger.info(f"BACKWARDS SCRAPE FORCE STOPPED")
            self.logger.info(f"Last completed page: {final_page}")
        else:
            self.logger.info(f"BACKWARDS SCRAPE COMPLETED")
            self.logger.info(f"Last page processed: {final_page}")
            
        self.logger.info(f"Total pages processed: {start_page - final_page + 1}")

    async def _process_single_page_parallel(self, page_num):
        """NEW: Process a single page using parallel video processing"""
        if self.use_parallel_video_processing:
            return await self.process_page_with_parallel_videos(page_num)
        else:
            return await self.process_page_sequential_optimized(page_num)

    async def process_page_with_parallel_videos(self, page_num):
        """Process page with full parallel video processing + FORCE STOP SUPPORT"""
        # Set page context
        self.file_downloader.current_page_num = page_num
        self.logger.info(f"Starting PARALLEL VIDEO PROCESSING for page {page_num} (max {self.max_concurrent_videos} videos simultaneously)")

        # Check for force stop at the beginning
        if self.check_force_stop():
            self.logger.warning("🛑 FORCE STOP REQUESTED - Skipping page processing")
            return False

        # Get video links
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return False

        self.logger.info(f"Found {len(video_links)} videos on page {page_num}")

        # Check for force stop after getting links
        if self.check_force_stop():
            self.logger.warning("🛑 FORCE STOP REQUESTED - Stopping after getting video links")
            return False

        # PHASE 1: Extract all video info in parallel (existing method)
        self.logger.info(f"PHASE 1: Extracting info for {len(video_links)} videos in parallel...")
        batch_results = await self.video_info_extractor.parallel_extract_multiple_videos(video_links)

        # Check for force stop after info extraction
        if self.check_force_stop():
            self.logger.warning("🛑 FORCE STOP REQUESTED - Stopping after info extraction")
            return False

        # Create complete video info objects with full metadata
        video_info_list = []
        for i, (video_url, crawl4ai_result) in enumerate(zip(video_links, batch_results)):
            # Check force stop during info creation
            if self.check_force_stop():
                self.logger.warning("🛑 FORCE STOP REQUESTED - Stopping during video info creation")
                return False
                
            video_info = await self.create_complete_video_info_fixed(video_url, crawl4ai_result)
            if video_info and video_info.get("video_src"):
                # CRITICAL: Ensure all required fields are present and valid
                self.ensure_complete_video_info(video_info)
                video_info_list.append(video_info)

        # Filter out already downloaded videos
        videos_to_process = []
        for video_info in video_info_list:
            video_id = video_info["video_id"]
            if not self.progress_tracker.is_video_downloaded(video_id) and not self.progress_tracker.is_video_failed(video_id):
                if not self.file_validator.validate_video_folder(video_id):
                    videos_to_process.append(video_info)

        self.logger.info(f"PHASE 1 COMPLETE: {len(videos_to_process)} videos need processing")

        if not videos_to_process:
            self.logger.info("All videos already downloaded or failed, skipping to next page")
            return True

        # Final force stop check before video processing
        if self.check_force_stop():
            self.logger.warning("🛑 FORCE STOP REQUESTED - Stopping before video processing phase")
            return False

        # PHASE 2: NEW - PARALLEL VIDEO PROCESSING (complete pipeline)
        self.logger.info(f"PHASE 2: PARALLEL PROCESSING {len(videos_to_process)} videos simultaneously...")
        self.logger.info(f"Each video will complete: scraping → downloading → validation → progress update")

        # Process videos in parallel using ThreadPoolExecutor
        loop = asyncio.get_event_loop()

        # Track results
        stats = {"successful": 0, "failed": 0, "skipped": 0}

        # ENHANCED: Process videos with force stop checking
        tasks = []
        for video_info in videos_to_process:
            if self.check_force_stop():
                self.logger.warning("🛑 FORCE STOP REQUESTED - Stopping video task creation")
                break
                
            # Pass force stop check to video processor
            task = loop.run_in_executor(
                None,  # Use default executor
                self._process_video_with_stop_check,
                video_info,
                3,  # max_retries
                page_num  # page_num
            )
            tasks.append((task, video_info["video_id"]))

        if not tasks:
            self.logger.warning("No video tasks created (force stop or error)")
            return False

        # Wait for all tasks to complete (with periodic force stop checking)
        try:
            results = []
            for task, video_id in tasks:
                if self.check_force_stop():
                    self.logger.warning(f"🛑 FORCE STOP REQUESTED - Cancelling remaining video tasks")
                    # Cancel remaining tasks
                    for remaining_task, _ in tasks[len(results):]:
                        remaining_task.cancel()
                    break
                    
                try:
                    result = await task
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Task for video {video_id} failed: {e}")
                    results.append(False)

            # Process results
            for i, result in enumerate(results):
                video_id = tasks[i][1]
                if isinstance(result, Exception):
                    stats["failed"] += 1
                    self.logger.error(f"✗ PARALLEL: Video {video_id} processing failed with exception: {result}")
                elif result:
                    stats["successful"] += 1
                    self.logger.info(f"✓ PARALLEL: Video {video_id} processed successfully")
                else:
                    stats["failed"] += 1
                    self.logger.error(f"✗ PARALLEL: Video {video_id} processing failed")

        except Exception as e:
            self.logger.error(f"Error during parallel video processing: {e}")
            if self.check_force_stop():
                self.logger.warning("🛑 FORCE STOP REQUESTED during video processing")
            return False

        # PHASE 3: Final report
        if self.check_force_stop():
            self.logger.info(f"PHASE 2 INTERRUPTED: Parallel video processing stopped by force stop")
        else:
            self.logger.info(f"PHASE 2 COMPLETE: Parallel video processing finished")
            
        self.generate_final_report_parallel(page_num, video_links, stats)
        return True

    def _process_video_with_stop_check(self, video_info, max_retries, page_num):
        """Process video with force stop checking"""
        try:
            # Check force stop before processing
            if self.check_force_stop():
                self.logger.warning(f"🛑 FORCE STOP - Skipping video {video_info['video_id']}")
                return False
                
            # Use existing video processor but with force stop awareness
            return self.video_processor.process_video_parallel_safe(video_info, max_retries, page_num)
            
        except Exception as e:
            if self.check_force_stop():
                self.logger.warning(f"🛑 FORCE STOP during video {video_info['video_id']} processing")
                return False
            else:
                self.logger.error(f"Error processing video {video_info['video_id']}: {e}")
                return False

    # Keep existing methods for backward compatibility and fallback
    async def create_complete_video_info_fixed(self, video_url, crawl4ai_result):
        """FIXED: Create complete video info structure from Crawl4AI results with proper metadata"""
        try:
            # Extract video ID
            video_id = self.video_info_extractor.extract_video_id(video_url)
            if not video_id:
                self.logger.error(f"Could not extract video ID from URL: {video_url}")
                return None
            
            # Initialize video info structure with all required fields
            video_info = {
                "video_id": video_id,
                "url": video_url,
                "title": "",
                "duration": "",
                "views": "",
                "uploader": "",
                "upload_date": "",
                "upload_date_epoch": None,
                "tags": [],
                "video_src": "",
                "thumbnail_src": ""
            }
            
            # CRITICAL: Merge Crawl4AI data properly
            if crawl4ai_result:
                # Create complete video info from Crawl4AI schemas
                basic_listing = {
                    "video_id": video_id,
                    "video_link": video_url,
                    "title": "",
                    "duration": "",
                    "upload_date": "",
                    "thumbnail": ""
                }
                
                # Use the video_info_extractor's method to create complete info
                video_info = self.video_info_extractor.create_complete_video_info_from_schemas(
                    basic_listing, crawl4ai_result
                )
                
                if not video_info:
                    video_info = {
                        "video_id": video_id,
                        "url": video_url,
                        "title": "",
                        "duration": "",
                        "views": "",
                        "uploader": "",
                        "upload_date": "",
                        "upload_date_epoch": None,
                        "tags": [],
                        "video_src": "",
                        "thumbnail_src": ""
                    }
            
            # If Crawl4AI data is incomplete, supplement with Selenium (in thread pool)
            if not self.video_info_extractor.is_video_info_complete(video_info):
                self.logger.info(f"Supplementing Crawl4AI data with Selenium extraction for {video_id}")
                # Use thread pool for Selenium operations to avoid blocking async loop
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(
                        executor,
                        self.video_info_extractor.supplement_with_selenium,
                        video_info,
                        video_url
                    )
            
            # CRITICAL: Set defaults for missing values using the extractor's method
            self.video_info_extractor.set_default_values(video_info)
            
            # Validate that we have essential data
            if not video_info.get("video_id"):
                video_info["video_id"] = video_id
            if not video_info.get("url"):
                video_info["url"] = video_url
            
            self.logger.debug(f"Created complete video info for {video_id}: title='{video_info.get('title', 'N/A')}', duration={video_info.get('duration', 'N/A')}")
            return video_info
            
        except Exception as e:
            self.logger.error(f"Error creating complete video info for {video_url}: {e}")
            traceback.print_exc()
            return None

    def ensure_complete_video_info(self, video_info):
        """CRITICAL: Ensure video_info has all required fields with valid values"""
        try:
            required_fields = {
                "video_id": video_info.get("video_id", ""),
                "url": video_info.get("url", ""),
                "title": video_info.get("title", f"Video_{video_info.get('video_id', 'unknown')}"),
                "duration": video_info.get("duration", "00:30"),
                "views": str(video_info.get("views", "0")),
                "uploader": video_info.get("uploader", "Unknown"),
                "upload_date": video_info.get("upload_date", int(time.time() * 1000)),
                "tags": video_info.get("tags", ["untagged"]),
                "video_src": video_info.get("video_src", ""),
                "thumbnail_src": video_info.get("thumbnail_src", "")
            }
            
            # Update video_info with ensured values
            for field, value in required_fields.items():
                if not video_info.get(field) or (isinstance(value, str) and video_info.get(field).strip() == ""):
                    video_info[field] = value
            
            # Ensure upload_date is integer timestamp
            if isinstance(video_info.get("upload_date"), str):
                try:
                    # Try to parse if it's a string
                    epoch = self.date_parser.parse_upload_date_to_epoch(video_info["upload_date"])
                    video_info["upload_date"] = int(epoch) if epoch else int(time.time() * 1000)
                except:
                    video_info["upload_date"] = int(time.time() * 1000)
            
            # Ensure tags is a list
            if not isinstance(video_info.get("tags"), list):
                video_info["tags"] = ["untagged"]
            
            # Remove any unwanted fields that might cause issues
            unwanted_fields = ["crawl4ai_data", "upload_date_epoch"]
            for field in unwanted_fields:
                if field in video_info:
                    del video_info[field]
                    
            self.logger.debug(f"Ensured complete video info for {video_info.get('video_id')}")
            
        except Exception as e:
            self.logger.error(f"Error ensuring complete video info: {e}")

    async def _wait_between_pages(self):
        """Wait between pages with force stop checking"""
        page_delay = self.config.get("general", {}).get("delay_between_pages", 5000) / 1000
        self.logger.info(f"Waiting {page_delay} seconds before next page...")
        
        # Wait in small increments so we can check force stop
        wait_increment = 0.5  # Check every 0.5 seconds
        total_waited = 0
        
        while total_waited < page_delay:
            if self.check_force_stop():
                self.logger.warning("🛑 FORCE STOP REQUESTED - Interrupting page delay")
                break
                
            await asyncio.sleep(min(wait_increment, page_delay - total_waited))
            total_waited += wait_increment

    def generate_final_report_parallel(self, page_num, video_links, stats):
        """Create summary of parallel scraping results"""
        current_usage = self.get_download_folder_size()
        usage_gb = current_usage / (1024**3)
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"PAGE {page_num} PARALLEL PROCESSING COMPLETE")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Processing Mode: PARALLEL VIDEO PROCESSING")
        self.logger.info(f"Max Concurrent Videos: {self.max_concurrent_videos}")
        self.logger.info(f"Total videos found: {len(video_links)}")
        self.logger.info(f"Successfully processed: {stats['successful']}")
        self.logger.info(f"Already existed (skipped): {stats['skipped']}")
        self.logger.info(f"Failed processing: {stats['failed']}")
        
        total_processed = stats['successful'] + stats['skipped']
        success_rate = (total_processed / len(video_links) * 100) if video_links else 0
        self.logger.info(f"Success rate: {success_rate:.1f}%")
        
        # Overall statistics
        overall_stats = self.progress_tracker.get_stats()
        self.logger.info(f"Total downloaded so far: {overall_stats['total_downloaded']}")
        if overall_stats.get("total_size_mb"):
            self.logger.info(f"Total size downloaded: {overall_stats['total_size_mb']:.2f} MB")
        
        # Storage usage
        self.logger.info(f"Current disk usage: {usage_gb:.2f} GB")
        storage_percentage = (usage_gb / self.max_storage_gb) * 100
        self.logger.info(f"Storage utilization: {storage_percentage:.1f}%")
        self.logger.info(f"{'='*60}")

    # Keep all existing methods for backward compatibility
    async def process_page_sequential_optimized(self, page_num):
        """Fallback: Optimized sequential processing"""
        # Set the page context so FileDownloader can include it in updates
        self.file_downloader.current_page_num = page_num
        self.logger.info(f"Starting SEQUENTIAL FALLBACK scrape from page {page_num}")
        
        # Get all video links from the page
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return False
        
        # Extract video info for all videos
        video_info_list = []
        for i, video_url in enumerate(video_links, start=1):
            try:
                video_id = self.smart_retry_extractor.extract_video_id_from_url(video_url)
                if self.progress_tracker.is_video_downloaded(video_id):
                    continue
                
                video_info = self.video_info_extractor.extract_video_info(video_url)
                if video_info and video_info.get("video_src"):
                    self.ensure_complete_video_info(video_info)
                    video_info_list.append(video_info)
                    
            except Exception as e:
                self.logger.error(f"Error extracting info for video {i}: {e}")
                continue
        
        # Process videos sequentially
        stats = {"successful": 0, "failed": 0, "skipped": len(video_links) - len(video_info_list)}
        
        for video_info in video_info_list:
            try:
                success = self.video_processor.process_video_parallel_safe(video_info, 3, page_num)
                if success:
                    stats["successful"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                self.logger.error(f"Error processing video {video_info['video_id']}: {e}")
                stats["failed"] += 1
        
        # Summary
        self.generate_final_report_parallel(page_num, video_links, stats)
        return True

    # Keep legacy methods for compatibility
    def generate_final_report(self, page_num, video_links, stats):
        """Legacy method - redirects to parallel version"""
        self.generate_final_report_parallel(page_num, video_links, stats)


# Usage
if __name__ == "__main__":
    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)
    
    scraper = VideoScraper()
    
    # Run with smart resume capability and parallel processing
    scraper.run()