# main_scraper.py - FIXED VERSION

import os
from pathlib import Path
import re
import time
import logging
import traceback
import asyncio
import json
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

        # Processing mode configuration
        self.processing_mode = self.config.get("processing", {}).get("mode", "hybrid")
        self.parallel_batch_size = self.config.get("processing", {}).get("parallel_batch_size", 5)
        self.use_parallel_processing = self.config.get("processing", {}).get("use_parallel", True)
        
        # NEW: Batch download configuration
        self.max_concurrent_downloads = self.config.get("processing", {}).get("max_concurrent_downloads", 8)

        # Storage management
        self.max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
        self.warning_threshold = 0.9 # 90% threshold for warning
        self.last_storage_check = 0
        self.storage_check_interval = 60 # Check storage every 60 seconds

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
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
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
        """Main execution loop - smart resume with backwards-only scraping"""
        try:
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
            self.logger.error(f"Unexpected error in main loop: {e}")
            traceback.print_exc()
        finally:
            try:
                self.web_driver_manager.close_driver()
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
            self.logger.info("Scraper finished")

    def determine_start_strategy(self):
        """
        FIXED & BULLETPROOF: Determine starting page with detailed logging
        GUARANTEE: When no downloads exist, ALWAYS start from last page
        """
        self.logger.info("=" * 80)
        self.logger.info("DETERMINING START STRATEGY")
        self.logger.info("=" * 80)
        
        # Step 1: Check progress.json for downloaded videos
        self.logger.info("Step 1: Checking progress.json for existing downloads...")
        downloaded_videos = self.progress_tracker.get_downloaded_videos()
        
        if not downloaded_videos or len(downloaded_videos) == 0:
            self.logger.info("RESULT: No downloads found in progress.json")
            self.logger.info("DECISION: Starting fresh scrape - will begin from HIGHEST page number")
            
            # Get last page from website
            self.logger.info("Step 2: Fetching highest page number from website...")
            try:
                last_page = self.page_navigator.get_last_page_number()
                self.logger.info(f"WEBSITE REPORTS: Highest page number is {last_page}")
                
                if last_page and last_page >= 1:
                    self.logger.info(f"CONFIRMED: Starting from page {last_page} (highest page)")
                    self.logger.info(f"STRATEGY: Will scrape backwards from {last_page} -> {last_page-1} -> ... -> 1")
                    self.logger.info("=" * 80)
                    return last_page
                else:
                    self.logger.error(f"ERROR: Invalid last page number: {last_page}")
                    self.logger.info("FALLBACK: Using page 1000 as default starting point")
                    self.logger.info("=" * 80)
                    return 1000
            except Exception as e:
                self.logger.error(f"ERROR: Could not fetch last page number: {e}")
                self.logger.info("FALLBACK: Using page 1000 as default starting point")
                self.logger.info("=" * 80)
                return 1000
        else:
            self.logger.info(f"RESULT: Found {len(downloaded_videos)} existing downloads in progress.json")
            
            # Step 2: Check last processed page
            self.logger.info("Step 2: Checking last processed page...")
            last_processed_page = self.progress_tracker.get_last_processed_page()
            
            if last_processed_page and last_processed_page >= 1:
                self.logger.info(f"FOUND: Last processed page was {last_processed_page}")
                self.logger.info(f"DECISION: Resuming from page {last_processed_page} going backwards")
                self.logger.info(f"STRATEGY: Will scrape backwards from {last_processed_page} -> {last_processed_page-1} -> ... -> 1")
                self.logger.info("REASON: This ensures we capture any new content added since last run")
                self.logger.info("=" * 80)
                return last_processed_page
            else:
                self.logger.info(f"WARNING: Invalid or missing last processed page: {last_processed_page}")
                self.logger.info("DECISION: Falling back to fresh start from highest page")
                
                # Get last page from website as fallback
                self.logger.info("Step 3: Fetching highest page number as fallback...")
                try:
                    last_page = self.page_navigator.get_last_page_number()
                    self.logger.info(f"WEBSITE REPORTS: Highest page number is {last_page}")
                    
                    if last_page and last_page >= 1:
                        self.logger.info(f"CONFIRMED: Starting from page {last_page} (highest page)")
                        self.logger.info(f"STRATEGY: Will scrape backwards from {last_page} -> {last_page-1} -> ... -> 1")
                        self.logger.info("=" * 80)
                        return last_page
                    else:
                        self.logger.error(f"ERROR: Invalid last page number: {last_page}")
                        self.logger.info("FALLBACK: Using page 1000 as default starting point")
                        self.logger.info("=" * 80)
                        return 1000
                except Exception as e:
                    self.logger.error(f"ERROR: Could not fetch last page number: {e}")
                    self.logger.info("FALLBACK: Using page 1000 as default starting point")
                    self.logger.info("=" * 80)
                    return 1000

    async def run_backwards_scrape(self, start_page):
        """Run scrape from high page numbers going backwards to page 1"""
        current_page = start_page
        
        self.logger.info(f"STARTING BACKWARDS SCRAPE")
        self.logger.info(f"Begin page: {start_page}")
        self.logger.info(f"Direction: BACKWARDS (high to low)")
        self.logger.info(f"Page sequence example: {start_page} -> {max(1, start_page-1)} -> {max(1, start_page-2)} -> ... -> 1")
        
        while current_page >= 1:
            # Check storage before each page
            limit_reached, usage_gb = self.check_storage_limits()
            if limit_reached:
                self.logger.info("Storage limit reached, stopping scrape")
                break
            
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"PROCESSING PAGE {current_page} (BACKWARDS SCRAPE)")
            self.logger.info(f"Remaining pages to process: {current_page} pages")
            self.logger.info(f"{'='*80}")
            
            try:
                page_processed = await self._process_single_page(current_page)
                if page_processed:
                    self.progress_tracker.update_last_processed_page(current_page)
                    self.logger.info(f"Page {current_page} completed successfully")
                else:
                    self.logger.warning(f"Page {current_page} processing failed")
                
                # Move to previous page (backwards)
                current_page -= 1
                self.logger.info(f"Moving backwards: Next page will be {current_page if current_page >= 1 else 'COMPLETE'}")
                
                # Wait between pages
                if current_page >= 1:
                    await self._wait_between_pages()
                    
            except Exception as e:
                self.logger.error(f"Error processing page {current_page}: {e}")
                traceback.print_exc()
                current_page -= 1
                continue
        
        final_page = current_page + 1
        self.logger.info(f"BACKWARDS SCRAPE COMPLETED")
        self.logger.info(f"Last page processed: {final_page}")
        self.logger.info(f"Total pages processed: {start_page - final_page + 1}")

    async def _process_single_page(self, page_num):
        """Process a single page using appropriate method"""
        if self.use_parallel_processing and self.processing_mode in ["crawl4ai", "hybrid"]:
            return await self.process_page_parallel_optimized(page_num)
        else:
            return await self.process_page_sequential_optimized(page_num)

    async def _wait_between_pages(self):
        """Wait between pages"""
        page_delay = self.config.get("general", {}).get("delay_between_pages", 5000) / 1000
        self.logger.info(f"Waiting {page_delay} seconds before next page...")
        await asyncio.sleep(page_delay)

    async def process_page_parallel_optimized(self, page_num):
        """
        FIXED: Process all videos from a page with batch downloading
        Flow: Extract all -> Download all -> Validate all -> Update progress ONLY after validation
        """
        # Set page context
        self.file_downloader.current_page_num = page_num
        self.logger.info(f"Starting OPTIMIZED PARALLEL scrape from page {page_num}")
        
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return False

        # PHASE 1: Extract all video info in parallel
        self.logger.info(f"PHASE 1: Extracting info for {len(video_links)} videos in parallel...")
        batch_results = await self.video_info_extractor.parallel_extract_multiple_videos(video_links)
        
        # Create complete video info objects with full metadata
        video_info_list = []
        for video_url, crawl4ai_result in zip(video_links, batch_results):
            video_info = await self.create_complete_video_info_fixed(video_url, crawl4ai_result)
            if video_info and video_info.get("video_src"):
                # CRITICAL: Ensure all required fields are present and valid
                self.ensure_complete_video_info(video_info)
                video_info_list.append(video_info)
        
        # Filter out already downloaded videos
        videos_to_download = []
        for video_info in video_info_list:
            video_id = video_info["video_id"]
            if not self.progress_tracker.is_video_downloaded(video_id):
                if not self.file_validator.validate_video_folder(video_id):
                    videos_to_download.append(video_info)
        
        self.logger.info(f"PHASE 1 COMPLETE: {len(videos_to_download)} videos need downloading")
        
        if not videos_to_download:
            self.logger.info("All videos already downloaded, skipping to next page")
            return True

        # PHASE 2: Batch download all videos
        self.logger.info(f"PHASE 2: Batch downloading {len(videos_to_download)} videos...")
        video_download_results = await self.batch_download_videos(videos_to_download)
        
        # PHASE 3: Batch download all thumbnails  
        self.logger.info(f"PHASE 3: Batch downloading thumbnails...")
        await self.batch_download_thumbnails(videos_to_download)
        
        # PHASE 4: FIXED - Batch validate, save JSON, and update progress ONLY after validation
        self.logger.info(f"PHASE 4: Batch validation, JSON saving, and progress tracking...")
        stats = await self.batch_validate_save_and_track_fixed(videos_to_download, video_download_results, page_num)
        
        # Final report for this page
        self.generate_final_report(page_num, video_links, stats)
        return True

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

    async def batch_validate_save_and_track_fixed(self, video_info_list, download_results, page_num):
        """
        FIXED: Validate downloads, save JSON metadata, and update progress ONLY after successful validation
        This ensures progress.json and individual JSON files are accurate
        """
        stats = {"successful": 0, "failed": 0, "skipped": 0}
        
        for video_info in video_info_list:
            video_id = video_info["video_id"]
            
            try:
                # Check if video downloaded successfully
                if not download_results.get(video_id, False):
                    stats["failed"] += 1
                    self.logger.error(f"Video download failed, skipping validation: {video_id}")
                    continue
                
                download_path = Path(self.config["general"]["download_path"])
                video_dir = download_path / video_id
                video_path = video_dir / f"{video_id}.mp4"
                
                # STEP 1: Validate the complete download first
                is_valid, validation_errors = self.file_validator.validate_complete_download(
                    video_info, video_dir
                )
                
                if not is_valid:
                    stats["failed"] += 1
                    self.logger.error(f"Validation failed for {video_id}: {validation_errors}")
                    # Clean up failed download
                    self.video_processor.cleanup_incomplete_folder(video_id)
                    continue
                
                # STEP 2: ONLY if validation passes, save the JSON metadata
                json_path = video_dir / f"{video_id}.json"
                try:
                    # Ensure the video_info is complete before saving
                    self.ensure_complete_video_info(video_info)
                    
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(video_info, f, indent=2, ensure_ascii=False)
                    
                    self.logger.info(f"✓ Saved complete metadata JSON for {video_id}")
                    
                    # Verify the JSON was saved correctly
                    if json_path.exists() and json_path.stat().st_size > 50:  # At least 50 bytes
                        with open(json_path, 'r', encoding='utf-8') as f:
                            saved_data = json.load(f)
                            if saved_data.get("video_id") == video_id:
                                self.logger.debug(f"✓ JSON validation passed for {video_id}")
                            else:
                                raise ValueError("JSON data validation failed")
                    else:
                        raise ValueError("JSON file is too small or missing")
                        
                except Exception as json_error:
                    stats["failed"] += 1
                    self.logger.error(f"Failed to save JSON for {video_id}: {json_error}")
                    continue
                
                # STEP 3: ONLY if JSON save succeeds, update progress tracker
                try:
                    file_size_mb = 0
                    if video_path.exists():
                        file_size_mb = video_path.stat().st_size / (1024 * 1024)
                    
                    # CRITICAL: Only update progress after complete success
                    self.progress_tracker.update_download_stats(video_id, file_size_mb, page_num)
                    
                    # Verify it was added to progress
                    if video_id in self.progress_tracker.get_downloaded_videos():
                        stats["successful"] += 1
                        self.logger.info(f"✓ Successfully processed and tracked {video_id} ({file_size_mb:.2f} MB)")
                    else:
                        self.logger.error(f"Failed to add {video_id} to progress tracker")
                        stats["failed"] += 1
                        
                except Exception as progress_error:
                    stats["failed"] += 1
                    self.logger.error(f"Failed to update progress for {video_id}: {progress_error}")
                    continue
                    
            except Exception as e:
                stats["failed"] += 1
                self.logger.error(f"Error processing video {video_id}: {e}")
                traceback.print_exc()
                
        self.logger.info(f"Batch processing completed: {stats['successful']} successful, {stats['failed']} failed")
        return stats

    # Keep all existing batch download methods unchanged
    async def batch_download_videos(self, video_info_list):
        """Download all videos concurrently"""
        download_tasks = []
        video_paths = {}
        
        for video_info in video_info_list:
            video_id = video_info["video_id"]
            download_path = Path(self.config["general"]["download_path"])
            video_dir = download_path / video_id
            video_dir.mkdir(parents=True, exist_ok=True)
            video_path = video_dir / f"{video_id}.mp4"
            video_paths[video_id] = video_path
            
            # Create download task
            task = self.download_single_video_async(video_info["video_src"], video_path)
            download_tasks.append((video_id, task))
        
        # Execute downloads with concurrency limit
        download_results = {}
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        
        async def bounded_download(video_id, task):
            async with semaphore:
                return video_id, await task
        
        # Execute all downloads
        bounded_tasks = [bounded_download(vid_id, task) for vid_id, task in download_tasks]
        results = await asyncio.gather(*bounded_tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple):
                video_id, success = result
                download_results[video_id] = success
            else:
                self.logger.error(f"Download task failed: {result}")
        
        successful_downloads = sum(1 for success in download_results.values() if success)
        self.logger.info(f"Video downloads completed: {successful_downloads}/{len(video_info_list)} successful")
        
        return download_results

    async def batch_download_thumbnails(self, video_info_list):
        """Download all thumbnails concurrently"""
        thumbnail_tasks = []
        
        for video_info in video_info_list:
            if not video_info.get("thumbnail_src"):
                continue
                
            video_id = video_info["video_id"]
            download_path = Path(self.config["general"]["download_path"])
            video_dir = download_path / video_id
            
            # Create thumbnail download task
            task = self.download_single_thumbnail_async(
                video_info["thumbnail_src"], video_id, video_dir
            )
            thumbnail_tasks.append(task)
        
        # Execute thumbnail downloads with concurrency limit
        if thumbnail_tasks:
            semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
            
            async def bounded_thumbnail_download(task):
                async with semaphore:
                    return await task
            
            bounded_tasks = [bounded_thumbnail_download(task) for task in thumbnail_tasks]
            await asyncio.gather(*bounded_tasks, return_exceptions=True)
        
        self.logger.info(f"Thumbnail downloads completed for {len(thumbnail_tasks)} videos")

    async def download_single_video_async(self, video_url, video_path):
        """Async wrapper for video download"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            try:
                success = await loop.run_in_executor(
                    executor, 
                    self.file_downloader.download_file,
                    video_url,
                    video_path
                )
                return success
            except Exception as e:
                self.logger.error(f"Async video download failed: {e}")
                return False

    async def download_single_thumbnail_async(self, thumbnail_url, video_id, video_dir):
        """Async wrapper for thumbnail download"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            try:
                success = await loop.run_in_executor(
                    executor,
                    self.file_downloader.download_thumbnail,
                    thumbnail_url,
                    video_id,
                    video_dir
                )
                return success
            except Exception as e:
                self.logger.error(f"Async thumbnail download failed for {video_id}: {e}")
                return False

    async def process_page_sequential_optimized(self, page_num):
        """Optimized sequential processing with batch downloads"""
        # Set the page context so FileDownloader can include it in updates
        self.file_downloader.current_page_num = page_num
        self.logger.info(f"Starting OPTIMIZED SEQUENTIAL scrape from page {page_num}")
        
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

        # Now do batch downloads like in parallel version
        if video_info_list:
            video_download_results = await self.batch_download_videos(video_info_list)
            await self.batch_download_thumbnails(video_info_list)
            stats = await self.batch_validate_save_and_track_fixed(video_info_list, video_download_results, page_num)
        else:
            stats = {"successful": 0, "failed": 0, "skipped": len(video_links)}

        # Summary
        self.generate_final_report(page_num, video_links, stats)
        return True

    # Keep all other existing methods unchanged
    def set_current_page_context(self, page_num):
        """Set current page context for all downloaders"""
        self.file_downloader.current_page_num = page_num
        # Also set for video processor if it exists
        if hasattr(self.video_processor, 'file_downloader'):
            self.video_processor.file_downloader.current_page_num = page_num

    def pre_filter_existing_videos(self, video_links):
        """Filter out videos that already exist and are valid"""
        videos_to_process = []
        for video_url in video_links:
            try:
                video_id = self.smart_retry_extractor.extract_video_id_from_url(video_url)
                if not self.file_validator.validate_video_folder(video_id):
                    videos_to_process.append(video_url)
                else:
                    self.progress_tracker.update_download_stats(video_id, 0)
            except Exception as e:
                self.logger.warning(f"Error pre-filtering video {video_url}: {e}")
                videos_to_process.append(video_url) # Include it to be safe
        return videos_to_process

    def split_into_batches(self, items, batch_size):
        """Split list into batches of specified size"""
        batches = []
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i + batch_size])
        return batches

    def save_video_info_json(self, video_info):
        """Save video information to JSON file"""
        try:
            videos_info_dir = os.path.join(
                self.config["general"]["download_path"],
                "videos_info"
            )
            os.makedirs(videos_info_dir, exist_ok=True)
            
            json_path = self.video_info_extractor.save_video_info_to_json(
                video_info,
                videos_info_dir
            )
            
            if json_path:
                self.logger.debug(f"Video info saved to: {json_path}")
        except Exception as e:
            self.logger.error(f"Error saving video info JSON: {e}")

    def _log_video_info_summary(self, video_info):
        """Log summary of extracted video information"""
        self.logger.info(f"Video Info Extracted and Validated:")
        self.logger.info(f"  ID: {video_info['video_id']}")
        self.logger.info(f"  Title: {video_info['title']}")
        self.logger.info(f"  Duration: {video_info['duration']}")
        self.logger.info(f"  Views: {video_info['views']}")
        self.logger.info(f"  Upload Date: {video_info['upload_date']}")
        self.logger.info(f"  Tags: {len(video_info['tags'])} tags")
        self.logger.info(f"  Has video source: {'Yes' if video_info.get('video_src') else 'No'}")
        self.logger.info(f"  Has thumbnail: {'Yes' if video_info.get('thumbnail_src') else 'No'}")

    def _wait_between_videos(self):
        """Wait between video processing to be respectful"""
        delay_seconds = self.config["general"]["delay_between_requests"] / 1000
        self.logger.info(f"Waiting {delay_seconds} seconds before next video...")
        time.sleep(delay_seconds)

    def generate_final_report(self, page_num, video_links, stats):
        """Create summary of scraping results"""
        current_usage = self.get_download_folder_size()
        usage_gb = current_usage / (1024**3)
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"PAGE {page_num} PROCESSING COMPLETE")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Processing Mode: {self.processing_mode.upper()} (OPTIMIZED)")
        if self.use_parallel_processing:
            self.logger.info(f"Max Concurrent Downloads: {self.max_concurrent_downloads}")
        self.logger.info(f"Total videos found: {len(video_links)}")
        self.logger.info(f"Successfully downloaded: {stats['successful']}")
        self.logger.info(f"Already existed (skipped): {stats['skipped']}")
        self.logger.info(f"Failed downloads: {stats['failed']}")
        
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


# Usage
if __name__ == "__main__":
    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)
    
    scraper = VideoScraper()
    # Run with smart resume capability
    scraper.run()