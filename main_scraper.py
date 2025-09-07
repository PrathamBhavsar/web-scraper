import os
import re
import time
import logging
import traceback
import asyncio
from concurrent.futures import ThreadPoolExecutor
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

        # Storage management
        self.max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
        self.warning_threshold = 0.9  # 90% threshold for warning
        self.last_storage_check = 0
        self.storage_check_interval = 60  # Check storage every 60 seconds

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
        FIXED: Determine starting page - always go backwards to capture new content
        """
        downloaded_videos = self.progress_tracker.get_downloaded_videos()
        
        if not downloaded_videos:
            # No downloads yet - start from last page backwards
            last_page = self.page_navigator.get_last_page_number()
            self.logger.info(f"No previous downloads found. Starting from highest page: {last_page}")
            return last_page
        else:
            # Resume from the last processed page and go backwards
            last_processed_page = self.progress_tracker.get_last_processed_page()
            
            if last_processed_page and last_processed_page >= 1:
                self.logger.info(f"Resuming from last processed page {last_processed_page}, going backwards")
                self.logger.info(f"This ensures we capture any new content added to earlier pages since last run")
                return last_processed_page
            else:
                # No valid last page or it's invalid, start from the beginning (highest page)
                last_page = self.page_navigator.get_last_page_number()
                self.logger.info(f"No valid last processed page found. Starting from highest page: {last_page}")
                return last_page

    async def run_backwards_scrape(self, start_page):
        """Run scrape from high page numbers going backwards to page 1"""
        current_page = start_page
        
        self.logger.info(f"Starting backwards scrape from page {start_page}")
        self.logger.info(f"Will process pages: {start_page} -> {max(1, start_page-10)} -> ... -> 1")
        
        while current_page >= 1:
            # Check storage before each page
            limit_reached, usage_gb = self.check_storage_limits()
            if limit_reached:
                self.logger.info("Storage limit reached, stopping scrape")
                break
                
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"PROCESSING PAGE {current_page} (BACKWARDS)")
            self.logger.info(f"{'='*80}")
            
            try:
                page_processed = await self._process_single_page(current_page)
                if page_processed:
                    self.progress_tracker.update_last_processed_page(current_page)
                
                current_page -= 1
                
                # Wait between pages
                if current_page >= 1:
                    await self._wait_between_pages()
                    
            except Exception as e:
                self.logger.error(f"Error processing page {current_page}: {e}")
                traceback.print_exc()
                current_page -= 1
                continue

        self.logger.info(f"Completed backwards scrape. Processed down to page {current_page + 1}")

    async def _process_single_page(self, page_num):
        """Process a single page using appropriate method"""
        if self.use_parallel_processing and self.processing_mode in ["crawl4ai", "hybrid"]:
            return await self.process_page_parallel(page_num)
        else:
            return self.process_page_sequential(page_num)

    async def _wait_between_pages(self):
        """Wait between pages"""
        page_delay = self.config.get("general", {}).get("delay_between_pages", 5000) / 1000
        self.logger.info(f"Waiting {page_delay} seconds before next page...")
        await asyncio.sleep(page_delay)

    async def process_page_parallel(self, page_num):
        """Process all videos from a single page using parallel Crawl4AI extraction"""
        self.logger.info(f"Starting PARALLEL scrape from page {page_num}")

        # Get all video links from the page
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return False

        self.logger.info(f"Found {len(video_links)} videos to process on page {page_num}")

        # Pre-filter videos that already exist
        videos_to_process = self.pre_filter_existing_videos(video_links)
        if not videos_to_process:
            self.logger.info("All videos already exist and are valid, skipping page")
            return True

        self.logger.info(f"After filtering: {len(videos_to_process)} videos need processing")

        # Process videos in parallel batches
        stats = {"successful": 0, "failed": 0, "skipped": len(video_links) - len(videos_to_process)}

        # Split videos into batches for parallel processing
        video_batches = self.split_into_batches(videos_to_process, self.parallel_batch_size)

        for batch_num, batch in enumerate(video_batches, 1):
            # Check storage before each batch
            limit_reached, usage_gb = self.check_storage_limits()
            if limit_reached:
                self.logger.info("Storage limit reached during batch processing")
                break
                
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"PROCESSING BATCH {batch_num}/{len(video_batches)} ({len(batch)} videos)")
            self.logger.info(f"{'='*60}")

            # Extract video information in parallel using Crawl4AI
            batch_results = await self.video_info_extractor.parallel_extract_multiple_videos(batch)

            # Process each video result (download and validation)
            await self.process_batch_results(batch, batch_results, stats, batch_num)

            # Wait between batches to be respectful
            if batch_num < len(video_batches):
                self.logger.info(f"Waiting between batches...")
                await asyncio.sleep(self.config["general"]["delay_between_requests"] / 1000)

        # Generate final report
        self.generate_final_report(page_num, video_links, stats)
        return True

    def process_page_sequential(self, page_num):
        """Traditional sequential processing (original method enhanced)"""
        self.logger.info(f"Starting SEQUENTIAL scrape from page {page_num}")

        # Get all video links from the page
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return False

        self.logger.info(f"Found {len(video_links)} videos to process on page {page_num}")

        # Process each video
        stats = {"successful": 0, "failed": 0, "skipped": 0}

        for i, video_url in enumerate(video_links, 1):
            self.logger.info(f"\n{'-'*50}")
            self.logger.info(f"Processing video {i}/{len(video_links)} on page {page_num}")
            self.logger.info(f"URL: {video_url}")
            self.logger.info(f"{'-'*50}")

            try:
                # Check storage limit before each video
                limit_reached, usage_gb = self.check_storage_limits()
                if limit_reached:
                    self.logger.info(f"Storage limit reached before video {i}. Usage: {usage_gb:.2f} GB")
                    break

                # Extract video ID for pre-check
                video_id = self.smart_retry_extractor.extract_video_id_from_url(video_url)

                # Check if already processed and valid
                if self.file_validator.validate_video_folder(video_id):
                    self.logger.info(f"Video {video_id} already exists and is valid, skipping")
                    self.progress_tracker.update_download_stats(video_id, 0)
                    stats["skipped"] += 1
                    continue

                # Extract video information
                video_info = self.video_info_extractor.extract_video_info(video_url)
                if not video_info:
                    self.logger.error(f"Failed to extract valid info for video {i}")
                    stats["failed"] += 1
                    continue

                # Log extracted information
                self._log_video_info_summary(video_info)

                # Save video info to JSON
                self.save_video_info_json(video_info)

                # Process (download) the video with validation
                if video_info.get("video_src"):
                    if self.video_processor.process_video(video_info):
                        stats["successful"] += 1
                        self.logger.info(f"Successfully processed and validated video {i}/{len(video_links)}")
                        
                        # Check storage after successful download
                        limit_reached, usage_gb = self.check_storage_limits(force_check=True)
                        if limit_reached:
                            self.logger.info(f"Storage limit reached after downloading video {i}")
                            break
                    else:
                        stats["failed"] += 1
                        self.logger.error(f"Failed to process video {i}/{len(video_links)} after all retries")
                else:
                    self.logger.warning(f"No video source found for video {i}/{len(video_links)}")
                    stats["failed"] += 1

                # Delay between videos
                self._wait_between_videos()

            except Exception as e:
                self.logger.error(f"Unexpected error processing video {i}: {e}")
                stats["failed"] += 1
                traceback.print_exc()
                continue

        # Generate report
        self.generate_final_report(page_num, video_links, stats)
        return True

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
                videos_to_process.append(video_url)  # Include it to be safe
        return videos_to_process

    def split_into_batches(self, items, batch_size):
        """Split list into batches of specified size"""
        batches = []
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i + batch_size])
        return batches

    async def process_batch_results(self, batch_urls, batch_results, stats, batch_num):
        """Process the results from a parallel batch extraction"""
        for i, (video_url, crawl4ai_result) in enumerate(zip(batch_urls, batch_results)):
            try:
                video_idx = (batch_num - 1) * self.parallel_batch_size + i + 1
                self.logger.info(f"\nProcessing batch result {i+1}/{len(batch_urls)} (Overall: {video_idx})")
                self.logger.info(f"URL: {video_url}")

                # Check storage before processing each video
                limit_reached, usage_gb = self.check_storage_limits()
                if limit_reached:
                    self.logger.info("Storage limit reached during batch processing")
                    return

                # Create complete video info from Crawl4AI result
                video_info = await self.create_complete_video_info(video_url, crawl4ai_result)
                if not video_info:
                    self.logger.error(f"Failed to create complete video info for batch item {i+1}")
                    stats["failed"] += 1
                    continue

                # Log extracted information
                self._log_video_info_summary(video_info)

                # Save video info to JSON
                self.save_video_info_json(video_info)

                # Process (download) the video with validation
                if video_info.get("video_src"):
                    # Use thread pool for video processing to avoid blocking async loop
                    loop = asyncio.get_event_loop()
                    with ThreadPoolExecutor() as executor:
                        success = await loop.run_in_executor(
                            executor,
                            self.video_processor.process_video,
                            video_info
                        )

                    if success:
                        stats["successful"] += 1
                        self.logger.info(f"Successfully processed batch item {i+1}")
                        
                        # Check storage after successful download
                        limit_reached, usage_gb = self.check_storage_limits(force_check=True)
                        if limit_reached:
                            self.logger.info("Storage limit reached during batch processing")
                            return
                    else:
                        stats["failed"] += 1
                        self.logger.error(f"Failed to process batch item {i+1}")
                else:
                    self.logger.warning(f"No video source found for batch item {i+1}")
                    stats["failed"] += 1

                # Small delay between items in batch
                if i < len(batch_urls) - 1:
                    await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"Error processing batch result {i+1}: {e}")
                stats["failed"] += 1
                traceback.print_exc()

    async def create_complete_video_info(self, video_url, crawl4ai_result):
        """Create complete video info structure from Crawl4AI results"""
        try:
            # Extract video ID
            video_id = self.video_info_extractor.extract_video_id(video_url)
            if not video_id:
                return None

            # Initialize video info structure
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
                "thumbnail_src": "",
                "crawl4ai_data": crawl4ai_result
            }

            # Merge Crawl4AI data if available
            if crawl4ai_result:
                self.video_info_extractor.merge_crawl4ai_data(video_info, crawl4ai_result)

            # If Crawl4AI data is incomplete, supplement with Selenium (in thread pool)
            if not self.video_info_extractor.is_video_info_complete(video_info):
                self.logger.info("Supplementing Crawl4AI data with Selenium extraction")
                # Use thread pool for Selenium operations to avoid blocking async loop
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as executor:
                    await loop.run_in_executor(
                        executor,
                        self.video_info_extractor.supplement_with_selenium,
                        video_info,
                        video_url
                    )

            # Set defaults for missing values
            self.video_info_extractor.set_default_values(video_info)

            return video_info

        except Exception as e:
            self.logger.error(f"Error creating complete video info: {e}")
            return None

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
        self.logger.info(f"  Upload Date: {video_info['upload_date']} (Epoch: {video_info.get('upload_date_epoch')})")
        self.logger.info(f"  Tags: {len(video_info['tags'])} tags")
        self.logger.info(f"  Has video source: {'Yes' if video_info.get('video_src') else 'No'}")
        self.logger.info(f"  Has thumbnail: {'Yes' if video_info.get('thumbnail_src') else 'No'}")
        self.logger.info(f"  Crawl4AI data: {'Available' if video_info.get('crawl4ai_data') else 'Not available'}")

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
        self.logger.info(f"Processing Mode: {self.processing_mode.upper()}")
        if self.use_parallel_processing:
            self.logger.info(f"Parallel Batch Size: {self.parallel_batch_size}")
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
