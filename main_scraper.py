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
        
        # New: Processing mode configuration
        self.processing_mode = self.config.get("processing", {}).get("mode", "hybrid")  # "selenium", "crawl4ai", "hybrid"
        self.parallel_batch_size = self.config.get("processing", {}).get("parallel_batch_size", 5)
        self.use_parallel_processing = self.config.get("processing", {}).get("use_parallel", True)
    
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
    
    def run(self):
        """Main execution loop - orchestrates the entire scraping process"""
        try:
            # Setup driver
            self.web_driver_manager.setup_driver()
            
            # Process single page for testing
            current_page = 1
            
            if self.use_parallel_processing and self.processing_mode in ["crawl4ai", "hybrid"]:
                # Use new parallel processing approach
                asyncio.run(self.process_page_parallel(current_page))
            else:
                # Use traditional sequential processing
                self.process_page_sequential(current_page)
            
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
            traceback.print_exc()
        finally:
            self.web_driver_manager.close_driver()
            self.logger.info("Scraper finished")
    
    async def process_page_parallel(self, page_num):
        """Process all videos from a single page using parallel Crawl4AI extraction"""
        self.logger.info(f"Starting PARALLEL scrape from page {page_num}")
        
        # Get all video links from the page
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return
        
        self.logger.info(f"Found {len(video_links)} videos to process on page {page_num}")
        
        # Pre-filter videos that already exist
        videos_to_process = self.pre_filter_existing_videos(video_links)
        
        if not videos_to_process:
            self.logger.info("All videos already exist and are valid, skipping page")
            return
        
        self.logger.info(f"After filtering: {len(videos_to_process)} videos need processing")
        
        # Process videos in parallel batches
        stats = {"successful": 0, "failed": 0, "skipped": len(video_links) - len(videos_to_process)}
        
        # Split videos into batches for parallel processing
        video_batches = self.split_into_batches(videos_to_process, self.parallel_batch_size)
        
        for batch_num, batch in enumerate(video_batches, 1):
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
    
    def process_page_sequential(self, page_num):
        """Traditional sequential processing (original method enhanced)"""
        self.logger.info(f"Starting SEQUENTIAL scrape from page {page_num}")
        
        # Get all video links from the page
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return
        
        self.logger.info(f"Found {len(video_links)} videos to process on page {page_num}")
        
        # Process each video
        stats = {"successful": 0, "failed": 0, "skipped": 0}
        
        for i, video_url in enumerate(video_links, 1):
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"Processing video {i}/{len(video_links)}")
            self.logger.info(f"URL: {video_url}")
            self.logger.info(f"{'='*50}")
            
            try:
                # Extract video ID for pre-check
                video_id = self.smart_retry_extractor.extract_video_id_from_url(video_url)
                
                # Check if already processed and valid
                if self.file_validator.validate_video_folder(video_id):
                    self.logger.info(f"Video {video_id} already exists and is valid, skipping")
                    self.progress_tracker.update_download_stats(video_id, 0)
                    stats["skipped"] += 1
                    continue
                
                # Extract video information (now uses Crawl4AI with Selenium fallback)
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
                    else:
                        stats["failed"] += 1
                        self.logger.error(f"Failed to process video {i}/{len(video_links)} after all retries")
                else:
                    self.logger.warning(f"⚠️ No video source found for video {i}/{len(video_links)}")
                    stats["failed"] += 1
                
                # Delay between videos
                self._wait_between_videos()
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing video {i}: {e}")
                stats["failed"] += 1
                traceback.print_exc()
                continue
        
        # Update page progress and generate report
        self.progress_tracker.update_page_progress(page_num)
        self.generate_final_report(page_num, video_links, stats)
    
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
                        self.logger.info(f" Successfully processed batch item {i+1}")
                    else:
                        stats["failed"] += 1
                        self.logger.error(f" Failed to process batch item {i+1}")
                else:
                    self.logger.warning(f" No video source found for batch item {i+1}")
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
                self.config["general"]["download_directory"],
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
        self.logger.info(f" ID: {video_info['video_id']}")
        self.logger.info(f" Title: {video_info['title']}")
        self.logger.info(f" Duration: {video_info['duration']}")
        self.logger.info(f" Views: {video_info['views']}")
        self.logger.info(f" Upload Date: {video_info['upload_date']} (Epoch: {video_info.get('upload_date_epoch')})")
        self.logger.info(f" Tags: {len(video_info['tags'])} tags")
        self.logger.info(f" Has video source: {'Yes' if video_info.get('video_src') else 'No'}")
        self.logger.info(f" Has thumbnail: {'Yes' if video_info.get('thumbnail_src') else 'No'}")
        self.logger.info(f" Crawl4AI data: {'Available' if video_info.get('crawl4ai_data') else 'Not available'}")
    
    def _wait_between_videos(self):
        """Wait between video processing to be respectful"""
        delay_seconds = self.config["general"]["delay_between_requests"] / 1000
        self.logger.info(f"Waiting {delay_seconds} seconds before next video...")
        time.sleep(delay_seconds)
    
    def generate_final_report(self, page_num, video_links, stats):
        """Create summary of scraping results"""
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
        self.logger.info(f"{'='*60}")
    
    async def run_full_site_scrape(self, start_page=1, end_page=None):
        """Run a full site scrape across multiple pages"""
        try:
            self.web_driver_manager.setup_driver()
            
            current_page = start_page
            
            while True:
                if end_page and current_page > end_page:
                    break
                
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"STARTING PAGE {current_page}")
                self.logger.info(f"{'='*80}")
                
                try:
                    if self.use_parallel_processing and self.processing_mode in ["crawl4ai", "hybrid"]:
                        await self.process_page_parallel(current_page)
                    else:
                        self.process_page_sequential(current_page)
                    
                    current_page += 1
                    
                    # Wait between pages
                    if not end_page or current_page <= end_page:
                        page_delay = self.config.get("general", {}).get("delay_between_pages", 5000) / 1000
                        self.logger.info(f"Waiting {page_delay} seconds before next page...")
                        await asyncio.sleep(page_delay)
                        
                except Exception as e:
                    self.logger.error(f"Error processing page {current_page}: {e}")
                    traceback.print_exc()
                    current_page += 1
                    continue
                    
        except KeyboardInterrupt:
            self.logger.info("Full site scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in full site scrape: {e}")
            traceback.print_exc()
        finally:
            self.web_driver_manager.close_driver()
            self.logger.info("Full site scraper finished")


# Usage
if __name__ == "__main__":
    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)
    
    scraper = VideoScraper()
    
    # Option 1: Single page processing (original behavior)
    # scraper.run()
    
    # Option 2: Full site scraping with parallel processing
    asyncio.run(scraper.run_full_site_scrape(start_page=1, end_page=5))