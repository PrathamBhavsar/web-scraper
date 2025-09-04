import os
import re
import time
import logging
import traceback
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
            self.process_page(current_page)
            
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
            traceback.print_exc()
        finally:
            self.web_driver_manager.close_driver()
            self.logger.info("Scraper finished")
    
    def process_page(self, page_num):
        """Process all videos from a single page"""
        self.logger.info(f"Starting scrape from page {page_num}")
        
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
                
                # Smart retry extraction of video information
                video_info = self.smart_retry_extractor.smart_retry_video_extraction(video_url)
                if not video_info:
                    self.logger.error(f"Failed to extract valid info for video {i} after all retries")
                    stats["failed"] += 1
                    continue
                
                # Log extracted information
                self._log_video_info_summary(video_info)
                
                # Process (download) the video with validation
                if video_info.get("video_src"):
                    if self.video_processor.process_video(video_info):
                        stats["successful"] += 1
                        self.logger.info(f"✅ Successfully processed and validated video {i}/{len(video_links)}")
                    else:
                        stats["failed"] += 1
                        self.logger.error(f"❌ Failed to process video {i}/{len(video_links)} after all retries")
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


# Usage
if __name__ == "__main__":
    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)
    
    scraper = VideoScraper()
    scraper.run()