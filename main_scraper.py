import os
import re
import time
import logging
import traceback
from pathlib import Path
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
        """Main execution loop - backward pagination with storage limit"""
        try:
            # Setup driver
            self.web_driver_manager.setup_driver()
            
            # Get the last page number
            last_page = self.page_navigator.get_last_page_number()
            self.logger.info(f"Found last page number: {last_page}")
            
            # Convert GB limit to bytes for comparison
            max_storage_bytes = self.config["general"]["max_storage_gb"] * 1024**3
            self.logger.info(f"Maximum storage limit: {self.config['general']['max_storage_gb']} GB ({max_storage_bytes:,} bytes)")
            
            # Log initial disk usage
            initial_usage = self._get_download_folder_size()
            self.logger.info(f"Initial disk usage: {initial_usage / (1024**3):.2f} GB ({initial_usage:,} bytes)")
            
            # Start from last page and work backward
            for current_page in range(last_page, 0, -1):
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"STARTING PAGE {current_page} (working backward from {last_page})")
                self.logger.info(f"{'='*80}")
                
                # Check storage limit before processing each page
                current_usage = self._get_download_folder_size()
                usage_gb = current_usage / (1024**3)
                
                self.logger.info(f"Current disk usage: {usage_gb:.2f} GB ({current_usage:,} bytes)")
                
                if current_usage >= max_storage_bytes:
                    self.logger.info(f"🛑 STORAGE LIMIT REACHED! Used: {usage_gb:.2f} GB, Limit: {self.config['general']['max_storage_gb']} GB")
                    self.logger.info("Stopping scraping process.")
                    break
                
                remaining_space = (max_storage_bytes - current_usage) / (1024**3)
                self.logger.info(f"Remaining storage space: {remaining_space:.2f} GB")
                
                # Process the current page
                page_success = self.process_page(current_page)
                
                if not page_success:
                    self.logger.warning(f"Failed to process page {current_page}, continuing to next page")
                    continue
                
                # Log final usage after page completion
                final_page_usage = self._get_download_folder_size()
                final_usage_gb = final_page_usage / (1024**3)
                page_downloaded = (final_page_usage - current_usage) / (1024**3)
                
                self.logger.info(f"Page {current_page} completed. Downloaded: {page_downloaded:.2f} GB")
                self.logger.info(f"Total usage after page {current_page}: {final_usage_gb:.2f} GB")
            
            # Final summary
            final_total_usage = self._get_download_folder_size()
            final_total_gb = final_total_usage / (1024**3)
            
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"SCRAPING COMPLETED")
            self.logger.info(f"{'='*80}")
            self.logger.info(f"Final total disk usage: {final_total_gb:.2f} GB ({final_total_usage:,} bytes)")
            self.logger.info(f"Storage limit: {self.config['general']['max_storage_gb']} GB")
            self.logger.info(f"Space utilization: {(final_total_gb / self.config['general']['max_storage_gb'] * 100):.1f}%")
            
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
            current_usage = self._get_download_folder_size()
            self.logger.info(f"Usage at interruption: {current_usage / (1024**3):.2f} GB")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
            traceback.print_exc()
        finally:
            self.web_driver_manager.close_driver()
            self.logger.info("Scraper finished")
    
    def process_page(self, page_num):
        """Process all videos from a single page with storage monitoring"""
        self.logger.info(f"Starting scrape from page {page_num}")
        
        # Get all video links from the page
        video_links = self.page_navigator.get_video_links_from_page(page_num)
        
        if not video_links:
            self.logger.error(f"No video links found on page {page_num}")
            return False
        
        self.logger.info(f"Found {len(video_links)} videos to process on page {page_num}")
        
        # Process each video
        stats = {"successful": 0, "failed": 0, "skipped": 0}
        max_storage_bytes = self.config["general"]["max_storage_gb"] * 1024**3
        
        for i, video_url in enumerate(video_links, 1):
            self.logger.info(f"\n{'-'*50}")
            self.logger.info(f"Processing video {i}/{len(video_links)} on page {page_num}")
            self.logger.info(f"URL: {video_url}")
            self.logger.info(f"{'-'*50}")
            
            try:
                # Check storage limit before each video
                pre_video_usage = self._get_download_folder_size()
                if pre_video_usage >= max_storage_bytes:
                    usage_gb = pre_video_usage / (1024**3)
                    self.logger.info(f"🛑 Storage limit reached before video {i}. Usage: {usage_gb:.2f} GB")
                    break
                
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
                        
                        # Log storage usage after successful download
                        post_video_usage = self._get_download_folder_size()
                        usage_gb = post_video_usage / (1024**3)
                        video_size = (post_video_usage - pre_video_usage) / (1024**3)
                        
                        self.logger.info(f"✅ Successfully processed video {i}/{len(video_links)}")
                        self.logger.info(f"📁 Video size: {video_size:.3f} GB, Total usage: {usage_gb:.2f} GB")
                        
                        # Check if we're approaching the limit
                        remaining = (max_storage_bytes - post_video_usage) / (1024**3)
                        if remaining < 1.0:  # Less than 1GB remaining
                            self.logger.warning(f"⚠️ Storage space running low: {remaining:.2f} GB remaining")
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
        
        return True
    
    def _get_download_folder_size(self):
        """Calculate total size of download folder in bytes"""
        try:
            download_path = Path(self.config["general"]["download_path"])
            if not download_path.exists():
                return 0
            
            total_size = 0
            for root, dirs, files in os.walk(download_path):
                for file in files:
                    try:
                        file_path = Path(root) / file
                        total_size += file_path.stat().st_size
                    except (OSError, FileNotFoundError):
                        # Skip files that can't be accessed
                        continue
            
            return total_size
            
        except Exception as e:
            self.logger.error(f"Error calculating download folder size: {e}")
            return 0
    
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
        current_usage = self._get_download_folder_size()
        usage_gb = current_usage / (1024**3)
        
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
        
        # Storage usage
        self.logger.info(f"Current disk usage: {usage_gb:.2f} GB")
        storage_percentage = (usage_gb / self.config["general"]["max_storage_gb"]) * 100
        self.logger.info(f"Storage utilization: {storage_percentage:.1f}%")
        
        self.logger.info(f"{'='*60}")


# Usage
if __name__ == "__main__":
    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)
    
    scraper = VideoScraper()
    scraper.run()
