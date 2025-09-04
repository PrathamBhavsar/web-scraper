import re
import time
import logging
from video_info_extractor import VideoInfoExtractor

class SmartRetryExtractor:
    def __init__(self, config, driver_manager, video_info_extractor, file_validator):
        self.config = config
        self.driver_manager = driver_manager
        self.video_info_extractor = video_info_extractor
        self.file_validator = file_validator
        self.logger = logging.getLogger('Rule34Scraper')
    
    def smart_retry_video_extraction(self, video_url, max_attempts=3):
        """Smart retry logic for video information extraction with increasing wait times"""
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"Extracting video info (attempt {attempt + 1}/{max_attempts})")
                
                # Add longer wait time for subsequent attempts
                if attempt > 0:
                    wait_time = 5 + (attempt * 3)  # Increasing wait time
                    self.logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                
                # Re-navigate to the page
                if not self.driver_manager.navigate_to_page(video_url):
                    continue
                
                # Extract video info
                video_info = self.video_info_extractor.extract_video_info(video_url)
                
                if video_info:
                    # Validate the extracted info
                    is_valid, errors = self.file_validator.validate_video_info(video_info)
                    if is_valid:
                        self.logger.info(f"Successfully extracted and validated video info on attempt {attempt + 1}")
                        return video_info
                    else:
                        self.logger.warning(f"Video info validation failed on attempt {attempt + 1}: {errors}")
                else:
                    self.logger.warning(f"Video info extraction returned None on attempt {attempt + 1}")
                    
            except Exception as e:
                self.logger.error(f"Error during video info extraction attempt {attempt + 1}: {e}")
        
        self.logger.error(f"Failed to extract valid video info after {max_attempts} attempts")
        return None
    
    def extract_video_id_from_url(self, video_url):
        """Extract video ID for pre-processing checks"""
        video_id_match = re.search(r'/video/(\d+)/', video_url)
        if video_id_match:
            return video_id_match.group(1)
        else:
            return video_url.split('/')[-2] if video_url.endswith('/') else video_url.split('/')[-1]