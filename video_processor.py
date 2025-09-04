import json
import time
import logging
import shutil
from pathlib import Path

class VideoProcessor:
    def __init__(self, config, file_validator, file_downloader, progress_tracker):
        self.config = config
        self.file_validator = file_validator
        self.file_downloader = file_downloader
        self.progress_tracker = progress_tracker
        self.logger = logging.getLogger('Rule34Scraper')
    
    def process_video(self, video_info, max_retries=3):
        """Main video processing orchestrator with retry logic"""
        if not video_info or not video_info.get("video_src"):
            self.logger.error("No video info or video source provided")
            return False
        
        video_id = video_info["video_id"]
        
        # Check if already processed and valid
        if self.file_validator.validate_video_folder(video_id):
            self.logger.info(f"Video {video_id} already exists and is valid, skipping")
            self.progress_tracker.update_download_stats(video_id, 0)
            return True
        
        # Clean up any existing incomplete folder
        self.cleanup_failed_download(video_id)
        
        # Retry processing logic
        return self.retry_video_processing(video_info, max_retries)
    
    def retry_video_processing(self, video_info, max_retries):
        """Handle retry logic for failed processing"""
        video_id = video_info["video_id"]
        
        for retry in range(max_retries):
            try:
                self.logger.info(f"Processing video {video_id} (attempt {retry + 1}/{max_retries})")
                
                # Create directory structure
                video_dir = self.create_video_directory(video_id)
                if not video_dir:
                    if retry == max_retries - 1:
                        return False
                    continue
                
                # Validate video info before processing
                if not self._validate_video_info_before_processing(video_info, video_id, retry, max_retries):
                    continue
                
                # Download video file
                if not self._download_video_with_validation(video_info, video_dir, video_id, retry, max_retries):
                    continue
                
                # Download thumbnail
                if not self._download_thumbnail_with_validation(video_info, video_dir, video_id, retry, max_retries):
                    continue
                
                # Save metadata
                if not self.save_metadata_json(video_info, video_dir, video_id, retry, max_retries):
                    continue
                
                # Final validation
                if not self._perform_final_validation(video_info, video_dir, video_id, retry, max_retries):
                    continue
                
                # Success - update progress
                self._update_success_progress(video_info, video_dir)
                return True
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing video {video_id} (attempt {retry + 1}): {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.cleanup_failed_download(video_id)
                
                if retry == max_retries - 1:
                    return False
                
                time.sleep(self.config["validation"]["validation_delay_seconds"])
        
        return False
    
    def create_video_directory(self, video_id):
        """Create directory structure for video files"""
        try:
            download_path = Path(self.config["general"]["download_path"])
            download_path.mkdir(parents=True, exist_ok=True)
            
            video_dir = download_path / video_id
            video_dir.mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"Created directory: {video_dir}")
            return video_dir
            
        except Exception as e:
            self.logger.error(f"Failed to create directory for {video_id}: {e}")
            return None
    
    def save_metadata_json(self, video_info, video_dir, video_id, retry, max_retries):
        """Save video information to JSON file"""
        try:
            json_path = video_dir / f"{video_id}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Metadata saved: {json_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {video_id}: {e}")
            self.cleanup_failed_download(video_id)
            if retry == max_retries - 1:
                return False
            time.sleep(self.config["validation"]["validation_delay_seconds"])
            return False
    
    def cleanup_failed_download(self, video_id):
        """Remove incomplete downloads on failure"""
        try:
            download_path = Path(self.config["general"]["download_path"])
            video_dir = download_path / video_id
            
            if video_dir.exists():
                shutil.rmtree(video_dir)
                self.logger.info(f"Removed incomplete folder: {video_dir}")
                
        except Exception as e:
            self.logger.error(f"Error removing folder {video_id}: {e}")
    
    def _validate_video_info_before_processing(self, video_info, video_id, retry, max_retries):
        """Validate video info before starting download process"""
        is_info_valid, info_errors = self.file_validator.validate_video_info(video_info)
        if not is_info_valid:
            self.logger.error(f"Video info validation failed for {video_id}: {info_errors}")
            if retry == max_retries - 1:
                return False
            time.sleep(self.config["validation"]["validation_delay_seconds"])
            return False
        return True
    
    def _download_video_with_validation(self, video_info, video_dir, video_id, retry, max_retries):
        """Download video file with validation"""
        video_file_path = video_dir / f"{video_id}.mp4"
        
        if not self.file_downloader.download_video(video_info["video_src"], video_file_path):
            self.logger.error(f"Failed to download video: {video_id} (attempt {retry + 1})")
            self.cleanup_failed_download(video_id)
            if retry == max_retries - 1:
                return False
            time.sleep(self.config["validation"]["validation_delay_seconds"])
            return False
        
        # Verify video file immediately after download
        if not self.file_validator.verify_video_file(video_file_path):
            self.logger.error(f"Video verification failed: {video_id} (attempt {retry + 1})")
            self.cleanup_failed_download(video_id)
            if retry == max_retries - 1:
                return False
            time.sleep(self.config["validation"]["validation_delay_seconds"])
            return False
        
        return True
    
    def _download_thumbnail_with_validation(self, video_info, video_dir, video_id, retry, max_retries):
        """Download thumbnail with validation"""
        if video_info.get("thumbnail_src"):
            thumbnail_downloaded = self.file_downloader.download_thumbnail(
                video_info["thumbnail_src"], video_id, video_dir
            )
            if not thumbnail_downloaded:
                self.logger.error(f"Thumbnail download failed for {video_id} (attempt {retry + 1})")
                self.cleanup_failed_download(video_id)
                if retry == max_retries - 1:
                    return False
                time.sleep(self.config["validation"]["validation_delay_seconds"])
                return False
        
        return True
    
    def _perform_final_validation(self, video_info, video_dir, video_id, retry, max_retries):
        """Perform final comprehensive validation"""
        is_valid, validation_errors = self.file_validator.validate_complete_download(video_info, video_dir)
        if not is_valid:
            self.logger.error(f"Final validation failed for {video_id} (attempt {retry + 1}): {validation_errors}")
            self.cleanup_failed_download(video_id)
            if retry == max_retries - 1:
                self.logger.error(f"Max retries exceeded for {video_id}, skipping permanently")
                return False
            time.sleep(self.config["validation"]["validation_delay_seconds"])
            return False
        
        return True
    
    def _update_success_progress(self, video_info, video_dir):
        """Update progress tracking after successful processing"""
        try:
            video_id = video_info["video_id"]
            video_file_path = video_dir / f"{video_id}.mp4"
            file_size_mb = self.file_downloader.get_download_stats(video_file_path)
            
            self.progress_tracker.update_download_stats(video_id, file_size_mb)
            self.logger.info(f"Successfully processed and validated video: {video_id} ({file_size_mb:.2f} MB)")
            
        except Exception as e:
            self.logger.error(f"Error updating progress: {e}")