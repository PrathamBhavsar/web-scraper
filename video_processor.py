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
        """Much more lenient video processing - focuses on getting videos downloaded"""
        if not video_info:
            self.logger.error("No video info provided")
            return False
        
        video_id = video_info["video_id"]
        video_src = video_info.get("video_src", "").strip()
        
        # Basic check - must have video source
        if not video_src:
            self.logger.error(f"No video source for {video_id}")
            return False
        
        # Check if already processed and valid (relaxed)
        if self.file_validator.validate_video_folder(video_id):
            self.logger.info(f"Video {video_id} already exists and is valid, skipping")
            self.progress_tracker.update_download_stats(video_id, 0)
            return True
        
        # Clean up any existing incomplete folder
        self.cleanup_failed_download(video_id)
        
        # Retry processing logic
        return self.retry_video_processing_relaxed(video_info, max_retries)
    
    def retry_video_processing_relaxed(self, video_info, max_retries):
        """Much more lenient retry logic - prioritize getting files downloaded"""
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
                
                # Download video file - this is the most important part
                video_file_path = video_dir / f"{video_id}.mp4"
                if not self.file_downloader.download_video(video_info["video_src"], video_file_path):
                    self.logger.error(f"Failed to download video: {video_id} (attempt {retry + 1})")
                    self.cleanup_failed_download(video_id)
                    if retry == max_retries - 1:
                        return False
                    time.sleep(2)
                    continue
                
                # Basic video file check - very lenient
                if not self._basic_video_file_check(video_file_path):
                    self.logger.error(f"Video file basic check failed: {video_id} (attempt {retry + 1})")
                    self.cleanup_failed_download(video_id)
                    if retry == max_retries - 1:
                        return False
                    time.sleep(2)
                    continue
                
                # Try to download thumbnail (optional - don't fail if this doesn't work)
                if video_info.get("thumbnail_src"):
                    thumbnail_success = self.file_downloader.download_thumbnail(
                        video_info["thumbnail_src"], video_id, video_dir
                    )
                    if thumbnail_success:
                        self.logger.info(f"Thumbnail downloaded for {video_id}")
                    else:
                        self.logger.warning(f"Thumbnail download failed for {video_id} - continuing anyway")
                
                # Save metadata (optional - don't fail if this doesn't work)
                try:
                    self.save_metadata_json_relaxed(video_info, video_dir)
                except Exception as e:
                    self.logger.warning(f"Metadata save failed for {video_id}: {e} - continuing anyway")
                
                # Success - update progress
                self._update_success_progress(video_info, video_dir)
                return True
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing video {video_id} (attempt {retry + 1}): {e}")
                self.cleanup_failed_download(video_id)
                
                if retry == max_retries - 1:
                    return False
                
                time.sleep(2)
        
        return False
    
    def _basic_video_file_check(self, video_file_path):
        """Very basic check - just ensure file exists and isn't empty"""
        try:
            if not video_file_path.exists():
                return False
            
            file_size = video_file_path.stat().st_size
            # Very lenient - just 1KB minimum
            if file_size < 1024:
                self.logger.warning(f"Video file very small: {file_size} bytes")
                return False
            
            self.logger.info(f"Video file basic check passed: {file_size} bytes")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in basic video file check: {e}")
            return False
    
    def save_metadata_json_relaxed(self, video_info, video_dir):
        """Save metadata with fallbacks for missing fields"""
        video_id = video_info["video_id"]
        json_path = video_dir / f"{video_id}.json"
        
        # Create safe metadata with fallbacks
        safe_metadata = {
            "video_id": video_info.get("video_id", "unknown"),
            "title": video_info.get("title", f"Video_{video_id}"),
            "duration": video_info.get("duration", "00:30"),
            "views": str(video_info.get("views", "0")),
            "upload_date_epoch": video_info.get("upload_date_epoch", int(time.time() * 1000)),
            "url": video_info.get("url", ""),
            "uploader": video_info.get("uploader", "Unknown"),
            "upload_date": video_info.get("upload_date", "Unknown"),
            "tags": video_info.get("tags", ["untagged"]),
            "video_src": video_info.get("video_src", ""),
            "thumbnail_src": video_info.get("thumbnail_src", "")
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(safe_metadata, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Metadata saved (with fallbacks): {json_path}")
    
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
    
    def _update_success_progress(self, video_info, video_dir):
        """Update progress tracking after successful processing"""
        try:
            video_id = video_info["video_id"]
            video_file_path = video_dir / f"{video_id}.mp4"
            
            file_size_mb = 0
            if video_file_path.exists():
                file_size_mb = video_file_path.stat().st_size / (1024 * 1024)
            
            self.progress_tracker.update_download_stats(video_id, file_size_mb)
            self.logger.info(f"Successfully processed video: {video_id} ({file_size_mb:.2f} MB)")
            
        except Exception as e:
            self.logger.error(f"Error updating progress: {e}")


import time