# video_processor.py

import os
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
        """Process and download a single video with comprehensive validation and retry logic"""
        if not video_info or not video_info.get("video_src"):
            self.logger.error("No video info or video source provided")
            return False

        video_id = video_info["video_id"]
        
        # CRITICAL CHECK: Verify if already downloaded before any processing
        if self.progress_tracker.is_video_downloaded(video_id):
            self.logger.info(f"SKIPPING: Video {video_id} already in progress.json")
            return True
        
        if self.file_validator.validate_video_folder(video_id):
            self.logger.info(f"SKIPPING: Video {video_id} folder exists and is valid")
            # Ensure it's in progress tracker
            if not self.progress_tracker.is_video_downloaded(video_id):
                self.progress_tracker.update_download_stats(video_id, 0)
            return True

        self.logger.info(f"PROCESSING: Video {video_id} - folder not found, will download")
        
        download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
        video_dir = download_path / video_id

        # Clean up any existing incomplete folder before starting
        if video_dir.exists():
            self.cleanup_incomplete_folder(video_id)

        # Retry logic for the entire video processing
        return self.retry_video_processing(video_info, max_retries)


    def retry_video_processing(self, video_info, max_retries):
        """Retry logic for video processing with safe config access"""
        video_id = video_info["video_id"]

        for retry in range(max_retries):
            try:
                self.logger.info(f"Processing video {video_id} (attempt {retry + 1}/{max_retries})")

                # Validate video info before proceeding
                if not self._validate_video_info_before_processing(video_info, video_id, retry, max_retries):
                    continue

                # Create video directory
                video_dir = self._create_video_directory(video_info, video_id, retry, max_retries)
                if not video_dir:
                    continue

                # Download and validate video file
                if not self._download_video_with_validation(video_info, video_dir, video_id, retry, max_retries):
                    continue

                # Download thumbnail (optional - don't fail if this doesn't work)
                self._download_thumbnail_optional(video_info, video_id, video_dir)

                # Save metadata JSON
                if not self._save_metadata_json(video_info, video_dir, video_id, retry, max_retries):
                    continue

                # Final validation
                if not self._final_validation(video_info, video_dir, video_id, retry, max_retries):
                    continue

                # Update progress and return success
                self._update_success_progress(video_info, video_dir)
                return True

            except Exception as e:
                self.logger.error(f"Unexpected error processing video {video_id} (attempt {retry + 1}): {e}")
                self.cleanup_incomplete_folder(video_id)
                if retry == max_retries - 1:
                    return False

                # Safe delay access with default
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)

        return False

    def _validate_video_info_before_processing(self, video_info, video_id, retry, max_retries):
        """Validate video info before processing"""
        try:
            is_info_valid, info_errors = self.file_validator.validate_video_info(video_info)
            if not is_info_valid:
                self.logger.error(f"Video info validation failed for {video_id}: {info_errors}")
                if retry == max_retries - 1:
                    return False
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error validating video info: {e}")
            return False

    def _create_video_directory(self, video_info, video_id, retry, max_retries):
        """Create video directory with error handling"""
        try:
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            download_path.mkdir(parents=True, exist_ok=True)

            video_dir = download_path / video_id
            video_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created directory: {video_dir}")
            return video_dir
        except Exception as e:
            self.logger.error(f"Failed to create directory for {video_id}: {e}")
            self.cleanup_incomplete_folder(video_id)
            if retry == max_retries - 1:
                return None
            validation_config = self.config.get("validation", {})
            delay = validation_config.get("validation_delay_seconds", 2)
            time.sleep(delay)
            return None

    def _download_video_with_validation(self, video_info, video_dir, video_id, retry, max_retries):
        """Download video file with validation"""
        try:
            video_file_path = video_dir / f"{video_id}.mp4"
            self.logger.info(f"Starting video download for {video_id}")

            # Download the video file
            if not self.file_downloader.download_file(video_info["video_src"], video_file_path):
                self.logger.error(f"Failed to download video: {video_id} (attempt {retry + 1})")
                self.cleanup_incomplete_folder(video_id)
                if retry == max_retries - 1:
                    return False
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False

            # Verify video file immediately after download
            if not self.file_validator.verify_video_file(video_file_path):
                self.logger.error(f"Video verification failed: {video_id} (attempt {retry + 1})")
                self.cleanup_incomplete_folder(video_id)
                if retry == max_retries - 1:
                    return False
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False

            return True
        except Exception as e:
            self.logger.error(f"Error downloading video {video_id}: {e}")
            return False

    def _download_thumbnail_optional(self, video_info, video_id, video_dir):
        """Download thumbnail (optional - don't fail if this doesn't work)"""
        try:
            if video_info.get("thumbnail_src"):
                thumbnail_downloaded = self.file_downloader.download_thumbnail(
                    video_info["thumbnail_src"], video_id, video_dir
                )
                if thumbnail_downloaded:
                    self.logger.info(f"Thumbnail downloaded for {video_id}")
                else:
                    self.logger.warning(f"Thumbnail download failed for {video_id} - continuing anyway")
        except Exception as e:
            self.logger.warning(f"Thumbnail download failed for {video_id}: {e} - continuing anyway")

    def _save_metadata_json(self, video_info, video_dir, video_id, retry, max_retries):
        """Save metadata JSON with error handling"""
        try:
            json_path = video_dir / f"{video_id}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Metadata saved: {json_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {video_id}: {e}")
            self.cleanup_incomplete_folder(video_id)
            if retry == max_retries - 1:
                return False
            validation_config = self.config.get("validation", {})
            delay = validation_config.get("validation_delay_seconds", 2)
            time.sleep(delay)
            return False

    def _final_validation(self, video_info, video_dir, video_id, retry, max_retries):
        """Final comprehensive validation"""
        try:
            is_valid, validation_errors = self.file_validator.validate_complete_download(video_info, video_dir)
            if not is_valid:
                self.logger.error(f"Final validation failed for {video_id} (attempt {retry + 1}): {validation_errors}")
                self.cleanup_incomplete_folder(video_id)
                if retry == max_retries - 1:
                    self.logger.error(f"Max retries exceeded for {video_id}, skipping permanently")
                    return False
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False
            return True
        except Exception as e:
            self.logger.error(f"Error in final validation for {video_id}: {e}")
            return False

    def _update_success_progress(self, video_info, video_dir):
        """Update progress tracking after successful processing - CRITICAL for preventing re-downloads"""
        try:
            video_id = video_info["video_id"]
            video_file_path = video_dir / f"{video_id}.mp4"
            
            file_size_mb = 0
            if video_file_path.exists():
                file_size_mb = video_file_path.stat().st_size / (1024 * 1024)

            # DOUBLE CHECK: Ensure video is marked as downloaded in progress tracker
            self.progress_tracker.update_download_stats(video_id, file_size_mb)
            
            # Additional verification
            if video_id not in self.progress_tracker.get_downloaded_videos():
                self.logger.error(f"CRITICAL: Video {video_id} not found in downloaded list after update!")
                # Force add it
                self.progress_tracker.progress["downloaded_videos"].append(video_id)
                self.progress_tracker.save_progress()
            
            self.logger.info(f"Successfully processed video: {video_id} ({file_size_mb:.2f} MB)")
            self.logger.info(f"Video {video_id} marked as downloaded - will be skipped in future runs")
            
        except Exception as e:
            self.logger.error(f"Error updating progress: {e}")


    def cleanup_incomplete_folder(self, video_id):
        """Remove incomplete video folders"""
        try:
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            video_dir = download_path / video_id

            if video_dir.exists():
                shutil.rmtree(video_dir)
                self.logger.info(f"Removed incomplete folder: {video_dir}")
        except Exception as e:
            self.logger.error(f"Error removing folder {video_id}: {e}")
