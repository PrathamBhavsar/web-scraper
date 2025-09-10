# video_processor.py - Parallel processing ready

import os
import json
import time
import logging
import shutil
import threading
from pathlib import Path

class VideoProcessor:
    def __init__(self, config, file_validator, file_downloader, progress_tracker):
        self.config = config
        self.file_validator = file_validator
        self.file_downloader = file_downloader
        self.progress_tracker = progress_tracker
        self.logger = logging.getLogger('Rule34Scraper')
        # Thread-local storage for thread-specific logging
        self._local = threading.local()
    
    def get_thread_logger(self):
        """Get a thread-specific logger prefix"""
        if not hasattr(self._local, 'thread_name'):
            self._local.thread_name = threading.current_thread().name
        return self._local.thread_name
    
    def log_info(self, message):
        """Thread-aware logging"""
        thread_name = self.get_thread_logger()
        self.logger.info(f"[{thread_name}] {message}")
    
    def log_error(self, message):
        """Thread-aware error logging"""
        thread_name = self.get_thread_logger()
        self.logger.error(f"[{thread_name}] {message}")
    
    def log_warning(self, message):
        """Thread-aware warning logging"""
        thread_name = self.get_thread_logger()
        self.logger.warning(f"[{thread_name}] {message}")
    
    def process_video_parallel_safe(self, video_info, max_retries=3, page_num=None):
        """
        Process and download a single video - DESIGNED FOR PARALLEL EXECUTION
        
        This method is thread-safe and designed to be called from multiple threads
        simultaneously without conflicts.
        """
        if not video_info or not video_info.get("video_src"):
            self.log_error("No video info or video source provided")
            return False

        video_id = video_info["video_id"]
        thread_name = self.get_thread_logger()
        
        self.log_info(f"Starting parallel processing for video {video_id}")
        
        # CRITICAL CHECK: Verify if already downloaded/failed before any processing
        if self.progress_tracker.is_video_downloaded(video_id):
            self.log_info(f"SKIPPING: Video {video_id} already in progress.json")
            return True
        
        if self.progress_tracker.is_video_failed(video_id):
            self.log_info(f"SKIPPING: Video {video_id} previously failed")
            return False

        if self.file_validator.validate_video_folder(video_id):
            self.log_info(f"SKIPPING: Video {video_id} folder exists and is valid")
            # Ensure it's in progress tracker
            if not self.progress_tracker.is_video_downloaded(video_id):
                self.progress_tracker.update_download_stats(video_id, 0, page_num)
            return True

        self.log_info(f"PROCESSING: Video {video_id} - folder not found, will download")
        
        # Set page context for downloader
        self.file_downloader.current_page_num = page_num
        
        download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
        video_dir = download_path / video_id

        # Clean up any existing incomplete folder before starting
        if video_dir.exists():
            self.cleanup_incomplete_folder(video_id)

        # Retry logic for the entire video processing
        return self.retry_video_processing_parallel(video_info, max_retries, page_num)

    def retry_video_processing_parallel(self, video_info, max_retries, page_num):
        """Thread-safe retry logic for video processing"""
        video_id = video_info["video_id"]
        
        for retry in range(max_retries):
            try:
                self.log_info(f"Processing video {video_id} (attempt {retry + 1}/{max_retries})")
                
                # Validate video info before proceeding
                if not self._validate_video_info_before_processing(video_info, video_id, retry, max_retries):
                    continue

                # Create video directory
                video_dir = self._create_video_directory(video_info, video_id, retry, max_retries)
                if not video_dir:
                    continue

                # Download and validate video file
                if not self._download_video_with_validation_parallel(video_info, video_dir, video_id, retry, max_retries):
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
                self._update_success_progress_parallel(video_info, video_dir, page_num)
                return True

            except Exception as e:
                self.log_error(f"Unexpected error processing video {video_id} (attempt {retry + 1}): {e}")
                import traceback
                self.log_error(f"Traceback: {traceback.format_exc()}")
                self.cleanup_incomplete_folder(video_id)
                
                if retry == max_retries - 1:
                    # Mark as failed to avoid future retries
                    self.progress_tracker.add_failed_video(video_id)
                    return False

            # Safe delay access with default
            validation_config = self.config.get("validation", {})
            delay = validation_config.get("validation_delay_seconds", 2)
            time.sleep(delay)

        # If we get here, all retries failed
        self.progress_tracker.add_failed_video(video_id)
        return False

    def process_video(self, video_info, max_retries=3):
        """Legacy method - redirects to parallel-safe version"""
        return self.process_video_parallel_safe(video_info, max_retries)

    def _validate_video_info_before_processing(self, video_info, video_id, retry, max_retries):
        """Validate video info before processing"""
        try:
            is_info_valid, info_errors = self.file_validator.validate_video_info(video_info)
            if not is_info_valid:
                self.log_error(f"Video info validation failed for {video_id}: {info_errors}")
                if retry == max_retries - 1:
                    return False
                
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False
            
            return True
        except Exception as e:
            self.log_error(f"Error validating video info: {e}")
            return False

    def _create_video_directory(self, video_info, video_id, retry, max_retries):
        """Create video directory with error handling"""
        try:
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            download_path.mkdir(parents=True, exist_ok=True)
            
            video_dir = download_path / video_id
            video_dir.mkdir(parents=True, exist_ok=True)
            
            self.log_info(f"Created directory: {video_dir}")
            return video_dir
            
        except Exception as e:
            self.log_error(f"Failed to create directory for {video_id}: {e}")
            self.cleanup_incomplete_folder(video_id)
            
            if retry == max_retries - 1:
                return None
                
            validation_config = self.config.get("validation", {})
            delay = validation_config.get("validation_delay_seconds", 2)
            time.sleep(delay)
            return None

    def _download_video_with_validation_parallel(self, video_info, video_dir, video_id, retry, max_retries):
        """Download video file with validation - parallel safe"""
        try:
            video_file_path = video_dir / f"{video_id}.mp4"
            
            self.log_info(f"Starting video download for {video_id}")
            
            # Download the video file
            if not self.file_downloader.download_file(video_info["video_src"], video_file_path):
                self.log_error(f"Failed to download video: {video_id} (attempt {retry + 1})")
                self.cleanup_incomplete_folder(video_id)
                
                if retry == max_retries - 1:
                    return False
                    
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False

            # Verify video file immediately after download
            if not self.file_validator.verify_video_file(video_file_path):
                self.log_error(f"Video verification failed: {video_id} (attempt {retry + 1})")
                self.cleanup_incomplete_folder(video_id)
                
                if retry == max_retries - 1:
                    return False
                    
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False
            
            return True
            
        except Exception as e:
            self.log_error(f"Error downloading video {video_id}: {e}")
            return False

    def _download_thumbnail_optional(self, video_info, video_id, video_dir):
        """Download thumbnail (optional - don't fail if this doesn't work)"""
        try:
            if video_info.get("thumbnail_src"):
                thumbnail_downloaded = self.file_downloader.download_thumbnail(
                    video_info["thumbnail_src"], video_id, video_dir
                )
                
                if thumbnail_downloaded:
                    self.log_info(f"Thumbnail downloaded for {video_id}")
                else:
                    self.log_warning(f"Thumbnail download failed for {video_id} - continuing anyway")
        except Exception as e:
            self.log_warning(f"Thumbnail download failed for {video_id}: {e} - continuing anyway")

    def _save_metadata_json(self, video_info, video_dir, video_id, retry, max_retries):
        """Save metadata JSON with error handling"""
        try:
            json_path = video_dir / f"{video_id}.json"
            
            # Ensure complete video info before saving
            self._ensure_complete_video_info(video_info)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=2, ensure_ascii=False)
            
            self.log_info(f"Metadata saved: {json_path}")
            return True
            
        except Exception as e:
            self.log_error(f"Failed to save metadata for {video_id}: {e}")
            self.cleanup_incomplete_folder(video_id)
            
            if retry == max_retries - 1:
                return False
                
            validation_config = self.config.get("validation", {})
            delay = validation_config.get("validation_delay_seconds", 2)
            time.sleep(delay)
            return False

    def _ensure_complete_video_info(self, video_info):
        """Ensure video_info has all required fields with valid values"""
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
                    from datetime import datetime
                    video_info["upload_date"] = int(datetime.now().timestamp() * 1000)
                except:
                    video_info["upload_date"] = int(time.time() * 1000)
            
            # Ensure tags is a list
            if not isinstance(video_info.get("tags"), list):
                video_info["tags"] = ["untagged"]
                
        except Exception as e:
            self.log_error(f"Error ensuring complete video info: {e}")

    def _final_validation(self, video_info, video_dir, video_id, retry, max_retries):
        """Final comprehensive validation"""
        try:
            is_valid, validation_errors = self.file_validator.validate_complete_download(video_info, video_dir)
            if not is_valid:
                self.log_error(f"Final validation failed for {video_id} (attempt {retry + 1}): {validation_errors}")
                self.cleanup_incomplete_folder(video_id)
                
                if retry == max_retries - 1:
                    self.log_error(f"Max retries exceeded for {video_id}, skipping permanently")
                    return False
                    
                validation_config = self.config.get("validation", {})
                delay = validation_config.get("validation_delay_seconds", 2)
                time.sleep(delay)
                return False
            
            return True
            
        except Exception as e:
            self.log_error(f"Error in final validation for {video_id}: {e}")
            return False

    def _update_success_progress_parallel(self, video_info, video_dir, page_num):
        """Update progress tracking after successful processing - Thread-safe"""
        try:
            video_id = video_info["video_id"]
            video_file_path = video_dir / f"{video_id}.mp4"
            
            file_size_mb = 0
            if video_file_path.exists():
                file_size_mb = video_file_path.stat().st_size / (1024 * 1024)
            
            # Thread-safe progress update
            self.progress_tracker.update_download_stats(video_id, file_size_mb, page_num)
            
            # Additional verification
            if video_id not in self.progress_tracker.get_downloaded_videos():
                self.log_error(f"CRITICAL: Video {video_id} not found in downloaded list after update!")
                # Force add it
                with self.progress_tracker._lock:
                    self.progress_tracker.progress["downloaded_videos"].append(video_id)
                    self.progress_tracker.save_progress()
            
            self.log_info(f"Successfully processed video: {video_id} ({file_size_mb:.2f} MB)")
            self.log_info(f"Video {video_id} marked as downloaded - will be skipped in future runs")
            
        except Exception as e:
            self.log_error(f"Error updating progress: {e}")

    def cleanup_incomplete_folder(self, video_id):
        """Remove incomplete video folders - Thread safe"""
        try:
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            video_dir = download_path / video_id
            
            if video_dir.exists():
                shutil.rmtree(video_dir)
                self.log_info(f"Removed incomplete folder: {video_dir}")
                
        except Exception as e:
            self.log_error(f"Error removing folder {video_id}: {e}")

    def batch_process_videos_parallel(self, video_info_list, max_workers=3):
        """
        NEW: Process multiple videos in parallel using ThreadPoolExecutor
        """
        import concurrent.futures
        
        if not video_info_list:
            self.log_info("No videos to process")
            return {"successful": 0, "failed": 0, "skipped": 0}
        
        self.log_info(f"Starting batch parallel processing of {len(video_info_list)} videos (max_workers={max_workers})")
        
        results = {"successful": 0, "failed": 0, "skipped": 0}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, 
                                                 thread_name_prefix="VideoProcessor") as executor:
            # Submit all video processing tasks
            future_to_video = {
                executor.submit(self.process_video_parallel_safe, video_info): video_info["video_id"]
                for video_info in video_info_list
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_video):
                video_id = future_to_video[future]
                try:
                    success = future.result(timeout=600)  # 10 minute timeout per video
                    
                    if success:
                        results["successful"] += 1
                        self.log_info(f"✓ Video {video_id} processed successfully")
                    else:
                        results["failed"] += 1
                        self.log_error(f"✗ Video {video_id} processing failed")
                        
                except concurrent.futures.TimeoutError:
                    results["failed"] += 1
                    self.log_error(f"✗ Video {video_id} processing timed out")
                    
                except Exception as e:
                    results["failed"] += 1
                    self.log_error(f"✗ Video {video_id} processing failed with exception: {e}")
        
        self.log_info(f"Batch processing completed: {results['successful']} successful, {results['failed']} failed")
        return results
    

    def process_video_parallel_safe(self, video_info, max_retries, page_num):
        """
        Enhanced video processing with comprehensive storage limit checking
        """
        video_id = video_info["video_id"]
        
        try:
            # PHASE 1: Pre-processing storage check
            if not self._check_storage_before_video_processing(video_id):
                self.logger.warning(f" STORAGE LIMIT: Skipping video {video_id} - storage limit reached")
                return False
            
            # PHASE 2: Check if already processed
            if self.progress_tracker.is_video_downloaded(video_id):
                self.logger.info(f"SKIPPING: Video {video_id} already downloaded")
                return True
            
            if self.progress_tracker.is_video_failed(video_id):
                self.logger.info(f"SKIPPING: Video {video_id} previously failed")
                return True
            
            # PHASE 3: Validate video folder exists and is complete
            if self.file_validator.validate_video_folder(video_id):
                self.logger.info(f"SKIPPING: Video {video_id} folder already valid")
                # Add to progress if not already there
                if not self.progress_tracker.is_video_downloaded(video_id):
                    # Calculate existing file size
                    existing_size = self._calculate_existing_video_size(video_id)
                    self.progress_tracker.update_download_stats(video_id, existing_size, page_num)
                return True
            
            # PHASE 4: Process the video with storage monitoring
            self.logger.info(f"PROCESSING: Video {video_id} needs full processing")
            
            success = self._process_video_with_storage_monitoring(video_info, max_retries, page_num)
            
            if success:
                self.logger.info(f"✓ SUCCESS: Video {video_id} processed completely")
            else:
                self.logger.error(f"✗ FAILED: Video {video_id} processing failed")
                self.progress_tracker.add_failed_video(video_id)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in parallel-safe video processing for {video_id}: {e}")
            self.progress_tracker.add_failed_video(video_id)
            return False

    def _check_storage_before_video_processing(self, video_id):
        """
        Check storage limits before starting video processing
        """
        try:
            max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
            
            # Check current storage status
            if self.progress_tracker.is_storage_limit_reached(max_storage_gb, threshold=0.95):
                self.logger.warning(f" STORAGE LIMIT: 95% threshold reached, stopping video processing")
                return False
            
            # Estimate space needed for average video (video + thumbnail + metadata)
            estimated_video_mb = 100  # Conservative estimate
            
            can_add, current_gb, projected_gb, warning = self.progress_tracker.check_storage_limit_before_update(
                estimated_video_mb, max_storage_gb
            )
            
            if not can_add:
                self.logger.warning(f" STORAGE LIMIT: Cannot process video {video_id}")
                self.logger.warning(f"Would exceed limit: {projected_gb:.2f} GB > {max_storage_gb} GB")
                return False
            
            if warning:
                self.logger.warning(f"  STORAGE WARNING: Close to limit, will process {video_id} but monitoring closely")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking storage before video processing: {e}")
            return True  # Allow processing on error

    def _process_video_with_storage_monitoring(self, video_info, max_retries, page_num):
        """
        Process video with continuous storage monitoring
        """
        video_id = video_info["video_id"]
        
        try:
            # Get download path
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            video_dir = download_path / video_id
            video_dir.mkdir(parents=True, exist_ok=True)
            
            # Pre-processing storage check
            max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
            pre_process_gb = self.progress_tracker.get_current_storage_usage_gb()
            
            self.logger.info(f"Starting video processing: {video_id}")
            self.logger.info(f"Pre-processing storage: {pre_process_gb:.2f} GB / {max_storage_gb} GB")
            
            # STEP 1: Save metadata with storage check
            if not self._save_metadata_with_storage_check(video_info, video_dir):
                return False
            
            # STEP 2: Download video with storage monitoring
            if not self._download_video_with_storage_monitoring(video_info, video_dir, page_num):
                return False
            
            # STEP 3: Download thumbnail with storage check
            if not self._download_thumbnail_with_storage_check(video_info, video_dir):
                # Thumbnail failure is not critical, continue
                self.logger.warning(f"Thumbnail failed for {video_id}, but continuing")
            
            # STEP 4: Final validation and progress update
            return self._finalize_video_processing_with_storage(video_info, video_dir, page_num)
            
        except Exception as e:
            self.logger.error(f"Error in storage-monitored video processing for {video_id}: {e}")
            return False

    def _save_metadata_with_storage_check(self, video_info, video_dir):
        """Save video metadata with storage checking"""
        try:
            video_id = video_info["video_id"]
            json_file = video_dir / f"{video_id}.json"
            
            # Check storage before saving (metadata is small, but check anyway)
            if self.progress_tracker.is_storage_limit_reached(
                self.config.get("general", {}).get("max_storage_gb", 100), 
                threshold=0.99
            ):
                self.logger.warning(f" STORAGE CRITICAL: Cannot save metadata for {video_id}")
                return False
            
            # Save metadata
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"✓ Metadata saved for {video_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving metadata for {video_info.get('video_id', 'unknown')}: {e}")
            return False

    def _download_video_with_storage_monitoring(self, video_info, video_dir, page_num):
        """Download video with continuous storage monitoring"""
        try:
            video_id = video_info["video_id"]
            video_src = video_info.get("video_src")
            
            if not video_src:
                self.logger.error(f"No video source URL for {video_id}")
                return False
            
            video_file = video_dir / f"{video_id}.mp4"
            
            # Pre-download storage check
            max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
            if self.progress_tracker.is_storage_limit_reached(max_storage_gb, threshold=0.95):
                self.logger.warning(f" STORAGE LIMIT: Cannot download video {video_id}")
                return False
            
            # Download video
            self.logger.info(f"Downloading video: {video_id}")
            success = self.file_downloader.download_video(video_src, video_file)
            
            if success and video_file.exists():
                # Calculate actual downloaded size
                actual_size_mb = video_file.stat().st_size / (1024 * 1024)
                
                # Post-download storage verification
                post_download_gb = self.progress_tracker.get_current_storage_usage_gb()
                usage_pct = self.progress_tracker.get_storage_usage_percentage(max_storage_gb)
                
                self.logger.info(f"✓ Video downloaded: {video_id} ({actual_size_mb:.2f} MB)")
                self.logger.info(f"Post-download storage: {post_download_gb:.2f} GB / {max_storage_gb} GB ({usage_pct:.1f}%)")
                
                # Check if we're now over the limit
                if post_download_gb > max_storage_gb:
                    self.logger.warning(f"  STORAGE EXCEEDED after downloading {video_id}")
                    self.progress_tracker.add_storage_limit_reached_flag()
                
                return True
            else:
                self.logger.error(f"Video download failed: {video_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error downloading video {video_info.get('video_id', 'unknown')}: {e}")
            return False

    def _download_thumbnail_with_storage_check(self, video_info, video_dir):
        """Download thumbnail with storage checking"""
        try:
            video_id = video_info["video_id"]
            thumbnail_src = video_info.get("thumbnail_src")
            
            if not thumbnail_src:
                self.logger.warning(f"No thumbnail source for {video_id}")
                return False
            
            # Quick storage check (thumbnails are small, but check if we're at critical level)
            max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
            if self.progress_tracker.is_storage_limit_reached(max_storage_gb, threshold=0.99):
                self.logger.warning(f" STORAGE CRITICAL: Skipping thumbnail for {video_id}")
                return False
            
            # Download thumbnail
            success = self.file_downloader.download_thumbnail(thumbnail_src, video_id, video_dir)
            
            if success:
                self.logger.info(f"✓ Thumbnail downloaded: {video_id}")
            else:
                self.logger.warning(f"Thumbnail download failed: {video_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error downloading thumbnail for {video_info.get('video_id', 'unknown')}: {e}")
            return False

    def _finalize_video_processing_with_storage(self, video_info, video_dir, page_num):
        """Finalize video processing with storage tracking"""
        try:
            video_id = video_info["video_id"]
            
            # Calculate total size of downloaded files
            total_size_mb = 0
            for file_path in video_dir.iterdir():
                if file_path.is_file():
                    total_size_mb += file_path.stat().st_size / (1024 * 1024)
            
            # Final validation
            is_valid, validation_errors = self.file_validator.validate_complete_download(video_info, video_dir)
            
            if is_valid:
                # Update progress tracker with actual size
                self.progress_tracker.update_download_stats(video_id, total_size_mb, page_num)
                
                # Final storage status
                final_usage_gb = self.progress_tracker.get_current_storage_usage_gb()
                max_storage_gb = self.config.get("general", {}).get("max_storage_gb", 100)
                usage_pct = self.progress_tracker.get_storage_usage_percentage(max_storage_gb)
                
                self.logger.info(f"✓ COMPLETE: Video {video_id} processing finished ({total_size_mb:.2f} MB)")
                self.logger.info(f"Current storage: {final_usage_gb:.2f} GB / {max_storage_gb} GB ({usage_pct:.1f}%)")
                
                # Check if we should stop due to storage limits
                if usage_pct >= 95:
                    self.logger.warning(f"  STORAGE WARNING: {usage_pct:.1f}% full - consider stopping soon")
                
                return True
            else:
                self.logger.error(f"Final validation failed for {video_id}: {validation_errors}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error finalizing video processing for {video_info.get('video_id', 'unknown')}: {e}")
            return False

    def _calculate_existing_video_size(self, video_id):
        """Calculate size of existing video files in MB"""
        try:
            download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
            video_dir = download_path / video_id
            
            if not video_dir.exists():
                return 0
            
            total_size_mb = 0
            for file_path in video_dir.iterdir():
                if file_path.is_file():
                    total_size_mb += file_path.stat().st_size / (1024 * 1024)
            
            return total_size_mb
            
        except Exception as e:
            self.logger.error(f"Error calculating existing video size for {video_id}: {e}")
            return 0