#!/usr/bin/env python3
"""
Scrape Orchestrator Module

Main coordinator for batch parsing, validation, retries, and cleanup.
Implements the robust retry + batching workflow for media failures.

Author: AI Assistant
Version: 1.0
"""

import asyncio
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

from config_manager import ConfigManager
from progress_manager import ProgressManager
from page_parser import PageParser, VideoMetadata
from idm_manager import IDMManager, DownloadItem
from validator import FileValidator
from utils import SafeFileOperations, TimestampHelper, wait_with_progress

logger = logging.getLogger(__name__)


@dataclass
class BatchResults:
    """Results from processing a batch of pages."""
    pages_processed: List[int]
    total_videos_found: int
    total_videos_enqueued: int
    failed_pages: List[int]
    permanent_failed_pages: List[int]
    processing_time_seconds: float


@dataclass
class PageRetryState:
    """State tracking for page retry attempts."""
    page_number: int
    attempt_count: int
    failed_video_ids: List[str]
    last_attempt_time: str
    max_attempts_reached: bool = False


class ScrapeOrchestrator:
    """Main coordinator for batch parsing, validation, and retry workflow."""

    def __init__(self, config_file: str = "config.json", 
                 progress_file: str = "progress.json",
                 base_url: str = "https://rule34video.com",
                 downloads_dir: str = "downloads",
                 dry_run: bool = False):
        """
        Initialize scrape orchestrator.

        Args:
            config_file: Path to configuration file
            progress_file: Path to progress tracking file
            base_url: Base URL for scraping
            downloads_dir: Base directory for downloads
            dry_run: If True, simulate operations without actual downloads
        """
        self.config_manager = ConfigManager(config_file)
        self.progress_manager = ProgressManager(progress_file)
        self.page_parser = PageParser(base_url, downloads_dir)
        self.idm_manager = IDMManager(base_download_dir=downloads_dir)
        self.validator = FileValidator()

        self.dry_run = dry_run
        self.should_stop = False
        self._batch_states: List[PageRetryState] = []

        # Load configuration
        self.config = self.config_manager.load_config()
        self.batch_config = self.config.get("batch", {})

        logger.info(f"Scrape orchestrator initialized (dry_run={dry_run})")
        logger.info(f"Batch config: {self.batch_config}")

    def run(self, start_page: Optional[int] = None, 
            max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Run the complete scraping workflow with batch processing and retries.

        Args:
            start_page: Starting page number (from progress if None)
            max_pages: Maximum pages to process (unlimited if None)

        Returns:
            Final results dictionary
        """
        logger.info("Starting batch scraping workflow")
        start_time = time.time()

        # Initialize state
        if start_page is None:
            progress = self.progress_manager.load_progress()
            start_page = progress.get("current_page", 1000)

        current_page = start_page
        pages_processed = 0
        batch_results = []

        try:
            while not self.should_stop and self._should_continue_processing(current_page, pages_processed, max_pages):
                # Process a batch of pages
                batch_pages = self._get_next_batch_pages(current_page)

                if not batch_pages:
                    logger.info("No more pages to process")
                    break

                logger.info(f"Processing batch: pages {batch_pages}")

                batch_result = asyncio.run(self._process_batch(batch_pages))
                batch_results.append(batch_result)

                pages_processed += len(batch_result.pages_processed)
                current_page = min(batch_pages) - 1  # Move to next batch

                # Update progress
                self.progress_manager.update_page_progress(current_page)

                logger.info(f"Batch completed. Next page: {current_page}")

                # Check storage limits
                if self._check_storage_limits():
                    logger.info("Storage limit reached, stopping")
                    break

            # Final summary
            total_time = time.time() - start_time
            final_results = self._compile_final_results(batch_results, total_time, start_page, current_page)

            logger.info(f"Scraping workflow completed in {total_time:.1f}s")
            return final_results

        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user")
            self.should_stop = True
            return {"success": False, "interrupted": True, "current_page": current_page}
        except Exception as e:
            logger.error(f"Error in scraping workflow: {e}")
            return {"success": False, "error": str(e), "current_page": current_page}

    async def _process_batch(self, page_numbers: List[int]) -> BatchResults:
        """
        Process a batch of pages with parsing, enqueuing, and retry logic.

        Args:
            page_numbers: List of page numbers to process

        Returns:
            BatchResults with processing summary
        """
        start_time = time.time()
        batch_result = BatchResults(
            pages_processed=[],
            total_videos_found=0,
            total_videos_enqueued=0,
            failed_pages=[],
            permanent_failed_pages=[],
            processing_time_seconds=0.0
        )

        logger.info(f"Starting batch processing for pages: {page_numbers}")

        # Phase 1: Parse all pages and enqueue to IDM
        all_videos = []
        page_video_map = {}

        for page_num in page_numbers:
            try:
                logger.info(f"Parsing page {page_num}")

                if self.dry_run:
                    logger.info(f"[DRY RUN] Would parse page {page_num}")
                    # Simulate parsing result
                    parse_result = type('ParseResult', (), {
                        'success': True, 'videos': [], 'video_count': 0
                    })()
                else:
                    parse_result = await self.page_parser.parse_page(page_num, save_metadata=True)

                if parse_result.success:
                    batch_result.pages_processed.append(page_num)
                    batch_result.total_videos_found += parse_result.video_count

                    all_videos.extend(parse_result.videos)
                    page_video_map[page_num] = parse_result.videos

                    logger.info(f"Page {page_num}: {parse_result.video_count} videos found")
                else:
                    batch_result.failed_pages.append(page_num)
                    logger.warning(f"Page {page_num} parsing failed")

            except Exception as e:
                logger.error(f"Error parsing page {page_num}: {e}")
                batch_result.failed_pages.append(page_num)

        # Phase 2: Enqueue all videos to IDM
        if all_videos and not self.dry_run:
            logger.info(f"Enqueuing {len(all_videos)} videos to IDM")

            # Create download items
            download_items = []
            for page_num, videos in page_video_map.items():
                items = self.idm_manager.create_download_items_from_videos(videos, page_num)
                download_items.extend(items)

            # Enqueue to IDM
            enqueue_result = self.idm_manager.enqueue(download_items)
            batch_result.total_videos_enqueued = enqueue_result.get("enqueued_count", 0)

            # Start IDM queue
            if enqueue_result.get("success"):
                self.idm_manager.start_queue()
                logger.info("IDM queue started")

        elif self.dry_run:
            logger.info(f"[DRY RUN] Would enqueue {len(all_videos)} videos to IDM")
            batch_result.total_videos_enqueued = len(all_videos)

        # Phase 3: Wait for initial download batch
        initial_wait = self.batch_config.get("batch_initial_wait_seconds", 240)
        logger.info(f"Waiting {initial_wait}s for initial batch downloads")

        if not self.dry_run:
            wait_with_progress(initial_wait, "Waiting for batch downloads")
        else:
            logger.info(f"[DRY RUN] Would wait {initial_wait}s for downloads")

        # Phase 4: Validate and retry each page
        for page_num in batch_result.pages_processed:
            try:
                logger.info(f"Starting validation and retry for page {page_num}")
                retry_result = await self._validate_and_retry_page(page_num, page_video_map.get(page_num, []))

                if retry_result.max_attempts_reached:
                    batch_result.permanent_failed_pages.append(page_num)
                    logger.warning(f"Page {page_num} exceeded max retry attempts")

            except Exception as e:
                logger.error(f"Error in retry workflow for page {page_num}: {e}")
                batch_result.failed_pages.append(page_num)

        batch_result.processing_time_seconds = time.time() - start_time

        logger.info(f"Batch processing completed in {batch_result.processing_time_seconds:.1f}s")
        logger.info(f"Summary: {len(batch_result.pages_processed)} pages, "
                   f"{batch_result.total_videos_found} videos found, "
                   f"{batch_result.total_videos_enqueued} enqueued, "
                   f"{len(batch_result.permanent_failed_pages)} permanent failures")

        return batch_result

    async def _validate_and_retry_page(self, page_number: int, 
                                     videos: List[VideoMetadata]) -> PageRetryState:
        """
        Validate page downloads and retry failed videos up to max attempts.

        Args:
            page_number: Page number to validate
            videos: List of videos for this page

        Returns:
            PageRetryState with retry results
        """
        max_retries = self.batch_config.get("max_failed_retries_per_page", 3)
        per_page_wait = self.batch_config.get("per_page_idm_wait_seconds", 120)

        retry_state = PageRetryState(
            page_number=page_number,
            attempt_count=0,
            failed_video_ids=[],
            last_attempt_time=TimestampHelper.get_current_timestamp()
        )

        logger.info(f"Starting validation/retry for page {page_number} "
                   f"(max {max_retries} attempts)")

        while retry_state.attempt_count < max_retries and not self.should_stop:
            retry_state.attempt_count += 1
            retry_state.last_attempt_time = TimestampHelper.get_current_timestamp()

            logger.info(f"Page {page_number} - Attempt {retry_state.attempt_count}/{max_retries}")

            # Validate current state
            failed_videos = self._validate_page_downloads(page_number, videos)
            retry_state.failed_video_ids = failed_videos

            if not failed_videos:
                logger.info(f"Page {page_number} validation successful - all downloads complete")
                self.progress_manager.remove_failed_videos_for_page(page_number)
                break

            logger.info(f"Page {page_number} has {len(failed_videos)} failed videos")

            # Record failed videos in progress
            video_ids = [video.video_id for video in videos if video.video_id in failed_videos]
            self.progress_manager.record_failed_videos(
                page_number, video_ids, self.page_parser.downloads_dir
            )

            # If not final attempt, retry failed videos
            if retry_state.attempt_count < max_retries:
                logger.info(f"Retrying {len(failed_videos)} failed videos for page {page_number}")

                if not self.dry_run:
                    # Re-enqueue failed videos
                    failed_video_objects = [v for v in videos if v.video_id in failed_videos]
                    retry_items = self.idm_manager.create_download_items_from_videos(
                        failed_video_objects, page_number
                    )

                    retry_result = self.idm_manager.enqueue(retry_items)
                    logger.info(f"Re-enqueued {retry_result.get('enqueued_count', 0)} failed videos")

                    # Wait for retry downloads
                    logger.info(f"Waiting {per_page_wait}s for retry downloads")
                    wait_with_progress(per_page_wait, f"Waiting for page {page_number} retries")
                else:
                    logger.info(f"[DRY RUN] Would retry {len(failed_videos)} videos")
                    time.sleep(1)  # Brief pause in dry run mode

        # Handle max attempts reached
        if retry_state.failed_video_ids and retry_state.attempt_count >= max_retries:
            retry_state.max_attempts_reached = True
            logger.warning(f"Page {page_number} exceeded max retry attempts, marking as permanent failure")

            # Clean up page folder and mark permanent failure
            if not self.dry_run:
                page_folder = self.page_parser._get_page_folder(page_number)
                SafeFileOperations.safe_delete_folder(str(page_folder), backup_to_trash=True)

                self.progress_manager.mark_page_permanent_failed(page_number)
                logger.info(f"Page {page_number} folder moved to trash and marked as permanent failure")
            else:
                logger.info(f"[DRY RUN] Would delete page {page_number} folder and mark permanent failure")

        return retry_state

    def _validate_page_downloads(self, page_number: int, 
                               videos: List[VideoMetadata]) -> List[str]:
        """
        Validate downloads for a page and return list of failed video IDs.

        Args:
            page_number: Page number to validate
            videos: List of video metadata for validation

        Returns:
            List of video IDs that failed validation
        """
        if self.dry_run:
            # In dry run, simulate some failures for testing
            failed_count = min(2, len(videos) // 4)  # 25% failure rate, max 2
            return [video.video_id for video in videos[:failed_count]]

        failed_video_ids = []

        for video in videos:
            video_folder = self.page_parser._get_video_folder(page_number, video.video_id)
            validation_result = self.validator.validate_video_folder(str(video_folder))

            if not validation_result["valid"]:
                failed_video_ids.append(video.video_id)
                logger.debug(f"Video {video.video_id} failed validation: "
                           f"missing {validation_result['missing_files']}")

        return failed_video_ids

    def _get_next_batch_pages(self, current_page: int) -> List[int]:
        """
        Get next batch of page numbers to process.

        Args:
            current_page: Current page number

        Returns:
            List of page numbers for next batch
        """
        batch_size = self.batch_config.get("batch_pages", 3)

        # Generate descending page numbers
        pages = []
        for i in range(batch_size):
            page_num = current_page - i
            if page_num >= 1:  # Don't go below page 1
                # Skip permanently failed pages
                if not self.progress_manager.is_page_permanently_failed(page_num):
                    pages.append(page_num)
            else:
                break

        return pages

    def _should_continue_processing(self, current_page: int, 
                                  pages_processed: int, 
                                  max_pages: Optional[int]) -> bool:
        """
        Check if processing should continue.

        Args:
            current_page: Current page number
            pages_processed: Number of pages processed so far
            max_pages: Maximum pages limit

        Returns:
            True if processing should continue
        """
        if current_page < 1:
            logger.info("Reached minimum page number (1)")
            return False

        if max_pages and pages_processed >= max_pages:
            logger.info(f"Reached maximum pages limit ({max_pages})")
            return False

        if self.should_stop:
            logger.info("Stop requested")
            return False

        return True

    def _check_storage_limits(self) -> bool:
        """
        Check if storage limits have been reached.

        Returns:
            True if storage limit reached
        """
        try:
            from utils import calculate_folder_size

            downloads_dir = self.page_parser.downloads_dir
            current_size_mb = calculate_folder_size(str(downloads_dir))
            max_size_gb = self.config_manager.get_max_storage_gb()
            max_size_mb = max_size_gb * 1024

            usage_percent = (current_size_mb / max_size_mb) * 100 if max_size_mb > 0 else 0

            if usage_percent >= 95.0:
                logger.warning(f"Storage limit reached: {usage_percent:.1f}% "
                             f"({current_size_mb:.1f}MB / {max_size_gb}GB)")
                return True

            logger.debug(f"Storage usage: {usage_percent:.1f}% "
                        f"({current_size_mb:.1f}MB / {max_size_gb}GB)")
            return False

        except Exception as e:
            logger.error(f"Error checking storage limits: {e}")
            return False

    def _compile_final_results(self, batch_results: List[BatchResults], 
                             total_time: float, start_page: int, 
                             end_page: int) -> Dict[str, Any]:
        """
        Compile final results from all batch results.

        Args:
            batch_results: List of batch results
            total_time: Total processing time
            start_page: Starting page number
            end_page: Ending page number

        Returns:
            Final results dictionary
        """
        total_pages_processed = sum(len(br.pages_processed) for br in batch_results)
        total_videos_found = sum(br.total_videos_found for br in batch_results)
        total_videos_enqueued = sum(br.total_videos_enqueued for br in batch_results)
        total_failed_pages = sum(len(br.failed_pages) for br in batch_results)
        total_permanent_failures = sum(len(br.permanent_failed_pages) for br in batch_results)

        # Get final progress statistics
        progress_stats = self.progress_manager.get_progress_stats()

        results = {
            "success": True,
            "dry_run": self.dry_run,
            "total_time_seconds": total_time,
            "start_page": start_page,
            "end_page": end_page,
            "pages_processed": total_pages_processed,
            "videos_found": total_videos_found,
            "videos_enqueued": total_videos_enqueued,
            "failed_pages": total_failed_pages,
            "permanent_failed_pages": total_permanent_failures,
            "batch_count": len(batch_results),
            "final_progress_stats": progress_stats,
            "timestamp": TimestampHelper.get_current_timestamp()
        }

        # Calculate success rates
        if total_videos_found > 0:
            results["enqueue_success_rate"] = (total_videos_enqueued / total_videos_found) * 100

        if total_pages_processed > 0:
            results["page_success_rate"] = ((total_pages_processed - total_permanent_failures) / total_pages_processed) * 100

        logger.info("Final results compiled:")
        logger.info(f"  Pages processed: {total_pages_processed}")
        logger.info(f"  Videos found: {total_videos_found}")
        logger.info(f"  Videos enqueued: {total_videos_enqueued}")
        logger.info(f"  Permanent failures: {total_permanent_failures}")
        logger.info(f"  Total time: {total_time:.1f}s")

        return results

    def stop(self) -> None:
        """Stop the orchestrator gracefully."""
        logger.info("Stop requested for scrape orchestrator")
        self.should_stop = True

    def get_current_state(self) -> Dict[str, Any]:
        """
        Get current orchestrator state.

        Returns:
            Current state information
        """
        progress_stats = self.progress_manager.get_progress_stats()
        idm_state = self.idm_manager.get_queue_state()

        return {
            "should_stop": self.should_stop,
            "dry_run": self.dry_run,
            "batch_config": self.batch_config,
            "progress_stats": progress_stats,
            "idm_state": idm_state,
            "batch_states_count": len(self._batch_states),
            "timestamp": TimestampHelper.get_current_timestamp()
        }


if __name__ == "__main__":
    # Demo usage
    import logging
    logging.basicConfig(level=logging.INFO)

    # Test orchestrator in dry run mode
    orchestrator = ScrapeOrchestrator(dry_run=True)

    print("Testing orchestrator in dry run mode...")
    results = orchestrator.run(start_page=1000, max_pages=6)  # Process 2 batches

    print("\nResults:")
    for key, value in results.items():
        print(f"  {key}: {value}")
