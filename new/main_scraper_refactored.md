#!/usr/bin/env python3

"""
main_scraper.py - Refactored for Parser/Downloader Separation

Entry point for parsing only (Phase 1). Orchestrates batches of 10 pages,
collects JSON metadata and creates manifests for media_downloader.py to process later.

Usage:
    python main_scraper.py --parse --start-page 1000 --batch-count 5
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from config_manager import ConfigManager
from manifest_manager import ManifestManager
from page_parser import PageParser, VideoMetadata
from progress_manager import ProgressManager
from utils import LoggingConfig, TimestampHelper


class MainScraper:
    def __init__(self, config_file: str = "config.json"):
        """Initialize the main scraper with configuration."""
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.load_config()
        
        # Setup logging with structured format
        LoggingConfig.setup_logging(
            log_level=self.config.get("logging", {}).get("log_level", "INFO"),
            log_file=self.config.get("logging", {}).get("log_file_path", "scraper.log")
        )
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.downloads_dir = self.config.get("download_directory", "downloads")
        self.manifest_manager = ManifestManager(
            self.config.get("manifest_directory", "manifests")
        )
        self.progress_manager = ProgressManager("progress.json")
        self.page_parser = PageParser(
            base_url=self.config.get("general", {}).get("base_url", "https://rule34video.com"),
            downloads_dir=self.downloads_dir
        )
        
        # Setup structured logging for JSON events
        if self.config.get("logging", {}).get("structured_logs", False):
            self._setup_structured_logging()

    def _setup_structured_logging(self):
        """Setup additional structured JSON logging."""
        json_handler = logging.FileHandler(
            self.config.get("logging", {}).get("json_log_file", "scraper_structured.log"),
            encoding='utf-8'
        )
        
        class StructuredFormatter(logging.Formatter):
            def format(self, record):
                log_entry = {
                    "timestamp": self.formatTime(record),
                    "level": record.levelname,
                    "module": record.module,
                    "message": record.getMessage()
                }
                
                # Add structured fields if present
                if hasattr(record, 'event'):
                    log_entry['event'] = record.event
                if hasattr(record, 'batch_id'):
                    log_entry['batch_id'] = record.batch_id
                if hasattr(record, 'page'):
                    log_entry['page'] = record.page
                if hasattr(record, 'video_id'):
                    log_entry['video_id'] = record.video_id
                    
                return json.dumps(log_entry)
        
        json_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(json_handler)

    async def parse_batch(self, batch_id: int, page_numbers: List[int]) -> Dict:
        """
        Parse a batch of pages and create manifest.
        
        Args:
            batch_id: Unique identifier for this batch
            page_numbers: List of page numbers to parse
            
        Returns:
            Dictionary with batch results
        """
        start_time = time.time()
        
        # Log batch start
        self.logger.info("Starting batch parsing", extra={
            "event": "batch_start",
            "batch_id": batch_id,
            "pages": page_numbers,
            "page_count": len(page_numbers)
        })
        
        # Initialize manifest for this batch
        self.manifest_manager.new_manifest(batch_id, page_numbers)
        
        total_videos = 0
        successful_pages = []
        failed_pages = []
        
        for page_number in page_numbers:
            try:
                self.logger.info(f"Parsing page {page_number}", extra={
                    "event": "page_parse_start",
                    "batch_id": batch_id,
                    "page": page_number
                })
                
                # Parse the page (save_metadata=True writes JSON files)
                parse_result = await self.page_parser.parse_page(
                    page_number, 
                    save_metadata=True
                )
                
                if parse_result.success and parse_result.videos:
                    successful_pages.append(page_number)
                    total_videos += parse_result.video_count
                    
                    # Add each video to manifest
                    for video in parse_result.videos:
                        self.manifest_manager.add_video_entry(
                            page=page_number,
                            video_id=video.video_id,
                            mp4_url=video.video_url,
                            jpg_url=video.thumbnail_url,
                            target_folder=video.folder_path
                        )
                    
                    self.logger.info("Page parsing completed", extra={
                        "event": "page_parse_success",
                        "batch_id": batch_id,
                        "page": page_number,
                        "video_count": parse_result.video_count,
                        "parse_time": parse_result.parse_time_seconds
                    })
                else:
                    failed_pages.append(page_number)
                    self.logger.warning("Page parsing failed", extra={
                        "event": "page_parse_failed",
                        "batch_id": batch_id,
                        "page": page_number,
                        "errors": parse_result.errors
                    })
                    
            except Exception as e:
                failed_pages.append(page_number)
                self.logger.error(f"Exception parsing page {page_number}: {e}", extra={
                    "event": "page_parse_exception",
                    "batch_id": batch_id,
                    "page": page_number,
                    "error": str(e)
                })
        
        # Save manifest
        try:
            manifest_path = self.manifest_manager.save()
            self.logger.info("Manifest saved successfully", extra={
                "event": "manifest_save_success",
                "batch_id": batch_id,
                "manifest_path": str(manifest_path),
                "total_videos": total_videos
            })
        except Exception as e:
            self.logger.error(f"Failed to save manifest: {e}", extra={
                "event": "manifest_save_failed",
                "batch_id": batch_id,
                "error": str(e)
            })
            raise
        
        batch_time = time.time() - start_time
        
        batch_result = {
            "batch_id": batch_id,
            "pages_attempted": page_numbers,
            "successful_pages": successful_pages,
            "failed_pages": failed_pages,
            "total_videos": total_videos,
            "batch_time_seconds": batch_time,
            "manifest_path": str(manifest_path),
            "timestamp": TimestampHelper.get_current_timestamp()
        }
        
        self.logger.info("Batch parsing completed", extra={
            "event": "batch_complete",
            "batch_id": batch_id,
            "success_count": len(successful_pages),
            "failed_count": len(failed_pages),
            "total_videos": total_videos,
            "batch_time": batch_time
        })
        
        return batch_result

    async def run_parsing(self, start_page: int, batch_count: int, batch_size: int = 10) -> Dict:
        """
        Run the parsing workflow for multiple batches.
        
        Args:
            start_page: Starting page number
            batch_count: Number of batches to process
            batch_size: Number of pages per batch
            
        Returns:
            Overall results dictionary
        """
        start_time = time.time()
        
        self.logger.info("Starting parsing workflow", extra={
            "event": "parsing_workflow_start",
            "start_page": start_page,
            "batch_count": batch_count,
            "batch_size": batch_size,
            "total_pages": batch_count * batch_size
        })
        
        batch_results = []
        current_page = start_page
        
        for batch_num in range(1, batch_count + 1):
            # Generate page numbers for this batch (descending)
            page_numbers = [current_page - i for i in range(batch_size)]
            page_numbers = [p for p in page_numbers if p >= 1]  # Don't go below page 1
            
            if not page_numbers:
                self.logger.info("No more valid pages to process")
                break
            
            try:
                batch_result = await self.parse_batch(batch_num, page_numbers)
                batch_results.append(batch_result)
                
                # Update progress
                self.progress_manager.update_page_progress(min(page_numbers))
                
                current_page = min(page_numbers) - 1  # Move to next batch starting point
                
            except Exception as e:
                self.logger.error(f"Batch {batch_num} failed: {e}", extra={
                    "event": "batch_failed",
                    "batch_id": batch_num,
                    "error": str(e)
                })
                break
        
        total_time = time.time() - start_time
        
        # Compile final results
        total_pages_attempted = sum(len(br["pages_attempted"]) for br in batch_results)
        total_pages_successful = sum(len(br["successful_pages"]) for br in batch_results)
        total_videos = sum(br["total_videos"] for br in batch_results)
        
        final_results = {
            "workflow": "parsing_only",
            "started_at": TimestampHelper.get_current_timestamp(),
            "total_time_seconds": total_time,
            "batches_processed": len(batch_results),
            "pages_attempted": total_pages_attempted,
            "pages_successful": total_pages_successful,
            "total_videos_found": total_videos,
            "success_rate": (total_pages_successful / total_pages_attempted * 100) if total_pages_attempted > 0 else 0,
            "batch_results": batch_results
        }
        
        self.logger.info("Parsing workflow completed", extra={
            "event": "parsing_workflow_complete",
            "batches_processed": len(batch_results),
            "total_videos": total_videos,
            "total_time": total_time,
            "success_rate": final_results["success_rate"]
        })
        
        return final_results


async def main():
    parser = argparse.ArgumentParser(description="Video scraper - parsing phase")
    parser.add_argument("--parse", action="store_true", help="Run parsing phase only")
    parser.add_argument("--start-page", type=int, default=1000, help="Starting page number")
    parser.add_argument("--batch-count", type=int, default=3, help="Number of batches to process")
    parser.add_argument("--batch-size", type=int, default=10, help="Pages per batch")
    parser.add_argument("--config", default="config.json", help="Configuration file path")
    
    args = parser.parse_args()
    
    if not args.parse:
        print("This script is for parsing only. Use --parse flag.")
        print("For downloading, use media_downloader.py with manifest files.")
        sys.exit(1)
    
    try:
        scraper = MainScraper(args.config)
        results = await scraper.run_parsing(
            start_page=args.start_page,
            batch_count=args.batch_count,
            batch_size=args.batch_size
        )
        
        # Save results summary
        results_file = f"parsing_results_{TimestampHelper.get_current_timestamp().replace(':', '-')}.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n{'='*50}")
        print("PARSING PHASE COMPLETED")
        print(f"{'='*50}")
        print(f"Batches processed: {results['batches_processed']}")
        print(f"Pages successful: {results['pages_successful']}")
        print(f"Total videos found: {results['total_videos_found']}")
        print(f"Success rate: {results['success_rate']:.1f}%")
        print(f"Total time: {results['total_time_seconds']:.1f}s")
        print(f"Results saved to: {results_file}")
        print(f"\nNext: Run media_downloader.py with the generated manifest files in manifests/")
        
    except KeyboardInterrupt:
        print("\nParsing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())