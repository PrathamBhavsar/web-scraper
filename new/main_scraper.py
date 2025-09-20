#!/usr/bin/env python3
"""
Enhanced Main Scraper with Fresh Restart Page Detection
Added logic to detect the true "last" page when starting fresh (last_page = 0)
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import re
from pathlib import Path
from typing import Optional, Dict, List, Any
from playwright.async_api import async_playwright

# Original components...
try:
    from scrape_orchestrator import ScrapeOrchestrator
    from config_manager import ConfigManager
    from utils import LoggingConfig, TimestampHelper
except ImportError as e:
    print(f"Core components not available: {e}")

try:
    from manifest_manager import ManifestManager
    from page_parser import PageParser, VideoMetadata
    from progress_manager import ProgressManager
    PARSER_MODE_AVAILABLE = True
except ImportError as e:
    print(f"Parser-only mode not available: {e}")
    PARSER_MODE_AVAILABLE = False

try:
    from media_downloader import MediaDownloader
    DOWNLOADER_MODE_AVAILABLE = True
except ImportError as e:
    print(f"Downloader-only mode not available: {e}")
    DOWNLOADER_MODE_AVAILABLE = False

# Check for video_data_parser legacy dependency...
try:
    import video_data_parser
    print("Video data parser available")
except ImportError:
    print("video_data_parser.py not found - parsing may fail")


class FreshRestartPageDetector:
    """
    Detects the actual "last" page number from the HTML when starting fresh.
    Handles the fresh restart logic when last_page is 0 in progress.json.
    """
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
    
    async def detect_last_page(self) -> Optional[int]:
        """
        Fetch the base URL and extract the actual last page number from HTML.
        
        Returns:
            Page number from "Last" link, or None if not found
        """
        try:
            self.logger.info("Detecting last page number from base URL", 
                           extra={"base_url": self.base_url})
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                try:
                    # Navigate to base URL
                    await page.goto(self.base_url, wait_until='domcontentloaded')
                    await page.wait_for_timeout(2000)  # Wait for page to fully load
                    
                    # Get the HTML content
                    html_content = await page.content()
                    
                    # Extract page number from "Last" link
                    last_page_num = self._extract_last_page_from_html(html_content)
                    
                    if last_page_num:
                        self.logger.info("Successfully detected last page", 
                                       extra={"page_number": last_page_num})
                    else:
                        self.logger.warning("Could not detect last page from HTML")
                    
                    return last_page_num
                    
                except Exception as e:
                    self.logger.error("Error navigating to base URL", 
                                    extra={"error": str(e), "url": self.base_url})
                    return None
                finally:
                    await browser.close()
                    
        except Exception as e:
            self.logger.error("Error in fresh restart page detection", 
                            extra={"error": str(e)})
            return None
    
    def _extract_last_page_from_html(self, html_content: str) -> Optional[int]:
        """
        Extract the page number from the "Last" link in HTML.
        
        Looks for pattern: <div class="item"><a href="/latest-updates/9666/">Last</a></div>
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Page number as integer, or None if not found
        """
        try:
            # Pattern to match the "Last" link structure
            # <div class="item"><a href="/latest-updates/XXXX/">Last</a></div>
            last_link_pattern = r'<div[^>]*class="[^"]*item[^"]*"[^>]*>\s*<a[^>]*href="[^"]*latest-updates/(\d+)[^"]*"[^>]*>\s*Last\s*</a>\s*</div>'
            
            match = re.search(last_link_pattern, html_content, re.IGNORECASE | re.MULTILINE)
            
            if match:
                page_num = int(match.group(1))
                self.logger.info("Found Last link", 
                               extra={"page_number": page_num, "pattern": "div.item > a"})
                return page_num
            
            # Alternative pattern - just look for "Last" text and nearby page numbers
            alt_patterns = [
                r'>\s*Last\s*</a>[^>]*href="[^"]*latest-updates/(\d+)[^"]*"',
                r'href="[^"]*latest-updates/(\d+)[^"]*"[^>]*>\s*Last\s*</a>',
                r'class="[^"]*last[^"]*"[^>]*href="[^"]*latest-updates/(\d+)[^"]*"',
                r'href="[^"]*latest-updates/(\d+)[^"]*"[^>]*class="[^"]*last[^"]*"'
            ]
            
            for pattern in alt_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    page_num = int(match.group(1))
                    self.logger.info("Found Last link with alternative pattern", 
                                   extra={"page_number": page_num})
                    return page_num
            
            # Fallback - find all latest-updates links and get the highest number
            all_page_links = re.findall(r'latest-updates/(\d+)', html_content)
            if all_page_links:
                highest_page = max(int(page) for page in all_page_links)
                self.logger.info("Using highest page number as fallback", 
                               extra={"page_number": highest_page, "total_links": len(all_page_links)})
                return highest_page
            
            self.logger.warning("No Last link or page numbers found in HTML")
            return None
            
        except Exception as e:
            self.logger.error("Error extracting last page from HTML", 
                            extra={"error": str(e)})
            return None


class EnhancedParserOnlyMode:
    """Enhanced Parser-Only Mode with Fresh Restart Page Detection."""
    
    def __init__(self, config_file: str = "config.json"):
        """Initialize enhanced parser-only mode."""
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.load_config()
        
        # Operation parameters...
        self.downloads_dir = self.config.get("download_directory", "downloads")
        self.manifest_manager = ManifestManager(self.config.get("manifest_directory", "manifests"))
        self.progress_manager = ProgressManager("progress.json")
        
        base_url = self.config.get("general", {}).get("base_url", "https://rule34video.com")
        self.page_parser = PageParser(base_url=base_url, downloads_dir=self.downloads_dir)
        self.page_detector = FreshRestartPageDetector(base_url)
        
        self.logger = logging.getLogger(__name__)
    
    async def determine_start_page(self, requested_start_page: Optional[int] = None) -> int:
        """
        Determine the correct starting page based on progress and fresh restart logic.
        
        Args:
            requested_start_page: Page number requested by user (overrides everything)
            
        Returns:
            Page number to start parsing from
        """
        # If user specified a page, use it
        if requested_start_page is not None:
            self.logger.info("Using user-specified start page", 
                           extra={"start_page": requested_start_page})
            return requested_start_page
        
        # Load current progress
        try:
            progress_data = self.progress_manager.load_progress()
            last_page = progress_data.get("last_page", 0)
            
            self.logger.info("Loaded progress data", 
                           extra={"last_page": last_page})
            
            # If last_page is not 0, continue from there
            if last_page != 0:
                self.logger.info("Continuing from previous page", 
                               extra={"continue_from": last_page})
                return last_page
            
            # Fresh restart - detect actual last page
            self.logger.info("Fresh restart detected, detecting actual last page")
            detected_page = await self.page_detector.detect_last_page()
            
            if detected_page:
                self.logger.info("Fresh restart page detection successful", 
                               extra={"detected_page": detected_page})
                return detected_page
            else:
                # Fallback to default if detection fails
                fallback_page = 1000
                self.logger.warning("Page detection failed, using fallback", 
                                  extra={"fallback_page": fallback_page})
                return fallback_page
                
        except Exception as e:
            self.logger.error("Error determining start page", extra={"error": str(e)})
            # Ultimate fallback
            return 1000
    
    async def run_parsing(self, start_page: Optional[int], batch_count: int, batch_size: int = 10) -> Dict:
        """
        Run the parsing workflow for multiple batches with enhanced start page logic.
        """
        start_time = time.time()
        
        # Determine actual start page
        actual_start_page = await self.determine_start_page(start_page)
        
        self.logger.info("Starting enhanced parsing workflow", 
                        extra={
                            "event": "parsing_workflow_start",
                            "requested_start_page": start_page,
                            "actual_start_page": actual_start_page,
                            "batch_count": batch_count,
                            "batch_size": batch_size,
                            "total_pages": batch_count * batch_size
                        })
        
        batch_results = []
        current_page = actual_start_page
        
        for batch_num in range(1, batch_count + 1):
            # Generate page numbers for this batch (descending...)
            page_numbers = [current_page - i for i in range(batch_size)]
            page_numbers = [p for p in page_numbers if p >= 1]  # Don't go below page 1
            
            if not page_numbers:
                self.logger.info("No more valid pages to process")
                break
            
            try:
                batch_result = await self.parse_batch(batch_num, page_numbers)
                batch_results.append(batch_result)
                
                # Update progress...
                self.progress_manager.update_page_progress(min(page_numbers))
                current_page = min(page_numbers) - 1  # Move to next batch starting point
                
            except Exception as e:
                self.logger.error(f"Batch {batch_num} failed: {e}", 
                                extra={"event": "batch_failed", "batch_id": batch_num, "error": str(e)})
                break
        
        total_time = time.time() - start_time
        
        # Compile final results...
        total_pages_attempted = sum(len(br["pages_attempted"]) for br in batch_results)
        total_pages_successful = sum(len(br["successful_pages"]) for br in batch_results)
        total_videos = sum(br["total_videos"] for br in batch_results)
        
        final_results = {
            "workflow": "parsing_only",
            "started_at": TimestampHelper.get_current_timestamp(),
            "total_time_seconds": total_time,
            "requested_start_page": start_page,
            "actual_start_page": actual_start_page,
            "batches_processed": len(batch_results),
            "pages_attempted": total_pages_attempted,
            "pages_successful": total_pages_successful,
            "total_videos_found": total_videos,
            "success_rate": (total_pages_successful / total_pages_attempted * 100) if total_pages_attempted > 0 else 0,
            "batch_results": batch_results
        }
        
        self.logger.info("Enhanced parsing workflow completed", 
                        extra={
                            "event": "parsing_workflow_complete",
                            "batches_processed": len(batch_results),
                            "total_videos": total_videos,
                            "total_time": total_time,
                            "success_rate": final_results["success_rate"]
                        })
        
        return final_results
    
    async def parse_batch(self, batch_id: int, page_numbers: List[int]) -> Dict:
        """Parse a batch of pages and create manifest - same as original."""
        start_time = time.time()
        
        self.logger.info("Starting batch parsing", 
                        extra={
                            "event": "batch_start", 
                            "batch_id": batch_id, 
                            "pages": page_numbers, 
                            "page_count": len(page_numbers)
                        })
        
        # Initialize manifest for this batch...
        self.manifest_manager.new_manifest(batch_id, page_numbers)
        
        total_videos = 0
        successful_pages = []
        failed_pages = []
        
        for page_number in page_numbers:
            try:
                self.logger.info(f"Parsing page {page_number}", 
                               extra={"event": "page_parse_start", "batch_id": batch_id, "page": page_number})
                
                # Parse the page (save_metadata=True writes JSON files...)
                parse_result = await self.page_parser.parse_page(
                    page_number, 
                    save_metadata=True
                )
                
                if parse_result.success and parse_result.videos:
                    successful_pages.append(page_number)
                    total_videos += parse_result.video_count
                    
                    # Add each video to manifest...
                    for video in parse_result.videos:
                        self.manifest_manager.add_video_entry(
                            page=page_number,
                            video_id=video.video_id,
                            mp4_url=video.video_url,
                            jpg_url=video.thumbnail_url,
                            target_folder=video.folder_path
                        )
                    
                    self.logger.info("Page parsing completed", 
                                   extra={
                                       "event": "page_parse_success",
                                       "batch_id": batch_id,
                                       "page": page_number,
                                       "video_count": parse_result.video_count,
                                       "parse_time": parse_result.parse_time_seconds
                                   })
                else:
                    failed_pages.append(page_number)
                    self.logger.warning("Page parsing failed", 
                                      extra={
                                          "event": "page_parse_failed",
                                          "batch_id": batch_id,
                                          "page": page_number,
                                          "errors": parse_result.errors
                                      })
                    
            except Exception as e:
                failed_pages.append(page_number)
                self.logger.error(f"Exception parsing page {page_number}: {e}", 
                                extra={
                                    "event": "page_parse_exception",
                                    "batch_id": batch_id,
                                    "page": page_number,
                                    "error": str(e)
                                })
        
        # Save manifest...
        try:
            manifest_path = self.manifest_manager.save()
            self.logger.info("Manifest saved successfully", 
                           extra={
                               "event": "manifest_save_success",
                               "batch_id": batch_id,
                               "manifest_path": str(manifest_path),
                               "total_videos": total_videos
                           })
        except Exception as e:
            self.logger.error(f"Failed to save manifest: {e}", 
                            extra={
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
        
        self.logger.info("Batch parsing completed", 
                        extra={
                            "event": "batch_complete",
                            "batch_id": batch_id,
                            "success_count": len(successful_pages),
                            "failed_count": len(failed_pages),
                            "total_videos": total_videos,
                            "batch_time": batch_time
                        })
        
        return batch_result


# Keep all the other existing classes unchanged...
class DownloaderOnlyMode:
    """Handler for downloader-only mode operations."""
    
    def __init__(self, max_workers: int = 4, max_retries: int = 3):
        """Initialize downloader-only mode."""
        self.downloader = MediaDownloader(max_retries=max_retries, workers=max_workers)
        self.logger = logging.getLogger(__name__)
    
    def run_downloading(self, manifest_path: Optional[str] = None, manifest_dir: str = "manifests") -> Dict:
        """Run downloader on specified manifests."""
        if manifest_path:
            # Process single manifest...
            return self.process_single_manifest(manifest_path)
        else:
            # Process all manifests in directory...
            return self.process_manifest_directory(manifest_dir)
    
    def process_single_manifest(self, manifest_path: str) -> Dict:
        """Process a single manifest file."""
        self.logger.info(f"Processing single manifest: {manifest_path}")
        try:
            results = self.downloader.download_from_manifest(manifest_path)
            success_count = len([r for r in results if r["status"] == "success"])
            failed_count = len([r for r in results if r["status"] == "failed"])
            
            return {
                "success": True,
                "manifests_processed": 1,
                "total_videos": len(results),
                "successful_videos": success_count,
                "failed_videos": failed_count,
                "success_rate": (success_count / len(results) * 100) if results else 0,
                "results": results
            }
        except Exception as e:
            self.logger.error(f"Failed to process manifest {manifest_path}: {e}")
            return {"success": False, "error": str(e), "manifest_path": manifest_path}
    
    def process_manifest_directory(self, manifest_dir: str) -> Dict:
        """Process all manifest files in a directory."""
        manifest_path = Path(manifest_dir)
        if not manifest_path.exists():
            error_msg = f"Manifest directory does not exist: {manifest_dir}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}
        
        # Find all manifest files...
        manifest_files = list(manifest_path.glob("*manifest.json"))
        if not manifest_files:
            error_msg = f"No manifest files found in {manifest_dir}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}
        
        self.logger.info(f"Found {len(manifest_files)} manifest files to process")
        
        all_results = []
        processed_count = 0
        
        for manifest_file in sorted(manifest_files):
            self.logger.info(f"Processing manifest: {manifest_file.name}")
            try:
                results = self.downloader.download_from_manifest(str(manifest_file))
                all_results.extend(results)
                processed_count += 1
            except Exception as e:
                self.logger.error(f"Failed to process {manifest_file.name}: {e}")
        
        success_count = len([r for r in all_results if r["status"] == "success"])
        failed_count = len([r for r in all_results if r["status"] == "failed"])
        
        return {
            "success": True,
            "manifests_found": len(manifest_files),
            "manifests_processed": processed_count,
            "total_videos": len(all_results),
            "successful_videos": success_count,
            "failed_videos": failed_count,
            "success_rate": (success_count / len(all_results) * 100) if all_results else 0,
            "results": all_results
        }


def setup_logging(config: dict) -> None:
    """Setup logging based on configuration."""
    log_config = config.get("logging", {})
    log_level = log_config.get("log_level", "INFO")
    log_file = log_config.get("log_file_path") if log_config.get("log_to_file", False) else None
    json_log_file = log_config.get("json_log_file") if log_config.get("structured_logs", False) else None
    
    LoggingConfig.setup_logging(log_level, log_file, json_log_file)


def validate_requirements() -> bool:
    """Validate that all required dependencies and files are available."""
    required_modules = ["config_manager", "progress_manager", "scrape_orchestrator", 
                       "page_parser", "idm_manager", "validator", "utils"]
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print(f"Missing required modules: {missing_modules}")
        return False
    
    # Check for video_data_parser legacy dependency...
    try:
        import video_data_parser
        print("Video data parser available")
    except ImportError:
        print("video_data_parser.py not found - parsing may fail")
        return False
    
    print("All required modules available")
    return True


def create_argument_parser() -> argparse.ArgumentParser:
    """Create comprehensive command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Enhanced Web Parser - Fresh Restart Page Detection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Fresh Restart Page Detection:
  When last_page is 0 in progress.json, the parser will:
  1. Fetch the base URL HTML content
  2. Look for <div class="item"><a href="/latest-updates/XXXX/">Last</a></div>
  3. Extract the page number (XXXX) and start parsing from there
  4. Fall back to page 1000 if detection fails

Operation Modes:
  Full Workflow (default):  Run complete parsing & downloading workflow
    %(prog)s --start-page 1000          Start from specific page
    %(prog)s --max-pages 10             Limit to 10 pages
    %(prog)s --dry-run                  Simulate without actual downloads

  Parser-Only Mode (Phase 1):         Parse metadata only, create manifests
    %(prog)s --parse                    Parse with fresh restart detection
    %(prog)s --parse --batch-count 5    Parse 5 batches of pages
    %(prog)s --parse --batch-size 10    10 pages per batch

  Downloader-Only Mode (Phase 2):     Process manifests
    %(prog)s --download --manifest manifests/batch001_manifest.json
    %(prog)s --download --manifest-dir manifests    Process all manifests
    %(prog)s --download --max-workers 6             Concurrent downloads

Examples:
  %(prog)s --parse                                   Fresh restart with auto-detection
  %(prog)s --parse --start-page 1000 --batch-count 3 Parse 30 pages (3 batches of 10)  
  %(prog)s --download --manifest-dir manifests --max-retries 5  Download from all manifests
        """
    )
    
    # Operation Mode Selection...
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--parse", action="store_true", 
                          help="Run parser-only mode (Phase 1): metadata collection & manifest creation")
    mode_group.add_argument("--download", action="store_true", 
                          help="Run downloader-only mode (Phase 2): process manifests")
    
    # Basic Operation Parameters...
    parser.add_argument("--start-page", type=int, default=None,
                       help="Starting page number (uses progress.json or auto-detection if not specified)")
    parser.add_argument("--max-pages", type=int, default=None,
                       help="Maximum number of pages to process (unlimited if not specified)")
    
    # Full Workflow Configuration (Original)...
    full_group = parser.add_argument_group("Full Workflow Options")
    full_group.add_argument("--batch-pages", type=int, default=None,
                          help="Number of pages to process per batch (overrides config)")
    full_group.add_argument("--batch-wait", type=int, default=None,
                          help="Initial wait time after batch enqueue in seconds (overrides config)")
    full_group.add_argument("--max-retries", type=int, default=None,
                          help="Maximum retry attempts per page (overrides config)")
    full_group.add_argument("--dry-run", action="store_true",
                          help="Simulate operations without actual downloads or file changes")
    
    # Parser-Only Configuration...
    parser_group = parser.add_argument_group("Parser-Only Options")
    parser_group.add_argument("--batch-count", type=int, default=3,
                            help="Number of batches to parse (parser-only mode), default: 3")
    parser_group.add_argument("--batch-size", type=int, default=10,
                            help="Pages per batch (parser-only mode), default: 10")
    
    # Downloader-Only Configuration...
    downloader_group = parser.add_argument_group("Downloader-Only Options")
    downloader_group.add_argument("--manifest", type=str,
                                help="Single manifest file to process (downloader-only mode)")
    downloader_group.add_argument("--manifest-dir", type=str, default="manifests",
                                help="Directory containing manifest files (downloader-only mode)")
    downloader_group.add_argument("--max-workers", type=int, default=4,
                                help="Maximum concurrent download workers (downloader-only mode)")
    downloader_group.add_argument("--download-retries", type=int, default=3,
                                help="Maximum download retry attempts per video (downloader-only mode)")
    
    # Configuration Files...
    parser.add_argument("--config", default="config.json",
                       help="Path to configuration file (default: config.json)")
    parser.add_argument("--progress", default="progress.json",
                       help="Path to progress file (default: progress.json)")
    parser.add_argument("--downloads-dir", default=None,
                       help="Downloads directory (overrides config)")
    
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Set logging level (overrides config)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging (sets DEBUG level)")
    
    return parser


def update_config_from_args(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """Update configuration based on command line arguments."""
    config_updated = False
    
    # Update batch configuration for full workflow...
    if args.batch_pages is not None:
        config_manager.update_config_value("batch", "batch_pages", args.batch_pages)
        config_updated = True
    if args.batch_wait is not None:
        config_manager.update_config_value("batch", "batch_initial_wait_seconds", args.batch_wait)
        config_updated = True
    if args.max_retries is not None:
        config_manager.update_config_value("batch", "max_failed_retries_per_page", args.max_retries)
        config_updated = True
    
    # Update downloads directory...
    if args.downloads_dir is not None:
        config_manager.update_config_value("general", "download_path", args.downloads_dir)
        # Also update new-style config if it exists...
        try:
            config_manager.update_config_value("download_directory", "value", args.downloads_dir)
        except:
            pass  # Ignore if this config structure doesn't exist
        config_updated = True
    
    # Update logging configuration...
    if args.log_level is not None:
        config_manager.update_config_value("logging", "log_level", args.log_level)
        config_updated = True
    elif args.verbose:
        config_manager.update_config_value("logging", "log_level", "DEBUG")
        config_updated = True
    
    # Update parser-only configuration (store in separate section)...
    if args.parse:
        try:
            config_manager.update_config_value("parser_mode", "batch_size", args.batch_size)
            config_manager.update_config_value("parser_mode", "max_retries", args.download_retries or 3)
        except:
            pass  # Config might not support this structure
        config_updated = True
    
    if config_updated:
        logging.info("Configuration updated based on command line arguments")
    
    return config_updated


def display_startup_info(config_manager: ConfigManager, args: argparse.Namespace) -> None:
    """Display startup information and configuration."""
    config = config_manager.load_config()
    
    print("=" * 80)
    if args.parse:
        print("Enhanced Web Parser - PARSER-ONLY MODE (Phase 1)")
        print("✨ Fresh Restart Page Detection Enabled")
        print("Metadata Collection & Manifest Creation")
    elif args.download:
        print("Web Parser - DOWNLOADER-ONLY MODE (Phase 2)")
        print("Manifest Processing & Media Downloads")
    else:
        print("Web Parser - FULL WORKFLOW MODE")
        print("Complete Parsing & Downloading with IDM Integration")
    print("=" * 80)
    
    # Display operation mode...
    if args.dry_run:
        print("🔸 DRY RUN MODE - No actual downloads or file changes")
    elif args.parse:
        print("🔸 PARSING MODE - Metadata collection only, no media downloads")
        if not args.start_page:
            print("🔸 FRESH RESTART DETECTION - Will auto-detect starting page")
    elif args.download:
        print("🔸 DOWNLOAD MODE - Media downloads from manifests")
    else:
        print("🔸 LIVE MODE - Full workflow with downloads and file changes enabled")
    print()
    
    print("Configuration:")
    # General settings...
    general = config.get("general", {})
    downloads_dir = args.downloads_dir or config.get("download_directory", general.get("download_path", "downloads"))
    print(f"  Download Directory: {downloads_dir}")
    print(f"  Storage Limit: {general.get('max_storage_gb', 940)} GB")
    
    if args.parse:
        # Parser-only settings...
        print(f"  Batch Count: {args.batch_count} batches")
        print(f"  Batch Size: {args.batch_size} pages per batch")
        print(f"  Total Pages: {args.batch_count * args.batch_size} pages")
        print(f"  Manifest Dir: {args.manifest_dir}")
    elif args.download:
        # Downloader-only settings...
        manifest_info = args.manifest if args.manifest else f"All manifests in {args.manifest_dir}"
        print(f"  Manifest: {manifest_info}")
        print(f"  Workers: {args.max_workers} concurrent downloads")
        print(f"  Retries: {args.download_retries} per video")
    else:
        # Full workflow settings...
        batch = config.get("batch", {})
        print(f"  Batch Size: {batch.get('batch_pages', 3)} pages")
        print(f"  Initial Wait: {batch.get('batch_initial_wait_seconds', 240)}s")
        print(f"  Max Retries: {batch.get('max_failed_retries_per_page', 3)} per page")
        print(f"  Retry Wait: {batch.get('per_page_idm_wait_seconds', 120)}s")
    
    print()
    print("Operation Parameters:")
    start_page_info = args.start_page or ("Auto-detect from HTML" if args.parse else "From progress.json")
    print(f"  Start Page: {start_page_info}")
    if not args.download:
        print(f"  Max Pages: {args.max_pages or 'Unlimited'}")
    print(f"  Log Level: {config.get('logging', {}).get('log_level', 'INFO')}")
    print("=" * 80)


def main() -> int:
    """Main entry point for the enhanced scraper."""
    # Check Python version...
    if sys.version_info < (3, 7):
        print("Python 3.7 or higher is required")
        sys.exit(1)
    
    # Parse command line arguments...
    parser = create_argument_parser()
    args = parser.parse_args()
    
    try:
        # Validate requirements...
        if not validate_requirements():
            print("Requirements validation failed")
            return 1
        
        # Initialize configuration manager...
        config_manager = ConfigManager(args.config)
        
        # Update config from command line arguments
        update_config_from_args(config_manager, args)
        
        # Setup logging...
        config = config_manager.load_config()
        setup_logging(config)
        logger = logging.getLogger(__name__)
        logger.info("Enhanced main scraper starting")
        
        # Set working directory info for debugging...
        print(f"Working Directory: {os.getcwd()}")
        print(f"Script Location: {Path(__file__).parent.absolute()}")
        
        # Determine operation mode and execute...
        if args.parse:
            # ENHANCED PARSER-ONLY MODE with Fresh Restart Detection...
            if not PARSER_MODE_AVAILABLE:
                print("Parser-only mode not available. Missing dependencies.")
                return 1
            
            print("Starting Enhanced Parser-Only Mode...")
            print("=" * 50)
            
            async def run_enhanced_parser():
                parser_mode = EnhancedParserOnlyMode(args.config)
                return await parser_mode.run_parsing(
                    start_page=args.start_page,  # May be None for auto-detection
                    batch_count=args.batch_count,
                    batch_size=args.batch_size
                )
            
            results = asyncio.run(run_enhanced_parser())
            
            print("=" * 50)
            print("PARSING PHASE COMPLETED")
            print("=" * 50)
            print(f"Requested Start Page: {args.start_page or 'Auto-detect'}")
            print(f"Actual Start Page: {results['actual_start_page']}")
            print(f"Batches processed: {results['batches_processed']}")
            print(f"Pages successful: {results['pages_successful']}")
            print(f"Total videos found: {results['total_videos_found']}")
            print(f"Success rate: {results['success_rate']:.1f}%")
            print(f"Total time: {results['total_time_seconds']:.1f}s")
            
            # Save results summary...
            results_file = f"parsing_results_{TimestampHelper.get_current_timestamp().replace(':', '-')}.json"
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            
            print(f"Results saved to: {results_file}")
            print(f"Run with --download to process the generated manifests")
            
            return 0 if results['batches_processed'] > 0 else 1
            
        elif args.download:
            # DOWNLOADER-ONLY MODE...
            if not DOWNLOADER_MODE_AVAILABLE:
                print("Downloader-only mode not available. Missing dependencies.")
                return 1
            
            print("Starting Downloader-Only Mode...")
            print("=" * 50)
            
            downloader_mode = DownloaderOnlyMode(
                max_workers=args.max_workers,
                max_retries=args.download_retries
            )
            results = downloader_mode.run_downloading(
                manifest_path=args.manifest,
                manifest_dir=args.manifest_dir
            )
            
            print("=" * 50)
            print("DOWNLOAD PHASE COMPLETED")
            print("=" * 50)
            if results["success"]:
                print(f"Manifests processed: {results.get('manifests_processed', 1)}")
                print(f"Total videos: {results['total_videos']}")
                print(f"Successful downloads: {results['successful_videos']}")
                print(f"Failed downloads: {results['failed_videos']}")
                print(f"Success rate: {results['success_rate']:.1f}%")
            else:
                print(f"Download failed: {results.get('error', 'Unknown error')}")
            
            return 0 if results["success"] else 1
            
        else:
            # FULL WORKFLOW MODE (Original)...
            print("Starting Full Workflow Mode...")
            print("=" * 50)
            
            # Display startup information...
            display_startup_info(config_manager, args)
            
            # Determine downloads directory...
            downloads_dir = args.downloads_dir or config.get("general", {}).get("download_path", "downloads")
            
            # Initialize orchestrator...
            orchestrator = ScrapeOrchestrator(
                config_file=args.config,
                progress_file=args.progress,
                downloads_dir=downloads_dir,
                dry_run=args.dry_run
            )
            logger.info(f"Orchestrator initialized (dry_run={args.dry_run})")
            
            # Display pre-run information...
            print("Pre-run Status Check:")
            state = orchestrator.get_current_state()
            progress_stats = state.get_progress_stats()
            print(f"  Current Page: {progress_stats.get_current_page('Unknown')}")
            print(f"  Downloaded Videos: {progress_stats.get_total_downloaded(0)}")
            print(f"  Total Size: {progress_stats.get_total_size_mb():.1f} MB")
            print(f"  Failed Videos: {progress_stats.get_failed_video_count(0)}")
            print(f"  Permanent Failed Pages: {progress_stats.get_permanent_failed_pages(0)}")
            print()
            print("Starting batch processing workflow...")
            print("Press Ctrl+C to stop gracefully at any time")
            print("-" * 80)
            
            # Run the orchestrator...
            results = orchestrator.run(
                start_page=args.start_page,
                max_pages=args.max_pages
            )
            
            # Display results...
            print("=" * 80)
            print("FINAL RESULTS")
            print("=" * 80)
            if results.get("success"):
                print("✅ Scraping completed successfully!")
            elif results.get("interrupted"):
                print("⚠️ Scraping interrupted by user")
            else:
                print("❌ Scraping failed")
                if "error" in results:
                    print(f"Error: {results['error']}")
            
            print()
            print("Statistics:")
            print(f"  Total Time: {results.get('total_time_seconds', 0):.1f}s")
            print(f"  Pages Processed: {results.get('pages_processed', 0)}")
            print(f"  Videos Found: {results.get('videos_found', 0)}")
            print(f"  Videos Enqueued: {results.get('videos_enqueued', 0)}")
            print(f"  Failed Pages: {results.get('failed_pages', 0)}")
            print(f"  Permanent Failures: {results.get('permanent_failed_pages', 0)}")
            
            if "enqueue_success_rate" in results:
                print(f"  Enqueue Success Rate: {results['enqueue_success_rate']:.1f}%")
            if "page_success_rate" in results:
                print(f"  Page Success Rate: {results['page_success_rate']:.1f}%")
            
            # Final progress stats...
            final_stats = results.get("final_progress_stats")
            if final_stats:
                print()
                print("Final Progress:")
                print(f"  Current Page: {final_stats.get_current_page('Unknown')}")
                print(f"  Total Downloaded: {final_stats.get_total_downloaded(0)}")
                print(f"  Total Size: {final_stats.get_total_size_mb():.1f} MB")
            
            print("=" * 80)
            
            # Provide next steps...
            if results.get("success"):
                print("Next Steps:")
                print("✓ Check IDM for any remaining downloads")
                print("✓ Verify completed videos in downloads folder")
                print("✓ Run again to continue from current position")
                print("✓ Or try new parser/downloader modes:")
                print("  - python main_scraper.py --parse --batch-count 5")
                print("  - python main_scraper.py --download --manifest-dir manifests")
            else:
                print("Troubleshooting:")
                print("• Check logs for error details")
                print("• Verify IDM is installed and accessible")
                print("• Check network connectivity")
                print("• Try running with --dry-run to test configuration")
                print("• Use --help for additional command line options")
            
            logger.info("Main scraper completed")
            return 0 if results.get("success") else 1
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        logging.info("Main scraper interrupted by user")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        logging.error(f"Unexpected error in main: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    """Entry point when run directly."""
    
    print(f"Parser-Only Mode: {'Available' if PARSER_MODE_AVAILABLE else 'Not Available'}")
    print(f"Downloader-Only Mode: {'Available' if DOWNLOADER_MODE_AVAILABLE else 'Not Available'}")
    print(f"Full Workflow Mode: Available")
    print("=" * 80)
    
    exit_code = main()
    print("Enhanced Web Parser finished")
    sys.exit(exit_code)