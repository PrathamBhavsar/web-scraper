#!/usr/bin/env python3

"""
page_parser.py - Refactored for Parser/Downloader Separation

Handles parsing of individual pages and extracting video metadata ONLY.
No longer attempts to download media files - only collects JSON metadata
and media URLs for manifest files.

Key Changes:
- Strictly metadata parsing only
- No media download attempts
- Enhanced structured logging
- Manifest-ready output format
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

# Import existing video data parser (assumed to exist)
try:
    from video_data_parser import OptimizedVideoDataParser
except ImportError:
    # Fallback for demonstration
    class OptimizedVideoDataParser:
        def __init__(self, base_url):
            self.base_url = base_url
        
        async def extract_video_urls(self):
            # Mock implementation
            return ["https://example.com/video1", "https://example.com/video2"]
        
        async def parse_single_video(self, url):
            # Mock implementation
            video_id = url.split("/")[-1]
            return {
                "video_id": video_id,
                "title": f"Sample Video {video_id}",
                "duration": "05:30",
                "thumbnail_src": f"https://cdn.example.com/thumb_{video_id}.jpg",
                "video_src": f"https://cdn.example.com/video_{video_id}.mp4",
                "upload_date": "2025-09-19",
                "tags": ["tag1", "tag2"]
            }

from utils import SafeFileOperations, TimestampHelper

# Setup structured logging
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage()
        }
        
        # Add structured fields
        for attr in ['event', 'page', 'video_id', 'video_count', 'parse_time']:
            if hasattr(record, attr):
                log_entry[attr] = getattr(record, attr)
                
        return json.dumps(log_entry)

logger = logging.getLogger(__name__)

# Add structured logging handler
if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
    file_handler = logging.FileHandler("page_parser_structured.log", encoding='utf-8')
    file_handler.setFormatter(StructuredFormatter())
    logger.addHandler(file_handler)


@dataclass
class VideoMetadata:
    """Container for video metadata - enhanced for manifest generation."""
    video_id: str
    title: str
    duration: str
    thumbnail_url: str
    video_url: str
    upload_date: str
    tags: List[str]
    page_url: str
    folder_path: str
    
    # Additional fields for manifest
    page_number: int = 0
    parsed_at: str = ""
    
    def to_manifest_entry(self) -> Dict[str, Any]:
        """Convert to manifest entry format."""
        return {
            "video_id": self.video_id,
            "page": self.page_number,
            "mp4_url": self.video_url,
            "jpg_url": self.thumbnail_url,
            "target_folder": self.folder_path,
            "metadata": {
                "title": self.title,
                "duration": self.duration,
                "upload_date": self.upload_date,
                "tags": self.tags,
                "page_url": self.page_url,
                "parsed_at": self.parsed_at
            }
        }


@dataclass
class PageParseResult:
    """Container for page parsing results - enhanced with timing and error details."""
    page_number: int
    success: bool
    video_count: int
    videos: List[VideoMetadata]
    errors: List[str]
    parse_time_seconds: float
    page_url: str
    
    # Additional metrics
    videos_with_media_urls: int = 0
    metadata_save_failures: int = 0
    
    def get_success_rate(self) -> float:
        """Get percentage of videos that were successfully parsed."""
        if self.video_count == 0:
            return 0.0
        return (len(self.videos) / self.video_count) * 100


class PageParser:
    """Handles parsing of individual pages for video metadata ONLY."""
    
    def __init__(self, base_url: str = "https://rule34video.com", 
                 downloads_dir: str = "downloads"):
        """
        Initialize page parser.
        
        Args:
            base_url: Base URL for the site
            downloads_dir: Base directory for downloads (metadata storage only)
        """
        self.base_url = base_url
        self.downloads_dir = Path(downloads_dir)
        self.parser = OptimizedVideoDataParser(base_url)
        
        logger.info("PageParser initialized", extra={
            "event": "parser_init",
            "base_url": base_url,
            "downloads_dir": downloads_dir
        })

    def _get_page_url(self, page_number: int) -> str:
        """Get URL for a specific page."""
        return f"{self.base_url}/latest-updates/{page_number}"

    def _get_page_folder(self, page_number: int) -> Path:
        """Get folder path for a page."""
        return self.downloads_dir / f"page_{page_number}"

    def _get_video_folder(self, page_number: int, video_id: str) -> Path:
        """Get folder path for a specific video."""
        return self._get_page_folder(page_number) / video_id

    async def parse_page(self, page_number: int, save_metadata: bool = True) -> PageParseResult:
        """
        Parse a single page and extract video metadata ONLY.
        
        NO MEDIA DOWNLOADING - only JSON metadata collection.
        
        Args:
            page_number: Page number to parse
            save_metadata: Whether to save metadata.json files to disk
            
        Returns:
            PageParseResult with parsing results
        """
        start_time = asyncio.get_event_loop().time()
        page_url = self._get_page_url(page_number)
        
        logger.info("Starting page parse", extra={
            "event": "page_parse_start",
            "page": page_number,
            "url": page_url
        })

        result = PageParseResult(
            page_number=page_number,
            success=False,
            video_count=0,
            videos=[],
            errors=[],
            parse_time_seconds=0.0,
            page_url=page_url
        )

        try:
            # Phase 1: Extract video URLs from the page
            logger.debug("Extracting video URLs", extra={
                "event": "extract_urls_start", 
                "page": page_number
            })
            
            video_urls = await self.parser.extract_video_urls()
            result.video_count = len(video_urls)
            
            logger.info("Video URLs extracted", extra={
                "event": "extract_urls_complete",
                "page": page_number,
                "video_count": len(video_urls)
            })

            if not video_urls:
                result.errors.append("No video URLs found on page")
                logger.warning("No video URLs found", extra={
                    "event": "no_videos_found",
                    "page": page_number
                })
                return result

            # Phase 2: Parse each video for metadata ONLY
            videos = []
            videos_with_media = 0
            
            for i, video_url in enumerate(video_urls):
                try:
                    logger.debug("Parsing video metadata", extra={
                        "event": "video_parse_start",
                        "page": page_number,
                        "video_index": i + 1,
                        "video_url": video_url
                    })

                    # Parse video metadata
                    video_data = await self.parser.parse_single_video(video_url)

                    if video_data and video_data.get("video_id"):
                        # Create video metadata object
                        video_metadata = VideoMetadata(
                            video_id=video_data["video_id"],
                            title=video_data.get("title", "Unknown Title"),
                            duration=video_data.get("duration", "00:00"),
                            thumbnail_url=video_data.get("thumbnail_src", ""),
                            video_url=video_data.get("video_src", ""),
                            upload_date=video_data.get("upload_date", ""),
                            tags=video_data.get("tags", []),
                            page_url=video_url,
                            folder_path=str(self._get_video_folder(page_number, video_data["video_id"])),
                            page_number=page_number,
                            parsed_at=TimestampHelper.get_current_timestamp()
                        )

                        videos.append(video_metadata)

                        # Check if video has media URLs
                        if video_metadata.video_url and video_metadata.thumbnail_url:
                            videos_with_media += 1

                        # Save metadata to disk if requested (JSON ONLY)
                        if save_metadata:
                            success = await self._save_video_metadata(
                                page_number, video_metadata, video_data
                            )
                            if not success:
                                result.metadata_save_failures += 1

                        logger.debug("Video parsed successfully", extra={
                            "event": "video_parse_success",
                            "page": page_number,
                            "video_id": video_data["video_id"],
                            "has_media_urls": bool(video_metadata.video_url and video_metadata.thumbnail_url)
                        })
                    else:
                        error_msg = f"Failed to extract video_id from {video_url}"
                        result.errors.append(error_msg)
                        logger.warning("Video parsing failed", extra={
                            "event": "video_parse_failed",
                            "page": page_number,
                            "video_url": video_url,
                            "reason": "no_video_id"
                        })

                except Exception as e:
                    error_msg = f"Error parsing video {video_url}: {e}"
                    result.errors.append(error_msg)
                    logger.error("Video parsing exception", extra={
                        "event": "video_parse_exception",
                        "page": page_number,
                        "video_url": video_url,
                        "error": str(e)
                    })

            # Update result
            result.videos = videos
            result.videos_with_media_urls = videos_with_media
            result.success = len(videos) > 0

            if result.success:
                logger.info("Page parsing completed successfully", extra={
                    "event": "page_parse_success",
                    "page": page_number,
                    "videos_found": len(videos),
                    "videos_with_media": videos_with_media,
                    "metadata_failures": result.metadata_save_failures
                })
            else:
                logger.warning("Page parsing completed with no videos", extra={
                    "event": "page_parse_no_videos",
                    "page": page_number,
                    "errors": len(result.errors)
                })

        except Exception as e:
            error_msg = f"Critical error parsing page {page_number}: {e}"
            result.errors.append(error_msg)
            logger.error("Page parsing critical error", extra={
                "event": "page_parse_critical_error",
                "page": page_number,
                "error": str(e)
            })

        finally:
            end_time = asyncio.get_event_loop().time()
            result.parse_time_seconds = end_time - start_time
            
            logger.info("Page parsing completed", extra={
                "event": "page_parse_complete",
                "page": page_number,
                "success": result.success,
                "video_count": len(result.videos),
                "parse_time": result.parse_time_seconds,
                "success_rate": result.get_success_rate()
            })

        return result

    async def _save_video_metadata(self, page_number: int, video_metadata: VideoMetadata, 
                                 raw_data: Dict[str, Any]) -> bool:
        """
        Save video metadata to disk (JSON ONLY - no media files).
        
        Args:
            page_number: Page number
            video_metadata: Video metadata object
            raw_data: Raw video data from parser
            
        Returns:
            True if saved successfully
        """
        try:
            video_folder = self._get_video_folder(page_number, video_metadata.video_id)

            # Create video folder
            if not SafeFileOperations.safe_create_folder(str(video_folder)):
                logger.error("Failed to create video folder", extra={
                    "event": "folder_create_failed",
                    "page": page_number,
                    "video_id": video_metadata.video_id,
                    "folder": str(video_folder)
                })
                return False

            # Save JSON metadata ONLY (filename: metadata.json)
            json_file = video_folder / "metadata.json"

            # Prepare comprehensive metadata for saving
            metadata_to_save = {
                **raw_data,
                "parsed_at": video_metadata.parsed_at,
                "page_number": page_number,
                "folder_path": str(video_folder),
                "parser_version": "2.0_refactored",
                "phase": "parsing_only"  # Indicates this is from parsing phase
            }

            try:
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata_to_save, f, indent=2, ensure_ascii=False)
                
                logger.debug("Metadata saved successfully", extra={
                    "event": "metadata_save_success",
                    "page": page_number,
                    "video_id": video_metadata.video_id,
                    "file": str(json_file)
                })
                return True

            except Exception as e:
                logger.error("Failed to write metadata file", extra={
                    "event": "metadata_save_failed",
                    "page": page_number,
                    "video_id": video_metadata.video_id,
                    "file": str(json_file),
                    "error": str(e)
                })
                return False

        except Exception as e:
            logger.error("Exception in metadata save", extra={
                "event": "metadata_save_exception",
                "page": page_number,
                "video_id": video_metadata.video_id,
                "error": str(e)
            })
            return False

    def get_videos_for_page(self, page_number: int) -> List[VideoMetadata]:
        """
        Get all video metadata for a specific page from disk.
        
        Args:
            page_number: Page number
            
        Returns:
            List of VideoMetadata objects
        """
        videos = []
        page_folder = self._get_page_folder(page_number)
        
        if not page_folder.exists():
            logger.debug("Page folder does not exist", extra={
                "event": "page_folder_missing",
                "page": page_number,
                "folder": str(page_folder)
            })
            return videos

        try:
            for video_dir in page_folder.iterdir():
                if video_dir.is_dir():
                    metadata_file = video_dir / "metadata.json"
                    if metadata_file.exists():
                        try:
                            with open(metadata_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            
                            # Create VideoMetadata object
                            video_metadata = VideoMetadata(
                                video_id=data.get("video_id", ""),
                                title=data.get("title", "Unknown Title"),
                                duration=data.get("duration", "00:00"),
                                thumbnail_url=data.get("thumbnail_src", ""),
                                video_url=data.get("video_src", ""),
                                upload_date=data.get("upload_date", ""),
                                tags=data.get("tags", []),
                                page_url=data.get("page_url", ""),
                                folder_path=str(video_dir),
                                page_number=page_number,
                                parsed_at=data.get("parsed_at", "")
                            )
                            
                            videos.append(video_metadata)
                            
                        except Exception as e:
                            logger.error("Failed to load video metadata", extra={
                                "event": "metadata_load_failed",
                                "page": page_number,
                                "video_dir": str(video_dir),
                                "error": str(e)
                            })

            logger.debug("Loaded videos from page folder", extra={
                "event": "videos_loaded_from_disk",
                "page": page_number,
                "video_count": len(videos)
            })

        except Exception as e:
            logger.error("Error loading videos for page", extra={
                "event": "page_videos_load_error",
                "page": page_number,
                "error": str(e)
            })

        return videos


if __name__ == "__main__":
    # Demo usage for testing
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    async def demo():
        parser = PageParser()
        
        # Test parsing a page
        result = await parser.parse_page(1000, save_metadata=True)
        
        print(f"\nPage Parse Results:")
        print(f"Success: {result.success}")
        print(f"Videos found: {len(result.videos)}")
        print(f"Videos with media URLs: {result.videos_with_media_urls}")
        print(f"Parse time: {result.parse_time_seconds:.2f}s")
        print(f"Success rate: {result.get_success_rate():.1f}%")
        print(f"Errors: {len(result.errors)}")
        
        if result.errors:
            print("First few errors:")
            for error in result.errors[:3]:
                print(f"  - {error}")
    
    if __name__ == "__main__":
        asyncio.run(demo())