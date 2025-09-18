#!/usr/bin/env python3
"""
Page Parser Module

Handles parsing of individual pages and extracting video metadata.
Integrates with existing video_data_parser for actual content extraction.

Author: AI Assistant
Version: 1.0
"""

import asyncio
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass

from video_data_parser import OptimizedVideoDataParser
from utils import SafeFileOperations, TimestampHelper

logger = logging.getLogger(__name__)


@dataclass
class VideoMetadata:
    """Container for video metadata."""
    video_id: str
    title: str
    duration: str
    thumbnail_url: str
    video_url: str
    upload_date: str
    tags: List[str]
    page_url: str
    folder_path: str


@dataclass
class PageParseResult:
    """Container for page parsing results."""
    page_number: int
    success: bool
    video_count: int
    videos: List[VideoMetadata]
    errors: List[str]
    parse_time_seconds: float
    page_url: str


class PageParser:
    """Handles parsing of individual pages for video metadata."""

    def __init__(self, base_url: str = "https://rule34video.com", 
                 downloads_dir: str = "downloads"):
        """
        Initialize page parser.

        Args:
            base_url: Base URL for the site
            downloads_dir: Base directory for downloads
        """
        self.base_url = base_url
        self.downloads_dir = Path(downloads_dir)
        self.parser = OptimizedVideoDataParser(base_url)

    def _get_page_url(self, page_number: int) -> str:
        """
        Get URL for a specific page.

        Args:
            page_number: Page number to parse

        Returns:
            URL for the page
        """
        return f"{self.base_url}/latest-updates/{page_number}"

    def _get_page_folder(self, page_number: int) -> Path:
        """
        Get folder path for a page.

        Args:
            page_number: Page number

        Returns:
            Path object for page folder
        """
        return self.downloads_dir / f"page_{page_number}"

    def _get_video_folder(self, page_number: int, video_id: str) -> Path:
        """
        Get folder path for a specific video.

        Args:
            page_number: Page number
            video_id: Video ID

        Returns:
            Path object for video folder
        """
        return self._get_page_folder(page_number) / video_id

    async def parse_page(self, page_number: int, 
                        save_metadata: bool = True) -> PageParseResult:
        """
        Parse a single page and extract video metadata.

        Args:
            page_number: Page number to parse
            save_metadata: Whether to save metadata files to disk

        Returns:
            PageParseResult with parsing results
        """
        start_time = asyncio.get_event_loop().time()
        page_url = self._get_page_url(page_number)

        logger.info(f"Starting parse of page {page_number}: {page_url}")

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
            # Extract video URLs from the page
            video_urls = await self.parser.extract_video_urls()
            logger.info(f"Found {len(video_urls)} video URLs on page {page_number}")

            if not video_urls:
                result.errors.append("No video URLs found on page")
                logger.warning(f"No videos found on page {page_number}")
                return result

            # Parse each video
            videos = []
            for i, video_url in enumerate(video_urls):
                try:
                    logger.debug(f"Parsing video {i+1}/{len(video_urls)}: {video_url}")

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
                            folder_path=str(self._get_video_folder(page_number, video_data["video_id"]))
                        )

                        videos.append(video_metadata)

                        # Save metadata to disk if requested
                        if save_metadata:
                            await self._save_video_metadata(page_number, video_metadata, video_data)

                        logger.debug(f"Successfully parsed video: {video_data['video_id']}")

                    else:
                        error_msg = f"Failed to parse video metadata from {video_url}"
                        result.errors.append(error_msg)
                        logger.warning(error_msg)

                except Exception as e:
                    error_msg = f"Error parsing video {video_url}: {e}"
                    result.errors.append(error_msg)
                    logger.error(error_msg)

            # Update result
            result.videos = videos
            result.video_count = len(videos)
            result.success = len(videos) > 0

            if result.success:
                logger.info(f"Successfully parsed page {page_number}: {len(videos)} videos")
            else:
                logger.warning(f"Page {page_number} parse completed but no videos extracted")

        except Exception as e:
            error_msg = f"Error parsing page {page_number}: {e}"
            result.errors.append(error_msg)
            logger.error(error_msg)

        finally:
            end_time = asyncio.get_event_loop().time()
            result.parse_time_seconds = end_time - start_time
            logger.info(f"Page {page_number} parse completed in {result.parse_time_seconds:.2f}s")

        return result

    async def _save_video_metadata(self, page_number: int, video_metadata: VideoMetadata,
                                 raw_data: Dict[str, Any]) -> bool:
        """
        Save video metadata to disk.

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
                logger.error(f"Failed to create folder: {video_folder}")
                return False

            # Save JSON metadata
            json_file = video_folder / f"{video_metadata.video_id}.json"

            # Prepare metadata for saving
            metadata_to_save = {
                **raw_data,
                "parsed_at": TimestampHelper.get_current_timestamp(),
                "page_number": page_number,
                "folder_path": str(video_folder)
            }

            try:
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata_to_save, f, indent=2, ensure_ascii=False)

                logger.debug(f"Saved metadata: {json_file}")
                return True

            except Exception as e:
                logger.error(f"Error saving metadata to {json_file}: {e}")
                return False

        except Exception as e:
            logger.error(f"Error in _save_video_metadata: {e}")
            return False

    def get_video_metadata_from_folder(self, video_folder: str) -> Optional[VideoMetadata]:
        """
        Load video metadata from a folder's JSON file.

        Args:
            video_folder: Path to video folder

        Returns:
            VideoMetadata object or None if not found/invalid
        """
        try:
            folder_path = Path(video_folder)

            # Find JSON file
            json_files = list(folder_path.glob("*.json"))
            if not json_files:
                logger.debug(f"No JSON file found in {video_folder}")
                return None

            json_file = json_files[0]  # Use first JSON file found

            with open(json_file, 'r', encoding='utf-8') as f:
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
                folder_path=video_folder
            )

            return video_metadata

        except Exception as e:
            logger.error(f"Error loading metadata from {video_folder}: {e}")
            return None

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
            logger.debug(f"Page folder does not exist: {page_folder}")
            return videos

        try:
            for video_dir in page_folder.iterdir():
                if video_dir.is_dir():
                    video_metadata = self.get_video_metadata_from_folder(str(video_dir))
                    if video_metadata:
                        videos.append(video_metadata)

            logger.debug(f"Loaded {len(videos)} videos for page {page_number}")

        except Exception as e:
            logger.error(f"Error loading videos for page {page_number}: {e}")

        return videos

    def get_page_statistics(self, page_number: int) -> Dict[str, Any]:
        """
        Get statistics for a specific page.

        Args:
            page_number: Page number

        Returns:
            Dictionary with page statistics
        """
        page_folder = self._get_page_folder(page_number)

        stats = {
            "page_number": page_number,
            "page_folder": str(page_folder),
            "exists": page_folder.exists(),
            "video_count": 0,
            "total_size_mb": 0.0,
            "videos_with_json": 0,
            "videos_with_thumbnail": 0,
            "videos_with_video": 0,
            "complete_videos": 0
        }

        if not page_folder.exists():
            return stats

        try:
            from utils import calculate_folder_size
            stats["total_size_mb"] = calculate_folder_size(str(page_folder))

            for video_dir in page_folder.iterdir():
                if video_dir.is_dir():
                    stats["video_count"] += 1

                    # Check for required files
                    has_json = any(video_dir.glob("*.json"))
                    has_thumbnail = any(video_dir.glob("*.jpg"))
                    has_video = any(video_dir.glob("*.mp4"))

                    if has_json:
                        stats["videos_with_json"] += 1
                    if has_thumbnail:
                        stats["videos_with_thumbnail"] += 1
                    if has_video:
                        stats["videos_with_video"] += 1

                    if has_json and has_thumbnail and has_video:
                        stats["complete_videos"] += 1

        except Exception as e:
            logger.error(f"Error calculating page statistics: {e}")

        return stats


if __name__ == "__main__":
    # Demo usage
    import logging
    logging.basicConfig(level=logging.INFO)

    async def demo():
        parser = PageParser()

        # Parse a page (this would normally parse a real page)
        result = await parser.parse_page(1000, save_metadata=False)

        print(f"Parse result for page 1000:")
        print(f"  Success: {result.success}")
        print(f"  Video count: {result.video_count}")
        print(f"  Parse time: {result.parse_time_seconds:.2f}s")
        print(f"  Errors: {len(result.errors)}")

        # Get page statistics
        stats = parser.get_page_statistics(1000)
        print(f"Page 1000 statistics: {stats}")

    # Run demo
    if __name__ == "__main__":
        asyncio.run(demo())
