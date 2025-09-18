#!/usr/bin/env python3
"""
File Validator Module

Validates video folder contents to ensure complete downloads.
Checks for presence of .json, .jpg, and .mp4 files.

Author: AI Assistant
Version: 1.0
"""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class FileValidator:
    """Validates video folder contents for completeness."""

    def __init__(self, min_video_size: int = 1024, min_thumbnail_size: int = 100):
        """
        Initialize file validator.

        Args:
            min_video_size: Minimum video file size in bytes
            min_thumbnail_size: Minimum thumbnail file size in bytes
        """
        self.min_video_size = min_video_size
        self.min_thumbnail_size = min_thumbnail_size

    def validate_video_folder(self, folder_path: str) -> Dict[str, Any]:
        """
        Validate a video folder for completeness.

        Args:
            folder_path: Path to video folder

        Returns:
            Dictionary with validation results:
            {
                "valid": bool,
                "missing_files": List[str],
                "found_files": Dict[str, str],
                "errors": List[str]
            }
        """
        folder = Path(folder_path)
        result = {
            "valid": False,
            "missing_files": [],
            "found_files": {},
            "errors": []
        }

        if not folder.exists():
            result["errors"].append(f"Folder does not exist: {folder_path}")
            return result

        if not folder.is_dir():
            result["errors"].append(f"Path is not a directory: {folder_path}")
            return result

        try:
            # Required file extensions
            required_extensions = ['.json', '.jpg', '.mp4']

            # Get all files in the folder
            all_files = list(folder.glob('*'))

            # Check for each required file type
            for ext in required_extensions:
                matching_files = [f for f in all_files if f.suffix.lower() == ext]

                if not matching_files:
                    result["missing_files"].append(ext)
                    logger.debug(f"Missing {ext} file in {folder_path}")
                else:
                    # Use the first matching file
                    file_path = matching_files[0]

                    # Check file size constraints
                    if ext == '.mp4' and file_path.stat().st_size < self.min_video_size:
                        result["errors"].append(f"Video file too small: {file_path.stat().st_size} bytes")
                        result["missing_files"].append(ext)
                    elif ext == '.jpg' and file_path.stat().st_size < self.min_thumbnail_size:
                        result["errors"].append(f"Thumbnail file too small: {file_path.stat().st_size} bytes")
                        result["missing_files"].append(ext)
                    else:
                        result["found_files"][ext] = str(file_path)

            # Check if validation passed
            result["valid"] = len(result["missing_files"]) == 0

            logger.debug(f"Validation result for {folder_path}: valid={result['valid']}, "
                        f"missing={result['missing_files']}")

        except Exception as e:
            error_msg = f"Error validating folder {folder_path}: {e}"
            logger.error(error_msg)
            result["errors"].append(error_msg)

        return result

    def validate_multiple_folders(self, folder_paths: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Validate multiple video folders.

        Args:
            folder_paths: List of folder paths to validate

        Returns:
            Dictionary mapping folder paths to validation results
        """
        results = {}

        for folder_path in folder_paths:
            results[folder_path] = self.validate_video_folder(folder_path)

        return results

    def get_missing_files_for_page(self, page_folder: str, video_ids: List[str]) -> List[str]:
        """
        Get list of video IDs with missing files for a page.

        Args:
            page_folder: Base page folder path
            video_ids: List of video IDs to check

        Returns:
            List of video IDs that have missing files
        """
        missing_videos = []
        page_path = Path(page_folder)

        for video_id in video_ids:
            video_folder = page_path / video_id
            validation = self.validate_video_folder(str(video_folder))

            if not validation["valid"]:
                missing_videos.append(video_id)
                logger.debug(f"Video {video_id} has missing files: {validation['missing_files']}")

        return missing_videos

    def get_validation_summary(self, base_download_dir: str) -> Dict[str, Any]:
        """
        Get validation summary for entire download directory.

        Args:
            base_download_dir: Base download directory

        Returns:
            Summary of validation results
        """
        base_dir = Path(base_download_dir)
        summary = {
            "total_folders": 0,
            "valid_folders": 0,
            "invalid_folders": 0,
            "missing_folders": 0,
            "validation_details": {}
        }

        if not base_dir.exists():
            summary["error"] = f"Base directory does not exist: {base_download_dir}"
            return summary

        try:
            # Find all page directories
            page_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith('page_')]

            for page_dir in page_dirs:
                video_dirs = [d for d in page_dir.iterdir() if d.is_dir()]

                for video_dir in video_dirs:
                    summary["total_folders"] += 1
                    validation = self.validate_video_folder(str(video_dir))

                    if validation["valid"]:
                        summary["valid_folders"] += 1
                    else:
                        summary["invalid_folders"] += 1

                    summary["validation_details"][str(video_dir)] = validation

        except Exception as e:
            summary["error"] = f"Error during validation summary: {e}"
            logger.error(summary["error"])

        return summary

    def find_corrupted_downloads(self, base_download_dir: str) -> List[str]:
        """
        Find video folders with corrupted or incomplete downloads.

        Args:
            base_download_dir: Base download directory

        Returns:
            List of paths to corrupted video folders
        """
        corrupted_folders = []
        base_dir = Path(base_download_dir)

        if not base_dir.exists():
            logger.warning(f"Download directory does not exist: {base_download_dir}")
            return corrupted_folders

        try:
            # Find all video directories
            for page_dir in base_dir.glob('page_*'):
                if page_dir.is_dir():
                    for video_dir in page_dir.iterdir():
                        if video_dir.is_dir():
                            validation = self.validate_video_folder(str(video_dir))
                            if not validation["valid"]:
                                corrupted_folders.append(str(video_dir))

        except Exception as e:
            logger.error(f"Error finding corrupted downloads: {e}")

        return corrupted_folders

    def cleanup_empty_folders(self, base_download_dir: str) -> int:
        """
        Remove empty folders from download directory.

        Args:
            base_download_dir: Base download directory

        Returns:
            Number of folders removed
        """
        removed_count = 0
        base_dir = Path(base_download_dir)

        if not base_dir.exists():
            return removed_count

        try:
            # Remove empty video folders first
            for page_dir in base_dir.glob('page_*'):
                if page_dir.is_dir():
                    for video_dir in list(page_dir.iterdir()):
                        if video_dir.is_dir():
                            try:
                                # Check if folder is empty
                                if not any(video_dir.iterdir()):
                                    video_dir.rmdir()
                                    removed_count += 1
                                    logger.info(f"Removed empty video folder: {video_dir}")
                            except OSError:
                                pass  # Folder not empty or permission issue

                    # Remove empty page folders
                    try:
                        if not any(page_dir.iterdir()):
                            page_dir.rmdir()
                            removed_count += 1
                            logger.info(f"Removed empty page folder: {page_dir}")
                    except OSError:
                        pass

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

        return removed_count


if __name__ == "__main__":
    # Demo usage
    import logging
    logging.basicConfig(level=logging.INFO)

    validator = FileValidator()

    # Test validation
    test_folder = "downloads/page_1000/test_video"
    result = validator.validate_video_folder(test_folder)
    print(f"Validation result: {result}")

    # Test summary
    summary = validator.get_validation_summary("downloads")
    print(f"Validation summary: {summary}")
