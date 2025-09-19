#!/usr/bin/env python3

"""
validator.py - Enhanced Validator with Comprehensive Logging

Validates video folder contents to ensure complete downloads.
Enhanced with detailed structured logging and specific file validation.

Key improvements:
- Structured JSON logging for all validation events
- Per-file validation details
- Batch validation capabilities
- Progress tracking integration
"""

import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import sys

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
        for attr in ['event', 'video_id', 'folder_path', 'missing_files', 'validation_result']:
            if hasattr(record, attr):
                log_entry[attr] = getattr(record, attr)
                
        return json.dumps(log_entry)

logger = logging.getLogger(__name__)

# Add structured logging handler if not already present
if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
    file_handler = logging.FileHandler("validator_structured.log", encoding='utf-8')
    file_handler.setFormatter(StructuredFormatter())
    logger.addHandler(file_handler)


class FileValidator:
    """Enhanced validator with comprehensive logging and detailed validation."""
    
    def __init__(self, min_video_size: int = 1024, min_thumbnail_size: int = 100):
        """
        Initialize file validator with size constraints.
        
        Args:
            min_video_size: Minimum video file size in bytes
            min_thumbnail_size: Minimum thumbnail file size in bytes
        """
        self.min_video_size = min_video_size
        self.min_thumbnail_size = min_thumbnail_size
        
        logger.info("FileValidator initialized", extra={
            "event": "validator_init",
            "min_video_size": min_video_size,
            "min_thumbnail_size": min_thumbnail_size
        })

    def validate_video_folder(self, folder_path: str) -> Dict[str, Any]:
        """
        Validate a video folder for completeness with detailed logging.
        
        Args:
            folder_path: Path to video folder
            
        Returns:
            Dictionary with validation results:
            {
                "valid": bool,
                "missing_files": List[str],
                "found_files": Dict[str, str],
                "file_sizes": Dict[str, int],
                "errors": List[str],
                "video_id": str
            }
        """
        folder = Path(folder_path)
        video_id = folder.name  # Extract video ID from folder name
        
        result = {
            "valid": False,
            "missing_files": [],
            "found_files": {},
            "file_sizes": {},
            "errors": [],
            "video_id": video_id,
            "folder_path": str(folder)
        }

        logger.debug("Starting folder validation", extra={
            "event": "validation_start",
            "video_id": video_id,
            "folder_path": str(folder)
        })

        # Basic folder checks
        if not folder.exists():
            error_msg = f"Folder does not exist: {folder_path}"
            result["errors"].append(error_msg)
            logger.warning("Folder does not exist", extra={
                "event": "folder_not_found",
                "video_id": video_id,
                "folder_path": str(folder)
            })
            return result

        if not folder.is_dir():
            error_msg = f"Path is not a directory: {folder_path}"
            result["errors"].append(error_msg)
            logger.error("Path is not a directory", extra={
                "event": "path_not_directory",
                "video_id": video_id,
                "folder_path": str(folder)
            })
            return result

        try:
            # Required file extensions
            required_extensions = {
                '.json': 'metadata.json',
                '.jpg': 'thumbnail',
                '.mp4': 'video'
            }

            # Get all files in the folder
            all_files = list(folder.glob('*'))
            
            logger.debug("Files found in folder", extra={
                "event": "files_enumerated",
                "video_id": video_id,
                "file_count": len(all_files),
                "files": [f.name for f in all_files]
            })

            # Check for each required file type
            for ext, file_type in required_extensions.items():
                matching_files = [f for f in all_files if f.suffix.lower() == ext]
                
                if not matching_files:
                    result["missing_files"].append(ext)
                    logger.debug("Missing file type", extra={
                        "event": "file_type_missing",
                        "video_id": video_id,
                        "file_type": file_type,
                        "extension": ext
                    })
                else:
                    # Use the first matching file
                    file_path = matching_files[0]
                    file_size = file_path.stat().st_size
                    result["file_sizes"][ext] = file_size

                    # Validate file size constraints
                    size_valid = True
                    if ext == '.mp4' and file_size < self.min_video_size:
                        error_msg = f"Video file too small: {file_size} bytes (min: {self.min_video_size})"
                        result["errors"].append(error_msg)
                        result["missing_files"].append(ext)
                        size_valid = False
                        
                        logger.warning("Video file too small", extra={
                            "event": "file_size_invalid",
                            "video_id": video_id,
                            "file_type": "video",
                            "actual_size": file_size,
                            "min_size": self.min_video_size
                        })
                        
                    elif ext == '.jpg' and file_size < self.min_thumbnail_size:
                        error_msg = f"Thumbnail file too small: {file_size} bytes (min: {self.min_thumbnail_size})"
                        result["errors"].append(error_msg)
                        result["missing_files"].append(ext)
                        size_valid = False
                        
                        logger.warning("Thumbnail file too small", extra={
                            "event": "file_size_invalid",
                            "video_id": video_id,
                            "file_type": "thumbnail",
                            "actual_size": file_size,
                            "min_size": self.min_thumbnail_size
                        })

                    if size_valid:
                        result["found_files"][ext] = str(file_path)
                        logger.debug("File validated successfully", extra={
                            "event": "file_valid",
                            "video_id": video_id,
                            "file_type": file_type,
                            "file_path": str(file_path),
                            "file_size": file_size
                        })

            # Check if validation passed
            result["valid"] = len(result["missing_files"]) == 0

            # Log final validation result
            logger.info("Validation completed", extra={
                "event": "validation_complete",
                "video_id": video_id,
                "validation_result": "valid" if result["valid"] else "invalid",
                "missing_files": result["missing_files"],
                "error_count": len(result["errors"])
            })

        except Exception as e:
            error_msg = f"Error validating folder {folder_path}: {e}"
            logger.error("Validation exception", extra={
                "event": "validation_exception",
                "video_id": video_id,
                "folder_path": str(folder),
                "error": str(e)
            })
            result["errors"].append(error_msg)

        return result

    def validate_multiple_folders(self, folder_paths: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Validate multiple video folders with batch logging.
        
        Args:
            folder_paths: List of folder paths to validate
            
        Returns:
            Dictionary mapping folder paths to validation results
        """
        logger.info("Starting batch validation", extra={
            "event": "batch_validation_start",
            "folder_count": len(folder_paths)
        })
        
        results = {}
        valid_count = 0
        
        for i, folder_path in enumerate(folder_paths):
            logger.debug("Validating folder in batch", extra={
                "event": "batch_folder_validation",
                "folder_index": i + 1,
                "total_folders": len(folder_paths),
                "folder_path": folder_path
            })
            
            validation_result = self.validate_video_folder(folder_path)
            results[folder_path] = validation_result
            
            if validation_result["valid"]:
                valid_count += 1

        success_rate = (valid_count / len(folder_paths) * 100) if folder_paths else 0
        
        logger.info("Batch validation completed", extra={
            "event": "batch_validation_complete",
            "total_folders": len(folder_paths),
            "valid_folders": valid_count,
            "invalid_folders": len(folder_paths) - valid_count,
            "success_rate": success_rate
        })

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
        page_path = Path(page_folder)
        page_number = page_path.name.replace("page_", "") if page_path.name.startswith("page_") else "unknown"
        
        logger.info("Starting page validation", extra={
            "event": "page_validation_start",
            "page": page_number,
            "page_folder": page_folder,
            "video_count": len(video_ids)
        })
        
        missing_videos = []
        
        for video_id in video_ids:
            video_folder = page_path / video_id
            validation = self.validate_video_folder(str(video_folder))
            
            if not validation["valid"]:
                missing_videos.append(video_id)
                logger.debug("Video has missing files", extra={
                    "event": "video_missing_files",
                    "page": page_number,
                    "video_id": video_id,
                    "missing_files": validation["missing_files"]
                })

        logger.info("Page validation completed", extra={
            "event": "page_validation_complete",
            "page": page_number,
            "total_videos": len(video_ids),
            "missing_videos": len(missing_videos),
            "success_rate": ((len(video_ids) - len(missing_videos)) / len(video_ids) * 100) if video_ids else 0
        })

        return missing_videos

    def get_validation_summary(self, base_download_dir: str) -> Dict[str, Any]:
        """
        Get comprehensive validation summary for entire download directory.
        
        Args:
            base_download_dir: Base download directory
            
        Returns:
            Summary of validation results with detailed statistics
        """
        base_dir = Path(base_download_dir)
        
        summary = {
            "total_folders": 0,
            "valid_folders": 0,
            "invalid_folders": 0,
            "missing_folders": 0,
            "pages_processed": 0,
            "validation_details": {},
            "missing_file_stats": {
                ".json": 0,
                ".mp4": 0, 
                ".jpg": 0
            },
            "size_violation_stats": {
                "small_videos": 0,
                "small_thumbnails": 0
            }
        }

        logger.info("Starting validation summary", extra={
            "event": "summary_start",
            "base_directory": str(base_dir)
        })

        if not base_dir.exists():
            error_msg = f"Base directory does not exist: {base_download_dir}"
            summary["error"] = error_msg
            logger.error("Base directory not found", extra={
                "event": "base_dir_not_found",
                "base_directory": str(base_dir)
            })
            return summary

        try:
            # Find all page directories
            page_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith('page_')]
            summary["pages_processed"] = len(page_dirs)
            
            logger.info("Found page directories", extra={
                "event": "pages_found",
                "page_count": len(page_dirs)
            })

            for page_dir in page_dirs:
                video_dirs = [d for d in page_dir.iterdir() if d.is_dir()]
                
                for video_dir in video_dirs:
                    summary["total_folders"] += 1
                    
                    validation = self.validate_video_folder(str(video_dir))
                    
                    if validation["valid"]:
                        summary["valid_folders"] += 1
                    else:
                        summary["invalid_folders"] += 1
                        
                        # Track missing file statistics
                        for missing_ext in validation["missing_files"]:
                            if missing_ext in summary["missing_file_stats"]:
                                summary["missing_file_stats"][missing_ext] += 1
                        
                        # Track size violations
                        for error in validation["errors"]:
                            if "Video file too small" in error:
                                summary["size_violation_stats"]["small_videos"] += 1
                            elif "Thumbnail file too small" in error:
                                summary["size_violation_stats"]["small_thumbnails"] += 1
                    
                    # Store detailed validation if there are issues
                    if not validation["valid"]:
                        summary["validation_details"][str(video_dir)] = validation

            # Calculate additional metrics
            if summary["total_folders"] > 0:
                summary["success_rate"] = (summary["valid_folders"] / summary["total_folders"]) * 100
            else:
                summary["success_rate"] = 0

            logger.info("Validation summary completed", extra={
                "event": "summary_complete",
                "total_folders": summary["total_folders"],
                "valid_folders": summary["valid_folders"],
                "success_rate": summary["success_rate"]
            })

        except Exception as e:
            error_msg = f"Error during validation summary: {e}"
            summary["error"] = error_msg
            logger.error("Summary generation failed", extra={
                "event": "summary_error",
                "error": str(e)
            })

        return summary

    def find_corrupted_downloads(self, base_download_dir: str) -> List[Dict[str, Any]]:
        """
        Find video folders with corrupted or incomplete downloads.
        Returns detailed information about each corrupted folder.
        
        Args:
            base_download_dir: Base download directory
            
        Returns:
            List of dictionaries with corrupted folder details
        """
        base_dir = Path(base_download_dir)
        corrupted_folders = []

        logger.info("Starting corrupted download scan", extra={
            "event": "corruption_scan_start",
            "base_directory": str(base_dir)
        })

        if not base_dir.exists():
            logger.warning("Download directory not found", extra={
                "event": "download_dir_not_found",
                "base_directory": str(base_dir)
            })
            return corrupted_folders

        try:
            # Find all video directories
            for page_dir in base_dir.glob('page_*'):
                if page_dir.is_dir():
                    for video_dir in page_dir.iterdir():
                        if video_dir.is_dir():
                            validation = self.validate_video_folder(str(video_dir))
                            
                            if not validation["valid"]:
                                corrupted_info = {
                                    "folder_path": str(video_dir),
                                    "video_id": validation["video_id"],
                                    "page": page_dir.name,
                                    "missing_files": validation["missing_files"],
                                    "errors": validation["errors"],
                                    "found_files": validation["found_files"],
                                    "file_sizes": validation.get("file_sizes", {})
                                }
                                corrupted_folders.append(corrupted_info)
                                
                                logger.debug("Found corrupted download", extra={
                                    "event": "corrupted_folder_found",
                                    "video_id": validation["video_id"],
                                    "page": page_dir.name,
                                    "missing_files": validation["missing_files"]
                                })

            logger.info("Corrupted download scan completed", extra={
                "event": "corruption_scan_complete",
                "corrupted_count": len(corrupted_folders)
            })

        except Exception as e:
            logger.error("Error during corruption scan", extra={
                "event": "corruption_scan_error",
                "error": str(e)
            })

        return corrupted_folders

    def cleanup_empty_folders(self, base_download_dir: str) -> int:
        """
        Remove empty folders from download directory with detailed logging.
        
        Args:
            base_download_dir: Base download directory
            
        Returns:
            Number of folders removed
        """
        removed_count = 0
        base_dir = Path(base_download_dir)

        logger.info("Starting empty folder cleanup", extra={
            "event": "cleanup_start",
            "base_directory": str(base_dir)
        })

        if not base_dir.exists():
            logger.warning("Directory does not exist for cleanup", extra={
                "event": "cleanup_dir_not_found",
                "base_directory": str(base_dir)
            })
            return removed_count

        try:
            # Remove empty video folders first
            for page_dir in base_dir.glob('page_*'):
                if page_dir.is_dir():
                    video_dirs_removed = 0
                    
                    for video_dir in list(page_dir.iterdir()):
                        if video_dir.is_dir():
                            try:
                                # Check if folder is empty
                                if not any(video_dir.iterdir()):
                                    video_dir.rmdir()
                                    removed_count += 1
                                    video_dirs_removed += 1
                                    
                                    logger.debug("Removed empty video folder", extra={
                                        "event": "empty_folder_removed",
                                        "folder_type": "video",
                                        "folder_path": str(video_dir)
                                    })
                            except OSError:
                                pass  # Folder not empty or permission issue

                    # Remove empty page folders
                    try:
                        if not any(page_dir.iterdir()):
                            page_dir.rmdir()
                            removed_count += 1
                            
                            logger.info("Removed empty page folder", extra={
                                "event": "empty_folder_removed",
                                "folder_type": "page",
                                "folder_path": str(page_dir),
                                "video_folders_removed": video_dirs_removed
                            })
                    except OSError:
                        pass

            logger.info("Empty folder cleanup completed", extra={
                "event": "cleanup_complete",
                "folders_removed": removed_count
            })

        except Exception as e:
            logger.error("Error during folder cleanup", extra={
                "event": "cleanup_error",
                "error": str(e)
            })

        return removed_count


if __name__ == "__main__":
    # Demo usage and testing
    logging.basicConfig(level=logging.INFO)
    
    validator = FileValidator()
    
    # Test validation on a sample folder
    test_folder = "downloads/page_1000/test_video"
    print(f"Testing validation on: {test_folder}")
    
    result = validator.validate_video_folder(test_folder)
    print(f"Validation result: {json.dumps(result, indent=2)}")
    
    # Test summary generation
    print("\nGenerating validation summary...")
    summary = validator.get_validation_summary("downloads")
    print(f"Summary: {json.dumps(summary, indent=2)}")
    
    # Test corrupted downloads detection
    print("\nScanning for corrupted downloads...")
    corrupted = validator.find_corrupted_downloads("downloads")
    print(f"Found {len(corrupted)} corrupted downloads")
    
    if corrupted:
        print("First corrupted download details:")
        print(json.dumps(corrupted[0], indent=2))