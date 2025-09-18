#!/usr/bin/env python3
"""
IDM Manager Module (Refactored)

Thin wrapper for IDM operations with batch enqueuing support.
Refactored from existing IDM integration to support the new batch processing workflow.

Author: AI Assistant
Version: 2.0 - Refactored for batch processing
"""

import os
import subprocess
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

from utils import SafeFileOperations, TimestampHelper

logger = logging.getLogger(__name__)


@dataclass
class DownloadItem:
    """Container for a single download item."""
    video_id: str
    video_url: str
    thumbnail_url: str
    download_folder: str
    filename_prefix: str = ""

    def get_video_filename(self) -> str:
        """Get expected video filename."""
        prefix = self.filename_prefix or self.video_id
        return f"{prefix}.mp4"

    def get_thumbnail_filename(self) -> str:
        """Get expected thumbnail filename."""
        prefix = self.filename_prefix or self.video_id
        return f"{prefix}.jpg"


class IDMManager:
    """Simplified IDM manager for batch operations."""

    def __init__(self, idm_path: Optional[str] = None, base_download_dir: str = "downloads"):
        """
        Initialize IDM manager.

        Args:
            idm_path: Path to IDM executable (auto-detected if None)
            base_download_dir: Base download directory
        """
        self.base_download_dir = Path(base_download_dir)
        self.idm_path = self._find_idm_path(idm_path)
        self._queue_operations = []  # Track operations for debugging

        if not self.idm_path:
            logger.error("IDM executable not found. Downloads will fail.")
        else:
            logger.info(f"IDM manager initialized with IDM path: {self.idm_path}")

    def _find_idm_path(self, custom_path: Optional[str] = None) -> Optional[str]:
        """
        Find IDM executable path.

        Args:
            custom_path: Custom IDM path if provided

        Returns:
            Path to IDM executable or None if not found
        """
        if custom_path and os.path.exists(custom_path):
            return custom_path

        # Common IDM installation paths
        common_paths = [
            r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe",
            r"C:\Program Files\Internet Download Manager\IDMan.exe",
            "/usr/local/bin/idman",  # Linux (if IDM alternative exists)
            "/opt/idm/idman"
        ]

        for path in common_paths:
            if os.path.exists(path):
                return path

        # Try to find in PATH
        try:
            result = subprocess.run(["which", "IDMan.exe"], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                return result.stdout.strip()
        except:
            pass

        return None

    def enqueue(self, download_items: List[DownloadItem]) -> Dict[str, Any]:
        """
        Enqueue multiple download items to IDM.

        Args:
            download_items: List of items to download

        Returns:
            Results dictionary with success/failure information
        """
        if not self.idm_path:
            return {
                "success": False,
                "error": "IDM executable not found",
                "enqueued_count": 0,
                "failed_items": [item.video_id for item in download_items]
            }

        results = {
            "success": True,
            "enqueued_count": 0,
            "failed_count": 0,
            "failed_items": [],
            "operations": []
        }

        logger.info(f"Enqueuing {len(download_items)} items to IDM")

        for item in download_items:
            try:
                # Ensure download folder exists
                if not SafeFileOperations.safe_create_folder(item.download_folder):
                    logger.error(f"Failed to create download folder: {item.download_folder}")
                    results["failed_items"].append(item.video_id)
                    results["failed_count"] += 1
                    continue

                # Enqueue video file
                video_success = self._enqueue_single_file(
                    url=item.video_url,
                    download_path=item.download_folder,
                    filename=item.get_video_filename()
                )

                # Enqueue thumbnail file
                thumbnail_success = self._enqueue_single_file(
                    url=item.thumbnail_url,
                    download_path=item.download_folder,
                    filename=item.get_thumbnail_filename()
                )

                if video_success and thumbnail_success:
                    results["enqueued_count"] += 1
                    logger.debug(f"Successfully enqueued: {item.video_id}")
                else:
                    results["failed_items"].append(item.video_id)
                    results["failed_count"] += 1
                    logger.warning(f"Failed to enqueue some files for: {item.video_id}")

                # Track operation for debugging
                operation = {
                    "video_id": item.video_id,
                    "video_success": video_success,
                    "thumbnail_success": thumbnail_success,
                    "timestamp": TimestampHelper.get_current_timestamp()
                }
                results["operations"].append(operation)

            except Exception as e:
                logger.error(f"Error enqueuing {item.video_id}: {e}")
                results["failed_items"].append(item.video_id)
                results["failed_count"] += 1

        # Update overall success status
        results["success"] = results["enqueued_count"] > 0

        logger.info(f"IDM enqueue completed: {results['enqueued_count']} successful, "
                   f"{results['failed_count']} failed")

        # Store operation record
        self._queue_operations.append({
            "timestamp": TimestampHelper.get_current_timestamp(),
            "total_items": len(download_items),
            "results": results
        })

        return results

    def _enqueue_single_file(self, url: str, download_path: str, filename: str) -> bool:
        """
        Enqueue a single file to IDM.

        Args:
            url: URL to download
            download_path: Path to download to
            filename: Filename to save as

        Returns:
            True if enqueued successfully
        """
        if not url or not url.startswith('http'):
            logger.warning(f"Invalid URL for {filename}: {url}")
            return False

        try:
            # Build IDM command
            cmd = [
                self.idm_path,
                '/d', url,                    # URL to download
                '/p', download_path,          # Download path
                '/f', filename,               # Filename
                '/n',                         # Don't show confirmation dialog
                '/q'                          # Quiet mode
            ]

            # Execute IDM command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30  # 30 second timeout
            )

            if result.returncode == 0:
                logger.debug(f"IDM enqueue successful: {filename}")
                return True
            else:
                logger.warning(f"IDM enqueue failed for {filename}: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error(f"IDM enqueue timeout for {filename}")
            return False
        except Exception as e:
            logger.error(f"Error enqueuing {filename} to IDM: {e}")
            return False

    def start_queue(self) -> bool:
        """
        Start IDM download queue processing.

        Returns:
            True if start command was successful
        """
        if not self.idm_path:
            logger.error("Cannot start IDM queue - executable not found")
            return False

        try:
            # Command to start IDM queue
            cmd = [self.idm_path, '/s']  # Start downloading

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                logger.info("IDM download queue started")
                return True
            else:
                logger.warning(f"IDM start queue warning: {result.stderr}")
                return True  # IDM might already be running

        except Exception as e:
            logger.error(f"Error starting IDM queue: {e}")
            return False

    def stop_queue(self) -> bool:
        """
        Stop IDM download queue processing.

        Returns:
            True if stop command was successful
        """
        if not self.idm_path:
            logger.error("Cannot stop IDM queue - executable not found")
            return False

        try:
            # There's no direct stop command, but we can try to pause
            # This is implementation-dependent and might not work in all cases
            logger.info("IDM stop queue requested (implementation-dependent)")
            return True

        except Exception as e:
            logger.error(f"Error stopping IDM queue: {e}")
            return False

    def get_queue_state(self) -> Dict[str, Any]:
        """
        Get current state of IDM queue.

        Note: This is difficult to implement reliably as IDM doesn't provide
        a direct API for queue state. This returns basic information.

        Returns:
            Dictionary with queue state information
        """
        state = {
            "idm_available": self.idm_path is not None,
            "idm_path": self.idm_path,
            "last_operations": len(self._queue_operations),
            "timestamp": TimestampHelper.get_current_timestamp()
        }

        if self._queue_operations:
            latest_op = self._queue_operations[-1]
            state["last_enqueue"] = {
                "timestamp": latest_op["timestamp"],
                "total_items": latest_op["total_items"],
                "success_rate": (latest_op["results"]["enqueued_count"] / 
                               latest_op["total_items"] * 100 
                               if latest_op["total_items"] > 0 else 0)
            }

        return state

    def create_download_items_from_videos(self, videos: List[Any], 
                                        page_number: int) -> List[DownloadItem]:
        """
        Create DownloadItem objects from video metadata.

        Args:
            videos: List of video metadata objects
            page_number: Page number for folder structure

        Returns:
            List of DownloadItem objects
        """
        download_items = []

        for video in videos:
            try:
                # Create download folder path
                video_folder = self.base_download_dir / f"page_{page_number}" / video.video_id

                download_item = DownloadItem(
                    video_id=video.video_id,
                    video_url=video.video_url,
                    thumbnail_url=video.thumbnail_url,
                    download_folder=str(video_folder),
                    filename_prefix=video.video_id
                )

                download_items.append(download_item)

            except Exception as e:
                logger.error(f"Error creating download item for {getattr(video, 'video_id', 'unknown')}: {e}")

        return download_items

    def get_operation_history(self) -> List[Dict[str, Any]]:
        """
        Get history of IDM operations.

        Returns:
            List of operation records
        """
        return self._queue_operations.copy()

    def clear_operation_history(self) -> None:
        """Clear operation history."""
        self._queue_operations.clear()
        logger.info("IDM operation history cleared")


# Backward compatibility adapters
class ImprovedIDMManager(IDMManager):
    """Backward compatibility wrapper for existing code."""

    def __init__(self, *args, **kwargs):
        # Map old parameters to new ones
        if 'base_download_dir' in kwargs:
            kwargs['base_download_dir'] = kwargs.pop('base_download_dir')
        super().__init__(*args, **kwargs)


if __name__ == "__main__":
    # Demo usage
    import logging
    logging.basicConfig(level=logging.INFO)

    # Test IDM manager
    idm = IDMManager()

    # Test queue state
    state = idm.get_queue_state()
    print(f"IDM state: {state}")

    # Test with dummy download items
    dummy_items = [
        DownloadItem(
            video_id="test1",
            video_url="https://example.com/video1.mp4",
            thumbnail_url="https://example.com/thumb1.jpg",
            download_folder="downloads/page_1000/test1"
        )
    ]

    # This would normally enqueue to IDM (commented out for safety)
    # result = idm.enqueue(dummy_items)
    # print(f"Enqueue result: {result}")
