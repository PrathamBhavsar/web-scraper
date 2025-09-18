#!/usr/bin/env python3
"""
Utility Functions Module

Common helper functions for file operations, timestamps, and logging.
Includes safe file operations and folder management utilities.

Author: AI Assistant
Version: 1.0
"""

import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional, List
from datetime import datetime
import logging
import threading

logger = logging.getLogger(__name__)


class SafeFileOperations:
    """Thread-safe file operations with error handling."""

    @staticmethod
    def safe_delete_folder(folder_path: str, backup_to_trash: bool = True) -> bool:
        """
        Safely delete a folder with optional backup.

        Args:
            folder_path: Path to folder to delete
            backup_to_trash: If True, move to .trash folder instead of deleting

        Returns:
            True if operation succeeded
        """
        folder = Path(folder_path)

        if not folder.exists():
            logger.warning(f"Folder does not exist: {folder_path}")
            return True  # Already "deleted"

        if not folder.is_dir():
            logger.error(f"Path is not a directory: {folder_path}")
            return False

        try:
            # Safety check - ensure we're only deleting within expected directories
            if not _is_safe_to_delete(folder):
                logger.error(f"Unsafe delete attempt blocked: {folder_path}")
                return False

            if backup_to_trash:
                trash_dir = folder.parent / ".trash"
                trash_dir.mkdir(exist_ok=True)

                # Create unique name in trash
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                trash_name = f"{folder.name}_{timestamp}"
                trash_path = trash_dir / trash_name

                shutil.move(str(folder), str(trash_path))
                logger.info(f"Moved to trash: {folder_path} -> {trash_path}")
            else:
                shutil.rmtree(folder)
                logger.info(f"Deleted folder: {folder_path}")

            return True

        except Exception as e:
            logger.error(f"Error deleting folder {folder_path}: {e}")
            return False

    @staticmethod
    def safe_create_folder(folder_path: str, parents: bool = True) -> bool:
        """
        Safely create a folder.

        Args:
            folder_path: Path to folder to create
            parents: Create parent directories if needed

        Returns:
            True if folder was created or already exists
        """
        try:
            folder = Path(folder_path)
            folder.mkdir(parents=parents, exist_ok=True)
            logger.debug(f"Created/ensured folder: {folder_path}")
            return True
        except Exception as e:
            logger.error(f"Error creating folder {folder_path}: {e}")
            return False

    @staticmethod
    def safe_copy_file(src: str, dst: str, backup_existing: bool = True) -> bool:
        """
        Safely copy a file with optional backup of existing destination.

        Args:
            src: Source file path
            dst: Destination file path
            backup_existing: Backup destination if it exists

        Returns:
            True if copy succeeded
        """
        try:
            src_path = Path(src)
            dst_path = Path(dst)

            if not src_path.exists():
                logger.error(f"Source file does not exist: {src}")
                return False

            # Backup existing destination
            if dst_path.exists() and backup_existing:
                backup_path = dst_path.with_suffix(f"{dst_path.suffix}.backup")
                shutil.copy2(dst_path, backup_path)
                logger.debug(f"Backed up existing file: {dst} -> {backup_path}")

            # Create destination directory if needed
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy file
            shutil.copy2(src_path, dst_path)
            logger.debug(f"Copied file: {src} -> {dst}")
            return True

        except Exception as e:
            logger.error(f"Error copying file {src} -> {dst}: {e}")
            return False


def _is_safe_to_delete(folder_path: Path) -> bool:
    """
    Check if a folder is safe to delete (within expected download directories).

    Args:
        folder_path: Path to check

    Returns:
        True if safe to delete
    """
    # Only allow deletion within common download directories
    safe_parents = ['downloads', 'scraper_downloads', 'temp', 'tmp']

    # Check if any parent directory is in safe list
    for parent in folder_path.parents:
        if parent.name in safe_parents:
            return True

    # Check if direct parent is a page folder (page_XXX)
    if folder_path.parent.name.startswith('page_'):
        return True

    return False


class TimestampHelper:
    """Helper functions for timestamp operations."""

    @staticmethod
    def get_current_timestamp() -> str:
        """Get current timestamp in ISO format."""
        return datetime.now().isoformat()

    @staticmethod
    def get_current_time_string() -> str:
        """Get current time as readable string."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp string to datetime object.

        Args:
            timestamp_str: Timestamp string in various formats

        Returns:
            Parsed datetime object or None if parsing failed
        """
        formats_to_try = [
            "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds
            "%Y-%m-%dT%H:%M:%S",     # ISO without microseconds
            "%Y-%m-%d %H:%M:%S",     # Standard format
            "%Y-%m-%d"               # Date only
        ]

        for fmt in formats_to_try:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue

        logger.warning(f"Could not parse timestamp: {timestamp_str}")
        return None

    @staticmethod
    def format_duration(seconds: float) -> str:
        """
        Format duration in seconds to human-readable string.

        Args:
            seconds: Duration in seconds

        Returns:
            Formatted duration string
        """
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"


class LoggingConfig:
    """Logging configuration utilities."""

    @staticmethod
    def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None,
                     format_string: Optional[str] = None) -> None:
        """
        Setup logging configuration.

        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            log_file: Optional log file path
            format_string: Optional custom format string
        """
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        # Convert string level to logging constant
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)

        # Configure root logger
        logging.basicConfig(
            level=numeric_level,
            format=format_string,
            handlers=[]
        )

        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric_level)
        console_formatter = logging.Formatter(format_string)
        console_handler.setFormatter(console_formatter)
        logging.getLogger().addHandler(console_handler)

        # Add file handler if specified
        if log_file:
            try:
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(numeric_level)
                file_formatter = logging.Formatter(format_string)
                file_handler.setFormatter(file_formatter)
                logging.getLogger().addHandler(file_handler)
                logging.info(f"Logging to file: {log_file}")
            except Exception as e:
                logging.error(f"Could not setup file logging: {e}")


class ThreadSafeCounter:
    """Thread-safe counter for tracking operations."""

    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.Lock()

    def increment(self) -> int:
        """Increment counter and return new value."""
        with self._lock:
            self._value += 1
            return self._value

    def decrement(self) -> int:
        """Decrement counter and return new value."""
        with self._lock:
            self._value -= 1
            return self._value

    def get_value(self) -> int:
        """Get current counter value."""
        with self._lock:
            return self._value

    def reset(self) -> int:
        """Reset counter to zero and return previous value."""
        with self._lock:
            old_value = self._value
            self._value = 0
            return old_value


def calculate_folder_size(folder_path: str) -> float:
    """
    Calculate total size of a folder in MB.

    Args:
        folder_path: Path to folder

    Returns:
        Size in MB
    """
    try:
        total_size = 0
        folder = Path(folder_path)

        if not folder.exists():
            return 0.0

        for file_path in folder.rglob('*'):
            if file_path.is_file():
                total_size += file_path.stat().st_size

        return total_size / (1024 * 1024)  # Convert to MB

    except Exception as e:
        logger.error(f"Error calculating folder size for {folder_path}: {e}")
        return 0.0


def ensure_directory_structure(base_dir: str, subdirs: List[str]) -> bool:
    """
    Ensure directory structure exists.

    Args:
        base_dir: Base directory path
        subdirs: List of subdirectories to create

    Returns:
        True if all directories were created/exist
    """
    try:
        base_path = Path(base_dir)
        base_path.mkdir(parents=True, exist_ok=True)

        for subdir in subdirs:
            subdir_path = base_path / subdir
            subdir_path.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Ensured directory structure: {base_dir} with subdirs {subdirs}")
        return True

    except Exception as e:
        logger.error(f"Error ensuring directory structure: {e}")
        return False


def get_available_disk_space(path: str) -> float:
    """
    Get available disk space for a path in GB.

    Args:
        path: Path to check

    Returns:
        Available space in GB
    """
    try:
        if os.name == 'nt':  # Windows
            free_bytes = shutil.disk_usage(path).free
        else:  # Unix-like
            statvfs = os.statvfs(path)
            free_bytes = statvfs.f_frsize * statvfs.f_bavail

        return free_bytes / (1024 ** 3)  # Convert to GB

    except Exception as e:
        logger.error(f"Error checking disk space for {path}: {e}")
        return 0.0


def wait_with_progress(seconds: int, message: str = "Waiting", 
                      progress_interval: int = 10) -> None:
    """
    Wait for specified seconds with progress updates.

    Args:
        seconds: Number of seconds to wait
        message: Message to display
        progress_interval: Interval between progress updates
    """
    logger.info(f"{message} for {seconds} seconds...")

    elapsed = 0
    while elapsed < seconds:
        time.sleep(min(progress_interval, seconds - elapsed))
        elapsed += progress_interval

        if elapsed < seconds:
            remaining = seconds - elapsed
            logger.info(f"{message}... {remaining} seconds remaining")

    logger.info(f"{message} completed")


if __name__ == "__main__":
    # Demo usage
    logging.basicConfig(level=logging.INFO)

    # Test timestamp helper
    ts_helper = TimestampHelper()
    current_time = ts_helper.get_current_timestamp()
    print(f"Current timestamp: {current_time}")

    # Test folder size calculation
    size = calculate_folder_size(".")
    print(f"Current directory size: {size:.2f} MB")

    # Test disk space
    space = get_available_disk_space(".")
    print(f"Available disk space: {space:.2f} GB")
