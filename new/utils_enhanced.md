#!/usr/bin/env python3

"""
utils.py - Enhanced Utilities with Structured Logging

Enhanced utility functions with comprehensive structured logging support,
JSON log formatting, and better error tracking.
"""

import json
import logging
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import threading


class StructuredJSONFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON logs."""
    
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage()
        }
        
        # Add any extra structured fields
        extra_fields = ['event', 'video_id', 'page', 'batch_id', 'file_path', 
                       'operation', 'duration', 'size_mb', 'error_type']
        
        for field in extra_fields:
            if hasattr(record, field):
                log_entry[field] = getattr(record, field)
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info) if record.exc_info else None
            }
            
        return json.dumps(log_entry, ensure_ascii=False)


class LoggingConfig:
    """Enhanced logging configuration with structured JSON support."""
    
    @staticmethod
    def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None,
                     json_log_file: Optional[str] = None,
                     format_string: Optional[str] = None) -> None:
        """
        Setup comprehensive logging configuration with both regular and structured JSON logs.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            log_file: Optional regular log file path
            json_log_file: Optional structured JSON log file path
            format_string: Optional custom format string for regular logs
        """
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        
        # Convert string level to logging constant
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(numeric_level)
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Console handler with regular format
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_formatter = logging.Formatter(format_string)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # Regular file handler if specified
        if log_file:
            try:
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(numeric_level)
                file_formatter = logging.Formatter(format_string)
                file_handler.setFormatter(file_formatter)
                root_logger.addHandler(file_handler)
                
                logging.info("Regular file logging enabled", extra={
                    "event": "file_logging_enabled",
                    "log_file": log_file
                })
            except Exception as e:
                logging.error(f"Could not setup regular file logging: {e}")
        
        # Structured JSON file handler if specified
        if json_log_file:
            try:
                json_handler = logging.FileHandler(json_log_file, encoding='utf-8')
                json_handler.setLevel(numeric_level)
                json_formatter = StructuredJSONFormatter()
                json_handler.setFormatter(json_formatter)
                root_logger.addHandler(json_handler)
                
                logging.info("Structured JSON logging enabled", extra={
                    "event": "json_logging_enabled",
                    "json_log_file": json_log_file
                })
            except Exception as e:
                logging.error(f"Could not setup JSON file logging: {e}")


class SafeFileOperations:
    """Enhanced file operations with comprehensive logging."""
    
    @staticmethod
    def safe_delete_folder(folder_path: str, backup_to_trash: bool = True) -> bool:
        """
        Safely delete a folder with optional backup and detailed logging.
        
        Args:
            folder_path: Path to folder to delete
            backup_to_trash: If True, move to .trash folder instead of deleting
            
        Returns:
            True if operation succeeded
        """
        folder = Path(folder_path)
        logger = logging.getLogger(__name__)
        
        logger.info("Starting folder deletion", extra={
            "event": "folder_delete_start",
            "folder_path": str(folder),
            "backup_to_trash": backup_to_trash
        })
        
        if not folder.exists():
            logger.warning("Folder does not exist", extra={
                "event": "folder_not_found",
                "folder_path": str(folder)
            })
            return True  # Already "deleted"
        
        if not folder.is_dir():
            logger.error("Path is not a directory", extra={
                "event": "path_not_directory",
                "folder_path": str(folder)
            })
            return False
        
        try:
            # Safety check - ensure we're only deleting within expected directories
            if not SafeFileOperations._is_safe_to_delete(folder):
                logger.error("Unsafe delete attempt blocked", extra={
                    "event": "unsafe_delete_blocked",
                    "folder_path": str(folder)
                })
                return False
            
            # Calculate folder size before deletion
            folder_size = SafeFileOperations._calculate_folder_size(folder)
            
            if backup_to_trash:
                trash_dir = folder.parent / ".trash"
                trash_dir.mkdir(exist_ok=True)
                
                # Create unique name in trash
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                trash_name = f"{folder.name}_{timestamp}"
                trash_path = trash_dir / trash_name
                
                shutil.move(str(folder), str(trash_path))
                
                logger.info("Folder moved to trash", extra={
                    "event": "folder_moved_to_trash",
                    "folder_path": str(folder),
                    "trash_path": str(trash_path),
                    "size_mb": folder_size
                })
            else:
                shutil.rmtree(folder)
                
                logger.info("Folder deleted permanently", extra={
                    "event": "folder_deleted",
                    "folder_path": str(folder),
                    "size_mb": folder_size
                })
            
            return True
            
        except Exception as e:
            logger.error("Folder deletion failed", extra={
                "event": "folder_delete_failed",
                "folder_path": str(folder),
                "error": str(e)
            })
            return False
    
    @staticmethod
    def _calculate_folder_size(folder: Path) -> float:
        """Calculate folder size in MB."""
        try:
            total_size = 0
            for file_path in folder.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            return total_size / (1024 * 1024)  # Convert to MB
        except Exception:
            return 0.0
    
    @staticmethod
    def safe_create_folder(folder_path: str, parents: bool = True) -> bool:
        """
        Safely create a folder with detailed logging.
        
        Args:
            folder_path: Path to folder to create
            parents: Create parent directories if needed
            
        Returns:
            True if folder was created or already exists
        """
        logger = logging.getLogger(__name__)
        
        try:
            folder = Path(folder_path)
            
            if folder.exists():
                logger.debug("Folder already exists", extra={
                    "event": "folder_exists",
                    "folder_path": str(folder)
                })
                return True
            
            folder.mkdir(parents=parents, exist_ok=True)
            
            logger.debug("Folder created successfully", extra={
                "event": "folder_created",
                "folder_path": str(folder),
                "parents": parents
            })
            return True
            
        except Exception as e:
            logger.error("Folder creation failed", extra={
                "event": "folder_create_failed",
                "folder_path": folder_path,
                "error": str(e)
            })
            return False
    
    @staticmethod
    def safe_copy_file(src: str, dst: str, backup_existing: bool = True) -> bool:
        """
        Safely copy a file with optional backup and detailed logging.
        
        Args:
            src: Source file path
            dst: Destination file path
            backup_existing: Backup destination if it exists
            
        Returns:
            True if copy succeeded
        """
        logger = logging.getLogger(__name__)
        
        logger.debug("Starting file copy", extra={
            "event": "file_copy_start",
            "src": src,
            "dst": dst,
            "backup_existing": backup_existing
        })
        
        try:
            src_path = Path(src)
            dst_path = Path(dst)
            
            if not src_path.exists():
                logger.error("Source file does not exist", extra={
                    "event": "src_file_not_found",
                    "src": src
                })
                return False
            
            src_size = src_path.stat().st_size
            
            # Backup existing destination
            if dst_path.exists() and backup_existing:
                backup_path = dst_path.with_suffix(f"{dst_path.suffix}.backup")
                shutil.copy2(dst_path, backup_path)
                
                logger.debug("Backed up existing file", extra={
                    "event": "file_backup_created",
                    "dst": dst,
                    "backup": str(backup_path)
                })
            
            # Create destination directory if needed
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            start_time = time.time()
            shutil.copy2(src_path, dst_path)
            copy_time = time.time() - start_time
            
            logger.info("File copied successfully", extra={
                "event": "file_copy_success",
                "src": src,
                "dst": dst,
                "size_bytes": src_size,
                "duration": copy_time
            })
            return True
            
        except Exception as e:
            logger.error("File copy failed", extra={
                "event": "file_copy_failed",
                "src": src,
                "dst": dst,
                "error": str(e)
            })
            return False
    
    @staticmethod
    def _is_safe_to_delete(folder_path: Path) -> bool:
        """
        Check if a folder is safe to delete (within expected download directories).
        
        Args:
            folder_path: Path to check
            
        Returns:
            True if safe to delete
        """
        # Only allow deletion within common download directories
        safe_parents = ['downloads', 'scraper_downloads', 'temp', 'tmp', 'manifests']
        
        # Check if any parent directory is in safe list
        for parent in folder_path.parents:
            if parent.name in safe_parents:
                return True
        
        # Check if direct parent is a page folder (page_XXX)
        if folder_path.parent.name.startswith('page_'):
            return True
        
        # Check if folder itself is in safe directories
        if folder_path.name in safe_parents:
            return True
            
        return False


class TimestampHelper:
    """Enhanced timestamp operations with logging."""
    
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
        Parse timestamp string to datetime object with enhanced format support.
        
        Args:
            timestamp_str: Timestamp string in various formats
            
        Returns:
            Parsed datetime object or None if parsing failed
        """
        logger = logging.getLogger(__name__)
        
        formats_to_try = [
            "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds
            "%Y-%m-%dT%H:%M:%S",     # ISO without microseconds
            "%Y-%m-%d %H:%M:%S",     # Standard format
            "%Y-%m-%d",              # Date only
            "%d/%m/%Y %H:%M:%S",     # European format
            "%m/%d/%Y %H:%M:%S"      # US format
        ]
        
        for fmt in formats_to_try:
            try:
                result = datetime.strptime(timestamp_str, fmt)
                logger.debug("Timestamp parsed successfully", extra={
                    "event": "timestamp_parsed",
                    "timestamp": timestamp_str,
                    "format": fmt
                })
                return result
            except ValueError:
                continue
        
        logger.warning("Could not parse timestamp", extra={
            "event": "timestamp_parse_failed",
            "timestamp": timestamp_str
        })
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
        if seconds < 1:
            return f"{seconds*1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"


class ThreadSafeCounter:
    """Thread-safe counter for tracking operations with logging."""
    
    def __init__(self, initial_value: int = 0, name: str = "counter"):
        self._value = initial_value
        self._lock = threading.Lock()
        self._name = name
        self.logger = logging.getLogger(__name__)
        
        self.logger.debug("Counter initialized", extra={
            "event": "counter_init",
            "counter_name": name,
            "initial_value": initial_value
        })
    
    def increment(self) -> int:
        """Increment counter and return new value."""
        with self._lock:
            old_value = self._value
            self._value += 1
            
            self.logger.debug("Counter incremented", extra={
                "event": "counter_increment",
                "counter_name": self._name,
                "old_value": old_value,
                "new_value": self._value
            })
            
            return self._value
    
    def decrement(self) -> int:
        """Decrement counter and return new value."""
        with self._lock:
            old_value = self._value
            self._value -= 1
            
            self.logger.debug("Counter decremented", extra={
                "event": "counter_decrement",
                "counter_name": self._name,
                "old_value": old_value,
                "new_value": self._value
            })
            
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
            
            self.logger.info("Counter reset", extra={
                "event": "counter_reset",
                "counter_name": self._name,
                "old_value": old_value
            })
            
            return old_value


def calculate_folder_size(folder_path: str) -> float:
    """
    Calculate total size of a folder in MB with detailed logging.
    
    Args:
        folder_path: Path to folder
        
    Returns:
        Size in MB
    """
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    try:
        total_size = 0
        file_count = 0
        folder = Path(folder_path)
        
        if not folder.exists():
            logger.warning("Folder does not exist for size calculation", extra={
                "event": "folder_size_not_found",
                "folder_path": folder_path
            })
            return 0.0
        
        for file_path in folder.rglob('*'):
            if file_path.is_file():
                total_size += file_path.stat().st_size
                file_count += 1
        
        size_mb = total_size / (1024 * 1024)  # Convert to MB
        calc_time = time.time() - start_time
        
        logger.debug("Folder size calculated", extra={
            "event": "folder_size_calculated",
            "folder_path": folder_path,
            "size_mb": size_mb,
            "file_count": file_count,
            "calculation_time": calc_time
        })
        
        return size_mb
        
    except Exception as e:
        logger.error("Folder size calculation failed", extra={
            "event": "folder_size_calc_failed",
            "folder_path": folder_path,
            "error": str(e)
        })
        return 0.0


def wait_with_progress(seconds: int, message: str = "Waiting", 
                      progress_interval: int = 10) -> None:
    """
    Wait for specified seconds with progress updates and structured logging.
    
    Args:
        seconds: Number of seconds to wait
        message: Message to display
        progress_interval: Interval between progress updates
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Starting wait period", extra={
        "event": "wait_start",
        "duration": seconds,
        "message": message,
        "progress_interval": progress_interval
    })
    
    start_time = time.time()
    elapsed = 0
    
    while elapsed < seconds:
        sleep_time = min(progress_interval, seconds - elapsed)
        time.sleep(sleep_time)
        elapsed += sleep_time
        
        if elapsed < seconds:
            remaining = seconds - elapsed
            logger.info("Wait progress update", extra={
                "event": "wait_progress",
                "elapsed": elapsed,
                "remaining": remaining,
                "progress_percent": (elapsed / seconds) * 100
            })
    
    total_time = time.time() - start_time
    
    logger.info("Wait period completed", extra={
        "event": "wait_complete",
        "requested_duration": seconds,
        "actual_duration": total_time
    })


if __name__ == "__main__":
    # Demo usage with enhanced logging
    LoggingConfig.setup_logging(
        log_level="DEBUG",
        log_file="utils_demo.log",
        json_log_file="utils_demo_structured.log"
    )
    
    logger = logging.getLogger(__name__)
    
    # Test timestamp helper
    ts_helper = TimestampHelper()
    current_time = ts_helper.get_current_timestamp()
    print(f"Current timestamp: {current_time}")
    
    # Test folder size calculation
    size = calculate_folder_size(".")
    print(f"Current directory size: {size:.2f} MB")
    
    # Test safe file operations
    test_folder = "test_folder"
    SafeFileOperations.safe_create_folder(test_folder)
    SafeFileOperations.safe_delete_folder(test_folder, backup_to_trash=True)
    
    # Test counter
    counter = ThreadSafeCounter(name="demo_counter")
    counter.increment()
    counter.increment()
    print(f"Counter value: {counter.get_value()}")
    
    print("Demo completed - check log files for structured output")