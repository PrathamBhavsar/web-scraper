#!/usr/bin/env python3
"""
Configuration Manager Module

Provides safe read/write operations for config.json with atomic writes
and default configuration management.

Author: AI Assistant
Version: 1.0
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration file operations with atomic writes."""

    def __init__(self, config_file: str = "config.json"):
        """
        Initialize configuration manager.

        Args:
            config_file: Path to configuration file
        """
        self.config_file = Path(config_file)
        self._default_config = self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""
        return {
            "general": {
                "base_url": "https://rule34video.com",
                "download_path": "scraper_downloads",
                "max_storage_gb": 940
            },
            "batch": {
                "batch_pages": 3,
                "batch_initial_wait_seconds": 240,
                "per_page_idm_wait_seconds": 120,
                "max_failed_retries_per_page": 3
            },
            "processing": {
                "mode": "direct",
                "use_parallel": True,
                "parallel_batch_size": 5,
                "max_concurrent_downloads": 8,
                "max_concurrent_videos": 3,
                "use_parallel_video_processing": True
            },
            "scraping": {
                "wait_time_ms": 3000,
                "max_retries": 3,
                "enable_crawl4ai": True
            },
            "download": {
                "download_method": "direct",
                "max_retries": 5,
                "chunk_size": 16384,
                "connect_timeout_seconds": 30,
                "read_timeout_seconds": 300
            },
            "validation": {
                "required_json_fields": ["video_id", "title", "video_src"],
                "min_video_size_bytes": 1024,
                "min_thumbnail_size_bytes": 100,
                "validation_delay_seconds": 2
            },
            "logging": {
                "log_level": "INFO",
                "log_to_file": True,
                "log_file_path": "scraper.log"
            }
        }

    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from file.

        Returns:
            Configuration dictionary
        """
        try:
            if not self.config_file.exists():
                logger.info(f"Config file {self.config_file} not found, using defaults")
                return self._default_config.copy()

            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # Merge with defaults to ensure all required keys exist
            merged_config = self._merge_with_defaults(config)
            logger.info(f"Config loaded from {self.config_file}")
            return merged_config

        except Exception as e:
            logger.error(f"Error loading config: {e}")
            logger.info("Using default configuration")
            return self._default_config.copy()

    def save_config(self, config: Dict[str, Any]) -> bool:
        """
        Save configuration to file using atomic write.

        Args:
            config: Configuration dictionary to save

        Returns:
            True if saved successfully
        """
        try:
            # Create parent directory if it doesn't exist
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            # Write to temporary file first
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.json',
                prefix=f'{self.config_file.name}.tmp.',
                dir=self.config_file.parent,
                delete=False,
                encoding='utf-8'
            ) as tmp_file:
                json.dump(config, tmp_file, indent=2, ensure_ascii=False)
                tmp_path = tmp_file.name

            # Atomic rename
            os.replace(tmp_path, self.config_file)
            logger.info(f"Config saved to {self.config_file}")
            return True

        except Exception as e:
            logger.error(f"Error saving config: {e}")
            # Clean up temporary file if it exists
            try:
                if 'tmp_path' in locals():
                    os.unlink(tmp_path)
            except:
                pass
            return False

    def _merge_with_defaults(self, user_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge user configuration with defaults.

        Args:
            user_config: User-provided configuration

        Returns:
            Merged configuration with all required keys
        """
        merged = self._default_config.copy()

        def deep_merge(default: Dict, user: Dict) -> Dict:
            result = default.copy()
            for key, value in user.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = value
            return result

        return deep_merge(merged, user_config)

    def get_batch_config(self) -> Dict[str, Any]:
        """Get batch processing configuration."""
        config = self.load_config()
        return config.get("batch", self._default_config["batch"])

    def get_download_directory(self) -> str:
        """Get download directory path."""
        config = self.load_config()
        return config.get("general", {}).get("download_path", "scraper_downloads")

    def get_max_storage_gb(self) -> float:
        """Get maximum storage limit in GB."""
        config = self.load_config()
        return config.get("general", {}).get("max_storage_gb", 940)

    def update_config_value(self, section: str, key: str, value: Any) -> bool:
        """
        Update a specific configuration value.

        Args:
            section: Configuration section name
            key: Configuration key
            value: New value

        Returns:
            True if updated successfully
        """
        try:
            config = self.load_config()
            if section not in config:
                config[section] = {}
            config[section][key] = value
            return self.save_config(config)
        except Exception as e:
            logger.error(f"Error updating config value {section}.{key}: {e}")
            return False


if __name__ == "__main__":
    # Demo usage
    import logging
    logging.basicConfig(level=logging.INFO)

    manager = ConfigManager()
    config = manager.load_config()
    print("Current config:")
    print(json.dumps(config, indent=2))

    # Test update
    manager.update_config_value("batch", "batch_pages", 5)
    print("\nBatch pages updated to 5")
