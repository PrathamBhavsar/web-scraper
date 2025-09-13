import json
from pathlib import Path

class ConfigManager:
    def __init__(self):
        self.config_path = Path("config.json")
        self.config = self.load_config()

    def load_config(self):
        """Load configuration from config.json or create it if missing."""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                # Corrupted config.json—create a fresh default
                return self.create_default_config()
        else:
            return self.create_default_config()

    def create_default_config(self):
        """Create default configuration file and return its contents."""
        default_config = {
            "general": {
                "download_path": "C:\\scraper_downloads\\",
                "max_storage_gb": 100,
                "parallel_downloads": 3,
                "delay_between_requests": 1000,
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            "download": {
                "download_method": "direct", 
                "idm_path": "C:\\Program Files (x86)\\Internet Download Manager\\idman.exe",
                "max_retries": 2,
                "connect_timeout_seconds": 10,
                "read_timeout_seconds": 120,
                "chunk_size": 8192,
                "verify_downloads": False
            },
            "scraping": {
                "start_from_last_page": True,
                "pages_per_batch": 10,
                "wait_time_ms": 1000,
                "max_concurrent_pages": 5,
                "skip_existing_files": True
            },
            "storage": {
                "create_subdirectories": True,
                "compress_json": False,
                "backup_progress": True,
                "cleanup_incomplete": False
            },
            "logging": {
                "log_level": "INFO",
                "log_to_file": True,
                "log_file_path": "scraper.log",
                "max_log_size_mb": 50
            },
            "validation": {
                "min_video_size_bytes": 1024,
                "min_thumbnail_size_bytes": 100,
                "required_json_fields": ["video_id"],
                "max_validation_retries": 1,
                "validation_delay_seconds": 1
            }
        }

        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2)
        return default_config


    def validate_config(self):
        """Validate configuration values are within acceptable ranges."""
        errors = []
        if self.config["general"]["max_storage_gb"] < 1:
            errors.append("max_storage_gb must be at least 1 GB")
        if self.config["download"]["timeout_seconds"] < 5:
            errors.append("timeout_seconds must be at least 5")
        if self.config["validation"]["min_video_size_bytes"] < 1024:
            errors.append("min_video_size_bytes must be at least 1024")
        return len(errors) == 0, errors

    def get_config(self):
        """Get the current loaded configuration."""
        return self.config
