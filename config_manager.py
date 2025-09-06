def create_default_config(self):
    """Create default configuration file and return its contents."""
    default_config = {
        "general": {
            "download_path": "C:\\scraper_downloads\\",
            "max_storage_gb": 100,
            "parallel_downloads": 3,
            "delay_between_requests": 2000,
            "delay_between_pages": 5000,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        },
        "download": {
            "download_method": "hybrid",  # "direct", "idm", "hybrid"
            "idm_path": "C:\\Program Files (x86)\\Internet Download Manager\\idman.exe",
            "max_retries": 3,
            "timeout_seconds": 60,
            "chunk_size": 8192,
            "verify_downloads": True
        },
        "scraping": {
            "start_from_last_page": True,
            "pages_per_batch": 10,
            "wait_time_ms": 1500,
            "max_concurrent_pages": 5,
            "skip_existing_files": True
        },
        "storage": {
            "create_subdirectories": True,
            "compress_json": False,
            "backup_progress": True,
            "cleanup_incomplete": True
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
            "max_validation_retries": 2,
            "validation_delay_seconds": 2
        },
        "processing": {
            "mode": "hybrid",
            "parallel_batch_size": 5,
            "use_parallel": True
        }
    }

    with open(self.config_path, 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=2)
    return default_config
