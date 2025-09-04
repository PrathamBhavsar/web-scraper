# Web Scraper Specifications

## Workflow

1. **Initial Navigation**
   - Access the target website
   - Navigate to the last available page

2. **Scraping Strategy**
   - Start the process from the last page towards the first (reverse order)
   - Process videos sequentially to maintain stability

## Execution Platform

**Operating System:** Windows

**Implementation Options:**
- **Python**: Using libraries like Selenium, BeautifulSoup, or Playwright
- **JavaScript**: Implementation with Puppeteer for browser automation

## Download Methods

The system will automatically select the download method based on the established configuration:

### 1. Direct Download
- **Activation**: When `download_method` = "direct" in configuration
- Native implementation within the scraper
- Full control over the download process
- Custom headers and cookies handling
- Automatic retries in case of failure

### 2. Internet Download Manager (IDM)
- **Activation**: When `download_method` = "idm" in configuration
- **Requirement**: IDM must be installed on the Windows system
- IDM integration via command line: `idman.exe /d [URL] /p [PATH] /f [FILENAME]`
- Leverages IDM's acceleration capabilities
- Support for bulk downloads and automatic recovery
- Prior verification of IDM installation

### 3. Hybrid Mode
- **Activation**: When `download_method` = "hybrid" in configuration
- First attempts with IDM, if it fails switches to direct download
- Best of both worlds

                        ## Data to Extract

                        - Complete information according to the fields shown in the reference image
                        - Video in MP4 format (maximum available quality)
                        - Video cover image (thumbnail)

## Storage Structure

                        ```
                        scraper/
                        ├── [video_id]/
                        │   ├── [video_id].json    # Extracted metadata
                        │   ├── [video_id].mp4     # High-quality video
                        │   └── [video_id].jpg     # Cover image
                        ├── progress.json          # Scraper state
                        └── config.json            # System configuration
                        ```

## Download Validation

### Verification Process
                    1. **Video Download**: First the MP4 file is downloaded     
                    2. **Integrity Verification**: Validates that the video was downloaded completely
                    - File size verification
                    - MP4 format validation
   - Basic playability check
3. **Structure Creation**: Only after validating the video:
                    - The `[video_id]/` folder is created
                    - The validated video is moved to the folder
                    - Metadata and cover are downloaded
                    - JSON file with information is created

### Prevention of Corrupted Files
                    - **No empty folders created**: Without a valid video, no structure is created
                    - **Automatic cleanup**: Temporary files are deleted if validation fails
- **Detailed logs**: Record of all validation failures
- **Smart retries**: Only retries downloads that failed validation

## Space Management and Continuity

### Space Limiter
- **Continuous monitoring**: Verification of total weight of downloaded files
- **Configurable limit**: User can set maximum storage limit
- **Automatic stop**: Scraper automatically stops when reaching the limit
- **Alerts**: Notifications when approaching the established limit

### Continuity System
- **Automatic checkpoint**: Saves progress after each successful download
- **Smart resumption**: When changing path, automatically detects where it left off
- **State file**: `progress.json` maintains record of:
  - Last processed page
  - Videos already downloaded
  - Last execution timestamp
- **Key functionality**: If the scraper stops or is manually stopped, the next start will automatically resume from the last valid completed download, without duplicating content or losing progress

### Path Configuration
```json
{
  "download_path": "C:\\scraper_downloads\\",
  "max_storage_gb": 100,
  "last_video_id": "video_12345",
  "last_page": 847,
  "total_downloaded": "45.2 GB"
}
```

## Basic System Configuration

### config.json File
```json
{
  "general": {
    "download_path": "C:\\scraper_downloads\\",
    "max_storage_gb": 100,
    "parallel_downloads": 3,
    "delay_between_requests": 2000,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
  },
  "download": {
    "download_method": "hybrid",
    "idm_path": "C:\\Program Files (x86)\\Internet Download Manager\\idman.exe",
    "max_retries": 3,
    "timeout_seconds": 30,
    "chunk_size": 8192,
    "verify_downloads": true
  },
  "scraping": {
    "start_from_last_page": true,
    "pages_per_batch": 10,
    "wait_time_ms": 1500,
    "max_concurrent_pages": 5,
    "skip_existing_files": true
  },
  "storage": {
    "create_subdirectories": true,
    "compress_json": false,
    "backup_progress": true,
    "cleanup_incomplete": true
  },
  "logging": {
    "log_level": "INFO",
    "log_to_file": true,
    "log_file_path": "scraper.log",
    "max_log_size_mb": 50
  }
}
```

### Default Configurations
- **Download method**: Hybrid (IDM + direct fallback)
- **Parallel downloads**: 3 simultaneous
- **Storage limit**: 100 GB
- **Time between requests**: 2 seconds
- **Maximum retries**: 3 per file
- **Download verification**: Enabled
- **Logs**: INFO level with automatic rotation

## Technical Considerations

- **Sequential processing**: Videos processed one by one for greater stability
- **Error management**: Robust handling of connection failures and timeouts
- **Rate limiting**: Respect speed limits to avoid blocking
- **Mandatory validation**: Integrity verification before creating file structure
- **Detailed logs**: Complete activity record for debugging
- **User interface**: Control panel to monitor progress and configure parameters