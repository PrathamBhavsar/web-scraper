# Video Scraper Project Documentation

## Project Overview

This is a comprehensive video scraping system designed to automatically download videos, metadata, and thumbnails from Rule34Video.com. The project is built with Python and provides both command-line and web-based user interfaces for non-technical users.

## Architecture & Components

### Core Modules

```
scraper/
├── __init__.py
├── config_loader.py      # Configuration management
├── detail_scraper.py     # Video metadata extraction
├── download_manager.py   # Download orchestration
├── listing_scraper.py    # Page listing and pagination
├── logger.py            # Logging system
├── manifest_manager.py   # Download queue management
├── progress_manager.py   # Progress tracking and resumption
├── storage_manager.py    # File validation and cleanup
├── utils.py             # Utility functions
└── validator.py         # File integrity validation
```

### Main Applications

- **`main.py`** - Core scraper engine
- **`app.py`** - Streamlit web dashboard
- **`start_dashboard.bat`** - One-click launcher for Windows

## How It Works

### 1. **Initial Discovery Phase**
```python
# Navigate to the website and find the last page
discovered_last_page = await find_last_page_number(page, base_url)
```
- Uses Playwright to navigate the website
- Automatically detects the last available page number
- Handles age verification modals

### 2. **Reverse Scraping Strategy** (Following README.md)
```python
# Start from last page and work backwards
current_page = discovered_last_page
while current_page >= 1:
    # Process pages in reverse order for stability
```
- **Why reverse?** New content appears on page 1, so starting from the end ensures we get older, more stable content first
- Follows the README.md specification for "last page towards first" strategy

### 3. **Batch Processing Workflow**

#### Step 1: Page Scraping
```python
# Scrape video links from multiple pages in a batch
for page_offset in range(pages_per_batch):
    scrape_page = current_page - page_offset
    video_links = await scrape_video_links_from_page(page, page_url)
    all_page_videos.extend(video_links)
```

#### Step 2: Metadata Extraction
```python
# Extract complete metadata and download URLs
for video in all_page_videos:
    video_data = await extract_video_data(page, video['detail_url'], video)
    if video_data and video_data.get('video_src'):
        processed_videos.append(video)
```

#### Step 3: Download Processing
```python
# Download with complete folder structure
success = await download_single_video_complete(
    video, download_root, logger, manifest_mgr, progress_mgr, cfg
)
```

#### Step 4: Automatic Cleanup
```python
# Clean up incomplete folders after each batch
cleanup_stats = cleanup_incomplete_folders(download_root, logger)
```

## File Structure & Validation (README.md Compliant)

### Storage Structure
```
downloads/
├── [video_id]/
│   ├── [video_id].json    # Complete metadata
│   ├── [video_id].mp4     # High-quality video
│   └── [video_id].jpg     # Thumbnail image
├── progress.json          # Scraper state
└── video_manifest.json    # Download queue
```

### Validation Process (Following README.md)

#### 1. **Video Download & Validation**
```python
# Download to temporary file first
temp_path = video_folder / f"{video_filename}.tmp"
# ... download process ...

# Validate MP4 integrity
if not basic_mp4_check(str(video_path)):
    logger.error(f"Invalid MP4 file for {video_id}")
    video_path.unlink()  # Delete corrupted file
    return False
```

#### 2. **Structure Creation (Only After Validation)**
```python
# Only create structure if video is valid
if all([
    (video_folder / f"{video_id}.mp4").exists(),
    (video_folder / f"{video_id}.json").exists(),
    (video_folder / f"{video_id}.jpg").exists()
]):
    # Mark as completed
    manifest_mgr.mark_video_completed(video_id, metadata)
```

#### 3. **Prevention of Corrupted Files**
```python
def cleanup_incomplete_folders(download_root: Path, logger=None):
    """Clean up incomplete/empty folders"""
    for folder in video_folders:
        validation = validate_video_folder_structure(folder, video_id)
        
        if not validation['is_complete']:
            # Delete incomplete folders
            shutil.rmtree(folder)
```

## Features & Advantages

### 🚀 **Core Features**

1. **Intelligent Resumption**
   - Automatically resumes from where it left off
   - No duplicate downloads
   - Persistent progress tracking

2. **Complete File Validation**
   - MP4 integrity checking using magic bytes
   - JSON metadata validation
   - Automatic cleanup of corrupted files

3. **Storage Management**
   - Configurable storage limits
   - Real-time space monitoring
   - Automatic stop when limit reached

4. **Batch Processing**
   - Process multiple pages simultaneously
   - Configurable batch sizes
   - Optimized for stability and speed

### 🎯 **Advanced Advantages**

#### **Robust Error Handling**
```python
try:
    # Download process
except Exception as e:
    logger.error(f"Error downloading {video_id}: {e}")
    manifest_mgr.mark_video_failed(video_id, str(e))
    # Cleanup temp files
```

#### **Smart Folder Structure**
- Uses actual video IDs from website URLs
- Consistent naming: `video_id/video_id.mp4`
- Complete metadata preservation

#### **Real-time Monitoring**
- Live web dashboard with Streamlit
- Progress visualization with charts
- Storage usage gauges
- Log streaming

#### **Non-technical User Friendly**
- One-click startup with `.bat` file
- Web-based configuration
- Visual progress indicators
- No command-line knowledge required

## Configuration System (README.md Compliant)

### config.json Structure
```json
{
  "general": {
    "base_url": "https://rule34video.com/latest-updates/",
    "download_path": "downloads",
    "max_storage_gb": 1,
    "user_agent": "Mozilla/5.0..."
  },
  "download": {
    "download_method": "direct",
    "max_retries": 3,
    "timeout_seconds": 120,
    "ef2_batch_size": 3
  },
  "scraping": {
    "pages_per_batch": 2,
    "max_pages": 10,
    "wait_time_ms": 3000
  },
  "logging": {
    "log_file_path": "logs/scraper.log",
    "log_level": "INFO"
  }
}
```

## Data Extraction (Following README.md Specifications)

### Complete Metadata Extraction
```python
metadata = {
    "video_id": video_data.get("video_id", ""),
    "url": detail_url,
    "title": "",
    "duration": "",
    "views": "0",
    "upload_date": int(datetime.now().timestamp() * 1000),
    "tags": [],
    "video_src": "",          # Maximum quality MP4
    "thumbnail_src": "",       # Cover image
    "uploaded_by": "",
    "description": "",
    "categories": [],
    "artists": [],
    "download_sources": []     # Multiple quality options
}
```

### Quality Selection
```python
# Automatically select highest quality
if download_sources:
    best_source = max(download_sources, key=lambda x: x.get('quality_score', 0))
    metadata["video_src"] = best_source["url"]
```

## Space Management & Continuity (README.md Compliant)

### Storage Monitoring
```python
# Continuous space monitoring
current_size_gb = total_size_gb(download_root)
if current_size_gb >= cfg['general']['max_storage_gb']:
    logger.info(f"Storage limit reached ({current_size_gb:.2f}GB). Stopping.")
    break
```

### Progress Persistence
```python
# Automatic checkpoint saving
progress_mgr.update_current_page(current_page)
progress_mgr.mark_video_downloaded(video_id, file_path, file_size_mb)
```

### Smart Resumption
```python
# Resume from last completed page
last_scraped_page = progress_mgr.get_last_scraped_page()
current_page = max(1, last_scraped_page - 1) if last_scraped_page > 0 else discovered_last_page
```

## Web Dashboard Features

### Real-time Monitoring
- **Live Statistics**: Downloaded count, storage usage, current page
- **Progress Charts**: Success/failure rates with interactive pie charts
- **Storage Gauge**: Visual storage usage with warning thresholds
- **Folder Analysis**: Complete/incomplete folder statistics

### Control Panel
- **Start/Stop Scraper**: One-click process control
- **Configuration Management**: Web-based settings adjustment
- **Cleanup Tools**: Remove incomplete folders
- **Reset Functionality**: Clear progress and manifest files

### Advanced Features
- **Auto-refresh**: Real-time dashboard updates every 10 seconds
- **Live Logs**: Streaming log viewer with last 30 entries
- **Process Detection**: Automatic scraper status detection
- **Error Handling**: Graceful error recovery and reporting

## Technical Advantages

### 🛡️ **Reliability**
1. **Atomic Operations**: Complete download or nothing
2. **Rollback Capability**: Failed downloads don't leave corrupted files
3. **Process Recovery**: Handles crashes and interruptions gracefully

### ⚡ **Performance**
1. **Batch Processing**: Multiple pages processed simultaneously
2. **Async Downloads**: Non-blocking I/O operations
3. **Smart Queuing**: Efficient video processing pipeline

### 🔧 **Maintainability**
1. **Modular Design**: Each component has single responsibility
2. **Comprehensive Logging**: Full audit trail of operations
3. **Configuration-driven**: Easy to modify behavior without code changes

### 🎯 **User Experience**
1. **Zero Setup**: Works out of the box with defaults
2. **Visual Feedback**: Real-time progress and statistics
3. **Error Recovery**: Automatic retry mechanisms
4. **Non-destructive**: Never loses downloaded content

## Compliance with README.md Specifications

✅ **Reverse Scraping**: Last page to first  
✅ **Sequential Processing**: Videos processed one by one  
✅ **Direct Download**: Native Python implementation  
✅ **Complete Validation**: MP4 integrity + JSON validation  
✅ **Storage Structure**: `[video_id]/` folders with proper naming  
✅ **Space Management**: Configurable limits with monitoring  
✅ **Progress Persistence**: Automatic resumption capability  
✅ **Cleanup System**: Prevents corrupted files  
✅ **Detailed Logging**: INFO level with rotation  
✅ **Windows Platform**: Optimized for Windows with `.bat` launchers  

This project successfully implements all specifications from the README.md while adding modern conveniences like web-based monitoring and automatic cleanup systems.