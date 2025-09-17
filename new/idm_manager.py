"""
Fixed IDM (Internet Download Manager) Integration Module

This module provides robust IDM download management with proper directory structure 
and enhanced error handling. Fixes common issues with path handling, directory creation,
and IDM command execution.

Features:
- Creates organized directory structure for each video (video_id/video_id.ext)
- Robust IDM executable detection and path handling
- Enhanced error handling and logging
- Proper Windows path formatting
- Verified IDM command parameters
- Directory pre-creation to avoid IDM issues

Author: AI Assistant (Fixed Version)
Version: 2.0 - Fixed
"""

import os
import subprocess
import sys
import time
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import asyncio
import shutil

class FixedIDMManager:
    """
    Fixed Internet Download Manager automation class.
    Handles adding downloads to IDM queue with proper directory structure
    and robust error handling.
    """

    def __init__(self, base_download_dir: str = "downloads", idm_path: str = None):
        """
        Initialize Fixed IDM Manager.
        
        Args:
            base_download_dir: Base directory for downloads
            idm_path: Path to IDM executable (auto-detected if None)
        """
        self.base_download_dir = Path(base_download_dir).resolve()  # Use absolute path
        self.idm_path = self._find_idm_executable(idm_path)
        self.download_queue = []
        self.stats = {
            'total_videos': 0,
            'successful_additions': 0,
            'failed_additions': 0,
            'directories_created': 0
        }

        # Create base download directory immediately
        self._ensure_directory_exists(self.base_download_dir)
        
        print(f"🔧 Fixed IDM Manager Initialized")
        print(f"📁 Base download directory: {self.base_download_dir}")
        print(f"🎯 IDM executable: {self.idm_path}")
        
        # Verify IDM is accessible
        if not self._verify_idm_access():
            print("⚠️ WARNING: IDM may not be accessible. Downloads might fail.")

    def _find_idm_executable(self, idm_path: str = None) -> str:
        """
        Enhanced IDM executable detection with better error handling.
        
        Args:
            idm_path: Custom IDM path
            
        Returns:
            Path to IDM executable
        """
        if idm_path and os.path.exists(idm_path):
            print(f"✅ Using custom IDM path: {idm_path}")
            return idm_path

        # Common IDM installation paths (most common first)
        common_paths = [
            r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe",
            r"C:\Program Files\Internet Download Manager\IDMan.exe",
            r"C:\IDM\IDMan.exe",
            os.path.expanduser(r"~\AppData\Local\Programs\Internet Download Manager\IDMan.exe")
        ]

        for path in common_paths:
            if os.path.exists(path):
                print(f"✅ Found IDM at: {path}")
                return path

        # Try to find IDM using Windows 'where' command
        try:
            result = subprocess.run(
                ['where', 'IDMan.exe'], 
                capture_output=True, 
                text=True, 
                shell=True,
                timeout=10
            )
            if result.returncode == 0:
                idm_path = result.stdout.strip().split('\n')[0]
                print(f"✅ Found IDM in PATH: {idm_path}")
                return idm_path
        except (subprocess.TimeoutExpired, Exception) as e:
            print(f"⚠️ Error searching for IDM in PATH: {e}")

        # Try alternative search using dir command
        try:
            for drive in ['C:', 'D:', 'E:']:
                if os.path.exists(drive + "\\"):
                    result = subprocess.run(
                        ['dir', '/s', '/b', drive + '\\IDMan.exe'], 
                        capture_output=True, 
                        text=True, 
                        shell=True,
                        timeout=30
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        idm_path = result.stdout.strip().split('\n')[0]
                        print(f"✅ Found IDM via search: {idm_path}")
                        return idm_path
        except Exception as e:
            print(f"⚠️ Error during IDM search: {e}")

        print("❌ IDM executable not found automatically")
        print("💡 Please ensure IDM is installed or specify idm_path parameter")
        return "IDMan.exe"  # Fallback

    def _verify_idm_access(self) -> bool:
        """
        Verify that IDM is accessible and responds to commands.
        
        Returns:
            True if IDM is accessible, False otherwise
        """
        try:
            # Try to run IDM with help parameter
            result = subprocess.run(
                [self.idm_path, '/?'], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            if result.returncode == 0 or "Internet Download Manager" in result.stdout:
                print("✅ IDM access verified")
                return True
            else:
                print(f"⚠️ IDM responded with unexpected output: {result.stdout}")
                return False
        except Exception as e:
            print(f"⚠️ Could not verify IDM access: {e}")
            return False

    def _ensure_directory_exists(self, directory_path: Path) -> bool:
        """
        Ensure directory exists with proper permissions.
        
        Args:
            directory_path: Path to directory
            
        Returns:
            True if directory exists or was created successfully
        """
        try:
            directory_path.mkdir(parents=True, exist_ok=True)
            
            # Verify directory is writable
            test_file = directory_path / ".write_test"
            try:
                test_file.touch()
                test_file.unlink()
                return True
            except Exception as e:
                print(f"⚠️ Directory not writable: {directory_path} - {e}")
                return False
                
        except Exception as e:
            print(f"❌ Could not create directory {directory_path}: {e}")
            return False

    def create_video_directory(self, video_id: str) -> Path:
        """
        Create organized directory structure for a video with validation.
        
        Args:
            video_id: Unique video identifier (will be sanitized)
            
        Returns:
            Path to video directory
        """
        # Sanitize video_id for filesystem
        sanitized_id = self._sanitize_filename(video_id)
        video_dir = self.base_download_dir / sanitized_id
        
        if self._ensure_directory_exists(video_dir):
            self.stats['directories_created'] += 1
            print(f"📁 Created directory: {video_dir}")
        else:
            print(f"❌ Failed to create directory: {video_dir}")
            
        return video_dir

    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename for Windows filesystem compatibility.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove trailing dots and spaces
        filename = filename.strip('. ')
        
        # Limit length
        if len(filename) > 200:
            filename = filename[:200]
            
        return filename if filename else "unknown"

    def prepare_video_downloads(self, video_data: Dict) -> Dict[str, Dict]:
        """
        Prepare download information for a video with enhanced validation.
        
        Args:
            video_data: Video metadata dictionary
            
        Returns:
            Dictionary with download information
        """
        video_id = video_data.get('video_id', 'unknown')
        video_dir = self.create_video_directory(video_id)
        
        downloads = {}
        sanitized_id = self._sanitize_filename(video_id)

        # Prepare JSON metadata file
        json_path = video_dir / f"{sanitized_id}.json"
        downloads['json'] = {
            'type': 'metadata',
            'path': json_path,
            'data': video_data
        }

        # Prepare thumbnail download
        thumbnail_url = video_data.get('thumbnail_src', '')
        if thumbnail_url and thumbnail_url.strip():
            jpg_path = video_dir / f"{sanitized_id}.jpg"
            downloads['thumbnail'] = {
                'type': 'thumbnail',
                'url': thumbnail_url.strip(),
                'path': jpg_path
            }

        # Prepare video download
        video_url = video_data.get('video_src', '')
        if video_url and video_url.strip():
            mp4_path = video_dir / f"{sanitized_id}.mp4"
            downloads['video'] = {
                'type': 'video',
                'url': video_url.strip(),
                'path': mp4_path
            }

        return downloads

    def save_json_metadata(self, json_path: Path, video_data: Dict) -> bool:
        """
        Save video metadata as JSON file with error handling.
        
        Args:
            json_path: Path to save JSON file
            video_data: Video metadata
            
        Returns:
            Success status
        """
        try:
            # Ensure parent directory exists
            json_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_data, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Saved metadata: {json_path.name}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving metadata {json_path.name}: {e}")
            return False

    def add_to_idm_queue(self, url: str, local_path: Path, filename: str) -> bool:
        """
        Add download to IDM queue using optimized command parameters.
        
        Args:
            url: Download URL
            local_path: Local directory path  
            filename: Local filename
            
        Returns:
            Success status
        """
        try:
            # Ensure directory exists before IDM tries to download
            if not self._ensure_directory_exists(local_path):
                print(f"❌ Cannot create directory: {local_path}")
                return False

            # Convert to Windows-style path
            windows_path = str(local_path).replace('/', '\\')
            
            print(f"🚀 Adding to IDM queue: {filename}")
            print(f"   URL: {url[:80]}{'...' if len(url) > 80 else ''}")
            print(f"   Path: {windows_path}")

            # Build optimized IDM command
            cmd = [
                self.idm_path,
                '/d', url,                    # Download URL
                '/p', windows_path,           # Local path (Windows format)
                '/f', filename,               # Filename
                '/a',                         # Add to queue without starting
                '/n',                         # Silent mode (no dialog)
                '/q'                          # Quiet mode
            ]

            print(f"🖥️ IDM Command: {' '.join(cmd[:3])} ... (truncated)")

            # Execute IDM command with timeout
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=30,
                shell=False  # Don't use shell for security
            )

            if result.returncode == 0:
                print(f"✅ Successfully added to IDM queue: {filename}")
                return True
            else:
                print(f"❌ IDM command failed for {filename}")
                print(f"   Return code: {result.returncode}")
                if result.stdout:
                    print(f"   Stdout: {result.stdout}")
                if result.stderr:
                    print(f"   Stderr: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout adding {filename} to IDM queue (30s)")
            return False
        except FileNotFoundError:
            print(f"❌ IDM executable not found: {self.idm_path}")
            return False
        except Exception as e:
            print(f"❌ Error adding {filename} to IDM queue: {e}")
            return False

    def add_video_to_idm_queue(self, video_data: Dict) -> Dict[str, bool]:
        """
        Add all files for a video to IDM download queue.
        
        Args:
            video_data: Video metadata dictionary
            
        Returns:
            Dictionary with success status for each file type
        """
        video_id = video_data.get('video_id', 'unknown')
        title = video_data.get('title', 'Unknown')
        
        print(f"\n🎬 Processing video: {title} (ID: {video_id})")

        # Prepare downloads
        downloads = self.prepare_video_downloads(video_data)
        results = {'metadata': False, 'thumbnail': False, 'video': False}

        # 1. Save JSON metadata (always first)
        if 'json' in downloads:
            json_info = downloads['json']
            results['metadata'] = self.save_json_metadata(json_info['path'], json_info['data'])

        # 2. Add thumbnail to IDM queue
        if 'thumbnail' in downloads:
            thumb_info = downloads['thumbnail']
            success = self.add_to_idm_queue(
                thumb_info['url'],
                thumb_info['path'].parent,
                thumb_info['path'].name
            )
            results['thumbnail'] = success
            
            if success:
                self.download_queue.append({
                    'type': 'thumbnail',
                    'video_id': video_id,
                    'url': thumb_info['url'],
                    'path': thumb_info['path']
                })

        # 3. Add video to IDM queue
        if 'video' in downloads:
            video_info = downloads['video']
            success = self.add_to_idm_queue(
                video_info['url'],
                video_info['path'].parent,
                video_info['path'].name
            )
            results['video'] = success
            
            if success:
                self.download_queue.append({
                    'type': 'video',
                    'video_id': video_id,
                    'url': video_info['url'],
                    'path': video_info['path']
                })

        # Update stats
        if any(results.values()):
            self.stats['successful_additions'] += 1
        else:
            self.stats['failed_additions'] += 1

        return results

    def start_idm_queue(self) -> bool:
        """
        Start IDM download queue with enhanced error handling.
        
        Returns:
            Success status
        """
        try:
            print("\n🚀 Starting IDM download queue...")
            
            # Method 1: Try to start queue using /s parameter
            cmd = [self.idm_path, '/s']
            print(f"🖥️ Executing: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                print("✅ IDM queue started successfully!")
                return True
            else:
                print(f"⚠️ Method 1 failed (return code: {result.returncode})")
                print(f"   Stdout: {result.stdout}")
                print(f"   Stderr: {result.stderr}")
                
                # Method 2: Try alternative start method
                print("🔄 Trying alternative method...")
                cmd_alt = [self.idm_path, '/startqueue']
                result2 = subprocess.run(cmd_alt, capture_output=True, text=True, timeout=15)
                
                if result2.returncode == 0:
                    print("✅ IDM queue started with alternative method!")
                    return True
                else:
                    print(f"❌ Alternative method also failed")
                    print(f"💡 Please start the queue manually in IDM")
                    return False

        except subprocess.TimeoutExpired:
            print("⏰ Timeout starting IDM queue")
            return False
        except Exception as e:
            print(f"❌ Error starting IDM queue: {e}")
            return False

    def process_all_videos(self, videos_data: List[Dict], start_queue: bool = True) -> Dict:
        """
        Process all videos - add to IDM queue and optionally start downloads.
        
        Args:
            videos_data: List of video metadata dictionaries
            start_queue: Whether to start IDM queue after adding all downloads
            
        Returns:
            Processing results dictionary
        """
        if not videos_data:
            print("❌ No video data provided")
            return {"success": False, "error": "No video data provided"}

        print(f"🎯 Processing {len(videos_data)} videos for IDM download...")
        print(f"📁 Download directory: {self.base_download_dir}")
        print("="*80)

        # Reset stats
        self.stats = {
            'total_videos': len(videos_data),
            'successful_additions': 0,
            'failed_additions': 0,
            'directories_created': 0
        }

        # Process each video
        video_results = {}
        for i, video_data in enumerate(videos_data, 1):
            video_id = video_data.get('video_id', f'unknown_{i}')
            print(f"\n📋 Processing video {i}/{len(videos_data)}: {video_id}")
            
            try:
                results = self.add_video_to_idm_queue(video_data)
                video_results[video_id] = results
                
                # Show progress
                progress = (i / len(videos_data)) * 100
                print(f"📊 Progress: {i}/{len(videos_data)} videos ({progress:.1f}%)")
                
            except Exception as e:
                print(f"❌ Error processing video {video_id}: {e}")
                video_results[video_id] = {'metadata': False, 'thumbnail': False, 'video': False}
                self.stats['failed_additions'] += 1

        print("\n" + "="*80)
        print("📋 BATCH ADDITION COMPLETE!")
        self.print_stats()

        # Start IDM queue if requested
        queue_started = False
        if start_queue and len(self.download_queue) > 0:
            print("\n🚀 Starting IDM download queue...")
            queue_started = self.start_idm_queue()
            
            if queue_started:
                print("✅ All downloads added to IDM and queue started!")
            else:
                print("⚠️ Downloads added but failed to start queue automatically.")
                print("💡 Please start the queue manually in IDM.")
        elif len(self.download_queue) == 0:
            print("⚠️ No downloads were added to queue.")
        else:
            print("ℹ️ Downloads added to IDM queue but not started (start_queue=False)")

        return {
            'success': True,
            'total_videos': self.stats['total_videos'],
            'successful_additions': self.stats['successful_additions'],
            'failed_additions': self.stats['failed_additions'],
            'directories_created': self.stats['directories_created'],
            'download_queue_size': len(self.download_queue),
            'queue_started': queue_started,
            'video_results': video_results,
            'download_directory': str(self.base_download_dir)
        }

    def print_stats(self):
        """Print detailed processing statistics."""
        print(f"📊 STATISTICS:")
        print(f"   📁 Total videos: {self.stats['total_videos']}")
        print(f"   ✅ Successful additions: {self.stats['successful_additions']}")
        print(f"   ❌ Failed additions: {self.stats['failed_additions']}")
        print(f"   📂 Directories created: {self.stats['directories_created']}")
        print(f"   📥 Items in download queue: {len(self.download_queue)}")
        
        if self.stats['total_videos'] > 0:
            success_rate = (self.stats['successful_additions'] / self.stats['total_videos']) * 100
            print(f"   🎯 Success rate: {success_rate:.1f}%")

    def get_queue_info(self) -> Dict:
        """
        Get information about current download queue.
        
        Returns:
            Queue information dictionary
        """
        thumbnails = [item for item in self.download_queue if item['type'] == 'thumbnail']
        videos = [item for item in self.download_queue if item['type'] == 'video']
        
        return {
            'total_items': len(self.download_queue),
            'thumbnails': len(thumbnails),
            'videos': len(videos),
            'unique_videos': len(set(item['video_id'] for item in self.download_queue)),
            'queue_items': self.download_queue
        }

    def clear_queue(self):
        """Clear the download queue."""
        self.download_queue.clear()
        print("🧹 Download queue cleared")


# Fixed Integration class
class FixedVideoIDMProcessor:
    """
    Fixed complete video processing workflow that combines video parsing 
    with IDM download management.
    """

    def __init__(self, base_url: str, download_dir: str = "downloads", idm_path: str = None):
        """
        Initialize fixed complete video to IDM processor.
        
        Args:
            base_url: Base URL of video site
            download_dir: Directory for downloads
            idm_path: Path to IDM executable
        """
        self.base_url = base_url
        self.download_dir = download_dir

        # Import the existing parser
        try:
            from video_data_parser import OptimizedVideoDataParser
            self.parser = OptimizedVideoDataParser(base_url)
            print("✅ Video parser initialized")
        except ImportError as e:
            print(f"❌ Could not import video parser: {e}")
            print("   Please ensure main.py is in the same directory")
            self.parser = None

        # Initialize fixed IDM manager
        self.idm_manager = FixedIDMManager(download_dir, idm_path)
        print("✅ Fixed IDM manager initialized")

    async def process_all_videos(self) -> Dict:
        """
        Complete processing workflow: parse videos and add to IDM.
        
        Returns:
            Complete processing results
        """
        if not self.parser:
            return {"success": False, "error": "Video parser not available"}

        print(f"🎬 Starting fixed video processing workflow")
        print(f"🌐 Source URL: {self.base_url}")
        print(f"📁 Download directory: {self.download_dir}")
        print("="*80)

        try:
            # Step 1: Extract video URLs
            print("🔍 Step 1: Extracting video URLs...")
            video_urls = await self.parser.extract_video_urls()
            
            if not video_urls:
                return {"success": False, "error": "No video URLs found"}
            
            print(f"✅ Found {len(video_urls)} video URLs")

            # Step 2: Parse video metadata
            print("\n📝 Step 2: Parsing video metadata...")
            videos_data = await self.parser.parse_all_videos()
            
            if not videos_data:
                return {"success": False, "error": "No video metadata could be parsed"}
            
            print(f"✅ Successfully parsed {len(videos_data)} videos")

            # Step 3: Add to IDM queue and start downloads
            print("\n📥 Step 3: Adding videos to IDM queue...")
            idm_results = self.idm_manager.process_all_videos(videos_data, start_queue=True)

            # Combine results
            return {
                "success": True,
                "urls_found": len(video_urls),
                "videos_parsed": len(videos_data),
                "idm_results": idm_results,
                "download_directory": self.download_dir
            }

        except Exception as e:
            print(f"❌ Error in processing workflow: {e}")
            return {"success": False, "error": str(e)}


# Example usage and main function
async def main():
    """
    Example usage of the fixed IDM integration system.
    """
    # Configuration
    BASE_URL = "https://rule34video.com/latest-updates/100"  # Change to your target URL
    DOWNLOAD_DIR = "my_downloads"          # Directory to save files
    IDM_PATH = None                        # Auto-detect IDM

    print("🎬 Fixed Video to IDM Processor")
    print("=" * 60)
    print("🔧 FIXES APPLIED:")
    print("   - Enhanced IDM executable detection")
    print("   - Proper Windows path formatting")
    print("   - Directory pre-creation")
    print("   - Better error handling")
    print("   - Optimized IDM command parameters")
    print("=" * 60)

    try:
        # Create fixed processor
        processor = FixedVideoIDMProcessor(
            base_url=BASE_URL,
            download_dir=DOWNLOAD_DIR,
            idm_path=IDM_PATH
        )

        # Process all videos
        results = await processor.process_all_videos()

        # Print final results
        print("\n" + "="*80)
        print("🎯 FINAL RESULTS")
        print("="*80)
        
        if results.get("success"):
            print("✅ Processing completed successfully!")
            print(f"🔍 URLs found: {results.get('urls_found', 0)}")
            print(f"📝 Videos parsed: {results.get('videos_parsed', 0)}")
            
            idm_results = results.get('idm_results', {})
            print(f"✅ Successful IDM additions: {idm_results.get('successful_additions', 0)}")
            print(f"❌ Failed IDM additions: {idm_results.get('failed_additions', 0)}")
            print(f"📂 Directories created: {idm_results.get('directories_created', 0)}")
            print(f"📥 Queue items: {idm_results.get('download_queue_size', 0)}")
            print(f"🚀 Queue started: {idm_results.get('queue_started', False)}")
            print(f"📁 Download directory: {results.get('download_directory', 'Unknown')}")
            
        else:
            print("❌ Processing failed!")
            print(f"Error: {results.get('error', 'Unknown error')}")

        return results

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    """
    Run the fixed IDM integration system.
    
    Requirements:
    - Internet Download Manager installed on Windows
    - main.py (video parser) in same directory
    """
    print("🎬 Fixed Video to IDM Integration System")
    print("=" * 70)
    print("🔧 This version includes major fixes for:")
    print("   - IDM executable detection")
    print("   - Directory creation and path handling")
    print("   - IDM command parameters")
    print("   - Error handling and reporting")
    print("=" * 70)

    # Run the fixed workflow
    results = asyncio.run(main())

    if results and results.get("success"):
        print("\n✅ All done! Check IDM for download progress.")
        print("📁 Your files should appear in the specified directory structure.")
    else:
        print("\n❌ Process completed with errors.")
        print("💡 Check the error messages above for troubleshooting.")