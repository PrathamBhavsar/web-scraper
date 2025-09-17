#!/usr/bin/env python3

"""
Updated IDM Manager with Enhanced Progress Tracking

This version integrates the enhanced progress tracking system to ensure
progress.json is updated accurately ONLY after downloads are verified.

Key Changes:
- Updates progress.json ONLY after downloads are completed
- Monitors downloads folder for actual completion
- Calculates real folder sizes
- Does NOT update progress before IDM queue addition
- Provides post-download verification and updates

Author: AI Assistant
Version: 4.0 - Enhanced progress tracking integration
"""

import os
import subprocess
import sys
import time
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import asyncio
import shutil
from duplicate_detection import DuplicateDetectionManager
from progress_tracking import EnhancedProgressTracker


class ProgressTrackingIDMManager:
    """
    IDM Manager with integrated progress tracking that updates progress.json
    ONLY after downloads are actually completed and verified.
    """

    def __init__(self, base_download_dir: str = "downloads", idm_path: str = None, 
                 enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                 progress_file: str = "progress.json"):
        """
        Initialize Progress Tracking IDM Manager.

        Args:
            base_download_dir: Base directory for downloads
            idm_path: Path to IDM executable (auto-detected if None)
            enable_duplicate_detection: Enable duplicate video detection
            duplicate_check_limit: Max number of recent downloads to check against
            progress_file: Path to progress.json file
        """
        self.base_download_dir = Path(base_download_dir).resolve()
        self.idm_path = self._find_idm_executable(idm_path)
        self.progress_file = progress_file
        self.download_queue = []

        # Initialize enhanced progress tracker
        self.progress_tracker = EnhancedProgressTracker(progress_file, base_download_dir)

        # Statistics tracking
        self.stats = {
            "total_videos": 0,
            "successful_additions": 0,
            "failed_additions": 0,
            "directories_created": 0,
            "videos_filtered_by_duplicates": 0,
            "videos_passed_duplicate_check": 0,
            "progress_updates": 0,
            "verified_completions": 0
        }

        # Initialize duplicate detection if enabled
        self.duplicate_detection_enabled = enable_duplicate_detection
        if self.duplicate_detection_enabled:
            self.duplicate_manager = DuplicateDetectionManager(
                progress_file=progress_file,
                check_limit=duplicate_check_limit
            )
            self.duplicate_manager.initialize_session()
        else:
            self.duplicate_manager = None

        # Ensure base directory exists
        self._ensure_directory_exists(self.base_download_dir)

        print(f"🚀 Progress Tracking IDM Manager Initialized")
        print(f"   📁 Base download directory: {self.base_download_dir}")
        print(f"   🔧 IDM executable: {self.idm_path}")
        print(f"   📄 Progress file: {self.progress_file}")
        print(f"   🔍 Duplicate detection: {'Enabled' if self.duplicate_detection_enabled else 'Disabled'}")
        print(f"   📊 Progress tracking: Enhanced with download verification")

        if not self._verify_idm_access():
            print("⚠️  WARNING: IDM may not be accessible. Downloads might fail.")

    def _find_idm_executable(self, idm_path: str = None) -> str:
        """Find IDM executable with enhanced detection."""
        if idm_path and os.path.exists(idm_path):
            print(f"✅ Using custom IDM path: {idm_path}")
            return idm_path

        # Common installation paths
        common_paths = [
            r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe",
            r"C:\Program Files\Internet Download Manager\IDMan.exe",
            r"C:\IDM\IDMan.exe",
            os.path.expanduser(r"~\Internet Download Manager\IDMan.exe")
        ]

        for path in common_paths:
            if os.path.exists(path):
                print(f"✅ Found IDM at: {path}")
                return path

        # Try Windows where command
        try:
            result = subprocess.run(
                ["where", "IDMan.exe"], 
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
            print(f"⚠️  Error searching for IDM in PATH: {e}")

        print("❌ IDM executable not found automatically")
        print("💡 Please ensure IDM is installed or specify idm_path parameter")
        return "IDMan.exe"  # Fallback

    def _verify_idm_access(self) -> bool:
        """Verify IDM is accessible."""
        try:
            result = subprocess.run(
                [self.idm_path, "/?"], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            if result.returncode == 0 or "Internet Download Manager" in result.stdout:
                print("✅ IDM access verified")
                return True
            else:
                print(f"⚠️  IDM responded with unexpected output: {result.stdout}")
                return False
        except Exception as e:
            print(f"⚠️  Could not verify IDM access: {e}")
            return False

    def _ensure_directory_exists(self, directory_path: Path) -> bool:
        """Ensure directory exists with proper permissions."""
        try:
            directory_path.mkdir(parents=True, exist_ok=True)

            # Test write permissions
            test_file = directory_path / ".write_test"
            try:
                test_file.touch()
                test_file.unlink()
                return True
            except Exception as e:
                print(f"⚠️  Directory not writable {directory_path}: {e}")
                return False

        except Exception as e:
            print(f"❌ Could not create directory {directory_path}: {e}")
            return False

    def _create_video_directory(self, video_id: str) -> Path:
        """Create organized directory structure for a video."""
        sanitized_id = self._sanitize_filename(video_id)
        video_dir = self.base_download_dir / sanitized_id

        if self._ensure_directory_exists(video_dir):
            self.stats["directories_created"] += 1
            print(f"📁 Created directory: {video_dir}")
        else:
            print(f"❌ Failed to create directory: {video_dir}")

        return video_dir

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for Windows filesystem compatibility."""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')

        filename = filename.strip('. ')
        if len(filename) > 200:
            filename = filename[:200]

        return filename if filename else "unknown"

    def _apply_duplicate_filtering(self, videos_data: List[Dict], current_page: int) -> List[Dict]:
        """Apply duplicate detection filtering to video data."""
        if not self.duplicate_detection_enabled or not self.duplicate_manager:
            print("🔍 Duplicate detection disabled - processing all videos")
            return videos_data

        print(f"\n🔍 Applying duplicate detection for page {current_page}...")

        # Filter videos using duplicate detection manager
        filtered_videos = self.duplicate_manager.filter_videos_for_page(videos_data, current_page)

        # Update statistics
        original_count = len(videos_data)
        filtered_count = len(filtered_videos)
        duplicates_found = original_count - filtered_count

        self.stats["videos_filtered_by_duplicates"] += duplicates_found
        self.stats["videos_passed_duplicate_check"] += filtered_count

        print(f"✅ Duplicate filtering completed:")
        print(f"   📥 Original videos: {original_count}")
        print(f"   ✅ Videos to process: {filtered_count}")
        print(f"   🚫 Duplicates filtered: {duplicates_found}")

        return filtered_videos

    def _prepare_video_downloads(self, video_data: Dict) -> Dict[str, Dict]:
        """Prepare download information for a video."""
        video_id = video_data.get("video_id", "unknown")
        video_dir = self._create_video_directory(video_id)

        downloads = {}
        sanitized_id = self._sanitize_filename(video_id)

        # JSON metadata file
        json_path = video_dir / f"{sanitized_id}.json"
        downloads["json"] = {
            "type": "metadata",
            "path": json_path,
            "data": video_data
        }

        # Thumbnail download
        thumbnail_url = video_data.get("thumbnail_src", "")
        if thumbnail_url and thumbnail_url.strip():
            jpg_path = video_dir / f"{sanitized_id}.jpg"
            downloads["thumbnail"] = {
                "type": "thumbnail",
                "url": thumbnail_url.strip(),
                "path": jpg_path
            }

        # Video download
        video_url = video_data.get("video_src", "")
        if video_url and video_url.strip():
            mp4_path = video_dir / f"{sanitized_id}.mp4"
            downloads["video"] = {
                "type": "video",
                "url": video_url.strip(),
                "path": mp4_path
            }

        return downloads

    def _save_json_metadata(self, json_path: Path, video_data: Dict) -> bool:
        """Save video metadata as JSON file."""
        try:
            json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Saved metadata: {json_path.name}")
            return True
        except Exception as e:
            print(f"❌ Error saving metadata {json_path.name}: {e}")
            return False

    def _add_to_idm_queue(self, url: str, local_path: Path, filename: str) -> bool:
        """Add download to IDM queue."""
        try:
            if not self._ensure_directory_exists(local_path):
                print(f"❌ Cannot create directory: {local_path}")
                return False

            windows_path = str(local_path).replace('/', '\\')

            print(f"📥 Adding to IDM queue: {filename}")
            print(f"   🌐 URL: {url[:80]}{'...' if len(url) > 80 else ''}")
            print(f"   📁 Path: {windows_path}")

            cmd = [
                self.idm_path,
                "/d", url,
                "/p", windows_path,
                "/f", filename,
                "/a",  # Add to queue
                "/n",  # Silent mode
                "/q"   # Quiet mode
            ]

            print(f"🔧 IDM Command: {' '.join(cmd[:3])} ... (truncated)")

            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=30, 
                shell=False
            )

            if result.returncode == 0:
                print(f"✅ Successfully added to IDM queue: {filename}")
                return True
            else:
                print(f"❌ IDM command failed for {filename}")
                print(f"   📄 Return code: {result.returncode}")
                if result.stdout:
                    print(f"   📄 Stdout: {result.stdout}")
                if result.stderr:
                    print(f"   📄 Stderr: {result.stderr}")
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

    def _add_video_to_idm_queue(self, video_data: Dict) -> Dict[str, bool]:
        """Add all files for a video to IDM download queue."""
        video_id = video_data.get("video_id", "unknown")
        title = video_data.get("title", "Unknown")

        print(f"\n🎬 Processing video: '{title}' (ID: {video_id})")

        downloads = self._prepare_video_downloads(video_data)
        results = {"metadata": False, "thumbnail": False, "video": False}

        # 1. Save JSON metadata (always first)
        if "json" in downloads:
            json_info = downloads["json"]
            results["metadata"] = self._save_json_metadata(json_info["path"], json_info["data"])

        # 2. Add thumbnail to IDM queue
        if "thumbnail" in downloads:
            thumb_info = downloads["thumbnail"]
            success = self._add_to_idm_queue(
                thumb_info["url"], 
                thumb_info["path"].parent, 
                thumb_info["path"].name
            )
            results["thumbnail"] = success
            if success:
                self.download_queue.append({
                    "type": "thumbnail",
                    "video_id": video_id,
                    "url": thumb_info["url"],
                    "path": thumb_info["path"]
                })

        # 3. Add video to IDM queue
        if "video" in downloads:
            video_info = downloads["video"]
            success = self._add_to_idm_queue(
                video_info["url"], 
                video_info["path"].parent, 
                video_info["path"].name
            )
            results["video"] = success
            if success:
                self.download_queue.append({
                    "type": "video",
                    "video_id": video_id,
                    "url": video_info["url"],
                    "path": video_info["path"]
                })

        # Update stats
        if any(results.values()):
            self.stats["successful_additions"] += 1
        else:
            self.stats["failed_additions"] += 1

        return results

    def _start_idm_queue(self) -> bool:
        """Start IDM download queue."""
        try:
            print("🚀 Starting IDM download queue...")

            cmd = [self.idm_path, "/s"]
            print(f"🔧 Executing: {' '.join(cmd)}")

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

            if result.returncode == 0:
                print("✅ IDM queue started successfully!")
                return True
            else:
                print(f"⚠️  Method 1 failed (return code {result.returncode})")
                print(f"📄 Stdout: {result.stdout}")
                print(f"📄 Stderr: {result.stderr}")

                # Try alternative method
                print("🔄 Trying alternative method...")
                cmd_alt = [self.idm_path, "/startqueue"]
                result2 = subprocess.run(cmd_alt, capture_output=True, text=True, timeout=15)

                if result2.returncode == 0:
                    print("✅ IDM queue started with alternative method!")
                    return True
                else:
                    print("❌ Alternative method also failed")
                    print("💡 Please start the queue manually in IDM")
                    return False

        except subprocess.TimeoutExpired:
            print("⏰ Timeout starting IDM queue")
            return False
        except Exception as e:
            print(f"❌ Error starting IDM queue: {e}")
            return False

    def update_progress_after_completion(self, wait_time: int = 30) -> Dict[str, Any]:
        """
        Wait for downloads to complete and update progress.json with verified data.

        Args:
            wait_time: Time to wait before checking for completion (seconds)

        Returns:
            Progress update results
        """
        print(f"\n⏳ Waiting {wait_time} seconds for downloads to process...")
        time.sleep(wait_time)

        print("\n🔍 CHECKING DOWNLOAD COMPLETION AND UPDATING PROGRESS")
        print("=" * 70)

        # Sync progress with actual download folder contents
        updated_progress = self.progress_tracker.sync_progress_with_downloads()
        self.stats["progress_updates"] += 1

        # Get verification stats
        verification = self.progress_tracker.verify_and_fix_progress()
        if verification["verification_passed"]:
            self.stats["verified_completions"] += 1

        print("=" * 70)
        print("✅ Progress update completed")

        return {
            "updated_progress": updated_progress,
            "verification_results": verification,
            "stats_updated": True
        }

    def process_all_videos(self, videos_data: List[Dict], start_queue: bool = True, 
                          current_page: Optional[int] = None, 
                          wait_for_completion: bool = True,
                          completion_wait_time: int = 30) -> Dict:
        """
        Process all videos with enhanced progress tracking.

        Args:
            videos_data: List of video metadata dictionaries
            start_queue: Whether to start IDM queue after adding downloads
            current_page: Current page being processed (for duplicate detection)
            wait_for_completion: Whether to wait and update progress after completion
            completion_wait_time: Time to wait before checking completion

        Returns:
            Processing results dictionary
        """
        if not videos_data:
            return {"success": False, "error": "No video data provided"}

        print(f"\n🎬 Processing {len(videos_data)} videos with progress tracking...")
        print(f"📁 Download directory: {self.base_download_dir}")
        print(f"📄 Progress file: {self.progress_file}")
        print("="*80)

        # Apply duplicate detection filtering
        if current_page is not None:
            videos_data = self._apply_duplicate_filtering(videos_data, current_page)

            if not videos_data:
                print("🔍 All videos were filtered as duplicates - nothing to process")
                return {
                    "success": True,
                    "total_videos": 0,
                    "videos_filtered_as_duplicates": len(videos_data),
                    "message": "All videos were duplicates"
                }

        # Reset stats for this batch
        self.stats.update({
            "total_videos": len(videos_data),
            "successful_additions": 0,
            "failed_additions": 0,
            "directories_created": 0
        })

        # Process each video (ADD TO IDM QUEUE - DO NOT UPDATE PROGRESS YET)
        video_results = {}

        for i, video_data in enumerate(videos_data, 1):
            video_id = video_data.get("video_id", f"unknown_{i}")
            print(f"\n📹 Processing video {i}/{len(videos_data)}: {video_id}")

            try:
                results = self._add_video_to_idm_queue(video_data)
                video_results[video_id] = results

                # Show progress
                progress = (i / len(videos_data)) * 100
                print(f"📊 Progress: {i}/{len(videos_data)} videos ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error processing video {video_id}: {e}")
                video_results[video_id] = {"metadata": False, "thumbnail": False, "video": False}
                self.stats["failed_additions"] += 1

        print("\n" + "="*80)
        print("📋 BATCH ADDITION TO IDM COMPLETE!")
        print("⚠️  PROGRESS.JSON NOT UPDATED YET - WAITING FOR DOWNLOADS")
        self._print_stats()

        # Start IDM queue if requested
        queue_started = False
        if start_queue and len(self.download_queue) > 0:
            print("\n🚀 Starting IDM download queue...")
            queue_started = self._start_idm_queue()

            if queue_started:
                print("✅ All downloads added to IDM and queue started!")
                print("⏳ Downloads are now processing...")
            else:
                print("⚠️  Downloads added but failed to start queue automatically.")
                print("💡 Please start the queue manually in IDM.")
        elif len(self.download_queue) == 0:
            print("📝 No downloads were added to queue.")
        else:
            print("📝 Downloads added to IDM queue but not started (start_queue=False)")

        # Wait for completion and update progress if requested
        progress_update_results = None
        if wait_for_completion and queue_started:
            progress_update_results = self.update_progress_after_completion(completion_wait_time)
        else:
            print("\n📝 Skipping automatic progress update")
            print("💡 Run update_progress_after_completion() manually when downloads finish")

        # Prepare final results
        results = {
            "success": True,
            "total_videos": self.stats["total_videos"],
            "successful_additions": self.stats["successful_additions"],
            "failed_additions": self.stats["failed_additions"],
            "directories_created": self.stats["directories_created"],
            "videos_filtered_by_duplicates": self.stats["videos_filtered_by_duplicates"],
            "videos_passed_duplicate_check": self.stats["videos_passed_duplicate_check"],
            "download_queue_size": len(self.download_queue),
            "queue_started": queue_started,
            "video_results": video_results,
            "download_directory": str(self.base_download_dir),
            "progress_update_results": progress_update_results,
            "wait_for_completion": wait_for_completion
        }

        # Add duplicate detection summary if enabled
        if self.duplicate_detection_enabled and self.duplicate_manager:
            results["duplicate_detection_summary"] = self.duplicate_manager.get_session_summary()

        return results

    def _print_stats(self):
        """Print detailed processing statistics."""
        print(f"📊 STATISTICS:")
        print(f"   🎬 Total videos: {self.stats['total_videos']}")
        print(f"   ✅ Successful additions: {self.stats['successful_additions']}")
        print(f"   ❌ Failed additions: {self.stats['failed_additions']}")
        print(f"   📁 Directories created: {self.stats['directories_created']}")
        print(f"   📥 Items in download queue: {len(self.download_queue)}")
        print(f"   📊 Progress updates: {self.stats['progress_updates']}")
        print(f"   ✅ Verified completions: {self.stats['verified_completions']}")

        if self.duplicate_detection_enabled:
            print(f"   🔍 Videos filtered by duplicates: {self.stats['videos_filtered_by_duplicates']}")
            print(f"   ✅ Videos passed duplicate check: {self.stats['videos_passed_duplicate_check']}")

        if self.stats["total_videos"] > 0:
            success_rate = (self.stats["successful_additions"] / self.stats["total_videos"]) * 100
            print(f"   📈 Success rate: {success_rate:.1f}%")

    def manual_progress_update(self) -> Dict[str, Any]:
        """
        Manually trigger progress update based on download folder contents.

        Returns:
            Progress update results
        """
        print("\n🔄 Manual progress update triggered...")
        return self.update_progress_after_completion(wait_time=0)

    def get_progress_summary(self) -> Dict[str, Any]:
        """Get comprehensive progress summary."""
        return self.progress_tracker.get_progress_summary()

    def verify_progress_accuracy(self) -> Dict[str, Any]:
        """Verify and fix progress accuracy."""
        return self.progress_tracker.verify_and_fix_progress()


# Integration class with progress tracking
class ProgressTrackingVideoIDMProcessor:
    """
    Complete video processing workflow with enhanced progress tracking.
    """

    def __init__(self, base_url: str, download_dir: str = "downloads", idm_path: str = None,
                 enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                 progress_file: str = "progress.json"):
        """
        Initialize complete video to IDM processor with progress tracking.
        """
        self.base_url = base_url
        self.download_dir = download_dir
        self.progress_file = progress_file

        # Extract page number from URL for duplicate detection
        self.current_page = self._extract_page_from_url(base_url)

        # Initialize video parser
        try:
            from video_data_parser import OptimizedVideoDataParser
            self.parser = OptimizedVideoDataParser(base_url)
            print("✅ Video parser initialized")
        except ImportError as e:
            print(f"❌ Could not import video parser: {e}")
            print("💡 Please ensure video_data_parser.py is in the same directory")
            self.parser = None

        # Initialize progress tracking IDM manager
        self.idm_manager = ProgressTrackingIDMManager(
            download_dir, 
            idm_path, 
            enable_duplicate_detection=enable_duplicate_detection,
            duplicate_check_limit=duplicate_check_limit,
            progress_file=progress_file
        )
        print("✅ Progress Tracking IDM manager initialized")

    def _extract_page_from_url(self, url: str) -> Optional[int]:
        """Extract page number from URL for duplicate detection context."""
        try:
            parts = url.rstrip('/').split('/')
            if parts and parts[-1].isdigit():
                page_num = int(parts[-1])
                print(f"📄 Extracted page number from URL: {page_num}")
                return page_num
        except Exception as e:
            print(f"⚠️  Could not extract page number from URL: {e}")

        return None

    async def process_all_videos(self, wait_for_completion: bool = True, 
                               completion_wait_time: int = 30) -> Dict:
        """
        Complete processing workflow with progress tracking.

        Args:
            wait_for_completion: Whether to wait and update progress after completion
            completion_wait_time: Time to wait before checking completion

        Returns:
            Complete processing results
        """
        if not self.parser:
            return {"success": False, "error": "Video parser not available"}

        print(f"\n🚀 Starting video processing workflow with progress tracking")
        print(f"🌐 Source URL: {self.base_url}")
        print(f"📁 Download directory: {self.download_dir}")
        print(f"📄 Progress file: {self.progress_file}")
        print(f"📄 Current page: {self.current_page}")
        print("="*80)

        try:
            # Step 1: Extract video URLs
            print("📋 Step 1: Extracting video URLs...")
            video_urls = await self.parser.extract_video_urls()

            if not video_urls:
                return {"success": False, "error": "No video URLs found"}

            print(f"✅ Found {len(video_urls)} video URLs")

            # Step 2: Parse video metadata
            print(f"\n📋 Step 2: Parsing video metadata...")
            videos_data = await self.parser.parse_all_videos()

            if not videos_data:
                return {"success": False, "error": "No video metadata could be parsed"}

            print(f"✅ Successfully parsed {len(videos_data)} videos")

            # Step 3: Add to IDM queue with progress tracking
            print(f"\n📋 Step 3: Adding videos to IDM queue with progress tracking...")
            idm_results = self.idm_manager.process_all_videos(
                videos_data, 
                start_queue=True,
                current_page=self.current_page,
                wait_for_completion=wait_for_completion,
                completion_wait_time=completion_wait_time
            )

            # Combine results
            return {
                "success": True,
                "urls_found": len(video_urls),
                "videos_parsed": len(videos_data),
                "current_page": self.current_page,
                "idm_results": idm_results,
                "download_directory": self.download_dir,
                "progress_file": self.progress_file
            }

        except Exception as e:
            print(f"❌ Error in processing workflow: {e}")
            return {"success": False, "error": str(e)}


if __name__ == "__main__":
    """
    Example usage of the progress tracking IDM system.
    """
    print("🚀 Progress Tracking Video to IDM Integration System")
    print("="*70)
    print("🆕 ENHANCED FEATURES:")
    print("   - Updates progress.json ONLY after downloads are verified")
    print("   - Monitors downloads folder for actual completion")
    print("   - Calculates real folder sizes and completion status")
    print("   - Does NOT update progress before IDM queue addition")
    print("   - Provides post-download verification and updates")
    print("="*70)

    # Example configuration
    BASE_URL = "https://rule34video.com/latest-updates/997"
    DOWNLOAD_DIR = "downloads"
    PROGRESS_FILE = "progress.json"

    async def demo():
        """Demonstration of the progress tracking system."""
        processor = ProgressTrackingVideoIDMProcessor(
            base_url=BASE_URL,
            download_dir=DOWNLOAD_DIR,
            progress_file=PROGRESS_FILE,
            enable_duplicate_detection=True,
            duplicate_check_limit=100
        )

        # Process videos with progress tracking
        results = await processor.process_all_videos(
            wait_for_completion=True,
            completion_wait_time=30
        )

        print("\n" + "="*80)
        print("📋 FINAL RESULTS WITH PROGRESS TRACKING")
        print("="*80)

        if results.get("success"):
            print("✅ Processing completed successfully!")
            print(f"   🌐 URLs found: {results.get('urls_found', 0)}")
            print(f"   📹 Videos parsed: {results.get('videos_parsed', 0)}")
            print(f"   📄 Page processed: {results.get('current_page', 'Unknown')}")
            print(f"   📄 Progress file: {results.get('progress_file', 'Unknown')}")

            idm_results = results.get("idm_results", {})
            print(f"   ✅ Successful IDM additions: {idm_results.get('successful_additions', 0)}")
            print(f"   🔍 Videos filtered by duplicates: {idm_results.get('videos_filtered_by_duplicates', 0)}")
            print(f"   📥 Queue items: {idm_results.get('download_queue_size', 0)}")
            print(f"   🚀 Queue started: {idm_results.get('queue_started', False)}")

            # Show progress update results
            progress_results = idm_results.get("progress_update_results")
            if progress_results:
                print(f"   📊 Progress updated: {progress_results.get('stats_updated', False)}")
                print(f"   ✅ Verification passed: {progress_results['verification_results'].get('verification_passed', False)}")

        else:
            print("❌ Processing failed!")
            print(f"   📄 Error: {results.get('error', 'Unknown error')}")

    # Run demo
    import asyncio
    asyncio.run(demo())
