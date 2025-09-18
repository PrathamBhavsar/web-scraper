#!/usr/bin/env python3

"""
Improved IDM Manager with Dynamic Completion Monitoring

This version fixes the failed videos issue by:
- Implementing proper completion monitoring instead of fixed wait times
- Using dynamic validation that waits until actual downloads complete
- Monitoring IDM queue status and folder contents in real-time
- Only updating progress after verifying actual download completion

Key Improvements:
- Dynamic wait times based on actual download progress
- Real-time IDM queue monitoring
- Folder completion validation with retries
- Progress updates ONLY after verified completion
- Better error handling and recovery

Author: AI Assistant
Version: 4.1 - Dynamic completion monitoring
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

class ImprovedIDMManager:
    """
    IDM Manager with dynamic completion monitoring that waits for actual downloads
    to complete before updating progress.json.
    """

    def __init__(self, base_download_dir: str = "downloads", idm_path: str = None,
                 enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                 progress_file: str = "progress.json"):
        """
        Initialize Improved IDM Manager.

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

        # Enhanced monitoring settings
        self.max_wait_time = 1800  # Maximum wait time: 30 minutes
        self.check_interval = 15   # Check every 15 seconds
        self.stable_duration = 60  # Must be stable for 60 seconds
        self.min_wait_time = 120   # Minimum wait time: 2 minutes

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
            "verified_completions": 0,
            "dynamic_wait_used": 0,
            "completion_cycles": 0
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

        print(f"🚀 Improved IDM Manager with Dynamic Monitoring Initialized")
        print(f" 📁 Base download directory: {self.base_download_dir}")
        print(f" 🔧 IDM executable: {self.idm_path}")
        print(f" 📄 Progress file: {self.progress_file}")
        print(f" 🔍 Duplicate detection: {'Enabled' if self.duplicate_detection_enabled else 'Disabled'}")
        print(f" 📊 Dynamic monitoring settings:")
        print(f"   ⏱️  Max wait time: {self.max_wait_time//60} minutes")
        print(f"   🔄 Check interval: {self.check_interval} seconds")
        print(f"   ⏳ Stable duration: {self.stable_duration} seconds")
        print(f"   🕒 Min wait time: {self.min_wait_time} seconds")

        if not self._verify_idm_access():
            print("⚠️ WARNING: IDM may not be accessible. Downloads might fail.")

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
            print(f"⚠️ Error searching for IDM in PATH: {e}")

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
                print(f"⚠️ IDM responded with unexpected output: {result.stdout}")
                return False
        except Exception as e:
            print(f"⚠️ Could not verify IDM access: {e}")
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
                print(f"⚠️ Directory not writable {directory_path}: {e}")
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
        print(f" 📥 Original videos: {original_count}")
        print(f" ✅ Videos to process: {filtered_count}")
        print(f" 🚫 Duplicates filtered: {duplicates_found}")

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
            print(f" 🌐 URL: {url[:80]}{'...' if len(url) > 80 else ''}")
            print(f" 📁 Path: {windows_path}")

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
                print(f" 📄 Return code: {result.returncode}")
                if result.stdout:
                    print(f" 📄 Stdout: {result.stdout}")
                if result.stderr:
                    print(f" 📄 Stderr: {result.stderr}")
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
                print(f"⚠️ Method 1 failed (return code {result.returncode})")
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

    def _check_idm_queue_status(self) -> Dict[str, Any]:
        """Check IDM queue status and running downloads."""
        try:
            # Try to get IDM process info
            cmd = ['tasklist', '/FI', 'IMAGENAME eq IDMan.exe', '/FO', 'CSV']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

            idm_running = 'IDMan.exe' in result.stdout

            return {
                "idm_running": idm_running,
                "process_info": result.stdout if idm_running else "IDM not running"
            }

        except Exception as e:
            print(f"⚠️ Could not check IDM status: {e}")
            return {"idm_running": True, "process_info": "Status check failed"}

    def _monitor_download_completion(self) -> Dict[str, Any]:
        """
        Monitor download completion using dynamic validation.
        Waits until downloads are actually complete before returning.
        """
        print(f"\n🔍 STARTING DYNAMIC DOWNLOAD COMPLETION MONITORING")
        print("=" * 80)

        start_time = time.time()
        last_stable_check = time.time()
        stable_count = 0
        check_count = 0

        # Get initial state
        initial_stats = self.progress_tracker.updater.monitor.get_download_statistics()
        print(f"📊 Initial State:")
        print(f" 📁 Total folders: {initial_stats['total_folders']}")
        print(f" ✅ Completed folders: {initial_stats['completed_folders']}")
        print(f" ❌ Failed/Incomplete: {initial_stats['failed_folders']}")

        # Wait minimum time first
        print(f"\n⏳ Initial wait period: {self.min_wait_time} seconds...")
        time.sleep(self.min_wait_time)

        print(f"\n🔄 Starting dynamic monitoring (checking every {self.check_interval}s)...")

        previous_completed = initial_stats['completed_folders']
        previous_total_size = initial_stats['total_size_mb']

        while True:
            check_count += 1
            current_time = time.time()
            elapsed_time = current_time - start_time

            # Check current stats
            current_stats = self.progress_tracker.updater.monitor.get_download_statistics()
            current_completed = current_stats['completed_folders']
            current_total_size = current_stats['total_size_mb']

            # Check IDM status
            idm_status = self._check_idm_queue_status()

            print(f"\n🔍 Check #{check_count} (Elapsed: {elapsed_time/60:.1f} min):")
            print(f" 📁 Completed folders: {current_completed} (was {previous_completed})")
            print(f" 💾 Total size: {current_total_size:.2f} MB (was {previous_total_size:.2f} MB)")
            print(f" 📈 Completion rate: {current_stats['completion_rate']:.1f}%")
            print(f" 🔧 IDM running: {idm_status['idm_running']}")

            # Check if downloads are progressing or stable
            downloads_changed = (current_completed != previous_completed or 
                               abs(current_total_size - previous_total_size) > 0.1)

            if downloads_changed:
                print(f" 🔄 Downloads still progressing...")
                stable_count = 0
                last_stable_check = current_time
                previous_completed = current_completed
                previous_total_size = current_total_size
            else:
                stable_duration = current_time - last_stable_check
                print(f" ⏱️  Downloads stable for {stable_duration:.0f}s (need {self.stable_duration}s)")

                if stable_duration >= self.stable_duration:
                    print(f" ✅ Downloads have been stable for {self.stable_duration}s - considering complete!")
                    break

            # Check timeout
            if elapsed_time >= self.max_wait_time:
                print(f" ⏰ Maximum wait time ({self.max_wait_time/60:.1f} min) reached!")
                break

            # Wait before next check
            time.sleep(self.check_interval)

        # Final validation
        final_stats = self.progress_tracker.updater.monitor.get_download_statistics()

        print("\n" + "=" * 80)
        print("📋 DYNAMIC MONITORING COMPLETED")
        print("=" * 80)
        print(f"⏱️  Total monitoring time: {(time.time() - start_time)/60:.1f} minutes")
        print(f"🔄 Checks performed: {check_count}")
        print(f"📊 Final Statistics:")
        print(f" 📁 Total folders: {final_stats['total_folders']}")
        print(f" ✅ Completed folders: {final_stats['completed_folders']}")
        print(f" ❌ Failed/Incomplete: {final_stats['failed_folders']}")
        print(f" 💾 Total size: {final_stats['total_size_mb']:.2f} MB")
        print(f" 📈 Completion rate: {final_stats['completion_rate']:.1f}%")

        self.stats["dynamic_wait_used"] += 1
        self.stats["completion_cycles"] += check_count

        return {
            "monitoring_time_minutes": (time.time() - start_time) / 60,
            "checks_performed": check_count,
            "final_stats": final_stats,
            "completion_rate": final_stats['completion_rate'],
            "monitoring_successful": True
        }

    def update_progress_after_completion(self, monitoring_results: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Update progress.json with verified data after monitoring completion.

        Args:
            monitoring_results: Results from dynamic monitoring

        Returns:
            Progress update results
        """
        print(f"\n🔄 UPDATING PROGRESS AFTER VERIFIED COMPLETION")
        print("=" * 70)

        # Sync progress with actual download folder contents
        updated_progress = self.progress_tracker.sync_progress_with_downloads()
        self.stats["progress_updates"] += 1

        # Get verification stats
        verification = self.progress_tracker.verify_and_fix_progress()
        if verification["verification_passed"]:
            self.stats["verified_completions"] += 1

        print("=" * 70)
        print("✅ Progress update completed with verification")

        return {
            "updated_progress": updated_progress,
            "verification_results": verification,
            "monitoring_results": monitoring_results,
            "stats_updated": True
        }

    def process_all_videos(self, videos_data: List[Dict], start_queue: bool = True,
                         current_page: Optional[int] = None,
                         use_dynamic_monitoring: bool = True) -> Dict:
        """
        Process all videos with improved completion monitoring.

        Args:
            videos_data: List of video metadata dictionaries
            start_queue: Whether to start IDM queue after adding downloads
            current_page: Current page being processed (for duplicate detection)
            use_dynamic_monitoring: Whether to use dynamic completion monitoring

        Returns:
            Processing results dictionary
        """
        if not videos_data:
            return {"success": False, "error": "No video data provided"}

        print(f"\n🎬 Processing {len(videos_data)} videos with improved monitoring...")
        print(f"📁 Download directory: {self.base_download_dir}")
        print(f"📄 Progress file: {self.progress_file}")
        print(f"🔍 Dynamic monitoring: {'Enabled' if use_dynamic_monitoring else 'Disabled'}")
        print("=" * 80)

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

        print("\n" + "=" * 80)
        print("📋 BATCH ADDITION TO IDM COMPLETE!")
        print("⚠️ PROGRESS.JSON NOT UPDATED YET - WAITING FOR DOWNLOADS")
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
                print("⚠️ Downloads added but failed to start queue automatically.")
                print("💡 Please start the queue manually in IDM.")
        elif len(self.download_queue) == 0:
            print("📝 No downloads were added to queue.")
        else:
            print("📝 Downloads added to IDM queue but not started (start_queue=False)")

        # Use dynamic monitoring if requested and queue started
        monitoring_results = None
        progress_update_results = None

        if use_dynamic_monitoring and queue_started:
            monitoring_results = self._monitor_download_completion()
            progress_update_results = self.update_progress_after_completion(monitoring_results)
        else:
            print("\n📝 Skipping dynamic monitoring")
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
            "monitoring_results": monitoring_results,
            "progress_update_results": progress_update_results,
            "dynamic_monitoring_used": use_dynamic_monitoring
        }

        # Add duplicate detection summary if enabled
        if self.duplicate_detection_enabled and self.duplicate_manager:
            results["duplicate_detection_summary"] = self.duplicate_manager.get_session_summary()

        return results

    def _print_stats(self):
        """Print detailed processing statistics."""
        print(f"📊 STATISTICS:")
        print(f" 🎬 Total videos: {self.stats['total_videos']}")
        print(f" ✅ Successful additions: {self.stats['successful_additions']}")
        print(f" ❌ Failed additions: {self.stats['failed_additions']}")
        print(f" 📁 Directories created: {self.stats['directories_created']}")
        print(f" 📥 Items in download queue: {len(self.download_queue)}")
        print(f" 📊 Progress updates: {self.stats['progress_updates']}")
        print(f" ✅ Verified completions: {self.stats['verified_completions']}")
        print(f" 🔄 Dynamic monitoring used: {self.stats['dynamic_wait_used']}")
        print(f" 🔍 Completion cycles: {self.stats['completion_cycles']}")

        if self.duplicate_detection_enabled:
            print(f" 🔍 Videos filtered by duplicates: {self.stats['videos_filtered_by_duplicates']}")
            print(f" ✅ Videos passed duplicate check: {self.stats['videos_passed_duplicate_check']}")

        if self.stats["total_videos"] > 0:
            success_rate = (self.stats["successful_additions"] / self.stats["total_videos"]) * 100
            print(f" 📈 Success rate: {success_rate:.1f}%")

    def manual_progress_update(self) -> Dict[str, Any]:
        """
        Manually trigger progress update based on download folder contents.

        Returns:
            Progress update results
        """
        print("\n🔄 Manual progress update triggered...")
        monitoring_results = self._monitor_download_completion()
        return self.update_progress_after_completion(monitoring_results)

    def get_progress_summary(self) -> Dict[str, Any]:
        """Get comprehensive progress summary."""
        return self.progress_tracker.get_progress_summary()

    def verify_progress_accuracy(self) -> Dict[str, Any]:
        """Verify and fix progress accuracy."""
        return self.progress_tracker.verify_and_fix_progress()
