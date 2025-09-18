#!/usr/bin/env python3
# Enhanced IDM Manager with Comprehensive Debugging and Verification
# This version ensures ALL videos found are processed and added to IDM

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
    def __init__(self, base_download_dir: str = "downloads", idm_path: str = None,
                 enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                 progress_file: str = "progress.json"):

        self.base_download_dir = Path(base_download_dir).resolve()
        self.idm_path = self._find_idm_executable(idm_path)
        self.progress_file = progress_file
        self.download_queue = []

        # Enhanced monitoring settings
        self.max_wait_time = 1800 # Maximum wait time: 30 minutes
        self.check_interval = 15 # Check every 15 seconds
        self.stable_duration = 60 # Must be stable for 60 seconds
        self.min_wait_time = 120 # Minimum wait time: 2 minutes

        # Initialize enhanced progress tracker
        self.progress_tracker = EnhancedProgressTracker(progress_file, base_download_dir)

        # Statistics tracking with detailed debugging
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
            "completion_cycles": 0,
            "videos_found_count": 0,
            "videos_processed_count": 0,
            "videos_added_to_idm_count": 0,
            "processing_failures": []
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

        print(f"🚀 Enhanced IDM Manager with Complete Debugging Initialized")
        print(f" 📁 Base download directory: {self.base_download_dir}")
        print(f" 🔧 IDM executable: {self.idm_path}")
        print(f" 📄 Progress file: {self.progress_file}")
        print(f" 🔍 Duplicate detection: {'Enabled' if self.duplicate_detection_enabled else 'Disabled'}")
        print(f" 🐛 DEBUG: Full video processing verification enabled")

    def _find_idm_executable(self, idm_path: str = None) -> str:
        # Find IDM executable with enhanced detection
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

        print("❌ IDM executable not found automatically")
        return "IDMan.exe" # Fallback

    def _ensure_directory_exists(self, directory_path: Path) -> bool:
        # Ensure directory exists with proper permissions
        try:
            directory_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            print(f"❌ Could not create directory {directory_path}: {e}")
            return False

    def _create_video_directory(self, video_id: str) -> Path:
        # Create organized directory structure for a video
        sanitized_id = self._sanitize_filename(video_id)
        video_dir = self.base_download_dir / sanitized_id

        if self._ensure_directory_exists(video_dir):
            self.stats["directories_created"] += 1
            print(f"📁 Created directory: {video_dir}")
        else:
            print(f"❌ Failed to create directory: {video_dir}")

        return video_dir

    def _sanitize_filename(self, filename: str) -> str:
        # Sanitize filename for Windows filesystem compatibility
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        filename = filename.strip('. ')
        if len(filename) > 200:
            filename = filename[:200]
        return filename if filename else "unknown"

    def _apply_duplicate_filtering(self, videos_data: List[Dict], current_page: int) -> List[Dict]:
        # Apply duplicate detection filtering to video data with debugging
        print(f"\n🐛 DEBUG: Starting duplicate filtering")
        print(f"🐛 DEBUG: Input videos count: {len(videos_data)}")

        if not self.duplicate_detection_enabled or not self.duplicate_manager:
            print("🔍 Duplicate detection disabled - processing all videos")
            self.stats["videos_passed_duplicate_check"] = len(videos_data)
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
        print(f"🐛 DEBUG: Duplicate filtering result - {filtered_count} videos passed")

        return filtered_videos

    def _add_to_idm_queue(self, url: str, local_path: Path, filename: str) -> bool:
        # Add download to IDM queue with debugging
        try:
            if not self._ensure_directory_exists(local_path):
                print(f"❌ Cannot create directory: {local_path}")
                return False

            windows_path = str(local_path).replace('/', '\\')
            print(f"📥 Adding to IDM queue: {filename}")
            print(f"🐛 DEBUG: IDM Add Details:")
            print(f"  URL: {url[:80]}{'...' if len(url) > 80 else ''}")
            print(f"  Path: {windows_path}")
            print(f"  Filename: {filename}")

            cmd = [
                self.idm_path,
                "/d", url,
                "/p", windows_path,
                "/f", filename,
                "/a", # Add to queue
                "/n", # Silent mode
                "/q" # Quiet mode
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
                print(f"🐛 DEBUG: IDM command executed successfully")
                return True
            else:
                print(f"❌ IDM command failed for {filename}")
                print(f"🐛 DEBUG: IDM failure details:")
                print(f"  Return code: {result.returncode}")
                if result.stdout:
                    print(f"  Stdout: {result.stdout}")
                if result.stderr:
                    print(f"  Stderr: {result.stderr}")
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
        # Add all files for a video to IDM download queue with debugging
        video_id = video_data.get("video_id", "unknown")
        title = video_data.get("title", "Unknown")

        print(f"\n🎬 Processing video: '{title}' (ID: {video_id})")
        print(f"🐛 DEBUG: Starting IDM queue addition for video {video_id}")

        video_dir = self._create_video_directory(video_id)
        sanitized_id = self._sanitize_filename(video_id)

        results = {"metadata": False, "thumbnail": False, "video": False}

        # 1. Save JSON metadata (always first)
        json_path = video_dir / f"{sanitized_id}.json"
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Saved metadata: {json_path.name}")
            results["metadata"] = True
        except Exception as e:
            print(f"❌ Error saving metadata {json_path.name}: {e}")
            results["metadata"] = False

        # 2. Add thumbnail to IDM queue
        thumbnail_url = video_data.get("thumbnail_src", "")
        if thumbnail_url and thumbnail_url.strip():
            jpg_path = video_dir / f"{sanitized_id}.jpg"
            success = self._add_to_idm_queue(
                thumbnail_url.strip(),
                video_dir,
                jpg_path.name
            )
            results["thumbnail"] = success
            if success:
                self.download_queue.append({
                    "type": "thumbnail",
                    "video_id": video_id,
                    "url": thumbnail_url,
                    "path": jpg_path
                })
                print(f"🐛 DEBUG: Thumbnail added to IDM queue for {video_id}")
        else:
            print(f"🐛 DEBUG: No thumbnail URL for {video_id}")

        # 3. Add video to IDM queue
        video_url = video_data.get("video_src", "")
        if video_url and video_url.strip():
            mp4_path = video_dir / f"{sanitized_id}.mp4"
            success = self._add_to_idm_queue(
                video_url.strip(),
                video_dir,
                mp4_path.name
            )
            results["video"] = success
            if success:
                self.download_queue.append({
                    "type": "video",
                    "video_id": video_id,
                    "url": video_url,
                    "path": mp4_path
                })
                print(f"🐛 DEBUG: Video added to IDM queue for {video_id}")
        else:
            print(f"🐛 DEBUG: No video URL for {video_id}")

        # Update stats and debug info
        if any(results.values()):
            self.stats["successful_additions"] += 1
            self.stats["videos_added_to_idm_count"] += 1
            print(f"✅ Video {video_id} successfully processed for IDM")
        else:
            self.stats["failed_additions"] += 1
            self.stats["processing_failures"].append({
                "video_id": video_id,
                "reason": "Failed to add any files to IDM queue",
                "results": results
            })
            print(f"❌ Video {video_id} failed to be added to IDM queue")

        print(f"🐛 DEBUG: Video {video_id} processing results: {results}")
        return results

    def process_all_videos(self, videos_data: List[Dict], start_queue: bool = True,
                         current_page: Optional[int] = None,
                         use_dynamic_monitoring: bool = True) -> Dict:
        # Process all videos with comprehensive debugging and verification
        if not videos_data:
            print("❌ No video data provided")
            return {"success": False, "error": "No video data provided"}

        print(f"\n🎬 STARTING COMPREHENSIVE VIDEO PROCESSING")
        print(f"=================================================================")
        print(f"🐛 DEBUG: Initial video count: {len(videos_data)}")
        print(f"📁 Download directory: {self.base_download_dir}")
        print(f"📄 Progress file: {self.progress_file}")
        print(f"🔍 Dynamic monitoring: {'Enabled' if use_dynamic_monitoring else 'Disabled'}")

        # CRITICAL: Update stats with initial count
        self.stats["videos_found_count"] = len(videos_data)
        self.stats["total_videos"] = len(videos_data)

        print(f"🐛 DEBUG: Set videos_found_count = {self.stats['videos_found_count']}")

        # Apply duplicate detection filtering
        original_count = len(videos_data)
        if current_page is not None:
            videos_data = self._apply_duplicate_filtering(videos_data, current_page)

        final_count = len(videos_data)
        print(f"🐛 DEBUG: After duplicate filtering:")
        print(f"  Original count: {original_count}")
        print(f"  Final count: {final_count}")
        print(f"  Filtered out: {original_count - final_count}")

        if not videos_data:
            print("🔍 All videos were filtered as duplicates - nothing to process")
            return {
                "success": True,
                "total_videos": original_count,
                "videos_found_count": original_count,
                "videos_passed_duplicate_check": 0,
                "videos_filtered_by_duplicates": original_count,
                "videos_added_to_idm_count": 0,
                "message": "All videos were duplicates"
            }

        # Reset processing stats for this batch
        self.stats.update({
            "videos_processed_count": 0,
            "successful_additions": 0,
            "failed_additions": 0,
            "directories_created": 0,
            "videos_added_to_idm_count": 0,
            "processing_failures": []
        })

        print(f"\n🔄 PROCESSING {len(videos_data)} VIDEOS FOR IDM QUEUE")
        print(f"=================================================================")

        # Process each video - CRITICAL: Process ALL videos
        video_results = {}
        for i, video_data in enumerate(videos_data, 1):
            video_id = video_data.get("video_id", f"unknown_{i}")
            print(f"\n📹 Processing video {i}/{len(videos_data)}: {video_id}")
            print(f"🐛 DEBUG: Video data keys: {list(video_data.keys())}")

            try:
                self.stats["videos_processed_count"] += 1
                results = self._add_video_to_idm_queue(video_data)
                video_results[video_id] = results

                # Show progress
                progress = (i / len(videos_data)) * 100
                print(f"📊 Progress: {i}/{len(videos_data)} videos ({progress:.1f}%)")
                print(f"🐛 DEBUG: Running totals:")
                print(f"  Videos processed: {self.stats['videos_processed_count']}")
                print(f"  Successfully added to IDM: {self.stats['videos_added_to_idm_count']}")
                print(f"  Failed additions: {self.stats['failed_additions']}")

            except Exception as e:
                print(f"❌ Exception processing video {video_id}: {e}")
                video_results[video_id] = {"metadata": False, "thumbnail": False, "video": False}
                self.stats["failed_additions"] += 1
                self.stats["processing_failures"].append({
                    "video_id": video_id,
                    "reason": f"Exception: {str(e)}",
                    "results": {"metadata": False, "thumbnail": False, "video": False}
                })

        print(f"\n=================================================================")
        print(f"🎯 CRITICAL VERIFICATION - VIDEOS PROCESSED vs FOUND")
        print(f"=================================================================")
        print(f"📹 Videos found on page: {self.stats['videos_found_count']}")
        print(f"📋 Videos after duplicate filtering: {len(videos_data)}")
        print(f"🔄 Videos actually processed: {self.stats['videos_processed_count']}")
        print(f"✅ Videos successfully added to IDM: {self.stats['videos_added_to_idm_count']}")
        print(f"❌ Videos failed to add to IDM: {self.stats['failed_additions']}")
        print(f"📥 Total items in IDM queue: {len(self.download_queue)}")

        # CRITICAL: Verify counts match
        if self.stats["videos_processed_count"] != len(videos_data):
            print(f"🚨 CRITICAL ERROR: Processed count mismatch!")
            print(f"  Expected to process: {len(videos_data)}")
            print(f"  Actually processed: {self.stats['videos_processed_count']}")

        if self.stats["videos_added_to_idm_count"] == 0:
            print(f"🚨 CRITICAL ERROR: NO VIDEOS WERE ADDED TO IDM!")
            print(f"🐛 DEBUG: Processing failures:")
            for failure in self.stats["processing_failures"]:
                print(f"  {failure['video_id']}: {failure['reason']}")

        # Print detailed stats
        self._print_stats()

        # Start IDM queue if requested
        queue_started = False
        if start_queue and len(self.download_queue) > 0:
            print(f"\n🚀 Starting IDM download queue with {len(self.download_queue)} items...")
            queue_started = self._start_idm_queue()

            if queue_started:
                print("✅ All downloads added to IDM and queue started!")
            else:
                print("⚠️ Downloads added but failed to start queue automatically.")
                print("💡 Please start the queue manually in IDM.")
        elif len(self.download_queue) == 0:
            print("📝 No downloads were added to queue.")
        else:
            print("📝 Downloads added to IDM queue but not started (start_queue=False)")

        # Prepare final results with comprehensive debugging info
        results = {
            "success": True,
            "videos_found_count": self.stats["videos_found_count"],
            "total_videos": self.stats["total_videos"],
            "videos_processed_count": self.stats["videos_processed_count"],
            "successful_additions": self.stats["successful_additions"],
            "failed_additions": self.stats["failed_additions"],
            "videos_added_to_idm_count": self.stats["videos_added_to_idm_count"],
            "directories_created": self.stats["directories_created"],
            "videos_filtered_by_duplicates": self.stats["videos_filtered_by_duplicates"],
            "videos_passed_duplicate_check": self.stats["videos_passed_duplicate_check"],
            "download_queue_size": len(self.download_queue),
            "queue_started": queue_started,
            "video_results": video_results,
            "download_directory": str(self.base_download_dir),
            "processing_failures": self.stats["processing_failures"],
            "verification_passed": self.stats["videos_processed_count"] == len(videos_data),
            "dynamic_monitoring_used": use_dynamic_monitoring
        }

        return results

    def _start_idm_queue(self) -> bool:
        # Start IDM download queue
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
                # Try alternative method
                cmd_alt = [self.idm_path, "/startqueue"]
                result2 = subprocess.run(cmd_alt, capture_output=True, text=True, timeout=15)

                if result2.returncode == 0:
                    print("✅ IDM queue started with alternative method!")
                    return True
                else:
                    print("❌ Both methods failed to start IDM queue")
                    return False

        except Exception as e:
            print(f"❌ Error starting IDM queue: {e}")
            return False

    def _print_stats(self):
        # Print detailed processing statistics with debugging
        print(f"\n📊 COMPREHENSIVE STATISTICS:")
        print(f"=================================================================")
        print(f"🎬 Videos found on page: {self.stats['videos_found_count']}")
        print(f"🔄 Videos processed: {self.stats['videos_processed_count']}")
        print(f"✅ Successfully added to IDM: {self.stats['videos_added_to_idm_count']}")
        print(f"❌ Failed additions: {self.stats['failed_additions']}")
        print(f"📁 Directories created: {self.stats['directories_created']}")
        print(f"📥 Items in download queue: {len(self.download_queue)}")

        if self.duplicate_detection_enabled:
            print(f"🔍 Videos filtered by duplicates: {self.stats['videos_filtered_by_duplicates']}")
            print(f"✅ Videos passed duplicate check: {self.stats['videos_passed_duplicate_check']}")

        if self.stats["videos_found_count"] > 0:
            success_rate = (self.stats["videos_added_to_idm_count"] / self.stats["videos_found_count"]) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")

        if self.stats["processing_failures"]:
            print(f"\n🐛 DEBUG: Processing failures ({len(self.stats['processing_failures'])}):")
            for failure in self.stats["processing_failures"][:5]:  # Show first 5
                print(f"  {failure['video_id']}: {failure['reason']}")

        print(f"=================================================================")
