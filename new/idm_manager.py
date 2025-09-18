#!/usr/bin/env python3

"""
Enhanced IDM Manager with Comprehensive Debugging and Verification
This version ensures ALL videos found are processed and added to IDM
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
    IDM Manager with comprehensive debugging and verification that ensures
    all videos are processed correctly and provides detailed audit trails.
    """

    def __init__(self, base_download_dir: str = "downloads", idm_path: str = None,
                 enable_duplicate_detection: bool = True, duplicate_check_limit: int = 100,
                 progress_file: str = "progress.json"):
        """
        Initialize Enhanced IDM Manager with debugging.

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
            # Debug-specific stats
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
        print(f"  📁 Base download directory: {self.base_download_dir}")
        print(f"  🔧 IDM executable: {self.idm_path}")
        print(f"  📄 Progress file: {self.progress_file}")
        print(f"  🔍 Duplicate detection: {'Enabled' if self.duplicate_detection_enabled else 'Disabled'}")
        print(f"  🐛 DEBUG: Full video processing verification enabled")
        print(f"  📊 Dynamic monitoring settings:")
        print(f"    ⏱️ Max wait time: {self.max_wait_time//60} minutes")
        print(f"    🔄 Check interval: {self.check_interval} seconds")
        print(f"    ⏳ Stable duration: {self.stable_duration} seconds")
        print(f"    🕒 Min wait time: {self.min_wait_time} seconds")

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
        """Apply duplicate detection filtering to video data with debugging."""
        print(f"\n🐛 DEBUG: Starting duplicate filtering")
        print(f"🐛 DEBUG: Input videos count: {len(videos_data)}")

        if not self.duplicate_detection_enabled or not self.duplicate_manager:
            print("🔍 Duplicate detection disabled - processing all videos")
            self.stats["videos_passed_duplicate_check"] = len(videos_data)
            print(f"🐛 DEBUG: All {len(videos_data)} videos will be processed")
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
        print(f"  📥 Original videos: {original_count}")
        print(f"  ✅ Videos to process: {filtered_count}")
        print(f"  🚫 Duplicates filtered: {duplicates_found}")

        print(f"🐛 DEBUG: Duplicate filtering results:")
        print(f"🐛 DEBUG: {duplicates_found} duplicates removed")
        print(f"🐛 DEBUG: {filtered_count} videos will be processed")

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
            # Determine video extension from URL
            video_ext = self._get_video_extension(video_url)
            video_path = video_dir / f"{sanitized_id}{video_ext}"
            downloads["video"] = {
                "type": "video",
                "url": video_url.strip(),
                "path": video_path
            }

        return downloads

    def _get_video_extension(self, video_url: str) -> str:
        """Determine video file extension from URL."""
        # Common video extensions
        extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm']

        video_url_lower = video_url.lower()
        for ext in extensions:
            if ext in video_url_lower:
                return ext

        return '.mp4'  # Default fallback

    def _add_video_to_idm_queue(self, video_data: Dict) -> Dict:
        """Add a single video to IDM download queue with debugging."""
        video_id = video_data.get("video_id", "unknown")
        print(f"\n🐛 DEBUG: Processing video: {video_id}")

        downloads = self._prepare_video_downloads(video_data)
        results = {"metadata": False, "thumbnail": False, "video": False}

        # Save JSON metadata first
        try:
            json_info = downloads.get("json")
            if json_info:
                with open(json_info["path"], 'w', encoding='utf-8') as f:
                    json.dump(json_info["data"], f, indent=2, ensure_ascii=False)
                results["metadata"] = True
                print(f"✅ Metadata saved: {json_info['path']}")
            else:
                print("⚠️ No metadata to save")
        except Exception as e:
            print(f"❌ Error saving metadata: {e}")

        # Add thumbnail to IDM queue
        thumbnail_info = downloads.get("thumbnail")
        if thumbnail_info:
            try:
                success = self._add_single_download_to_idm(
                    thumbnail_info["url"],
                    str(thumbnail_info["path"])
                )
                results["thumbnail"] = success
                if success:
                    print(f"📥 Thumbnail added to IDM: {thumbnail_info['url']}")
                else:
                    print(f"❌ Failed to add thumbnail to IDM")
            except Exception as e:
                print(f"❌ Error adding thumbnail to IDM: {e}")
        else:
            print("⚠️ No thumbnail URL available")

        # Add video to IDM queue
        video_info = downloads.get("video")
        if video_info:
            try:
                success = self._add_single_download_to_idm(
                    video_info["url"],
                    str(video_info["path"])
                )
                results["video"] = success
                if success:
                    print(f"🎬 Video added to IDM: {video_info['url']}")
                else:
                    print(f"❌ Failed to add video to IDM")
            except Exception as e:
                print(f"❌ Error adding video to IDM: {e}")
        else:
            print("⚠️ No video URL available")

        # Update statistics
        successful_items = sum([1 for success in results.values() if success])
        if successful_items > 0:
            self.stats["successful_additions"] += 1
            print(f"✅ Video {video_id}: {successful_items}/3 items added successfully")
        else:
            self.stats["failed_additions"] += 1
            failure_info = {
                "video_id": video_id,
                "reason": "No items successfully added to IDM",
                "timestamp": time.time()
            }
            self.stats["processing_failures"].append(failure_info)
            print(f"❌ Video {video_id}: Failed to add any items to IDM")

        print(f"🐛 DEBUG: Video {video_id} processing results: {results}")
        return results

    def _add_single_download_to_idm(self, url: str, output_path: str) -> bool:
        """Add a single download to IDM queue."""
        try:
            # Build IDM command
            idm_command = [
                self.idm_path,
                "/d", url,  # Download URL
                "/p", str(Path(output_path).parent),  # Download directory
                "/f", Path(output_path).name,  # Filename
                "/n",  # Don't start download immediately
                "/a"   # Add to queue
            ]

            print(f"🐛 DEBUG: IDM command: {' '.join(idm_command)}")

            # Execute IDM command
            result = subprocess.run(
                idm_command,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                # Add to internal queue tracking
                self.download_queue.append({
                    "url": url,
                    "output_path": output_path,
                    "added_time": time.time()
                })
                return True
            else:
                print(f"⚠️ IDM returned non-zero exit code: {result.returncode}")
                if result.stderr:
                    print(f"⚠️ IDM error output: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print("⚠️ IDM command timed out")
            return False
        except Exception as e:
            print(f"❌ Error executing IDM command: {e}")
            return False

    def _start_idm_queue(self) -> bool:
        """Start IDM download queue."""
        try:
            print("🚀 Starting IDM download queue...")

            # Command to start IDM queue
            start_command = [self.idm_path, "/s"]

            result = subprocess.run(
                start_command,
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                print("✅ IDM queue started successfully")
                return True
            else:
                print(f"⚠️ IDM queue start returned code: {result.returncode}")
                return False

        except Exception as e:
            print(f"❌ Error starting IDM queue: {e}")
            return False

    def _monitor_download_completion(self) -> Dict[str, Any]:
        """
        Monitor download completion using dynamic validation.
        Returns monitoring results with detailed statistics.
        """
        print(f"\n🔄 STARTING DYNAMIC DOWNLOAD MONITORING")
        print("=" * 70)
        print(f"📊 Monitoring Settings:")
        print(f"  ⏱️ Max wait time: {self.max_wait_time//60} minutes")
        print(f"  🔄 Check interval: {self.check_interval} seconds")
        print(f"  ⏳ Stable duration: {self.stable_duration} seconds")
        print(f"  🕒 Min wait time: {self.min_wait_time//60} minutes")
        print("=" * 70)

        start_time = time.time()
        check_count = 0
        last_stable_time = None
        stable_consecutive_checks = 0
        required_stable_checks = self.stable_duration // self.check_interval

        print(f"🔍 Required stable checks: {required_stable_checks}")

        # Initial state capture
        initial_stats = self.progress_tracker.updater.monitor.get_download_statistics()
        print(f"📊 Initial Download Statistics:")
        print(f"  📁 Total folders: {initial_stats['total_folders']}")
        print(f"  ✅ Completed: {initial_stats['completed_folders']}")
        print(f"  ❌ Failed/Incomplete: {initial_stats['failed_folders']}")
        print(f"  💾 Total size: {initial_stats['total_size_mb']:.2f} MB")

        # Wait minimum time before checking
        print(f"\n⏳ Waiting minimum time ({self.min_wait_time} seconds)...")
        time.sleep(self.min_wait_time)

        print(f"\n🔄 Starting dynamic monitoring loop...")

        while (time.time() - start_time) < self.max_wait_time:
            check_count += 1
            elapsed_minutes = (time.time() - start_time) / 60

            print(f"\n🔍 Check {check_count} (Elapsed: {elapsed_minutes:.1f} minutes)")

            # Get current download statistics
            current_stats = self.progress_tracker.updater.monitor.get_download_statistics()

            print(f"📊 Current Statistics:")
            print(f"  📁 Total folders: {current_stats['total_folders']}")
            print(f"  ✅ Completed: {current_stats['completed_folders']}")
            print(f"  ❌ Failed/Incomplete: {current_stats['failed_folders']}")
            print(f"  💾 Size: {current_stats['total_size_mb']:.2f} MB")
            print(f"  📈 Completion rate: {current_stats['completion_rate']:.1f}%")

            # Check if downloads are still active
            try:
                # Try to get IDM process information
                idm_processes = []
                try:
                    result = subprocess.run(
                        ["tasklist", "/FI", "IMAGENAME eq IDMan.exe", "/FO", "CSV"],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0 and "IDMan.exe" in result.stdout:
                        lines = result.stdout.strip().split('\n')[1:]  # Skip header
                        idm_processes = [line for line in lines if "IDMan.exe" in line]
                except:
                    pass

                idm_running = len(idm_processes) > 0
                print(f"🔧 IDM Status: {'Running' if idm_running else 'Not detected'}")

                # Determine if downloads appear stable
                downloads_stable = True

                # Check if no changes in folder count/size for stable duration
                if hasattr(self, '_last_stats'):
                    size_changed = abs(current_stats['total_size_mb'] - self._last_stats['total_size_mb']) > 0.1
                    folders_changed = current_stats['total_folders'] != self._last_stats['total_folders']

                    if size_changed or folders_changed:
                        downloads_stable = False
                        stable_consecutive_checks = 0
                        print(f"📈 Activity detected - resetting stability counter")
                    else:
                        stable_consecutive_checks += 1
                        print(f"⏳ Stable for {stable_consecutive_checks * self.check_interval} seconds")
                else:
                    stable_consecutive_checks = 0

                self._last_stats = current_stats.copy()

                # Check if we've been stable long enough
                if stable_consecutive_checks >= required_stable_checks:
                    print(f"✅ Downloads appear stable for {self.stable_duration} seconds")
                    print(f"🔍 Completion rate: {current_stats['completion_rate']:.1f}%")
                    break

                # If completion rate is very high and stable, consider complete
                if current_stats['completion_rate'] >= 90.0 and stable_consecutive_checks >= (required_stable_checks // 2):
                    print(f"✅ High completion rate ({current_stats['completion_rate']:.1f}%) with moderate stability")
                    break

            except Exception as e:
                print(f"⚠️ Error checking download status: {e}")

            # Wait before next check
            print(f"⏳ Waiting {self.check_interval} seconds for next check...")
            time.sleep(self.check_interval)

        # Final validation
        final_stats = self.progress_tracker.updater.monitor.get_download_statistics()

        print("\n" + "=" * 80)
        print("📋 DYNAMIC MONITORING COMPLETED")
        print("=" * 80)
        print(f"⏱️ Total monitoring time: {(time.time() - start_time)/60:.1f} minutes")
        print(f"🔄 Checks performed: {check_count}")
        print(f"📊 Final Statistics:")
        print(f"  📁 Total folders: {final_stats['total_folders']}")
        print(f"  ✅ Completed folders: {final_stats['completed_folders']}")
        print(f"  ❌ Failed/Incomplete: {final_stats['failed_folders']}")
        print(f"  💾 Total size: {final_stats['total_size_mb']:.2f} MB")
        print(f"  📈 Completion rate: {final_stats['completion_rate']:.1f}%")

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
        Process all videos with comprehensive debugging and verification.

        Args:
            videos_data: List of video metadata dictionaries
            start_queue: Whether to start IDM queue after adding downloads
            current_page: Current page being processed (for duplicate detection)
            use_dynamic_monitoring: Whether to use dynamic completion monitoring

        Returns:
            Processing results dictionary with debugging information
        """
        if not videos_data:
            return {"success": False, "error": "No video data provided"}

        print(f"\n🎬 Processing {len(videos_data)} videos with complete debugging...")
        print(f"📁 Download directory: {self.base_download_dir}")
        print(f"📄 Progress file: {self.progress_file}")
        print(f"🔍 Dynamic monitoring: {'Enabled' if use_dynamic_monitoring else 'Disabled'}")
        print(f"🐛 Complete debugging: ENABLED")
        print("=" * 80)

        # Debug: Log initial video count
        self.stats["videos_found_count"] = len(videos_data)
        print(f"🐛 DEBUG: Initial videos_found_count = {self.stats['videos_found_count']}")

        # Apply duplicate detection filtering
        if current_page is not None:
            videos_data = self._apply_duplicate_filtering(videos_data, current_page)

        if not videos_data:
            print("🔍 All videos were filtered as duplicates - nothing to process")
            return {
                "success": True,
                "videos_found_count": self.stats["videos_found_count"],
                "videos_processed_count": 0,
                "videos_added_to_idm_count": 0,
                "videos_filtered_as_duplicates": self.stats["videos_found_count"],
                "message": "All videos were duplicates"
            }

        # Debug: Log processed count after filtering
        self.stats["videos_processed_count"] = len(videos_data)
        print(f"🐛 DEBUG: After filtering, videos_processed_count = {self.stats['videos_processed_count']}")

        # Reset stats for this batch
        self.stats.update({
            "total_videos": len(videos_data),
            "successful_additions": 0,
            "failed_additions": 0,
            "directories_created": 0,
            "videos_added_to_idm_count": 0,
            "processing_failures": []
        })

        # Process each video (ADD TO IDM QUEUE - DO NOT UPDATE PROGRESS YET)
        video_results = {}
        successful_idm_additions = 0

        for i, video_data in enumerate(videos_data, 1):
            video_id = video_data.get("video_id", f"unknown_{i}")
            print(f"\n📹 Processing video {i}/{len(videos_data)}: {video_id}")

            try:
                results = self._add_video_to_idm_queue(video_data)
                video_results[video_id] = results

                # Count successful IDM additions
                successful_items = sum([1 for success in results.values() if success])
                if successful_items > 0:
                    successful_idm_additions += 1

                # Show progress
                progress = (i / len(videos_data)) * 100
                print(f"📊 Progress: {i}/{len(videos_data)} videos ({progress:.1f}%)")
                print(f"🐛 DEBUG: Video {i} added {successful_items}/3 items to IDM")

            except Exception as e:
                print(f"❌ Error processing video {video_id}: {e}")
                video_results[video_id] = {"metadata": False, "thumbnail": False, "video": False}
                self.stats["failed_additions"] += 1
                failure_info = {
                    "video_id": video_id,
                    "reason": f"Exception: {str(e)}",
                    "timestamp": time.time()
                }
                self.stats["processing_failures"].append(failure_info)

        # Update debug statistics
        self.stats["videos_added_to_idm_count"] = successful_idm_additions
        print(f"🐛 DEBUG: Final videos_added_to_idm_count = {self.stats['videos_added_to_idm_count']}")

        print("\n" + "=" * 80)
        print("📋 BATCH ADDITION TO IDM COMPLETE!")
        print("⚠️ PROGRESS.JSON NOT UPDATED YET - WAITING FOR DOWNLOADS")
        self._print_comprehensive_stats()

        # Verify processing counts
        expected_processed = self.stats["videos_found_count"] - self.stats["videos_filtered_by_duplicates"]
        actual_processed = self.stats["videos_processed_count"]
        actual_added_to_idm = self.stats["videos_added_to_idm_count"]

        print(f"\n🐛 DEBUG: Critical Count Verification:")
        print(f"🐛 DEBUG: Videos found: {self.stats['videos_found_count']}")
        print(f"🐛 DEBUG: Duplicates filtered: {self.stats['videos_filtered_by_duplicates']}")
        print(f"🐛 DEBUG: Expected to process: {expected_processed}")
        print(f"🐛 DEBUG: Actually processed: {actual_processed}")
        print(f"🐛 DEBUG: Added to IDM: {actual_added_to_idm}")

        # Critical verification
        verification_passed = True
        if actual_processed != expected_processed:
            print(f"🚨 CRITICAL WARNING: Processing count mismatch!")
            verification_passed = False

        if actual_added_to_idm == 0 and actual_processed > 0:
            print(f"🚨 CRITICAL WARNING: Videos processed but NONE added to IDM!")
            verification_passed = False

        if actual_added_to_idm > actual_processed:
            print(f"🚨 CRITICAL WARNING: More videos added to IDM than processed!")
            verification_passed = False

        print(f"🐛 DEBUG: Verification passed: {verification_passed}")

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

        # Prepare final results with debugging information
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
            "dynamic_monitoring_used": use_dynamic_monitoring,
            # Debug-specific results
            "videos_found_count": self.stats["videos_found_count"],
            "videos_processed_count": self.stats["videos_processed_count"],
            "videos_added_to_idm_count": self.stats["videos_added_to_idm_count"],
            "processing_failures": self.stats["processing_failures"],
            "verification_passed": verification_passed,
            "complete_debugging_enabled": True
        }

        # Add duplicate detection summary if enabled
        if self.duplicate_detection_enabled and self.duplicate_manager:
            results["duplicate_detection_summary"] = self.duplicate_manager.get_session_summary()

        return results

    def _print_comprehensive_stats(self):
        """Print detailed processing statistics with debugging."""
        print(f"\n📊 COMPREHENSIVE STATISTICS:")
        print(f"=" * 65)
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

        print(f"=" * 65)

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
