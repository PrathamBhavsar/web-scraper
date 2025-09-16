
"""
Enhanced IDM Manager with Video Parsing Integration

This enhanced version combines video parsing with IDM downloading.
It can take video URLs and handle the complete workflow:
1. Parse video data from URLs
2. Download JPG and MP4 files via IDM
3. Create organized directory structure

Integrates with the existing scraper's progress tracking and storage management.
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
import requests
import re
from urllib.parse import urljoin
import logging

class EnhancedIDMManager:
    """
    Enhanced IDM Manager that handles both video parsing and downloading.
    Integrates with existing scraper components.
    """

    def __init__(self, base_download_dir: str = "downloads", idm_path: str = None, web_driver_manager=None):
        """
        Initialize Enhanced IDM Manager.

        Args:
            base_download_dir: Base directory for downloads
            idm_path: Path to IDM executable (auto-detected if None)
            web_driver_manager: WebDriverManager instance for parsing
        """
        self.base_download_dir = Path(base_download_dir).resolve()
        self.idm_path = self._find_idm_executable(idm_path)
        self.web_driver_manager = web_driver_manager
        self.download_queue = []
        self.logger = logging.getLogger('EnhancedIDMManager')

        self.stats = {
            'total_videos': 0,
            'successful_additions': 0,
            'failed_additions': 0,
            'directories_created': 0,
            'parsed_videos': 0,
            'parsing_failed': 0
        }

        # Create base download directory immediately
        self._ensure_directory_exists(self.base_download_dir)
        print(f"🔧 Enhanced IDM Manager Initialized")
        print(f"📁 Base download directory: {self.base_download_dir}")
        print(f"🎯 IDM executable: {self.idm_path}")

        # Verify IDM is accessible
        if not self._verify_idm_access():
            print("⚠️ WARNING: IDM may not be accessible. Downloads might fail.")

    def _find_idm_executable(self, idm_path: str = None) -> str:
        """Find IDM executable (keep existing logic from your IDM manager)"""
        if idm_path and os.path.exists(idm_path):
            print(f"✅ Using custom IDM path: {idm_path}")
            return idm_path

        # Common IDM installation paths
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

        # Try Windows 'where' command
        try:
            result = subprocess.run(['where', 'IDMan.exe'], capture_output=True, text=True, shell=True, timeout=10)
            if result.returncode == 0:
                idm_path = result.stdout.strip().split('\n')[0]
                print(f"✅ Found IDM in PATH: {idm_path}")
                return idm_path
        except Exception as e:
            print(f"⚠️ Error searching for IDM: {e}")

        print("❌ IDM executable not found automatically")
        return "IDMan.exe"  # Fallback

    def _verify_idm_access(self) -> bool:
        """Verify IDM is accessible"""
        try:
            result = subprocess.run([self.idm_path, '/?'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0 or "Internet Download Manager" in result.stdout:
                print("✅ IDM access verified")
                return True
            return False
        except Exception as e:
            print(f"⚠️ Could not verify IDM access: {e}")
            return False

    def _ensure_directory_exists(self, directory_path: Path) -> bool:
        """Ensure directory exists with proper permissions"""
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

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for Windows filesystem compatibility"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        filename = filename.strip('. ')
        if len(filename) > 200:
            filename = filename[:200]
        return filename if filename else "unknown"

    def extract_video_id_from_url(self, video_url: str) -> str:
        """Extract video ID from URL"""
        video_id_match = re.search(r'/video/([^/]+)', video_url)
        if video_id_match:
            return video_id_match.group(1)
        else:
            return video_url.split('/')[-2] if video_url.endswith('/') else video_url.split('/')[-1]

    def parse_video_from_url(self, video_url: str) -> Dict:
        """
        Parse video data from URL using web driver.
        This replaces the video_info_extractor functionality.
        """
        try:
            if not self.web_driver_manager or not self.web_driver_manager.driver:
                self.logger.error("Web driver not available for parsing")
                return None

            video_id = self.extract_video_id_from_url(video_url)
            self.logger.info(f"🔍 Parsing video data for: {video_id}")

            # Navigate to video page
            if not self.web_driver_manager.navigate_to_page(video_url):
                self.logger.error(f"Failed to navigate to video page: {video_url}")
                return None

            # Wait for page to load
            time.sleep(3)

            video_data = {
                "video_id": video_id,
                "url": video_url,
                "video_src": "",
                "thumbnail_src": "",
                "title": f"Video_{video_id}",
                "duration": "",
                "views": "",
                "uploader": "Unknown",
                "upload_date": int(time.time() * 1000),
                "tags": ["untagged"]
            }

            # Extract video source
            try:
                video_elements = self.web_driver_manager.driver.find_elements("xpath", "//video[@src]")
                for video_element in video_elements:
                    src = video_element.get_attribute("src")
                    if src and any(quality in src for quality in ["1080p", "720p", "480p", "360p", "mp4"]):
                        if not src.startswith("http"):
                            src = urljoin(video_url, src)
                        video_data["video_src"] = src
                        self.logger.info(f"✅ Found video source: {src[:100]}...")
                        break
            except Exception as e:
                self.logger.warning(f"Error extracting video source: {e}")

            # Extract thumbnail
            try:
                # Try video poster first
                video_elements = self.web_driver_manager.driver.find_elements("xpath", "//video[@poster]")
                for video_element in video_elements:
                    poster = video_element.get_attribute("poster")
                    if poster:
                        if not poster.startswith("http"):
                            poster = urljoin(video_url, poster)
                        video_data["thumbnail_src"] = poster
                        self.logger.info(f"✅ Found thumbnail: {poster[:100]}...")
                        break

                # Fallback to img tags
                if not video_data["thumbnail_src"]:
                    img_elements = self.web_driver_manager.driver.find_elements("xpath", "//img[contains(@src, 'thumb') or contains(@src, 'preview')]")
                    for img in img_elements:
                        src = img.get_attribute("src")
                        if src:
                            if not src.startswith("http"):
                                src = urljoin(video_url, src)
                            video_data["thumbnail_src"] = src
                            self.logger.info(f"✅ Found thumbnail (fallback): {src[:100]}...")
                            break

            except Exception as e:
                self.logger.warning(f"Error extracting thumbnail: {e}")

            # Extract title
            try:
                title_selectors = [
                    "//h1",
                    "//title",
                    "//*[@class='title']",
                    "//*[contains(@class, 'video-title')]"
                ]
                for selector in title_selectors:
                    try:
                        title_element = self.web_driver_manager.driver.find_element("xpath", selector)
                        title = title_element.text.strip()
                        if title and len(title) > 3:
                            video_data["title"] = title[:200]  # Limit length
                            self.logger.info(f"✅ Found title: {title[:50]}...")
                            break
                    except:
                        continue
            except Exception as e:
                self.logger.warning(f"Error extracting title: {e}")

            # Validate that we have essential data
            if not video_data["video_src"]:
                self.logger.warning(f"No video source found for {video_id}")
                return None

            self.stats['parsed_videos'] += 1
            self.logger.info(f"✅ Successfully parsed video data for: {video_id}")
            return video_data

        except Exception as e:
            self.logger.error(f"Error parsing video from URL {video_url}: {e}")
            self.stats['parsing_failed'] += 1
            return None

    def create_video_directory(self, video_id: str) -> Path:
        """Create organized directory structure for a video"""
        sanitized_id = self._sanitize_filename(video_id)
        video_dir = self.base_download_dir / sanitized_id

        if self._ensure_directory_exists(video_dir):
            self.stats['directories_created'] += 1
            self.logger.info(f"📁 Created directory: {video_dir}")
        else:
            self.logger.error(f"❌ Failed to create directory: {video_dir}")

        return video_dir

    def save_json_metadata(self, json_path: Path, video_data: Dict) -> bool:
        """Save video metadata as JSON file"""
        try:
            json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"💾 Saved metadata: {json_path.name}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error saving metadata {json_path.name}: {e}")
            return False

    def add_to_idm_queue(self, url: str, local_path: Path, filename: str) -> bool:
        """Add download to IDM queue using optimized command parameters"""
        try:
            if not self._ensure_directory_exists(local_path):
                self.logger.error(f"❌ Cannot create directory: {local_path}")
                return False

            windows_path = str(local_path).replace('/', '\\')
            self.logger.info(f"🚀 Adding to IDM queue: {filename}")

            cmd = [
                self.idm_path,
                '/d', url,
                '/p', windows_path,
                '/f', filename,
                '/a',  # Add to queue without starting
                '/n',  # Silent mode
                '/q'   # Quiet mode
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, shell=False)

            if result.returncode == 0:
                self.logger.info(f"✅ Successfully added to IDM queue: {filename}")
                return True
            else:
                self.logger.error(f"❌ IDM command failed for {filename}")
                self.logger.error(f" Return code: {result.returncode}")
                return False

        except Exception as e:
            self.logger.error(f"❌ Error adding {filename} to IDM queue: {e}")
            return False

    def start_idm_queue(self) -> bool:
        """Start IDM download queue"""
        try:
            self.logger.info("🚀 Starting IDM download queue...")
            cmd = [self.idm_path, '/s']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

            if result.returncode == 0:
                self.logger.info("✅ IDM queue started successfully!")
                return True
            else:
                # Try alternative method
                cmd_alt = [self.idm_path, '/startqueue']
                result2 = subprocess.run(cmd_alt, capture_output=True, text=True, timeout=15)
                if result2.returncode == 0:
                    self.logger.info("✅ IDM queue started with alternative method!")
                    return True
                else:
                    self.logger.error("❌ All queue start methods failed")
                    return False

        except Exception as e:
            self.logger.error(f"❌ Error starting IDM queue: {e}")
            return False

    def process_video_url(self, video_url: str) -> Dict[str, bool]:
        """
        Complete processing of a single video URL:
        1. Parse video data from URL
        2. Add to IDM queue for download

        Returns dict with success status for each step.
        """
        try:
            # Step 1: Parse video data from URL
            video_data = self.parse_video_from_url(video_url)
            if not video_data:
                return {'parsing': False, 'metadata': False, 'thumbnail': False, 'video': False}

            video_id = video_data['video_id']
            self.logger.info(f"\n🎬 Processing video: {video_data.get('title', 'Unknown')} (ID: {video_id})")

            # Step 2: Create directory and save metadata
            video_dir = self.create_video_directory(video_id)
            sanitized_id = self._sanitize_filename(video_id)

            results = {'parsing': True, 'metadata': False, 'thumbnail': False, 'video': False}

            # Save JSON metadata
            json_path = video_dir / f"{sanitized_id}.json"
            results['metadata'] = self.save_json_metadata(json_path, video_data)

            # Add thumbnail to IDM queue
            if video_data.get('thumbnail_src'):
                jpg_path = video_dir / f"{sanitized_id}.jpg"
                success = self.add_to_idm_queue(video_data['thumbnail_src'], video_dir, f"{sanitized_id}.jpg")
                results['thumbnail'] = success
                if success:
                    self.download_queue.append({
                        'type': 'thumbnail',
                        'video_id': video_id,
                        'url': video_data['thumbnail_src'],
                        'path': jpg_path
                    })

            # Add video to IDM queue
            if video_data.get('video_src'):
                mp4_path = video_dir / f"{sanitized_id}.mp4"
                success = self.add_to_idm_queue(video_data['video_src'], video_dir, f"{sanitized_id}.mp4")
                results['video'] = success
                if success:
                    self.download_queue.append({
                        'type': 'video',
                        'video_id': video_id,
                        'url': video_data['video_src'],
                        'path': mp4_path
                    })

            # Update stats
            if any(results.values()):
                self.stats['successful_additions'] += 1
            else:
                self.stats['failed_additions'] += 1

            return results

        except Exception as e:
            self.logger.error(f"Error processing video URL {video_url}: {e}")
            self.stats['failed_additions'] += 1
            return {'parsing': False, 'metadata': False, 'thumbnail': False, 'video': False}

    def process_video_urls(self, video_urls: List[str], start_queue: bool = True) -> Dict:
        """
        Process multiple video URLs:
        1. Parse each video's data
        2. Add all to IDM queue
        3. Optionally start downloads

        This is the main method that replaces the old workflow.
        """
        if not video_urls:
            self.logger.error("❌ No video URLs provided")
            return {"success": False, "error": "No video URLs provided"}

        self.logger.info(f"🎯 Processing {len(video_urls)} video URLs with Enhanced IDM Manager...")
        self.logger.info(f"📁 Download directory: {self.base_download_dir}")
        self.logger.info("="*80)

        # Reset stats
        self.stats = {
            'total_videos': len(video_urls),
            'successful_additions': 0,
            'failed_additions': 0,
            'directories_created': 0,
            'parsed_videos': 0,
            'parsing_failed': 0
        }

        # Process each video URL
        video_results = {}
        for i, video_url in enumerate(video_urls, 1):
            video_id = self.extract_video_id_from_url(video_url)
            self.logger.info(f"\n📋 Processing video {i}/{len(video_urls)}: {video_id}")

            try:
                results = self.process_video_url(video_url)
                video_results[video_id] = results

                # Show progress
                progress = (i / len(video_urls)) * 100
                self.logger.info(f"📊 Progress: {i}/{len(video_urls)} videos ({progress:.1f}%)")

            except Exception as e:
                self.logger.error(f"❌ Error processing video {video_id}: {e}")
                video_results[video_id] = {'parsing': False, 'metadata': False, 'thumbnail': False, 'video': False}
                self.stats['failed_additions'] += 1

        self.logger.info("\n" + "="*80)
        self.logger.info("📋 ENHANCED IDM MANAGER PROCESSING COMPLETE!")
        self.print_stats()

        # Start IDM queue if requested
        queue_started = False
        if start_queue and len(self.download_queue) > 0:
            self.logger.info("\n🚀 Starting IDM download queue...")
            queue_started = self.start_idm_queue()
            if queue_started:
                self.logger.info("✅ All videos processed and IDM queue started!")
            else:
                self.logger.warning("⚠️ Videos processed but failed to start queue automatically.")
        elif len(self.download_queue) == 0:
            self.logger.warning("⚠️ No downloads were added to queue.")
        else:
            self.logger.info("ℹ️ Videos processed and added to IDM queue but not started")

        return {
            'success': True,
            'total_videos': self.stats['total_videos'],
            'successful_additions': self.stats['successful_additions'],
            'failed_additions': self.stats['failed_additions'],
            'parsed_videos': self.stats['parsed_videos'],
            'parsing_failed': self.stats['parsing_failed'],
            'directories_created': self.stats['directories_created'],
            'download_queue_size': len(self.download_queue),
            'queue_started': queue_started,
            'video_results': video_results,
            'download_directory': str(self.base_download_dir)
        }

    def print_stats(self):
        """Print detailed processing statistics"""
        self.logger.info(f"📊 ENHANCED IDM MANAGER STATISTICS:")
        self.logger.info(f" 📁 Total videos: {self.stats['total_videos']}")
        self.logger.info(f" 🔍 Successfully parsed: {self.stats['parsed_videos']}")
        self.logger.info(f" ❌ Parsing failed: {self.stats['parsing_failed']}")
        self.logger.info(f" ✅ Successful additions: {self.stats['successful_additions']}")
        self.logger.info(f" ❌ Failed additions: {self.stats['failed_additions']}")
        self.logger.info(f" 📂 Directories created: {self.stats['directories_created']}")
        self.logger.info(f" 📥 Items in download queue: {len(self.download_queue)}")

        if self.stats['total_videos'] > 0:
            success_rate = (self.stats['successful_additions'] / self.stats['total_videos']) * 100
            self.logger.info(f" 🎯 Success rate: {success_rate:.1f}%")

    # Legacy method for backward compatibility
    def process_all_videos(self, videos_data: List[Dict], start_queue: bool = True) -> Dict:
        """
        Legacy method - if video data is already parsed.
        For integration with existing code that might pass parsed data.
        """
        if not videos_data:
            return {"success": False, "error": "No video data provided"}

        # Extract URLs from video data if available
        video_urls = []
        for video_data in videos_data:
            if 'url' in video_data:
                video_urls.append(video_data['url'])
            elif 'video_id' in video_data:
                # Try to construct URL - this might need adjustment based on your site
                video_urls.append(f"https://yoursite.com/video/{video_data['video_id']}")

        if video_urls:
            return self.process_video_urls(video_urls, start_queue)
        else:
            return {"success": False, "error": "Could not extract URLs from video data"}
