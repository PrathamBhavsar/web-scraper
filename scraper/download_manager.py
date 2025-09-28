import json
import os
import subprocess
import asyncio
import aiohttp
import ssl
from pathlib import Path
from typing import List, Dict, Optional
import time
from scraper.validator import basic_mp4_check

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

class DownloadManager:
    def __init__(self, config: dict):
        self.config = config
        self.download_method = config['download']['download_method']
        self.max_retries = config['download']['max_retries']
        self.timeout = config['download']['timeout_seconds']
        self.idm_path = config['download'].get('idm_path', 'C:\\Program Files (x86)\\Internet Download Manager\\idman.exe')
    
    async def add_to_queue(self, video: Dict, download_root: Path) -> bool:
        """Add both video and thumbnail to IDM download queue and start immediately"""
        video_id = video.get('video_id', 'unknown')
        download_url = video.get('video_src', '')
        thumbnail_url = video.get('thumbnail_src', '')

        if not download_url:
            print(f"No download URL for video {video_id}")
            return False

        # Verify IDM is installed
        if not Path(self.idm_path).exists():
            print(f"ERROR: IDM not found at {self.idm_path}")
            return False

        try:
            # Create video folder using video_id
            video_folder = download_root / video_id
            video_folder.mkdir(exist_ok=True)

            # Define file paths with video_id naming
            video_filename = f"{video_id}.mp4"
            thumbnail_filename = f"{video_id}.jpg"

            print(f"[IDM QUEUE] Adding to queue: {video_id}: {video.get('title', 'Unknown')}")

            files_queued = 0

            # 1. Queue the video file
            video_cmd = [
                self.idm_path,
                '/d', download_url,        # Download URL
                '/p', str(video_folder),   # Save path
                '/f', video_filename,      # Filename
                '/n',                      # Don't show confirmation
                '/a'                       # Add to queue but don't start
            ]

            print(f"[IDM QUEUE] Queuing video: {' '.join(video_cmd)}")
            result = subprocess.run(video_cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                print(f"[IDM QUEUE] Successfully queued video {video_id}")
                files_queued += 1
            else:
                print(f"[IDM QUEUE] Failed to queue video {video_id}: {result.stderr}")

            # 2. Queue the thumbnail file (if URL exists)
            if thumbnail_url:
                thumbnail_cmd = [
                    self.idm_path,
                    '/d', thumbnail_url,       # Download URL
                    '/p', str(video_folder),   # Save path
                    '/f', thumbnail_filename,  # Filename
                    '/n',                      # Don't show confirmation
                    '/a'                       # Add to queue but don't start
                ]

                print(f"[IDM QUEUE] Queuing thumbnail: {' '.join(thumbnail_cmd)}")
                result = subprocess.run(thumbnail_cmd, capture_output=True, text=True, timeout=30)

                if result.returncode == 0:
                    print(f"[IDM QUEUE] Successfully queued thumbnail {video_id}")
                    files_queued += 1
                else:
                    print(f"[IDM QUEUE] Failed to queue thumbnail {video_id}: {result.stderr}")
            else:
                print(f"[IDM QUEUE] No thumbnail URL for {video_id}, skipping")

            # 3. Start the IDM queue immediately after adding files
            if files_queued > 0:
                start_cmd = [
                    self.idm_path,
                    '/s'  # Start queue
                ]

                print(f"[IDM QUEUE] Starting IDM queue...")
                result = subprocess.run(start_cmd, capture_output=True, text=True, timeout=10)

                if result.returncode == 0:
                    print(f"[IDM QUEUE] IDM queue started successfully")
                    return True
                else:
                    print(f"[IDM QUEUE] Failed to start IDM queue: {result.stderr}")
                    return files_queued > 0  # Return true if we at least queued files
            else:
                print(f"[IDM QUEUE] No files were queued for {video_id}")
                return False

        except subprocess.TimeoutExpired:
            print(f"[IDM QUEUE] IDM queue timeout for {video_id}")
            return False
        except Exception as e:
            print(f"[IDM QUEUE] Error queuing {video_id}: {e}")
            return False
    async def add_batch_to_queue(self, videos: List[Dict], download_root: Path) -> bool:
        """Add multiple videos (both video and thumbnail) to IDM queue and start once"""
        if not videos:
            print("[IDM BATCH] No videos to queue")
            return False

        # Verify IDM is installed
        if not Path(self.idm_path).exists():
            print(f"ERROR: IDM not found at {self.idm_path}")
            return False

        try:
            files_queued = 0
            print(f"[IDM BATCH] Queuing {len(videos)} videos with thumbnails...")

            for video in videos:
                video_id = video.get('video_id', 'unknown')
                download_url = video.get('video_src', '')
                thumbnail_url = video.get('thumbnail_src', '')

                if not download_url:
                    print(f"[IDM BATCH] No download URL for video {video_id}, skipping")
                    continue

                # Create video folder using video_id
                video_folder = download_root / video_id
                video_folder.mkdir(exist_ok=True)

                # Define file paths with video_id naming
                video_filename = f"{video_id}.mp4"
                thumbnail_filename = f"{video_id}.jpg"

                # Queue the video file
                video_cmd = [
                    self.idm_path,
                    '/d', download_url,        # Download URL
                    '/p', str(video_folder),   # Save path
                    '/f', video_filename,      # Filename
                    '/n',                      # Don't show confirmation
                    '/a'                       # Add to queue but don't start
                ]

                result = subprocess.run(video_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    files_queued += 1
                    print(f"[IDM BATCH] Queued video {video_id}")
                else:
                    print(f"[IDM BATCH] Failed to queue video {video_id}: {result.stderr}")

                # Queue the thumbnail file (if URL exists)
                if thumbnail_url:
                    thumbnail_cmd = [
                        self.idm_path,
                        '/d', thumbnail_url,       # Download URL
                        '/p', str(video_folder),   # Save path
                        '/f', thumbnail_filename,  # Filename
                        '/n',                      # Don't show confirmation
                        '/a'                       # Add to queue but don't start
                    ]

                    result = subprocess.run(thumbnail_cmd, capture_output=True, text=True, timeout=30)
                    if result.returncode == 0:
                        files_queued += 1
                        print(f"[IDM BATCH] Queued thumbnail {video_id}")
                    else:
                        print(f"[IDM BATCH] Failed to queue thumbnail {video_id}: {result.stderr}")

                # Small delay between videos to avoid overwhelming IDM
                await asyncio.sleep(0.5)

            # Start the IDM queue once after adding all files
            if files_queued > 0:
                start_cmd = [
                    self.idm_path,
                    '/s'  # Start queue
                ]

                print(f"[IDM BATCH] Starting IDM queue with {files_queued} files...")
                result = subprocess.run(start_cmd, capture_output=True, text=True, timeout=10)

                if result.returncode == 0:
                    print(f"[IDM BATCH] IDM queue started successfully with {files_queued} files")
                    return True
                else:
                    print(f"[IDM BATCH] Failed to start IDM queue: {result.stderr}")
                    return files_queued > 0
            else:
                print("[IDM BATCH] No files were queued")
                return False

        except Exception as e:
            print(f"[IDM BATCH] Error in batch queueing: {e}")
            return False
    async def save_video_metadata(self, video: Dict, download_root: Path):
        """Save video metadata immediately without waiting for download"""
        try:
            video_id = video.get('video_id', 'unknown')
            video_folder = download_root / video_id
            video_folder.mkdir(exist_ok=True)
            
            metadata_path = video_folder / f"{video_id}.json"
            
            # Remove unwanted fields before saving
            metadata = {k: v for k, v in video.items() if v is not None and k not in ["download_sources"]}
            
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"[METADATA] Metadata saved: {video_id}.json")
            return True
            
        except Exception as e:
            print(f"[METADATA] Error saving metadata for {video_id}: {e}")
            return False
    
    async def download_video_thumbnail(self, video: Dict, download_root: Path):
        """Download video thumbnail immediately without waiting for video download"""
        thumbnail_url = video.get('thumbnail_src', '')
        video_id = video.get('video_id', 'unknown')
        
        if not thumbnail_url:
            print(f"[THUMBNAIL] No thumbnail URL for {video_id}")
            return False
        
        try:
            video_folder = download_root / video_id
            video_folder.mkdir(exist_ok=True)
            
            thumbnail_path = video_folder / f"{video_id}.jpg"
            
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
                async with session.get(thumbnail_url) as resp:
                    resp.raise_for_status()
                    with open(thumbnail_path, 'wb') as f:
                        f.write(await resp.read())
            
            print(f"[THUMBNAIL] Thumbnail downloaded: {video_id}.jpg")
            return True
            
        except Exception as e:
            print(f"[THUMBNAIL] Error downloading thumbnail for {video_id}: {e}")
            return False

    # Keep existing methods for compatibility...
    async def process_video_batch(self, video_batch: List[Dict], download_root: Path):
        """Process a batch of videos using configured download method"""
        if self.download_method == "idm":
            return await self._process_batch_idm(video_batch, download_root)
        elif self.download_method == "direct":
            return await self._process_batch_direct(video_batch, download_root)
        elif self.download_method == "hybrid":
            return await self._process_batch_hybrid(video_batch, download_root)
        else:
            print(f"Unknown download method: {self.download_method}")
            return False

    async def _process_batch_idm(self, video_batch: List[Dict], download_root: Path):
        """Process batch using IDM downloads - PRIORITY METHOD"""
        successful_downloads = 0
        
        # Verify IDM is installed
        if not Path(self.idm_path).exists():
            print(f"ERROR: IDM not found at {self.idm_path}")
            print("Please install IDM or update the idm_path in config.json")
            return False
        
        print(f"[IDM] Using Internet Download Manager at: {self.idm_path}")
        
        for video in video_batch:
            success = await self._idm_download_video(video, download_root)
            if success:
                successful_downloads += 1
            # Delay between IDM calls
            await asyncio.sleep(2)
        
        print(f"IDM batch: {successful_downloads}/{len(video_batch)} successful")
        return successful_downloads > 0

    async def _idm_download_video(self, video: Dict, download_root: Path) -> bool:
        """Download single video using IDM"""
        video_id = video.get('video_id', 'unknown')
        download_url = video.get('video_src', '')
        
        if not download_url:
            print(f"No download URL for video {video_id}")
            return False
        
        try:
            # Create video folder using video_id
            video_folder = download_root / video_id
            video_folder.mkdir(exist_ok=True)
            
            # Define file paths with video_id naming
            video_filename = f"{video_id}.mp4"
            video_path = video_folder / video_filename
            
            print(f"[IDM] Downloading {video_id}: {video.get('title', 'Unknown')}")
            
            # Call IDM with proper parameters
            cmd = [
                self.idm_path,
                '/d', download_url,  # Download URL
                '/p', str(video_folder),  # Save path
                '/f', video_filename,  # Filename
                '/n',  # Don't show confirmation
                '/q'   # Quiet mode
            ]
            
            print(f"[IDM] Calling IDM: {' '.join(cmd)}")
            
            # Execute IDM command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
            
            if result.returncode == 0:
                print(f"[IDM] IDM command executed successfully for {video_id}")
                
                # Wait for file to appear (IDM downloads asynchronously)
                max_wait = 60  # Wait up to 60 seconds
                wait_time = 0
                
                while wait_time < max_wait:
                    if video_path.exists() and video_path.stat().st_size > 0:
                        break
                    await asyncio.sleep(1)
                    wait_time += 1
                
                if video_path.exists():
                    # Validate MP4 file
                    if basic_mp4_check(str(video_path)):
                        file_size_mb = video_path.stat().st_size / (1024 * 1024)
                        print(f"[IDM] Video downloaded successfully: {video_id} ({file_size_mb:.1f}MB)")
                        
                        # Save metadata and thumbnail
                        await self._save_video_metadata(video, video_folder, "IDM_Complete")
                        await self._download_thumbnail(video, video_folder)
                        
                        print(f"[IDM] Complete structure created for {video_id}")
                        return True
                    else:
                        print(f"[IDM] Invalid MP4 file for {video_id}")
                        if video_path.exists():
                            video_path.unlink()
                        return False
                else:
                    print(f"[IDM] File not found after IDM download: {video_id}")
                    return False
            else:
                print(f"[IDM] IDM command failed for {video_id}: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"[IDM] IDM timeout for {video_id}")
            return False
        except Exception as e:
            print(f"[IDM] Error with IDM download {video_id}: {e}")
            return False

    async def _process_batch_hybrid(self, video_batch: List[Dict], download_root: Path):
        """Process batch using hybrid method: IDM first, then direct fallback"""
        successful_downloads = 0
        
        # Check if IDM is available
        idm_available = Path(self.idm_path).exists()
        
        if idm_available:
            print("[HYBRID] Attempting IDM downloads first...")
            for video in video_batch:
                video_id = video.get('video_id', 'unknown')
                
                # Try IDM first
                idm_success = await self._idm_download_video(video, download_root)
                if idm_success:
                    successful_downloads += 1
                    print(f"[HYBRID] IDM success for {video_id}")
                else:
                    print(f"[HYBRID] IDM failed for {video_id}, trying direct download...")
                    # Fallback to direct download
                    direct_success = await self._direct_download_video(video, download_root, "Hybrid_Direct")
                    if direct_success:
                        successful_downloads += 1
                        print(f"[HYBRID] Direct fallback success for {video_id}")
                    else:
                        print(f"[HYBRID] Both methods failed for {video_id}")
                
                await asyncio.sleep(1)
        else:
            print("[HYBRID] IDM not available, using direct downloads only...")
            successful_downloads = await self._process_batch_direct(video_batch, download_root)
        
        print(f"Hybrid batch: {successful_downloads}/{len(video_batch)} successful")
        return successful_downloads > 0

    async def _process_batch_direct(self, video_batch: List[Dict], download_root: Path):
        """Process batch using direct downloads"""
        successful_downloads = 0
        
        for video in video_batch:
            success = await self._direct_download_video(video, download_root, "Direct_Complete")
            if success:
                successful_downloads += 1
            await asyncio.sleep(1)
        
        print(f"Direct download batch: {successful_downloads}/{len(video_batch)} successful")
        return successful_downloads > 0

    async def _direct_download_video(self, video: Dict, download_root: Path, method_name: str = "Direct_Complete") -> bool:
        """Download single video with direct method - FALLBACK ONLY"""
        video_id = video.get('video_id', 'unknown')
        download_url = video.get('video_src', '')
        
        if not download_url:
            print(f"No download URL for video {video_id}")
            return False
        
        try:
            # Create video folder using video_id
            video_folder = download_root / video_id
            video_folder.mkdir(exist_ok=True)
            
            # Define file paths with video_id naming
            video_filename = f"{video_id}.mp4"
            video_path = video_folder / video_filename
            temp_path = video_folder / f"{video_filename}.tmp"
            
            print(f"[DIRECT] Downloading {video_id}: {video.get('title', 'Unknown')}")
            
            # Download video file
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as client:
                async with client.get(download_url) as response:
                    response.raise_for_status()
                    with open(temp_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
            
            # Move temp file to final location
            temp_path.rename(video_path)
            
            # Validate MP4 file
            if not basic_mp4_check(str(video_path)):
                print(f"[DIRECT] Invalid MP4 file for {video_id}")
                video_path.unlink()
                return False
            
            file_size_mb = video_path.stat().st_size / (1024 * 1024)
            print(f"[DIRECT] Video downloaded successfully: {video_id} ({file_size_mb:.1f}MB)")
            
            # Save metadata and thumbnail
            await self._save_video_metadata(video, video_folder, method_name)
            await self._download_thumbnail(video, video_folder)
            
            print(f"[DIRECT] Complete structure created for {video_id}")
            return True
            
        except Exception as e:
            print(f"[DIRECT] Error downloading {video_id}: {e}")
            if temp_path and temp_path.exists():
                temp_path.unlink()
            return False

    async def _save_video_metadata(self, video: Dict, video_folder: Path, download_method: str):
        """Save video metadata with video_id naming and download method"""
        try:
            video_id = video.get('video_id', 'unknown')
            metadata_path = video_folder / f"{video_id}.json"
            
            # Remove unwanted fields before saving
            metadata = {k: v for k, v in video.items() if v is not None and k not in ["download_sources", "download_method_used"]}
            
            # Optionally, you can still log the method if needed, but do not store it in the file
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"[METADATA] Metadata saved: {video_id}.json")
            
        except Exception as e:
            print(f"[METADATA] Error saving metadata: {e}")

    async def _download_thumbnail(self, video: Dict, video_folder: Path):
        """Download video thumbnail with video_id naming"""
        thumbnail_url = video.get('thumbnail_src', '')
        video_id = video.get('video_id', 'unknown')
        
        if not thumbnail_url:
            print(f"[THUMBNAIL] No thumbnail URL for {video_id}")
            return False
        
        try:
            thumbnail_path = video_folder / f"{video_id}.jpg"
            
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
                async with session.get(thumbnail_url) as resp:
                    resp.raise_for_status()
                    with open(thumbnail_path, 'wb') as f:
                        f.write(await resp.read())
            
            print(f"[THUMBNAIL] Thumbnail downloaded: {video_id}.jpg")
            return True
            
        except Exception as e:
            print(f"[THUMBNAIL] Error downloading thumbnail for {video_id}: {e}")
            return False