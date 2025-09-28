import json
from pathlib import Path
from datetime import datetime
from typing import Dict

class ProgressManager:
    def __init__(self, progress_file: str = "progress.json"):
        self.progress_file = Path(progress_file)
        self.progress_data = self._load_progress()
    
    def _load_progress(self) -> Dict:
        """Load existing progress or create new structure as per README.md"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading progress: {e}")
        
        return {
            "scraper_version": "1.0",
            "last_run_at": "",
            "current_page": 0,
            "last_scraped_page": 0,
            "total_videos_downloaded": 0,
            "total_size_gb": 0.0,
            "session_stats": {
                "videos_processed": 0,
                "videos_completed": 0,
                "videos_failed": 0,
                "session_start": "",
                "last_activity": ""
            },
            "failed_videos": [],
            "completed_videos": []
        }
    
    def save_progress(self):
        """Save current progress to JSON file"""
        self.progress_data["last_run_at"] = datetime.now().isoformat()
        self.progress_data["session_stats"]["last_activity"] = datetime.now().isoformat()
        
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving progress: {e}")
    
    def update_current_page(self, page_num: int):
        """Update the current page being processed"""
        self.progress_data["current_page"] = page_num
        self.progress_data["last_scraped_page"] = page_num
        self.save_progress()
    
    def mark_video_downloaded(self, video_id: str, download_root: Path):
        """Mark video as successfully downloaded ONLY if all three files exist"""
        video_folder = download_root / video_id
        
        # Check that all three required files exist
        required_files = {
            'video': video_folder / f"{video_id}.mp4",
            'metadata': video_folder / f"{video_id}.json", 
            'thumbnail': video_folder / f"{video_id}.jpg"
        }
        
        all_files_exist = all(file_path.exists() and file_path.stat().st_size > 0 
                             for file_path in required_files.values())
        
        if not all_files_exist:
            print(f"[PROGRESS] Cannot mark {video_id} as complete - missing files:")
            for file_type, file_path in required_files.items():
                exists = file_path.exists() and file_path.stat().st_size > 0
                print(f"  {file_type}: {'✅' if exists else '❌'} {file_path}")
            return False
        
        # Calculate total size of all files
        total_size_mb = sum(file_path.stat().st_size for file_path in required_files.values()) / (1024 * 1024)
        
        self.progress_data["total_videos_downloaded"] += 1
        self.progress_data["total_size_gb"] += total_size_mb / 1024
        self.progress_data["session_stats"]["videos_completed"] += 1
        
        self.progress_data["completed_videos"].append({
            "video_id": video_id,
            "file_path": str(video_folder),
            "size_mb": total_size_mb,
            "completed_at": datetime.now().isoformat(),
            "files": {
                "video": f"{video_id}.mp4",
                "metadata": f"{video_id}.json", 
                "thumbnail": f"{video_id}.jpg"
            }
        })
        
        self.save_progress()
        print(f"[PROGRESS] Marked {video_id} as completed ({total_size_mb:.1f}MB)")
        return True
    
    def mark_video_failed(self, video_id: str, page: int, error: str = ""):
        """Mark video as failed with retry tracking"""
        self.progress_data["session_stats"]["videos_failed"] += 1
        
        self.progress_data["failed_videos"].append({
            "video_id": video_id,
            "page": page,
            "error": error,
            "failed_at": datetime.now().isoformat()
        })
        
        self.save_progress()
        print(f"[PROGRESS] Marked {video_id} as failed: {error}")
    
    def mark_video_processed(self, video_id: str):
        """Mark video as processed (metadata extracted, queued for download)"""
        self.progress_data["session_stats"]["videos_processed"] += 1
        self.save_progress()
    
    def check_video_completion_status(self, video_id: str, download_root: Path) -> str:
        """Check if video is complete, incomplete, or failed"""
        video_folder = download_root / video_id
        
        if not video_folder.exists():
            return "not_started"
        
        required_files = {
            'video': video_folder / f"{video_id}.mp4",
            'metadata': video_folder / f"{video_id}.json",
            'thumbnail': video_folder / f"{video_id}.jpg"
        }
        
        files_status = {
            file_type: file_path.exists() and file_path.stat().st_size > 0
            for file_type, file_path in required_files.items()
        }
        
        if all(files_status.values()):
            return "complete"
        elif any(files_status.values()):
            return "incomplete" 
        else:
            return "failed"
    
    def get_completion_stats(self, download_root: Path) -> Dict:
        """Get detailed completion statistics by checking actual files"""
        stats = {
            "total_processed": self.progress_data["session_stats"]["videos_processed"],
            "complete_count": 0,
            "incomplete_count": 0,
            "failed_count": 0,
            "not_started_count": 0
        }
        
        # Check all videos that were marked as processed
        completed_videos = [v["video_id"] for v in self.progress_data.get("completed_videos", [])]
        
        for video_id in completed_videos:
            status = self.check_video_completion_status(video_id, download_root)
            stats[f"{status}_count"] += 1
        
        return stats
    
    def get_last_scraped_page(self) -> int:
        """Get the last successfully scraped page for resume functionality"""
        return self.progress_data.get("last_scraped_page", 0)
    
    def should_resume_from_page(self, discovered_last_page: int) -> int:
        """Determine resume point based on progress and last page"""
        last_scraped = self.get_last_scraped_page()
        
        if last_scraped > 0:
            # Resume from the page after last scraped
            return max(1, last_scraped - 1)
        else:
            # First run - start from last page
            return discovered_last_page 