# progress_manager.py
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List

class ProgressManager:
    def __init__(self, progress_file: str = "progress.json"):
        self.progress_file = Path(progress_file)
        self.progress_data = self._load_progress()
    
    def _load_progress(self) -> Dict:
        """Load existing progress or create new structure"""
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
            "last_completed_page": 0,
            "total_videos_downloaded": 0,
            "total_size_mb": 0.0,
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
    
    def update_last_completed_page(self, page_num: int):
        """Update the last page whose videos were fully downloaded and validated"""
        self.progress_data["last_completed_page"] = page_num
        self.save_progress()
        print(f"[PROGRESS] Updated last completed page to {page_num}")
    
    def get_last_completed_page(self) -> int:
        """Get the last page whose videos were fully downloaded"""
        return self.progress_data.get("last_completed_page", 0)
    
    def update_total_size(self, total_size_mb: float):
        """Update total download size in MB"""
        self.progress_data["total_size_mb"] = total_size_mb
        self.save_progress()
    
    def get_total_size_mb(self) -> float:
        """Get current total size in MB"""
        return self.progress_data.get("total_size_mb", 0.0)
    
    def mark_video_downloaded(self, video_id: str, download_root: Path):
        """Mark video as successfully downloaded ONLY if all required files exist"""
        video_folder = download_root / video_id
        
        required_files = {
            'video': video_folder / f"{video_id}.mp4",
            'metadata': video_folder / f"{video_id}.json"
        }
        
        all_files_exist = all(file_path.exists() and file_path.stat().st_size > 0 
                             for file_path in required_files.values())
        
        if not all_files_exist:
            print(f"[PROGRESS] Cannot mark {video_id} as complete - missing files:")
            for file_type, file_path in required_files.items():
                exists = file_path.exists() and file_path.stat().st_size > 0
                print(f"  {file_type}: {'✅' if exists else '❌'} {file_path}")
            return False
        
        total_size_mb = sum(file_path.stat().st_size for file_path in required_files.values()) / (1024 * 1024)
        
        self.progress_data["total_videos_downloaded"] += 1
        self.progress_data["session_stats"]["videos_completed"] += 1
        
        self.save_progress()
        print(f"[PROGRESS] Marked {video_id} as completed ({total_size_mb:.1f}MB)")
        return True
    
    def mark_video_failed(self, video_id: str, page: int, error: str = ""):
        """Mark video as failed with retry tracking"""
        self.progress_data["session_stats"]["videos_failed"] += 1
        self.save_progress()
        print(f"[PROGRESS] Marked {video_id} as failed: {error}")
    
    def mark_video_processed(self, video_id: str):
        """Mark video as processed (metadata extracted, queued for download)"""
        self.progress_data["session_stats"]["videos_processed"] += 1
        self.save_progress()
    
    def update_final_lists(self, completed_ids: List[int], failed_ids: List[int]):
        """Update the final completed and failed video lists with integer IDs only"""
        self.progress_data["completed_videos"] = completed_ids
        self.progress_data["failed_videos"] = failed_ids
        self.save_progress()
    
    def check_video_completion_status(self, video_id: str, download_root: Path) -> str:
        """Check if video is complete, incomplete, or failed"""
        video_folder = download_root / video_id
        
        if not video_folder.exists():
            return "not_started"
        
        required_files = {
            'video': video_folder / f"{video_id}.mp4",
            'metadata': video_folder / f"{video_id}.json"
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
        
        completed_videos = self.progress_data.get("completed_videos", [])
        
        for video_id in completed_videos:
            status = self.check_video_completion_status(str(video_id), download_root)
            stats[f"{status}_count"] += 1
        
        return stats
    
    def get_last_scraped_page(self) -> int:
        """Get the last successfully scraped page for resume functionality"""
        return self.progress_data.get("last_scraped_page", 0)
    
    def should_resume_from_page(self, discovered_last_page: int) -> int:
        """Determine resume point based on progress and last page"""
        last_completed = self.get_last_completed_page()
        
        if last_completed > 0:
            return last_completed + 1
        else:
            return 1