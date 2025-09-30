# manifest_manager.py
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List

class ManifestManager:
    def __init__(self, manifest_file: str = "video_manifest.json"):
        self.manifest_file = Path(manifest_file)
        self.manifest_data = self._load_manifest()
    
    def _load_manifest(self) -> Dict:
        """Load existing manifest or create new structure"""
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading manifest: {e}")
        
        return {
            "manifest_version": "1.0",
            "created_at": datetime.now().isoformat(),
            "last_updated": "",
            "total_videos_discovered": 0,
            "videos_by_page": {},
            "video_queue": [],
            "processed_videos": [],
            "failed_extractions": []
        }
    
    def save_manifest(self):
        """Save manifest to JSON file"""
        self.manifest_data["last_updated"] = datetime.now().isoformat()
        
        try:
            with open(self.manifest_file, 'w', encoding='utf-8') as f:
                json.dump(self.manifest_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving manifest: {e}")
    
    def add_page_videos(self, page_num: int, video_list: List[Dict]) -> int:
        """Add videos with deduplication and return count of new videos added"""
        page_key = str(page_num)
        
        self.manifest_data["videos_by_page"][page_key] = {
            "page_number": page_num,
            "video_count": len(video_list),
            "scraped_at": datetime.now().isoformat(),
            "videos": video_list
        }
        
        added_count = 0
        existing_ids = {v["video_id"] for v in self.manifest_data["video_queue"]}
        
        for video in video_list:
            if video["video_id"] not in existing_ids:
                queue_entry = {
                    **video,
                    "page_source": page_num,
                    "queue_status": "pending",
                    "added_to_queue_at": datetime.now().isoformat()
                }
                
                self.manifest_data["video_queue"].append(queue_entry)
                existing_ids.add(video["video_id"])
                added_count += 1
        
        self.manifest_data["total_videos_discovered"] = len(self.manifest_data["video_queue"])
        self.save_manifest()
        
        return added_count
    
    def add_single_video(self, video: Dict, page_num: int):
        """Add a single video to manifest with deduplication"""
        existing_ids = {v["video_id"] for v in self.manifest_data["video_queue"]}
        
        if video["video_id"] not in existing_ids:
            queue_entry = {
                **video,
                "page_source": page_num,
                "queue_status": "pending", 
                "added_to_queue_at": datetime.now().isoformat()
            }
            
            self.manifest_data["video_queue"].append(queue_entry)
            self.manifest_data["total_videos_discovered"] = len(self.manifest_data["video_queue"])
            self.save_manifest()
            return True
        
        return False
    
    def get_pending_videos(self) -> List[Dict]:
        """Get all pending videos from the queue"""
        return [v for v in self.manifest_data["video_queue"] if v.get("queue_status") == "pending"]
    
    def get_queue_statistics(self) -> Dict:
        """Get queue statistics"""
        all_videos = self.manifest_data["video_queue"]
        
        return {
            "total_discovered": len(all_videos),
            "pending": len([v for v in all_videos if v.get("queue_status") == "pending"]),
            "completed": len([v for v in all_videos if v.get("queue_status") == "completed"]),
            "failed": len([v for v in all_videos if v.get("queue_status") == "failed"])
        }
    
    def mark_video_completed(self, video_id: str, download_root: Path):
        """Mark video as completed ONLY if all required files exist"""
        video_folder = download_root / video_id
        
        required_files = {
            'video': video_folder / f"{video_id}.mp4",
            'metadata': video_folder / f"{video_id}.json"
        }
        
        files_status = {}
        total_size_mb = 0
        
        for file_type, file_path in required_files.items():
            exists = file_path.exists() and file_path.stat().st_size > 0
            files_status[file_type] = exists
            
            if exists:
                total_size_mb += file_path.stat().st_size / (1024 * 1024)
        
        all_files_exist = all(files_status.values())
        
        if not all_files_exist:
            print(f"[MANIFEST] Cannot mark {video_id} as complete - missing files:")
            for file_type, exists in files_status.items():
                print(f"  {file_type}: {'OK' if exists else 'FAIL'}")
            return False
        
        for video in self.manifest_data["video_queue"]:
            if video["video_id"] == video_id:
                video["queue_status"] = "completed"
                video["completed_at"] = datetime.now().isoformat()
                video["completion_metadata"] = {
                    "total_size_mb": total_size_mb,
                    "files_verified": files_status,
                    "completion_method": "all_files_present"
                }
                break
        
        completion_entry = {
            "video_id": video_id,
            "completed_at": datetime.now().isoformat(),
            "metadata": {
                "folder_path": str(video_folder),
                "total_size_mb": total_size_mb,
                "files": {
                    "video": f"{video_id}.mp4",
                    "metadata": f"{video_id}.json"
                },
                "files_verified": files_status
            }
        }
        
        self.manifest_data["processed_videos"].append(completion_entry)
        self.save_manifest()
        
        print(f"[MANIFEST] Marked {video_id} as completed ({total_size_mb:.1f}MB)")
        return True
    
    def mark_video_failed(self, video_id: str, error_message: str):
        """Mark video as failed"""
        for video in self.manifest_data["video_queue"]:
            if video["video_id"] == video_id:
                video["queue_status"] = "failed"
                video["failed_at"] = datetime.now().isoformat()
                video["error_message"] = error_message
                break
        
        self.manifest_data["failed_extractions"].append({
            "video_id": video_id,
            "failed_at": datetime.now().isoformat(),
            "error": error_message
        })
        
        self.save_manifest()
        print(f"[MANIFEST] Marked {video_id} as failed: {error_message}")
    
    def mark_video_processing(self, video_id: str):
        """Mark video as currently being processed (metadata extraction started)"""
        for video in self.manifest_data["video_queue"]:
            if video["video_id"] == video_id:
                video["queue_status"] = "processing"
                video["processing_started_at"] = datetime.now().isoformat()
                break
        
        self.save_manifest()
    
    def mark_video_queued_for_download(self, video_id: str):
        """Mark video as queued for download (metadata complete, video queued in IDM)"""
        for video in self.manifest_data["video_queue"]:
            if video["video_id"] == video_id:
                video["queue_status"] = "downloading"
                video["download_queued_at"] = datetime.now().isoformat()
                break
        
        self.save_manifest()
    
    def verify_and_update_completions(self, download_root: Path) -> Dict:
        """Verify completion status by checking actual files and update manifest"""
        verification_stats = {
            "verified_complete": 0,
            "found_incomplete": 0,
            "corrected_statuses": 0
        }
        
        for video in self.manifest_data["video_queue"]:
            video_id = video["video_id"]
            current_status = video.get("queue_status", "pending")
            
            video_folder = download_root / video_id
            if video_folder.exists():
                required_files = {
                    'video': video_folder / f"{video_id}.mp4",
                    'metadata': video_folder / f"{video_id}.json"
                }
                
                all_files_exist = all(
                    file_path.exists() and file_path.stat().st_size > 0
                    for file_path in required_files.values()
                )
                
                if all_files_exist and current_status != "completed":
                    video["queue_status"] = "completed"
                    video["completed_at"] = datetime.now().isoformat()
                    verification_stats["corrected_statuses"] += 1
                    verification_stats["verified_complete"] += 1
                    
                elif not all_files_exist and current_status == "completed":
                    video["queue_status"] = "failed"
                    video["failed_at"] = datetime.now().isoformat()
                    video["error_message"] = "Files missing during verification"
                    verification_stats["corrected_statuses"] += 1
                    verification_stats["found_incomplete"] += 1
                    
                elif all_files_exist:
                    verification_stats["verified_complete"] += 1
                    
        self.save_manifest()
        return verification_stats