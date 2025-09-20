#!/usr/bin/env python3

"""
manifest_manager.py

Simple manager for creating/saving batch manifest files that list all videos (mp4 + jpg urls)
collected during fast parsing. Manifests are atomic-written JSON files that downstream
media_downloader.py will consume.
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class ManifestManager:
    def __init__(self, manifest_dir: str = "manifests"):
        """
        :param manifest_dir: Directory to write manifest files
        """
        self.manifest_dir = Path(manifest_dir)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self._manifest = None

    def new_manifest(self, batch_id: int, pages: List[int]) -> None:
        """
        Initialize a new manifest in memory for this batch.
        """
        logger.info(f"Creating new manifest for batch {batch_id}, pages: {pages}")
        self._manifest = {
            "batch_id": batch_id,
            "pages": pages,
            "videos": [],
            "created_ts": datetime.now(timezone.utc).isoformat(),
        }

    def add_video_entry(self, page: int, video_id: str, mp4_url: str, jpg_url: str, target_folder: str) -> None:
        """
        Add a single video's urls to manifest.
        """
        if self._manifest is None:
            raise RuntimeError("Manifest not initialized. Call new_manifest() first.")
        
        entry = {
            "page": page,
            "video_id": video_id,
            "mp4_url": mp4_url,
            "jpg_url": jpg_url,
            "target_folder": target_folder,
        }
        self._manifest["videos"].append(entry)
        
        # Structured logging
        log_entry = {
            "event": "video_added_to_manifest",
            "video_id": video_id,
            "page": page,
            "batch_id": self._manifest["batch_id"]
        }
        logger.debug(json.dumps(log_entry))

    def save(self, filename: Optional[str] = None) -> Path:
        """
        Atomically write the manifest to disk. Returns path to manifest file.
        """
        if self._manifest is None:
            raise RuntimeError("Nothing to save; manifest is empty.")
        
        batch_id = self._manifest["batch_id"]
        if filename is None:
            filename = f"batch_{batch_id:03d}_manifest.json"
        dest = self.manifest_dir / filename

        # atomic write: write to temp file then os.replace
        tmp_fd, tmp_path = tempfile.mkstemp(dir=str(self.manifest_dir), prefix="._tmp_manifest_")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(self._manifest, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, dest)
            
            # Structured logging
            log_entry = {
                "event": "manifest_saved",
                "batch_id": batch_id,
                "video_count": len(self._manifest["videos"]),
                "path": str(dest)
            }
            logger.info(json.dumps(log_entry))
            
        except Exception as e:
            logger.error(f"Failed to save manifest: {e}")
            raise
        finally:
            # cleanup if tmp still exists
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    logger.debug("Could not remove tmp manifest file", exc_info=True)
        return dest

    def load(self, manifest_path: str) -> Dict:
        """
        Load and return manifest JSON from disk.
        """
        p = Path(manifest_path)
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Structured logging
        log_entry = {
            "event": "manifest_loaded",
            "path": str(p),
            "video_count": len(data.get("videos", [])),
            "batch_id": data.get("batch_id")
        }
        logger.info(json.dumps(log_entry))
        
        return data

    def get_current_manifest_info(self) -> Optional[Dict]:
        """
        Get information about the current manifest in memory.
        """
        if self._manifest is None:
            return None
        
        return {
            "batch_id": self._manifest["batch_id"],
            "pages": self._manifest["pages"],
            "video_count": len(self._manifest["videos"]),
            "created_ts": self._manifest["created_ts"]
        }
