"""
media_downloader.py

Reads a batch manifest (created by the parser via ManifestManager), downloads mp4 + jpg for each
video into the target_folder where metadata.json already exists, validates files, and retries up to
`max_retries`. Produces structured logs for each action.

Usage:
    python media_downloader.py --manifest manifests/batch_001_manifest.json --max-retries 3 --workers 4
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List
import requests

# Configure structured logging
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage()
        }
        # Add extra fields if they exist
        if hasattr(record, 'video_id'):
            log_entry['video_id'] = record.video_id
        if hasattr(record, 'event'):
            log_entry['event'] = record.event
        return json.dumps(log_entry)

# Set up logger
logger = logging.getLogger("media_downloader")
logger.setLevel(logging.DEBUG)

# Console handler with regular format
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler with structured JSON format
file_handler = logging.FileHandler("media_downloader.log", encoding='utf-8')
file_formatter = StructuredFormatter()
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

def setup_requests_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """
    Create a requests.Session for reuse.
    """
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    return s

def validate_video_folder(target_folder: Path) -> List[str]:
    """
    Return list of missing expected files. Expected: metadata.json, .mp4, .jpg
    """
    missing = []
    if not (target_folder / "metadata.json").exists():
        missing.append("metadata.json")
    mp4s = list(target_folder.glob("*.mp4"))
    jpgs = list(target_folder.glob("*.jpg")) + list(target_folder.glob("*.jpeg"))
    if not mp4s:
        missing.append("mp4")
    if not jpgs:
        missing.append("jpg")
    return missing

class MediaDownloader:
    def __init__(self, session: requests.Session = None, max_retries: int = 3, workers: int = 4):
        self.session = session or setup_requests_session()
        self.max_retries = max_retries
        self.workers = workers

    def download_file(self, url: str, dest_path: Path, timeout: int = 60) -> None:
        """
        Download a single file to dest_path. Raises on failure.
        """
        logger.info("Starting download", extra={
            "event": "download_start",
            "url": url,
            "dest": str(dest_path)
        })
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.session.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            f.write(chunk)
                os.replace(tmp_path, dest_path)
            logger.info("Download completed", extra={
                "event": "download_complete",
                "url": url,
                "dest": str(dest_path),
                "size_bytes": dest_path.stat().st_size
            })
        except Exception as exc:
            logger.error("Download failed", extra={
                "event": "download_error",
                "url": url,
                "dest": str(dest_path),
                "error": str(exc)
            })
            raise

    def process_video_entry(self, entry: Dict) -> Dict:
        """
        Download mp4 + jpg for a single entry, validate, retry up to max_retries.
        Returns a dict with result status and details.
        """
        video_id = entry["video_id"]
        page = entry.get("page")
        target_folder = Path(entry["target_folder"])
        mp4_url = entry["mp4_url"]
        jpg_url = entry["jpg_url"]

        result = {
            "video_id": video_id,
            "page": page,
            "attempts": 0,
            "status": "failed",
            "missing": []
        }

        for attempt in range(1, self.max_retries + 1):
            result["attempts"] = attempt
            logger.info("Starting video processing attempt", extra={
                "event": "video_attempt_start",
                "video_id": video_id,
                "attempt": attempt,
                "max_attempts": self.max_retries
            })
            try:
                # Download files if missing
                if not any(target_folder.glob("*.mp4")):
                    dest_mp4 = target_folder / f"{video_id}.mp4"
                    self.download_file(mp4_url, dest_mp4)
                else:
                    logger.debug("MP4 already exists", extra={
                        "event": "mp4_already_exists",
                        "video_id": video_id
                    })

                if not any(target_folder.glob("*.jpg")) and not any(target_folder.glob("*.jpeg")):
                    dest_jpg = target_folder / f"{video_id}.jpg"
                    self.download_file(jpg_url, dest_jpg)
                else:
                    logger.debug("JPG already exists", extra={
                        "event": "jpg_already_exists",
                        "video_id": video_id
                    })

                # Validate
                missing = validate_video_folder(target_folder)
                if not missing:
                    result["status"] = "success"
                    result["missing"] = []
                    logger.info("Video validation successful", extra={
                        "event": "video_validation_success",
                        "video_id": video_id,
                        "attempt": attempt
                    })
                    return result
                else:
                    result["missing"] = missing
                    logger.warning("Video validation failed - files missing", extra={
                        "event": "video_validation_failed",
                        "video_id": video_id,
                        "missing": missing,
                        "attempt": attempt
                    })
            except Exception as exc:
                logger.error("Exception during download attempt", extra={
                    "event": "video_attempt_exception",
                    "video_id": video_id,
                    "attempt": attempt,
                    "error": str(exc)
                })
                result["missing"] = validate_video_folder(target_folder)
            if attempt < self.max_retries:
                wait_seconds = 5 * attempt
                logger.info("Waiting before retry", extra={
                    "event": "retry_wait",
                    "video_id": video_id,
                    "wait_seconds": wait_seconds
                })
                time.sleep(wait_seconds)
        # permanent failure
        result["status"] = "failed"
        logger.error("Video failed permanently after all retries", extra={
            "event": "video_failed_permanently",
            "video_id": video_id,
            "missing": result["missing"],
            "total_attempts": self.max_retries
        })
        return result

    def download_from_manifest(self, manifest_path: str, update_progress_cb=None) -> List[Dict]:
        """
        Read manifest and download videos concurrently. update_progress_cb(video_result) optional callback.
        Returns list of result dicts.
        """
        p = Path(manifest_path)
        with p.open("r", encoding="utf-8") as f:
            manifest = json.load(f)

        videos = manifest.get("videos", [])
        results = []
        logger.info("Starting manifest processing", extra={
            "event": "start_manifest_processing",
            "manifest": str(p),
            "video_count": len(videos),
            "batch_id": manifest.get("batch_id")
        })

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {ex.submit(self.process_video_entry, v): v for v in videos}
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as exc:
                    v = futures[fut]
                    logger.error("Unhandled exception processing video", extra={
                        "event": "video_unhandled_exception",
                        "video_id": v.get("video_id"),
                        "error": str(exc)
                    })
                    res = {"video_id": v.get("video_id"), "status": "failed", "error": str(exc)}
                results.append(res)
                if update_progress_cb:
                    try:
                        update_progress_cb(res)
                    except Exception:
                        logger.debug("Progress callback failed", exc_info=True)

        # Log final statistics
        success_count = len([r for r in results if r["status"] == "success"])
        failed_count = len([r for r in results if r["status"] == "failed"])
        logger.info("Manifest processing completed", extra={
            "event": "manifest_processing_complete",
            "manifest": str(p),
            "total_videos": len(results),
            "successful": success_count,
            "failed": failed_count,
            "success_rate": (success_count / len(results) * 100) if results else 0
        })
        return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download media for a given manifest file.")
    parser.add_argument("--manifest", required=True, help="Path to batch manifest JSON")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per video")
    parser.add_argument("--workers", type=int, default=4, help="Parallel downloads")
    args = parser.parse_args()

    md = MediaDownloader(max_retries=args.max_retries, workers=args.workers)
    results = md.download_from_manifest(args.manifest)

    # write a simple result summary next to manifest
    summary_path = Path(args.manifest).with_name(Path(args.manifest).stem + "_results.json")
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump({"results": results}, f, indent=2)

    logger.info(f"Wrote results summary to {summary_path}")

    # Print final summary to console
    success_count = len([r for r in results if r["status"] == "success"])
    failed_count = len([r for r in results if r["status"] == "failed"])
    print(f"\n=== Download Results ===")
    print(f"Total videos: {len(results)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {failed_count}")
    print(f"Success rate: {(success_count / len(results) * 100):.1f}%" if results else "0%")
