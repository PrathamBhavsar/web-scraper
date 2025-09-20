#!/usr/bin/env python3

"""
media_downloader.py - ENHANCED WITH DETAILED ERROR LOGGING

Reads a batch manifest, downloads mp4 + jpg files, validates completeness,
and retries with comprehensive error reporting. This version shows exactly 
what files are missing and why downloads fail.

Usage:
    python media_downloader.py --manifest manifests/batch_001_manifest.json --max-retries 3 --workers 4
"""

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

# Enhanced structured logging
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage()
        }
        
        # Add extra structured fields
        extra_fields = ['video_id', 'event', 'url', 'dest', 'error', 'missing', 'attempt', 
                       'size_bytes', 'file_path', 'validation_details', 'error_type', 'status_code']
        
        for field in extra_fields:
            if hasattr(record, field):
                log_entry[field] = getattr(record, field)
                
        return json.dumps(log_entry, ensure_ascii=False)

# Set up logger with enhanced formatting
logger = logging.getLogger("media_downloader")
logger.setLevel(logging.DEBUG)

# Clear existing handlers
logger.handlers.clear()

# Console handler with detailed format
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler with structured JSON format
file_handler = logging.FileHandler("media_downloader_detailed.log", encoding='utf-8')
file_formatter = StructuredFormatter()
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)


def setup_requests_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """Create a requests.Session with proper headers."""
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    return s


def validate_video_folder(target_folder: Path, video_id: str) -> Dict[str, any]:
    """
    Enhanced validation with detailed file checking and logging.
    Returns detailed validation results including file sizes and existence.
    """
    validation_result = {
        "valid": False,
        "missing_files": [],
        "found_files": {},
        "file_details": {},
        "folder_exists": False,
        "folder_path": str(target_folder)
    }
    
    # Check if folder exists
    if not target_folder.exists():
        validation_result["missing_files"].append("folder")
        logger.error(f"Target folder does not exist: {target_folder}", extra={
            "event": "folder_missing",
            "video_id": video_id,
            "file_path": str(target_folder)
        })
        return validation_result
    
    validation_result["folder_exists"] = True
    
    # Check metadata.json
    metadata_file = target_folder / "metadata.json"
    if metadata_file.exists():
        try:
            size = metadata_file.stat().st_size
            validation_result["found_files"]["metadata.json"] = str(metadata_file)
            validation_result["file_details"]["metadata.json"] = {"size": size, "exists": True}
            
            # Validate JSON content
            with open(metadata_file, 'r', encoding='utf-8') as f:
                json.load(f)  # This will raise an exception if invalid JSON
                
            logger.debug(f"metadata.json validated successfully ({size} bytes)", extra={
                "event": "file_validated",
                "video_id": video_id,
                "file_type": "metadata",
                "file_path": str(metadata_file),
                "size_bytes": size
            })
        except Exception as e:
            validation_result["missing_files"].append("metadata.json")
            validation_result["file_details"]["metadata.json"] = {"size": 0, "exists": True, "error": str(e)}
            logger.error(f"metadata.json is corrupted: {e}", extra={
                "event": "file_corrupted",
                "video_id": video_id,
                "file_type": "metadata",
                "error": str(e)
            })
    else:
        validation_result["missing_files"].append("metadata.json")
        validation_result["file_details"]["metadata.json"] = {"size": 0, "exists": False}
        logger.warning(f"metadata.json missing for video {video_id}", extra={
            "event": "file_missing",
            "video_id": video_id,
            "file_type": "metadata"
        })
    
    # Check MP4 files
    mp4_files = list(target_folder.glob("*.mp4"))
    if mp4_files:
        mp4_file = mp4_files[0]  # Use first MP4 found
        try:
            size = mp4_file.stat().st_size
            validation_result["found_files"]["mp4"] = str(mp4_file)
            validation_result["file_details"]["mp4"] = {"size": size, "exists": True, "name": mp4_file.name}
            
            if size < 1024:  # Less than 1KB is suspicious
                logger.warning(f"MP4 file is very small ({size} bytes) - might be incomplete", extra={
                    "event": "file_size_warning",
                    "video_id": video_id,
                    "file_type": "mp4",
                    "size_bytes": size,
                    "file_path": str(mp4_file)
                })
            
            logger.debug(f"MP4 file validated ({size} bytes): {mp4_file.name}", extra={
                "event": "file_validated",
                "video_id": video_id,
                "file_type": "mp4",
                "file_path": str(mp4_file),
                "size_bytes": size
            })
        except Exception as e:
            validation_result["missing_files"].append("mp4")
            validation_result["file_details"]["mp4"] = {"size": 0, "exists": True, "error": str(e)}
            logger.error(f"MP4 file access error: {e}", extra={
                "event": "file_access_error",
                "video_id": video_id,
                "file_type": "mp4",
                "error": str(e)
            })
    else:
        validation_result["missing_files"].append("mp4")
        validation_result["file_details"]["mp4"] = {"size": 0, "exists": False}
        logger.warning(f"MP4 file missing for video {video_id}", extra={
            "event": "file_missing",
            "video_id": video_id,
            "file_type": "mp4"
        })
    
    # Check JPG files
    jpg_files = list(target_folder.glob("*.jpg")) + list(target_folder.glob("*.jpeg"))
    if jpg_files:
        jpg_file = jpg_files[0]  # Use first JPG found
        try:
            size = jpg_file.stat().st_size
            validation_result["found_files"]["jpg"] = str(jpg_file)
            validation_result["file_details"]["jpg"] = {"size": size, "exists": True, "name": jpg_file.name}
            
            if size < 100:  # Less than 100 bytes is suspicious
                logger.warning(f"JPG file is very small ({size} bytes) - might be incomplete", extra={
                    "event": "file_size_warning",
                    "video_id": video_id,
                    "file_type": "jpg",
                    "size_bytes": size,
                    "file_path": str(jpg_file)
                })
            
            logger.debug(f"JPG file validated ({size} bytes): {jpg_file.name}", extra={
                "event": "file_validated",
                "video_id": video_id,
                "file_type": "jpg",
                "file_path": str(jpg_file),
                "size_bytes": size
            })
        except Exception as e:
            validation_result["missing_files"].append("jpg")
            validation_result["file_details"]["jpg"] = {"size": 0, "exists": True, "error": str(e)}
            logger.error(f"JPG file access error: {e}", extra={
                "event": "file_access_error",
                "video_id": video_id,
                "file_type": "jpg",
                "error": str(e)
            })
    else:
        validation_result["missing_files"].append("jpg")
        validation_result["file_details"]["jpg"] = {"size": 0, "exists": False}
        logger.warning(f"JPG file missing for video {video_id}", extra={
            "event": "file_missing",
            "video_id": video_id,
            "file_type": "jpg"
        })
    
    # Determine if validation passed
    validation_result["valid"] = len(validation_result["missing_files"]) == 0
    
    # Log validation summary
    if validation_result["valid"]:
        logger.info(f"✅ Video {video_id} validation PASSED - all files present", extra={
            "event": "validation_success",
            "video_id": video_id,
            "validation_details": validation_result["file_details"]
        })
    else:
        logger.warning(f"❌ Video {video_id} validation FAILED - missing: {validation_result['missing_files']}", extra={
            "event": "validation_failed",
            "video_id": video_id,
            "missing": validation_result["missing_files"],
            "validation_details": validation_result["file_details"]
        })
    
    return validation_result


class MediaDownloader:
    def __init__(self, session: requests.Session = None, max_retries: int = 3, workers: int = 4):
        self.session = session or setup_requests_session()
        self.max_retries = max_retries
        self.workers = workers

    def download_file(self, url: str, dest_path: Path, video_id: str, file_type: str, timeout: int = 60) -> Dict[str, any]:
        """
        Download a single file with detailed error reporting.
        Returns download result with status and details.
        """
        result = {
            "success": False,
            "url": url,
            "dest_path": str(dest_path),
            "file_type": file_type,
            "video_id": video_id,
            "error": None,
            "status_code": None,
            "size_bytes": 0,
            "response_headers": {}
        }
        
        logger.info(f"🔄 Starting {file_type} download for {video_id}", extra={
            "event": "download_start",
            "video_id": video_id,
            "file_type": file_type,
            "url": url,
            "dest": str(dest_path)
        })
        
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with self.session.get(url, stream=True, timeout=timeout) as r:
                result["status_code"] = r.status_code
                result["response_headers"] = dict(r.headers)
                
                # Check response status
                r.raise_for_status()
                
                # Check content length
                content_length = r.headers.get('content-length')
                if content_length:
                    expected_size = int(content_length)
                    logger.debug(f"Expected {file_type} size: {expected_size} bytes", extra={
                        "event": "download_size_info",
                        "video_id": video_id,
                        "file_type": file_type,
                        "expected_size": expected_size
                    })
                
                # Download to temp file first
                tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
                downloaded_size = 0
                
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                
                # Verify download size
                actual_size = tmp_path.stat().st_size
                if actual_size != downloaded_size:
                    logger.warning(f"Size mismatch: expected {downloaded_size}, got {actual_size}", extra={
                        "event": "download_size_mismatch",
                        "video_id": video_id,
                        "file_type": file_type,
                        "expected": downloaded_size,
                        "actual": actual_size
                    })
                
                # Atomic move to final location
                os.replace(tmp_path, dest_path)
                
                result["success"] = True
                result["size_bytes"] = actual_size
                
                logger.info(f"✅ {file_type} download completed for {video_id} ({actual_size} bytes)", extra={
                    "event": "download_complete",
                    "video_id": video_id,
                    "file_type": file_type,
                    "url": url,
                    "dest": str(dest_path),
                    "size_bytes": actual_size,
                    "status_code": r.status_code
                })
                
        except requests.exceptions.HTTPError as e:
            result["error"] = f"HTTP {e.response.status_code}: {e.response.reason}"
            result["status_code"] = e.response.status_code if e.response else None
            logger.error(f"❌ HTTP error downloading {file_type} for {video_id}: {result['error']}", extra={
                "event": "download_http_error",
                "video_id": video_id,
                "file_type": file_type,
                "url": url,
                "error": result["error"],
                "status_code": result["status_code"]
            })
            
        except requests.exceptions.Timeout as e:
            result["error"] = f"Download timeout after {timeout}s"
            logger.error(f"❌ Timeout downloading {file_type} for {video_id}", extra={
                "event": "download_timeout",
                "video_id": video_id,
                "file_type": file_type,
                "url": url,
                "timeout": timeout
            })
            
        except requests.exceptions.ConnectionError as e:
            result["error"] = f"Connection error: {str(e)}"
            logger.error(f"❌ Connection error downloading {file_type} for {video_id}: {str(e)}", extra={
                "event": "download_connection_error",
                "video_id": video_id,
                "file_type": file_type,
                "url": url,
                "error": str(e)
            })
            
        except Exception as e:
            result["error"] = f"Unexpected error: {str(e)}"
            logger.error(f"❌ Unexpected error downloading {file_type} for {video_id}: {str(e)}", extra={
                "event": "download_unexpected_error",
                "video_id": video_id,
                "file_type": file_type,
                "url": url,
                "error": str(e)
            })
        
        return result

    def process_video_entry(self, entry: Dict) -> Dict:
        """
        Process a single video entry with enhanced error reporting.
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
            "missing_files": [],
            "download_results": {},
            "validation_details": {},
            "final_error": None
        }

        logger.info(f"🎬 Processing video {video_id} (page {page})", extra={
            "event": "video_processing_start",
            "video_id": video_id,
            "page": page,
            "target_folder": str(target_folder)
        })

        for attempt in range(1, self.max_retries + 1):
            result["attempts"] = attempt
            
            logger.info(f"🔄 Attempt {attempt}/{self.max_retries} for video {video_id}", extra={
                "event": "video_attempt_start",
                "video_id": video_id,
                "attempt": attempt,
                "max_attempts": self.max_retries
            })
            
            try:
                # Check what files need downloading
                current_validation = validate_video_folder(target_folder, video_id)
                
                # Download MP4 if missing
                if "mp4" in current_validation["missing_files"]:
                    logger.info(f"📹 Downloading MP4 for {video_id}")
                    dest_mp4 = target_folder / f"{video_id}.mp4"
                    mp4_result = self.download_file(mp4_url, dest_mp4, video_id, "mp4")
                    result["download_results"]["mp4"] = mp4_result
                    
                    if not mp4_result["success"]:
                        logger.error(f"MP4 download failed: {mp4_result['error']}")
                else:
                    logger.debug(f"MP4 already exists for {video_id}", extra={
                        "event": "mp4_already_exists",
                        "video_id": video_id
                    })

                # Download JPG if missing
                if "jpg" in current_validation["missing_files"]:
                    logger.info(f"🖼️  Downloading JPG for {video_id}")
                    dest_jpg = target_folder / f"{video_id}.jpg"
                    jpg_result = self.download_file(jpg_url, dest_jpg, video_id, "jpg")
                    result["download_results"]["jpg"] = jpg_result
                    
                    if not jpg_result["success"]:
                        logger.error(f"JPG download failed: {jpg_result['error']}")
                else:
                    logger.debug(f"JPG already exists for {video_id}", extra={
                        "event": "jpg_already_exists",
                        "video_id": video_id
                    })

                # Final validation
                final_validation = validate_video_folder(target_folder, video_id)
                result["validation_details"] = final_validation
                result["missing_files"] = final_validation["missing_files"]
                
                if final_validation["valid"]:
                    result["status"] = "success"
                    logger.info(f"✅ Video {video_id} completed successfully after {attempt} attempts", extra={
                        "event": "video_success",
                        "video_id": video_id,
                        "attempt": attempt,
                        "file_details": final_validation["file_details"]
                    })
                    return result
                else:
                    logger.warning(f"⚠️  Video {video_id} attempt {attempt} failed - still missing: {final_validation['missing_files']}", extra={
                        "event": "video_attempt_failed", 
                        "video_id": video_id,
                        "attempt": attempt,
                        "missing": final_validation["missing_files"],
                        "validation_details": final_validation["file_details"]
                    })
                    
            except Exception as exc:
                error_msg = f"Exception during attempt {attempt}: {str(exc)}"
                result["final_error"] = error_msg
                logger.error(f"💥 Exception processing video {video_id}: {error_msg}", extra={
                    "event": "video_attempt_exception",
                    "video_id": video_id,
                    "attempt": attempt,
                    "error": str(exc)
                })

            # Wait before retry (except on last attempt)
            if attempt < self.max_retries:
                wait_seconds = 5 * attempt
                logger.info(f"⏳ Waiting {wait_seconds}s before retry for video {video_id}", extra={
                    "event": "retry_wait",
                    "video_id": video_id,
                    "wait_seconds": wait_seconds
                })
                time.sleep(wait_seconds)

        # All attempts failed
        result["status"] = "failed"
        final_validation = validate_video_folder(target_folder, video_id)
        result["validation_details"] = final_validation
        result["missing_files"] = final_validation["missing_files"]
        
        logger.error(f"💀 Video {video_id} PERMANENTLY FAILED after {self.max_retries} attempts", extra={
            "event": "video_failed_permanently",
            "video_id": video_id,
            "missing": result["missing_files"],
            "total_attempts": self.max_retries,
            "validation_details": final_validation["file_details"],
            "download_results": result["download_results"]
        })
        
        return result

    def download_from_manifest(self, manifest_path: str, update_progress_cb=None) -> List[Dict]:
        """
        Process manifest with enhanced progress reporting.
        """
        p = Path(manifest_path)
        
        logger.info(f"📋 Loading manifest: {p}")
        
        with p.open("r", encoding="utf-8") as f:
            manifest = json.load(f)

        videos = manifest.get("videos", [])
        batch_id = manifest.get("batch_id", "unknown")
        
        logger.info(f"🚀 Starting batch {batch_id} processing: {len(videos)} videos", extra={
            "event": "manifest_processing_start",
            "manifest": str(p),
            "video_count": len(videos),
            "batch_id": batch_id
        })

        results = []

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {ex.submit(self.process_video_entry, v): v for v in videos}
            
            for i, fut in enumerate(as_completed(futures)):
                try:
                    res = fut.result()
                    progress_pct = ((i + 1) / len(videos)) * 100
                    
                    status_emoji = "✅" if res["status"] == "success" else "❌"
                    logger.info(f"{status_emoji} Progress: {i+1}/{len(videos)} ({progress_pct:.1f}%) - Video {res['video_id']}: {res['status'].upper()}")
                    
                except Exception as exc:
                    v = futures[fut]
                    logger.error(f"💥 Unhandled exception processing video {v.get('video_id', 'unknown')}: {str(exc)}", extra={
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

        # Final statistics
        success_count = len([r for r in results if r["status"] == "success"])
        failed_count = len([r for r in results if r["status"] == "failed"])
        success_rate = (success_count / len(results) * 100) if results else 0
        
        logger.info(f"🏁 Batch {batch_id} completed: {success_count}/{len(results)} successful ({success_rate:.1f}%)", extra={
            "event": "manifest_processing_complete",
            "manifest": str(p),
            "batch_id": batch_id,
            "total_videos": len(results),
            "successful": success_count,
            "failed": failed_count,
            "success_rate": success_rate
        })
        
        return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced Media Downloader with detailed error reporting")
    parser.add_argument("--manifest", required=True, help="Path to batch manifest JSON")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per video")
    parser.add_argument("--workers", type=int, default=4, help="Parallel downloads")
    args = parser.parse_args()

    print(f"🎬 Enhanced Media Downloader Starting...")
    print(f"📋 Manifest: {args.manifest}")
    print(f"🔄 Max retries: {args.max_retries}")
    print(f"🧵 Workers: {args.workers}")
    print("=" * 60)

    md = MediaDownloader(max_retries=args.max_retries, workers=args.workers)
    results = md.download_from_manifest(args.manifest)

    # Save detailed results
    summary_path = Path(args.manifest).with_name(Path(args.manifest).stem + "_detailed_results.json")
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump({
            "summary": {
                "total_videos": len(results),
                "successful": len([r for r in results if r["status"] == "success"]),
                "failed": len([r for r in results if r["status"] == "failed"]),
                "success_rate": (len([r for r in results if r["status"] == "success"]) / len(results) * 100) if results else 0
            },
            "results": results
        }, f, indent=2)

    logger.info(f"📊 Detailed results saved to: {summary_path}")

    # Console summary
    success_count = len([r for r in results if r["status"] == "success"])
    failed_count = len([r for r in results if r["status"] == "failed"])
    
    print("\n" + "=" * 60)
    print("📊 FINAL RESULTS")
    print("=" * 60)
    print(f"Total videos: {len(results)}")
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📈 Success rate: {(success_count / len(results) * 100):.1f}%" if results else "0%")
    print(f"📋 Detailed log: media_downloader_detailed.log")
    print(f"📊 Detailed results: {summary_path}")
    
    if failed_count > 0:
        print(f"\n❌ Failed videos summary:")
        for result in results:
            if result["status"] == "failed":
                missing = result.get("missing_files", [])
                print(f"  - {result['video_id']}: missing {', '.join(missing) if missing else 'unknown files'}")