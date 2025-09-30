# storage_manager.py
import json
import os
import shutil
from pathlib import Path
from typing import List

def total_size_mb(folder_path: Path) -> float:
    """Calculate total size of folder in MB"""
    try:
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size / (1024 ** 2)
    except Exception:
        return 0.0

def total_size_gb(folder_path: Path) -> float:
    """Calculate total size of folder in GB"""
    return total_size_mb(folder_path) / 1024

def is_download_in_progress(video_folder: Path, video_id: str) -> bool:
    """Check if video download is currently in progress"""
    try:
        temp_extensions = ['.part', '.tmp', '.crdownload', '.download', '.temp']
        
        for temp_ext in temp_extensions:
            temp_file = video_folder / f"{video_id}.mp4{temp_ext}"
            if temp_file.exists():
                return True
        
        idm_temp_patterns = [f"{video_id}.mp4.!idm", f"{video_id}.mp4.idmtemp"]
        for pattern in idm_temp_patterns:
            if (video_folder / pattern).exists():
                return True
        
        mp4_file = video_folder / f"{video_id}.mp4"
        if mp4_file.exists():
            try:
                with open(mp4_file, 'a') as f:
                    pass
            except (OSError, IOError):
                return True
        
        return False
        
    except Exception as e:
        print(f"Error checking download progress for {video_id}: {e}")
        return False

def validate_video_folder_structure(video_folder: Path, video_id: str) -> dict:
    """Validate that video folder has the correct structure"""
    expected_files = {
        'video': video_folder / f"{video_id}.mp4",
        'metadata': video_folder / f"{video_id}.json"
    }
    
    validation_results = {
        'video_id': video_id,
        'folder_path': str(video_folder),
        'files_found': {},
        'validation_status': {},
        'total_size_mb': 0,
        'is_complete': False,
        'download_in_progress': is_download_in_progress(video_folder, video_id)
    }
    
    total_size = 0
    for file_type, file_path in expected_files.items():
        exists = file_path.exists()
        validation_results['files_found'][file_type] = exists
        
        if exists:
            file_size = file_path.stat().st_size

            _size = file_path.stat().st_size
            total_size += file_size
            
            if file_type == 'video':
                validation_results['validation_status'][file_type] = (
                    file_size > 1024 * 1024 and
                    _validate_mp4_file(file_path)
                )
            elif file_type == 'metadata':
                validation_results['validation_status'][file_type] = _validate_json_file(file_path)
        else:
            validation_results['validation_status'][file_type] = False
    
    validation_results['total_size_mb'] = total_size / (1024 * 1024)
    
    validation_results['is_complete'] = (
        validation_results['validation_status'].get('video', False) and
        validation_results['validation_status'].get('metadata', False)
    )
    
    return validation_results

def validate_page_completion(video_ids: List[str], download_root: Path) -> dict:
    """Validate that all videos from a page are complete"""
    results = {
        'total': len(video_ids),
        'complete': 0,
        'incomplete': 0,
        'failed': 0,
        'all_complete': False
    }
    
    for video_id in video_ids:
        video_folder = download_root / str(video_id)
        
        if not video_folder.exists():
            results['failed'] += 1
            continue
        
        validation = validate_video_folder_structure(video_folder, str(video_id))
        
        if validation['is_complete']:
            results['complete'] += 1
        elif validation['download_in_progress']:
            results['incomplete'] += 1
        else:
            results['failed'] += 1
    
    results['all_complete'] = (results['complete'] == results['total'])
    
    return results

def cleanup_incomplete_folders(download_root: Path, logger=None) -> dict:
    """Clean up incomplete/empty folders, but skip those with downloads in progress"""
    cleanup_stats = {
        'folders_checked': 0,
        'empty_folders_deleted': 0,
        'incomplete_folders_deleted': 0,
        'corrupted_files_deleted': 0,
        'space_freed_mb': 0,
        'folders_skipped_downloading': 0
    }
    
    try:
        video_folders = [f for f in download_root.iterdir() if f.is_dir()]
        cleanup_stats['folders_checked'] = len(video_folders)
        
        for folder in video_folders:
            video_id = folder.name
            
            if not video_id.isdigit():
                continue
            
            validation = validate_video_folder_structure(folder, video_id)
            folder_size_mb = validation['total_size_mb']
            
            if validation['download_in_progress']:
                cleanup_stats['folders_skipped_downloading'] += 1
                if logger:
                    logger.info(f"[CLEANUP] Skipping {video_id}: download in progress")
                continue
            
            should_delete = False
            delete_reason = ""
            
            folder_contents = list(folder.iterdir())
            if not folder_contents:
                should_delete = True
                delete_reason = "empty folder"
                cleanup_stats['empty_folders_deleted'] += 1
            
            elif not validation['is_complete']:
                has_metadata = validation['validation_status'].get('metadata', False)
                has_valid_video = validation['validation_status'].get('video', False)
                
                if has_metadata and not has_valid_video:
                    should_delete = True
                    delete_reason = "video download failed or corrupted"
                    cleanup_stats['incomplete_folders_deleted'] += 1
                elif not has_metadata and not has_valid_video:
                    should_delete = True
                    delete_reason = "completely empty/failed folder"
                    cleanup_stats['incomplete_folders_deleted'] += 1
            
            if should_delete:
                try:
                    if logger:
                        logger.info(f"[CLEANUP] Deleting {video_id}: {delete_reason}")
                    shutil.rmtree(folder)
                    cleanup_stats['space_freed_mb'] += folder_size_mb
                except Exception as e:
                    if logger:
                        logger.error(f"[CLEANUP] Error deleting {video_id}: {e}")
            
            else:
                for file_type, is_valid in validation['validation_status'].items():
                    if validation['files_found'][file_type] and not is_valid:
                        file_extensions = {'video': 'mp4', 'metadata': 'json'}
                        corrupted_file = folder / f"{video_id}.{file_extensions[file_type]}"
                        try:
                            if corrupted_file.exists():
                                file_size_mb = corrupted_file.stat().st_size / (1024 * 1024)
                                corrupted_file.unlink()
                                cleanup_stats['corrupted_files_deleted'] += 1
                                cleanup_stats['space_freed_mb'] += file_size_mb
                                if logger:
                                    logger.info(f"[CLEANUP] Deleted corrupted {file_type} file: {video_id}")
                        except Exception as e:
                            if logger:
                                logger.error(f"[CLEANUP] Error deleting corrupted file {video_id}: {e}")
                
    except Exception as e:
        if logger:
            logger.error(f"[CLEANUP] Error during cleanup: {e}")
    
    return cleanup_stats

def _validate_mp4_file(file_path: Path) -> bool:
    """Validate MP4 file by checking magic bytes"""
    try:
        with open(file_path, 'rb') as f:
            header = f.read(512)
            return b'ftyp' in header or b'moov' in header
    except Exception:
        return False

def _validate_json_file(file_path: Path) -> bool:
    """Validate JSON file by trying to parse it"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json.load(f)
        return True
    except Exception:
        return False

def scan_download_folder(download_root: Path) -> dict:
    """Scan entire download folder and return structure analysis"""
    scan_results = {
        'total_folders': 0,
        'complete_videos': 0,
        'incomplete_videos': 0,
        'downloading_videos': 0,
        'invalid_folders': 0,
        'total_size_gb': 0,
        'folder_details': [],
        'completed_video_ids': [],
        'failed_video_ids': []
    }
    
    try:
        video_folders = [f for f in download_root.iterdir() if f.is_dir()]
        scan_results['total_folders'] = len(video_folders)
        
        for folder in video_folders:
            video_id = folder.name
            
            if not video_id.isdigit():
                continue
            
            validation = validate_video_folder_structure(folder, video_id)
            scan_results['folder_details'].append(validation)
            
            if validation['download_in_progress']:
                scan_results['downloading_videos'] += 1
            elif validation['is_complete']:
                scan_results['complete_videos'] += 1
                scan_results['completed_video_ids'].append(int(video_id))
            elif any(validation['files_found'].values()):
                scan_results['incomplete_videos'] += 1
                scan_results['failed_video_ids'].append(int(video_id))
            else:
                scan_results['invalid_folders'] += 1
                scan_results['failed_video_ids'].append(int(video_id))
        
        scan_results['total_size_gb'] = total_size_gb(download_root)
        
    except Exception as e:
        print(f"Error scanning download folder: {e}")
    
    return scan_results

def print_folder_analysis(download_root: Path):
    """Print detailed analysis of download folder"""
    scan_results = scan_download_folder(download_root)
    
    print("\n" + "="*60)
    print("DOWNLOAD FOLDER ANALYSIS")
    print("="*60)
    print(f"Total folders: {scan_results['total_folders']}")
    print(f"Complete videos: {scan_results['complete_videos']}")
    print(f"Incomplete videos: {scan_results['incomplete_videos']}")
    print(f"Currently downloading: {scan_results['downloading_videos']}")
    print(f"Invalid folders: {scan_results['invalid_folders']}")
    print(f"Total size: {scan_results['total_size_gb']:.2f} GB")
    
    if scan_results['total_folders'] > 0:
        completion_rate = scan_results['complete_videos'] / scan_results['total_folders'] * 100
        print(f"Completion rate: {completion_rate:.1f}%")
    
    incomplete_examples = [
        folder for folder in scan_results['folder_details']
        if not folder['is_complete'] and not folder['download_in_progress']
    ][:5]
    
    if incomplete_examples:
        print("\nExamples of incomplete folders:")
        for folder in incomplete_examples:
            print(f"  {folder['video_id']}: ", end="")
            status = []
            for file_type in ['video', 'metadata']:
                if folder['validation_status'].get(file_type, False):
                    status.append(f"{file_type}[OK]")
                else:
                    status.append(f"{file_type}[FAIL]")
            print(" | ".join(status))
    
    print("="*60)