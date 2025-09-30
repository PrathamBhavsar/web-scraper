# main.py
import asyncio
import json
import os
from pathlib import Path
from playwright.async_api import async_playwright
from scraper.config_loader import load_config
from scraper.logger import setup_logger
from scraper.listing_scraper import find_last_page_number, scrape_video_links_from_page
from scraper.progress_manager import ProgressManager
from scraper.manifest_manager import ManifestManager
from scraper.download_manager import DownloadManager
from scraper.detail_scraper import extract_complete_metadata as extract_video_data
from scraper.storage_manager import total_size_gb, total_size_mb, cleanup_incomplete_folders, scan_download_folder, validate_page_completion
from scraper.validator import basic_mp4_check
import subprocess
import psutil
import aiohttp

async def save_video_metadata(video, video_folder):
    """Save video metadata with proper video_id naming"""
    try:
        video_id = video.get('video_id', 'unknown')
        metadata_path = video_folder / f"{video_id}.json"

        clean_metadata = {k: v for k, v in video.items() if v is not None and k not in ["download_sources", "download_method_used"]}

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(clean_metadata, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving metadata for {video_id}: {e}")
        return False

async def wait_and_validate_page_completion(page_video_ids, download_root, logger, wait_minutes=2):  # Changed default to 2
    """Wait for downloads and validate all videos from a page are complete"""
    logger.info(f"[PAGE-WAIT] Waiting {wait_minutes} minutes for IDM to complete page downloads...")
    await asyncio.sleep(wait_minutes * 60)
    
    logger.info("[PAGE-WAIT] Validating page completion...")
    completion_result = validate_page_completion(page_video_ids, download_root)
    
    logger.info(f"[PAGE-WAIT] Complete: {completion_result['complete']}/{completion_result['total']}, "
                f"Incomplete: {completion_result['incomplete']}, Failed: {completion_result['failed']}")
    
    return completion_result

async def fast_batch_scraper(browser, config, start_page, end_page, manifest_manager, progress_manager, download_manager):
    """Modified batch scraper with page-by-page completion tracking"""
    download_root = Path(config['general']['download_path'])
    max_storage_gb = config['general'].get('max_storage_gb', 100)

    logger = setup_logger("logs/scraper.log", "INFO")

    for page_num in range(start_page, end_page + 1):
        logger.info(f"[BATCH SCRAPER] Parsing page {page_num}")
        print(f"\n[BATCH SCRAPER] Processing page {page_num}")

        current_size_mb = total_size_mb(download_root)
        current_size_gb = current_size_mb / 1024

        page_videos = await scrape_page_videos(browser, page_num, config)
        if not page_videos:
            print(f"No videos found on page {page_num}")
            continue

        page_video_ids = [v['video_id'] for v in page_videos]
        print(f"[BATCH SCRAPER] Extracting metadata for {len(page_videos)} videos...")
        videos_with_metadata = []

        for video in page_videos:
            try:
                metadata = await extract_video_data(browser, video['detail_url'], video)
                if metadata and metadata.get('video_src'):
                    videos_with_metadata.append(metadata)
                    manifest_manager.mark_video_processing(video['video_id'])
                else:
                    manifest_manager.mark_video_failed(video['video_id'], "Failed to extract metadata or download URL")
            except Exception as e:
                print(f"Error extracting metadata for {video['video_id']}: {e}")
                manifest_manager.mark_video_failed(video['video_id'], str(e))

        print(f"[BATCH SCRAPER] Saving metadata for {len(videos_with_metadata)} videos...")
        for video in videos_with_metadata:
            try:
                await download_manager.save_video_metadata(video, download_root)
                progress_manager.mark_video_processed(video['video_id'])
                print(f"[BATCH SCRAPER] Metadata saved for {video['video_id']}")
            except Exception as e:
                print(f"Error saving metadata for {video['video_id']}: {e}")
                manifest_manager.mark_video_failed(video['video_id'], f"Failed to save metadata: {e}")

        print(f"[BATCH SCRAPER] Batch queuing {len(videos_with_metadata)} videos...")
        try:
            batch_success = await download_manager.add_batch_to_queue(videos_with_metadata, download_root)

            if batch_success:
                for video in videos_with_metadata:
                    manifest_manager.mark_video_queued_for_download(video['video_id'])
                    print(f"[BATCH SCRAPER] Queued {video['video_id']} in IDM batch")
                print(f"[BATCH SCRAPER] Successfully batch queued {len(videos_with_metadata)} videos in IDM")
            else:
                print("[BATCH SCRAPER] Batch queueing failed, falling back to individual queueing...")
                for video in videos_with_metadata:
                    try:
                        success = await download_manager.add_to_queue(video, download_root)
                        if success:
                            manifest_manager.mark_video_queued_for_download(video['video_id'])
                            print(f"[BATCH SCRAPER] Individual queue success for {video['video_id']}")
                        else:
                            manifest_manager.mark_video_failed(video['video_id'], "Failed to queue in IDM")
                    except Exception as e:
                        print(f"Error queuing {video['video_id']} individually: {e}")
                        manifest_manager.mark_video_failed(video['video_id'], f"Individual queue error: {e}")

        except Exception as e:
            print(f"[BATCH SCRAPER] Error in batch queueing: {e}")
            for video in videos_with_metadata:
                try:
                    success = await download_manager.add_to_queue(video, download_root)
                    if success:
                        manifest_manager.mark_video_queued_for_download(video['video_id'])
                    else:
                        manifest_manager.mark_video_failed(video['video_id'], "Failed to queue in IDM")
                except Exception as individual_e:
                    print(f"Error queuing {video['video_id']}: {individual_e}")
                    manifest_manager.mark_video_failed(video['video_id'], f"Queue error: {individual_e}")

        print(f"[BATCH SCRAPER] Waiting and validating page {page_num} completion...")
        completion_result = await wait_and_validate_page_completion(
            page_video_ids, 
            download_root, 
            logger,
            wait_minutes=2
        )

        if completion_result['all_complete']:
            progress_manager.update_last_completed_page(page_num)
            print(f"[BATCH SCRAPER] Page {page_num} fully completed and validated")
        else:
            print(f"[BATCH SCRAPER] Page {page_num} has incomplete downloads: "
                  f"{completion_result['incomplete']} incomplete, {completion_result['failed']} failed")

        progress_manager.update_current_page(page_num)
        current_size_mb = total_size_mb(download_root)
        current_size_gb = current_size_mb / 1024
        progress_manager.update_total_size(current_size_mb)
        print(f"[BATCH SCRAPER] Current total size: {current_size_gb:.2f}GB / {max_storage_gb}GB")

        await asyncio.sleep(2)

    print(f"\n[BATCH SCRAPER] Batch scraping complete. All videos queued for download.")

async def scrape_page_videos(browser, page_num, config):
    """Extract video links from a single page"""
    try:
        page = await browser.new_page()
        base_url = config.get('scraping', {}).get('base_url', 'https://rule34video.com')

        if page_num == 1:
            page_url = f"{base_url}/latest-updates/"
        else:
            page_url = f"{base_url}/latest-updates/{page_num}/"

        print(f"DEBUG: Scraping page URL: {page_url}")
        results = await scrape_video_links_from_page(page, page_url)
        await page.close()
        return results

    except Exception as e:
        print(f"ERROR in scrape_page_videos for page {page_num}: {e}")
        try:
            await page.close()
        except:
            pass
        return []

def check_downloads_completion(download_root, manifest_manager):
    """Check which downloads have completed and update manifest accordingly"""
    scan_results = scan_download_folder(download_root)
    updated_count = 0

    for folder_detail in scan_results['folder_details']:
        video_id = folder_detail['video_id']
        if folder_detail['is_complete']:
            if manifest_manager.mark_video_completed(video_id, download_root):
                updated_count += 1

    print(f"[COMPLETION CHECK] Updated {updated_count} videos to completed status")
    return updated_count

async def process_download_batch(videos, download_root, logger, manifest_mgr, progress_mgr, cfg):
    """Process batch downloads with proper folder structure"""
    batch_size = cfg['download'].get('ef2_batch_size', 5)
    successful_downloads = 0

    for i in range(0, len(videos), batch_size):
        batch = videos[i:i + batch_size]
        logger.info(f"[BATCH] Processing download batch {i//batch_size + 1}: {len(batch)} videos")

        for video in batch:
            success = await download_single_video_complete(
                video, download_root, logger, manifest_mgr, progress_mgr, cfg
            )
            if success:
                successful_downloads += 1
            await asyncio.sleep(1)

        await asyncio.sleep(2)

    return successful_downloads

async def download_single_video_complete(video, download_root, logger, manifest_mgr, progress_mgr, cfg):
    """Download single video using the configured download manager"""
    video_id = video.get('video_id', 'unknown')
    download_mgr = DownloadManager(cfg)

    try:
        success = await download_mgr.process_video_batch([video], download_root)

        if success:
            video_folder = download_root / video_id
            video_path = video_folder / f"{video_id}.mp4"

            if video_path.exists():
                file_size_mb = video_path.stat().st_size / (1024 * 1024)
                method_used = cfg['download']['download_method']
                if method_used == "hybrid":
                    metadata_path = video_folder / f"{video_id}.json"
                    if metadata_path.exists():
                        try:
                            with open(metadata_path, 'r', encoding='utf-8') as f:
                                metadata = json.load(f)
                                method_used = metadata.get('download_method_used', 'Unknown')
                        except:
                            method_used = "Hybrid_Unknown"

                manifest_mgr.mark_video_completed(video_id, {
                    "file_path": str(video_path),
                    "file_size_mb": file_size_mb,
                    "download_method": method_used
                })

                progress_mgr.mark_video_downloaded(video_id, str(video_path), file_size_mb)
                logger.info(f"[SUCCESS] Complete structure created for {video_id} using {method_used}")
                return True

        logger.error(f"[FAILED] Download failed for {video_id}")
        manifest_mgr.mark_video_failed(video_id, "Download failed")
        return False

    except Exception as e:
        logger.error(f"[ERROR] Error downloading {video_id}: {e}")
        manifest_mgr.mark_video_failed(video_id, str(e))
        return False

def count_pending_idm_downloads(download_root: Path) -> int:
    """Count folders that were created/queued by parser but do not have a valid .mp4 yet"""
    pending = 0
    try:
        for entry in download_root.iterdir():
            if not entry.is_dir():
                continue

            video_id = entry.name
            json_path = entry / f"{video_id}.json"
            mp4_path = entry / f"{video_id}.mp4"

            if not json_path.exists():
                continue

            if not mp4_path.exists() or not basic_mp4_check(str(mp4_path)):
                pending += 1

    except Exception as e:
        print(f"[IDM-WAIT][ERROR] Error while counting pending downloads: {e}")

    return pending

async def wait_for_idm_pending_and_delay(download_root: Path, logger,
                                         max_wait_minutes: int = 10,
                                         interval_minutes: int = 5) -> None:
    """Wait for IDM pending downloads before final cleanup"""
    total_waited = 0
    interval_minutes = max(1, int(interval_minutes))
    max_wait_minutes = max(interval_minutes, int(max_wait_minutes))

    while total_waited < max_wait_minutes:
        pending = count_pending_idm_downloads(download_root)

        idm_running = False
        try:
            for p in psutil.process_iter(attrs=("name",)):
                name = (p.info.get("name") or "").lower()
                if name.startswith("idman"):
                    idm_running = True
                    break
        except Exception:
            idm_running = False

        if pending == 0:
            logger.info("[IDM-WAIT] No pending downloads detected before final cleanup.")
            return

        msg = (f"[IDM-WAIT] Detected {pending} pending downloads. "
               f"IDM running: {idm_running}. Waiting {interval_minutes} minute(s) "
               f"({total_waited}/{max_wait_minutes} minutes elapsed).")
        logger.info(msg)
        print(msg)

        await asyncio.sleep(interval_minutes * 60)
        total_waited += interval_minutes

    pending_final = count_pending_idm_downloads(download_root)
    logger.info(f"[IDM-WAIT] Waited {total_waited} minute(s). Pending after wait: {pending_final}. Proceeding to final validation/cleanup.")
    print(f"[IDM-WAIT] Waited {total_waited} minute(s). Pending after wait: {pending_final}. Proceeding to final validation/cleanup.")
    return

async def run():
    """Main execution with page-by-page completion tracking"""
    try:
        cfg = load_config()
        logger = setup_logger("logs/scraper.log", "INFO")
        progress_mgr = ProgressManager()
        manifest_mgr = ManifestManager()
        download_mgr = DownloadManager(cfg)

        download_root = Path(cfg['general']['download_path'])
        download_root.mkdir(parents=True, exist_ok=True)
        
        max_storage_gb = cfg['general'].get('max_storage_gb', 100)

        logger.info("[START] STARTING ENHANCED BATCH SCRAPER WITH PAGE COMPLETION TRACKING")
        logger.info(f"[CONFIG] Download path: {cfg['general']['download_path']}")
        logger.info(f"[CONFIG] Storage limit: {max_storage_gb}GB")
        logger.info(f"[CONFIG] Pages per batch: {cfg['scraping']['pages_per_batch']}")
        logger.info(f"[CONFIG] Videos per batch: {cfg['download'].get('ef2_batch_size', 5)}")

        current_size_mb = total_size_mb(download_root)
        current_size_gb = current_size_mb / 1024
        progress_mgr.update_total_size(current_size_mb)

        # --- PATCH START ---
        progress_path = Path("progress.json")
        if progress_path.exists():
            last_completed_page = progress_mgr.get_last_completed_page()
            start_page = last_completed_page + 1
            logger.info(f"[STARTUP] Found progress.json, last completed page: {last_completed_page}")
        else:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                logger.info("[STARTUP] progress.json not found, finding last page number from website...")
                last_page = await find_last_page_number(browser, cfg)
                start_page = last_page
                logger.info(f"[STARTUP] No progress.json, starting from last page on site: {last_page}")
                await browser.close()
        end_page = start_page + cfg['scraping']['pages_per_batch'] - 1
        # --- PATCH END ---

        logger.info(f"[STARTUP] Resuming from page: {start_page}")
        logger.info(f"[STARTUP] Current storage used: {current_size_gb:.2f}GB / {max_storage_gb}GB")
        
        if current_size_gb >= max_storage_gb:
            logger.info(f"[STARTUP] Storage limit reached ({current_size_gb:.2f}GB >= {max_storage_gb}GB). Skipping parsing.")
            print(f"[STARTUP] Storage limit reached. Current: {current_size_gb:.2f}GB, Limit: {max_storage_gb}GB")
            print("[STARTUP] Skipping to validation and cleanup.")
        else:
            logger.info("[INITIAL_CLEANUP] Cleaning existing incomplete folders...")
            initial_cleanup = cleanup_incomplete_folders(download_root, logger)
            logger.info(f"[INITIAL_CLEANUP] Freed {initial_cleanup['space_freed_mb']:.1f}MB")

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)

                await fast_batch_scraper(
                    browser,
                    cfg,
                    start_page,
                    end_page,
                    manifest_mgr,
                    progress_mgr,
                    download_mgr
                )

                await browser.close()

        logger.info("[IDM-WAIT] Scraper finished queueing. Waiting for IDM downloads to complete...")
        print("\n[IDM-WAIT] ========== WAITING FOR IDM DOWNLOADS ==========")
        print("[IDM-WAIT] The scraper will now wait up to 10 minutes for IDM to complete downloads.")
        print("[IDM-WAIT] This prevents deletion of files that are still downloading.")
        print("[IDM-WAIT] =================================================\n")

        await wait_for_idm_pending_and_delay(
            download_root=download_root,
            logger=logger,
            max_wait_minutes=10,
            interval_minutes=5
        )

        logger.info("[FINAL_CLEANUP] IDM wait completed. Running final validation and cleanup...")
        final_cleanup = cleanup_incomplete_folders(download_root, logger)

        logger.info("[VALIDATION] Scanning download folder for final validation...")
        scan_results = scan_download_folder(download_root)
        
        completed_ids = scan_results['completed_video_ids']
        failed_ids = scan_results['failed_video_ids']
        
        progress_mgr.update_final_lists(completed_ids, failed_ids)
        logger.info(f"[VALIDATION] Updated progress.json with {len(completed_ids)} completed and {len(failed_ids)} failed videos")

        final_stats = manifest_mgr.get_queue_statistics()
        final_storage = total_size_gb(download_root)
        progress_mgr.update_total_size(final_storage * 1024)

        logger.info("[COMPLETE] === SCRAPER COMPLETED ===")
        logger.info(f"[STATS] Videos completed: {final_stats['completed']}")
        logger.info(f"[STATS] Videos failed: {final_stats['failed']}")
        logger.info(f"[STATS] Storage used: {final_storage:.2f}GB")
        logger.info(f"[FINAL_CLEANUP] Final cleanup freed: {final_cleanup['space_freed_mb']:.1f}MB")

    except KeyboardInterrupt:
        logger.info("[INTERRUPT] Scraper interrupted by user")
    except Exception as e:
        logger.error(f"[CRASH] Scraper failed: {e}")
        raise

if __name__ == '__main__':
    import os
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    asyncio.run(run())