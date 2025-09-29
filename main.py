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
from scraper.storage_manager import total_size_gb, cleanup_incomplete_folders
from scraper.validator import basic_mp4_check
import subprocess
import psutil
import aiohttp

async def download_video_thumbnail(video, video_folder):
    """Download video thumbnail with proper video_id naming"""
    try:
        thumbnail_url = video.get('thumbnail_src', '')
        video_id = video.get('video_id', 'unknown')

        if thumbnail_url:
            async with aiohttp.ClientSession() as session:
                async with session.get(thumbnail_url) as resp:
                    if resp.status == 200:
                        thumbnail_path = video_folder / f"{video_id}.jpg"
                        with open(thumbnail_path, 'wb') as f:
                            f.write(await resp.read())
                        return True
    except Exception as e:
        print(f"Failed to download thumbnail for {video_id}: {e}")
        return False

async def save_video_metadata(video, video_folder):
    """Save video metadata with proper video_id naming"""
    try:
        video_id = video.get('video_id', 'unknown')
        metadata_path = video_folder / f"{video_id}.json"

        # Remove unwanted fields before saving
        clean_metadata = {k: v for k, v in video.items() if v is not None and k not in ["download_sources", "download_method_used"]}

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(clean_metadata, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving metadata for {video_id}: {e}")
        return False

async def fast_batch_scraper(browser, config, start_page, end_page, manifest_manager, progress_manager, download_manager):
    """
    Modified batch scraper that decouples metadata extraction from video download
    """
    # FIX: Use the correct config key for download path
    download_root = Path(config['general']['download_path'])

    for page_num in range(start_page, end_page + 1):
        print(f"\n[BATCH SCRAPER] Processing page {page_num}")

        # Step 1: Scrape video links from the page (existing logic)
        page_videos = await scrape_page_videos(browser, page_num, config)
        if not page_videos:
            print(f"No videos found on page {page_num}")
            continue

        # Step 2: Extract metadata for all videos in batch
        print(f"[BATCH SCRAPER] Extracting metadata for {len(page_videos)} videos...")
        videos_with_metadata = []

        for video in page_videos:
            try:
                # Extract complete metadata (including download URLs)
                metadata = await extract_video_data(browser, video['detail_url'], video)
                if metadata and metadata.get('video_src'):
                    videos_with_metadata.append(metadata)
                    manifest_manager.mark_video_processing(video['video_id'])
                else:
                    manifest_manager.mark_video_failed(video['video_id'], "Failed to extract metadata or download URL")
            except Exception as e:
                print(f"Error extracting metadata for {video['video_id']}: {e}")
                manifest_manager.mark_video_failed(video['video_id'], str(e))

        # Step 3: IMMEDIATELY save JSON metadata and download thumbnails
        print(f"[BATCH SCRAPER] Saving metadata and thumbnails for {len(videos_with_metadata)} videos...")
        for video in videos_with_metadata:
            try:
                # Save JSON metadata immediately
                await download_manager.save_video_metadata(video, download_root)

                # Download thumbnail immediately
                await download_manager.download_video_thumbnail(video, download_root)

                # Mark as processed (metadata extraction complete)
                progress_manager.mark_video_processed(video['video_id'])
                print(f"[BATCH SCRAPER] Metadata and thumbnail saved for {video['video_id']}")
            except Exception as e:
                print(f"Error saving metadata/thumbnail for {video['video_id']}: {e}")
                manifest_manager.mark_video_failed(video['video_id'], f"Failed to save metadata: {e}")

        # Step 4: Queue ALL video downloads in IDM without waiting (BATCH MODE)
        print(f"[BATCH SCRAPER] Batch queuing {len(videos_with_metadata)} videos and thumbnails...")
        try:
            # Use batch queueing for better efficiency
            batch_success = await download_manager.add_batch_to_queue(videos_with_metadata, download_root)

            if batch_success:
                # Mark all videos as queued for download
                for video in videos_with_metadata:
                    manifest_manager.mark_video_queued_for_download(video['video_id'])
                    print(f"[BATCH SCRAPER] Queued {video['video_id']} in IDM batch")

                print(f"[BATCH SCRAPER] Successfully batch queued {len(videos_with_metadata)} videos in IDM")
            else:
                # Fallback to individual queuing if batch fails
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
            # Fallback to individual queuing
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

        # Update progress for the page
        progress_manager.update_current_page(page_num)

        # Small delay between pages to avoid overwhelming the server
        await asyncio.sleep(2)

    print(f"\n[BATCH SCRAPER] Batch scraping complete. All videos queued for download.")
    print("Note: Videos will continue downloading in IDM. Use cleanup function to check completion status.")

# Additional helper functions that would be needed:
async def scrape_page_videos(browser, page_num, config):
    """Extract video links from a single page (existing logic)"""
    try:
        # Get a page from browser
        page = await browser.new_page()

        # Construct the URL for the specific page
        # Based on the URL pattern seen in listing_scraper.py: /latest-updates/page_num/
        base_url = config.get('scraping', {}).get('base_url', 'https://rule34video.com')

        if page_num == 1:
            page_url = f"{base_url}/latest-updates/"
        else:
            page_url = f"{base_url}/latest-updates/{page_num}/"

        print(f"DEBUG: Scraping page URL: {page_url}")

        # Call the actual scraping function from listing_scraper
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
    """
    Check which downloads have completed and update manifest accordingly
    This should be called periodically or at the end of scraping
    """
    from scraper.storage_manager import scan_download_folder

    scan_results = scan_download_folder(download_root)
    updated_count = 0

    for folder_detail in scan_results['folder_details']:
        video_id = folder_detail['video_id']
        if folder_detail['is_complete']:
            # Mark as completed in manifest (will verify files exist)
            if manifest_manager.mark_video_completed(video_id, download_root):
                updated_count += 1

    print(f"[COMPLETION CHECK] Updated {updated_count} videos to completed status")
    return updated_count

async def process_download_batch(videos, download_root, logger, manifest_mgr, progress_mgr, cfg):
    """Process batch downloads with proper folder structure"""
    batch_size = cfg['download'].get('ef2_batch_size', 5)
    successful_downloads = 0

    # Process videos in smaller batches
    for i in range(0, len(videos), batch_size):
        batch = videos[i:i + batch_size]
        logger.info(f"[BATCH] Processing download batch {i//batch_size + 1}: {len(batch)} videos")

        # Download each video in the batch
        for video in batch:
            success = await download_single_video_complete(
                video, download_root, logger, manifest_mgr, progress_mgr, cfg
            )
            if success:
                successful_downloads += 1

            # Small delay between downloads
            await asyncio.sleep(1)

        # Brief pause between batches
        await asyncio.sleep(2)

    return successful_downloads

async def download_single_video_complete(video, download_root, logger, manifest_mgr, progress_mgr, cfg):
    """Download single video using the configured download manager"""
    video_id = video.get('video_id', 'unknown')

    # Use the download manager instead of direct implementation
    download_mgr = DownloadManager(cfg)

    try:
        # Process single video as a batch of 1
        success = await download_mgr.process_video_batch([video], download_root)

        if success:
            # Get the actual file info for progress tracking
            video_folder = download_root / video_id
            video_path = video_folder / f"{video_id}.mp4"

            if video_path.exists():
                file_size_mb = video_path.stat().st_size / (1024 * 1024)

                # Determine the actual download method used
                method_used = cfg['download']['download_method']
                if method_used == "hybrid":
                    # Check if IDM was used or direct fallback
                    metadata_path = video_folder / f"{video_id}.json"
                    if metadata_path.exists():
                        try:
                            with open(metadata_path, 'r', encoding='utf-8') as f:
                                metadata = json.load(f)
                                method_used = metadata.get('download_method_used', 'Unknown')
                        except:
                            method_used = "Hybrid_Unknown"

                # Mark as completed with proper method
                manifest_mgr.mark_video_completed(video_id, {
                    "file_path": str(video_path),
                    "file_size_mb": file_size_mb,
                    "download_method": method_used,
                    "has_thumbnail": (video_folder / f"{video_id}.jpg").exists()
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
    """
    Count folders that were created/queued by parser (have .json)
    but do not have a valid .mp4 yet. Those are treated as 'pending'.
    """
    pending = 0
    try:
        for entry in download_root.iterdir():
            if not entry.is_dir():
                continue

            video_id = entry.name
            json_path = entry / f"{video_id}.json"
            mp4_path = entry / f"{video_id}.mp4"

            # We only consider folders that the parser definitely created (metadata exists)
            if not json_path.exists():
                continue

            # If mp4 is missing or invalid -> pending
            if not mp4_path.exists() or not basic_mp4_check(str(mp4_path)):
                pending += 1

    except Exception as e:
        # keep failure non-fatal; return best-effort count
        print(f"[IDM-WAIT][ERROR] Error while counting pending downloads: {e}")

    return pending

async def wait_for_idm_pending_and_delay(download_root: Path, logger,
                                         max_wait_minutes: int = 10,
                                         interval_minutes: int = 5) -> None:
    """
    Before final cleanup: check for pending downloads and wait up to max_wait_minutes.

    Behavior:
    - If there are pending downloads (as detected by missing/invalid mp4 files),
      print the number and wait interval_minutes.
    - Re-check, wait again if needed, up to max_wait_minutes total.
    - After max_wait_minutes elapsed, proceed (i.e. call validation/cleanup).

    Notes:
    - Uses presence of parser-created JSON files + basic_mp4_check to detect pending items.
    - Logs whether an 'idman' process is running (helpful debug info).
    """
    total_waited = 0

    # Defensive lower bounds
    interval_minutes = max(1, int(interval_minutes))
    max_wait_minutes = max(interval_minutes, int(max_wait_minutes))

    while total_waited < max_wait_minutes:
        pending = count_pending_idm_downloads(download_root)

        # Detect if IDM process exists (IDMan.exe / idman.exe) for visibility
        idm_running = False
        try:
            for p in psutil.process_iter(attrs=("name",)):
                name = (p.info.get("name") or "").lower()
                if name.startswith("idman"):  # covers idman.exe/IDMan.exe etc.
                    idm_running = True
                    break
        except Exception:
            # in case psutil can't iterate; continue with best-effort
            idm_running = False

        # If nothing pending -> return immediately
        if pending == 0:
            logger.info("[IDM-WAIT] No pending downloads detected before final cleanup.")
            return

        # There are pending downloads -> print and wait interval
        msg = (f"[IDM-WAIT] Detected {pending} pending downloads. "
               f"IDM running: {idm_running}. Waiting {interval_minutes} minute(s) "
               f"({total_waited}/{max_wait_minutes} minutes elapsed).")
        logger.info(msg)
        print(msg)  # keep console output visible as you wanted

        await asyncio.sleep(interval_minutes * 60)
        total_waited += interval_minutes

    # Final check after waiting window ended
    pending_final = count_pending_idm_downloads(download_root)
    logger.info(f"[IDM-WAIT] Waited {total_waited} minute(s). Pending after wait: {pending_final}. Proceeding to final validation/cleanup.")
    print(f"[IDM-WAIT] Waited {total_waited} minute(s). Pending after wait: {pending_final}. Proceeding to final validation/cleanup.")
    return

async def run():
    """Main execution with IDM waiting before cleanup"""
    try:
        cfg = load_config()
        logger = setup_logger("logs/scraper.log", "INFO")
        progress_mgr = ProgressManager()
        manifest_mgr = ManifestManager()
        download_mgr = DownloadManager(cfg)

        # FIX: Use the correct config key for download path
        # Use 'download_path' instead of 'download_folder'
        download_root = Path(cfg['general']['download_path'])
        download_root.mkdir(parents=True, exist_ok=True)

        logger.info("[START] STARTING ENHANCED BATCH SCRAPER WITH IDM WAIT LOGIC")
        logger.info(f"[CONFIG] Download path: {cfg['general']['download_path']}")
        logger.info(f"[CONFIG] Storage limit: {cfg['general']['max_storage_gb']}GB")
        logger.info(f"[CONFIG] Pages per batch: {cfg['scraping']['pages_per_batch']}")
        logger.info(f"[CONFIG] Videos per batch: {cfg['download'].get('ef2_batch_size', 5)}")
        logger.info("[STRUCTURE] Expected structure: [video_id]/[video_id].mp4/.json/.jpg")

        # Initial cleanup - DO NOT WAIT (this is initialization)
        logger.info("[INITIAL_CLEANUP] Cleaning existing incomplete folders...")
        initial_cleanup = cleanup_incomplete_folders(download_root, logger)
        logger.info(f"[INITIAL_CLEANUP] Freed {initial_cleanup['space_freed_mb']:.1f}MB")

        # Run the batch scraper
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            await fast_batch_scraper(
                browser,
                cfg,
                1,  # You may want to set start_page dynamically
                cfg['scraping']['pages_per_batch'],  # You may want to set end_page dynamically
                manifest_mgr,
                progress_mgr,
                download_mgr
            )

            await browser.close()

        # CRITICAL: Wait for IDM downloads before final cleanup - THIS IS THE FIX
        logger.info("[IDM-WAIT] Scraper finished queueing. Waiting for IDM downloads to complete...")
        print("\n[IDM-WAIT] ========== WAITING FOR IDM DOWNLOADS ==========")
        print("[IDM-WAIT] The scraper will now wait up to 10 minutes for IDM to complete downloads.")
        print("[IDM-WAIT] This prevents deletion of files that are still downloading.")
        print("[IDM-WAIT] =================================================\n")

        # FIXED: Actually call the IDM wait function
        await wait_for_idm_pending_and_delay(
            download_root=download_root,
            logger=logger,
            max_wait_minutes=10,  # Wait up to 10 minutes total
            interval_minutes=5    # Check every 5 minutes
        )

        # ONLY AFTER waiting, run final cleanup
        logger.info("[FINAL_CLEANUP] IDM wait completed. Running final validation and cleanup...")
        final_cleanup = cleanup_incomplete_folders(download_root, logger)

        # Final stats
        final_stats = manifest_mgr.get_queue_statistics()
        final_storage = total_size_gb(download_root)

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
    # Set UTF-8 encoding for Windows
    import os
    os.environ['PYTHONIOENCODING'] = 'utf-8'

    asyncio.run(run())
