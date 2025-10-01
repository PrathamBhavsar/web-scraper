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
from scraper.storage_manager import (
    total_size_gb, total_size_mb, cleanup_incomplete_folders,
    scan_download_folder, validate_page_completion
)
from scraper.validator import basic_mp4_check
import subprocess
import psutil
import aiohttp
import traceback


async def save_video_metadata(video, video_folder):
    """Save video metadata with proper video_id naming"""
    try:
        video_id = video.get('video_id', 'unknown')
        metadata_path = video_folder / f'{video_id}.json'

        clean_metadata = {k: v for k, v in video.items() if v is not None and k not in ['download_sources', 'download_method_used']}

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(clean_metadata, f, indent=2, ensure_ascii=False)

        return True
    except Exception as e:
        print(f"Error saving metadata for {video_id}: {e}")
        return False


async def wait_and_validate_page_completion(page_video_ids, download_root, logger, wait_minutes=2):
    """Wait for downloads and validate all videos from a page are complete"""
    logger.info(f"PAGE-WAIT: Waiting {wait_minutes} minutes for IDM to complete page downloads...")
    await asyncio.sleep(wait_minutes * 60)

    logger.info("PAGE-WAIT: Validating page completion...")
    completion_result = validate_page_completion(page_video_ids, download_root)

    logger.info(f"PAGE-WAIT: Complete: {completion_result['complete']}/{completion_result['total']}, "
                f"Incomplete: {completion_result['incomplete']}, Failed: {completion_result['failed']}")

    return completion_result


async def fast_batch_scraper(browser, config, start_page, manifest_manager, progress_manager, download_manager):
    """Modified batch scraper with continuous page-by-page processing until disk space is full"""
    download_root = Path(config['general']['download_path'])
    max_storage_gb = config['general'].get('max_storage_gb', 100)
    logger = setup_logger("logs/scraper.log", "INFO")

    page_num = start_page

    # Changed from for loop to while True for continuous processing
    while True:
        logger.info(f"BATCH SCRAPER: Parsing page {page_num}")
        print(f"BATCH SCRAPER: Processing page {page_num}")

        page_videos = await scrape_page_videos(browser, page_num, config)

        if not page_videos:
            print(f"No videos found on page {page_num}")
            page_num += 1
            continue

        page_video_ids = [v['video_id'] for v in page_videos]

        print(f"BATCH SCRAPER: Extracting metadata for {len(page_videos)} videos...")
        logger.info(f"BATCH SCRAPER: Starting metadata extraction for {len(page_videos)} videos on page {page_num}")

        videos_with_metadata = []

        for video in page_videos:
            video_id = video.get('video_id', 'unknown')
            detail_url = video.get('detail_url', 'unknown')

            try:
                logger.info(f"METADATA EXTRACTION: Starting for video_id='{video_id}' page={page_num} url='{detail_url}'")

                metadata = await extract_video_data(browser, detail_url, video, logger=logger, page_num=page_num)

                if metadata and metadata.get('video_src'):
                    videos_with_metadata.append(metadata)
                    manifest_manager.mark_video_processing(video['video_id'])
                    logger.info(f"METADATA SUCCESS: video_id='{video_id}' page={page_num} - Successfully extracted metadata and download URL")
                else:
                    # ENHANCED ERROR LOGGING: Detailed failure analysis
                    error_details = []

                    if not metadata:
                        error_details.append("metadata extraction returned None/empty")
                    else:
                        if not metadata.get('video_src'):
                            error_details.append("no video_src/download URL found")

                        # Log what was actually found in metadata
                        found_fields = [key for key, value in metadata.items() if value]
                        download_sources_count = len(metadata.get('download_sources', []))

                        error_details.append(f"found fields: {found_fields}")
                        error_details.append(f"download_sources count: {download_sources_count}")

                    failure_reason = "; ".join(error_details)

                    logger.error(f"METADATA FAILURE: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                               f"Expected: valid metadata with video_src field containing download URL; "
                               f"Found: {failure_reason}")

                    manifest_manager.mark_video_failed(video['video_id'], f"Metadata extraction failed: {failure_reason}")

            except Exception as e:
                # ENHANCED ERROR LOGGING: Full exception details
                error_traceback = traceback.format_exc()

                logger.error(f"METADATA EXCEPTION: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                           f"Exception during metadata extraction: {str(e)}")
                logger.error(f"METADATA EXCEPTION TRACEBACK: video_id='{video_id}'\n{error_traceback}")

                print(f"Error extracting metadata for {video_id}: {e}")
                manifest_manager.mark_video_failed(video['video_id'], f"Exception during metadata extraction: {str(e)}")

        logger.info(f"BATCH SCRAPER: Metadata extraction completed. Successfully processed {len(videos_with_metadata)}/{len(page_videos)} videos on page {page_num}")

        print(f"BATCH SCRAPER: Saving metadata for {len(videos_with_metadata)} videos...")
        for video in videos_with_metadata:
            try:
                await download_manager.save_video_metadata(video, download_root)
                progress_manager.mark_video_processed(video['video_id'])
                print(f"BATCH SCRAPER: Metadata saved for {video['video_id']}")
            except Exception as e:
                print(f"Error saving metadata for {video['video_id']}: {e}")
                manifest_manager.mark_video_failed(video['video_id'], f"Failed to save metadata: {e}")

        print(f"BATCH SCRAPER: Batch queuing {len(videos_with_metadata)} videos...")
        try:
            batch_success = await download_manager.add_batch_to_queue(videos_with_metadata, download_root)
            if batch_success:
                for video in videos_with_metadata:
                    manifest_manager.mark_video_queued_for_download(video['video_id'])
                    print(f"BATCH SCRAPER: Queued {video['video_id']} in IDM batch")
                print(f"BATCH SCRAPER: Successfully batch queued {len(videos_with_metadata)} videos in IDM")
            else:
                print("BATCH SCRAPER: Batch queueing failed, falling back to individual queueing...")
                for video in videos_with_metadata:
                    try:
                        success = await download_manager.add_to_queue(video, download_root)
                        if success:
                            manifest_manager.mark_video_queued_for_download(video['video_id'])
                            print(f"BATCH SCRAPER: Individual queue success for {video['video_id']}")
                        else:
                            manifest_manager.mark_video_failed(video['video_id'], "Failed to queue in IDM")
                    except Exception as e:
                        print(f"Error queuing {video['video_id']} individually: {e}")
                        manifest_manager.mark_video_failed(video['video_id'], f"Individual queue error: {e}")
        except Exception as e:
            print(f"BATCH SCRAPER: Error in batch queueing: {e}")
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

        print(f"BATCH SCRAPER: Waiting and validating page {page_num} completion...")
        completion_result = await wait_and_validate_page_completion(
            page_video_ids, download_root, logger, wait_minutes=2
        )

        if completion_result['all_complete']:
            progress_manager.update_last_completed_page(page_num)
            print(f"BATCH SCRAPER: Page {page_num} fully completed and validated")
        else:
            print(f"BATCH SCRAPER: Page {page_num} has incomplete downloads: "
                  f"{completion_result['incomplete']} incomplete, {completion_result['failed']} failed")

        progress_manager.update_current_page(page_num)

        # Check disk usage after processing and waiting
        current_size_mb = total_size_mb(download_root)
        current_size_gb = current_size_mb / 1024
        progress_manager.update_total_size(current_size_mb)

        print(f"BATCH SCRAPER: Current total size: {current_size_gb:.2f}GB / {max_storage_gb}GB")
        logger.info(f"DISK CHECK: Current size {current_size_gb:.2f}GB, limit {max_storage_gb}GB")

        # CRITICAL: Check if disk usage exceeds limit and stop if so
        if current_size_gb >= max_storage_gb:
            logger.info(f"DISK LIMIT REACHED: Storage usage ({current_size_gb:.2f}GB) has reached or exceeded the limit ({max_storage_gb}GB). Stopping scraper.")
            print(f"BATCH SCRAPER: Storage limit reached! {current_size_gb:.2f}GB >= {max_storage_gb}GB")
            print("BATCH SCRAPER: Stopping continuous scraping due to disk space limit.")
            break

        await asyncio.sleep(2)
        page_num += 1  # Move to next page

    print(f"BATCH SCRAPER: Scraping stopped due to storage limit. Final size: {current_size_gb:.2f}GB")


# Rest of the file remains the same as before...
async def scrape_page_videos(browser, page_num, config):
    """Extract video links from a single page"""
    try:
        page = await browser.new_page()
        base_url = config.get('scraping', {}).get('base_url', 'https://rule34video.com/')

        if page_num == 1:
            page_url = f"{base_url}latest-updates/"
        else:
            page_url = f"{base_url}latest-updates/{page_num}"

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


# [Rest of the functions remain the same as in the previous implementation]
# ... [continuing with run() function and other utilities]


async def run():
    """Main execution with continuous page processing until disk limit is reached"""
    try:
        cfg = load_config()
        logger = setup_logger("logs/scraper.log", "INFO")

        progress_mgr = ProgressManager()
        manifest_mgr = ManifestManager()
        download_mgr = DownloadManager(cfg)

        download_root = Path(cfg['general']['download_path'])
        download_root.mkdir(parents=True, exist_ok=True)

        max_storage_gb = cfg['general'].get('max_storage_gb', 100)

        logger.info("START: STARTING ENHANCED BATCH SCRAPER WITH CONTINUOUS PROCESSING AND IMPROVED ERROR LOGGING")
        logger.info(f"CONFIG: Download path: {cfg['general']['download_path']}")
        logger.info(f"CONFIG: Storage limit: {max_storage_gb}GB")
        logger.info("CONFIG: Processing pages continuously until disk limit reached")

        current_size_mb = total_size_mb(download_root)
        current_size_gb = current_size_mb / 1024
        progress_mgr.update_total_size(current_size_mb)

        # Determine starting page
        progress_path = Path("progress.json")
        if progress_path.exists():
            last_scraped_page = progress_mgr.get_last_scraped_page()
            if last_scraped_page and last_scraped_page > 0:
                start_page = last_scraped_page
                logger.info(f"STARTUP: Found progress.json, last scraped page: {last_scraped_page}")
            else:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page(user_agent=cfg['general']['user_agent'])
                    discovered_last_page = await find_last_page_number(page, cfg['general']['base_url'])
                    start_page = discovered_last_page
                    logger.info(f"STARTUP: progress.json empty, discovered last page: {discovered_last_page}")
                    await browser.close()
        else:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(user_agent=cfg['general']['user_agent'])
                discovered_last_page = await find_last_page_number(page, cfg['general']['base_url'])
                start_page = discovered_last_page
                logger.info(f"STARTUP: No progress.json, discovered last page: {discovered_last_page}")
                await browser.close()

        logger.info(f"STARTUP: Starting from page {start_page}")
        logger.info(f"STARTUP: Current storage used: {current_size_gb:.2f}GB / {max_storage_gb}GB")

        if current_size_gb >= max_storage_gb:
            logger.info(f"STARTUP: Storage limit already reached ({current_size_gb:.2f}GB >= {max_storage_gb}GB). Skipping parsing.")
            print(f"STARTUP: Storage limit reached. Current: {current_size_gb:.2f}GB, Limit: {max_storage_gb}GB")
            print("STARTUP: Skipping to validation and cleanup.")
        else:
            logger.info("INITIAL/CLEANUP: Cleaning existing incomplete folders...")
            initial_cleanup = cleanup_incomplete_folders(download_root, logger)
            logger.info(f"INITIAL/CLEANUP: Freed {initial_cleanup['space_freed_mb']:.1f}MB")

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                await fast_batch_scraper(browser, cfg, start_page, manifest_mgr, progress_mgr, download_mgr)
                await browser.close()

        # [Rest of the cleanup and validation logic remains the same...]

    except KeyboardInterrupt:
        logger.info("INTERRUPT: Scraper interrupted by user")
    except Exception as e:
        logger.error(f"CRASH: Scraper failed: {e}")
        raise


if __name__ == "__main__":
    import os
    os.environ["PYTHONIOENCODING"] = "utf-8"
    asyncio.run(run())
