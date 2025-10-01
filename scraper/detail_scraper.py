import re
import json
import traceback
from urllib.parse import urljoin, urlparse
from datetime import datetime


async def extract_complete_metadata(browser, detail_url: str, video_data: dict, logger=None, page_num=None):
    """Extract complete metadata from video detail page with robust error logging and download handling"""
    page = None
    video_id = video_data.get('video_id', 'unknown')

    try:
        if logger:
            logger.info(f"DETAIL SCRAPER: Starting metadata extraction for video_id='{video_id}' page={page_num} url='{detail_url}'")

        # Create a new page from the browser
        page = await browser.new_page()
        await page.goto(detail_url, wait_until='networkidle')

        # Initialize metadata structure as specified
        metadata = {
            "video_id": video_data.get("video_id", ""),
            "url": detail_url,
            "title": video_data.get("title", ""),
            "duration": "",
            "views": 0,
            "upload_date": int(datetime.now().timestamp() * 1000),
            "tags": [],
            "video_src": "",
            "thumbnail_src": video_data.get("thumbnail", ""),
            "uploaded_by": "Anonymous",
            "description": "No description available",
            "categories": [],
            "artists": [],
            "download_sources": []
        }

        # Extract title from page (h1.title_video)
        try:
            title_elem = await page.query_selector('h1.title_video')
            if title_elem:
                title = await title_elem.inner_text()
                if title.strip():
                    metadata["title"] = title.strip()
                    if logger:
                        logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted title: '{title.strip()}'")
        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting title: {e}")

        # Extract video info (duration, views, upload date) from .item_info elements
        try:
            info_items = await page.query_selector_all('.info .row .item_info')
            for item in info_items:
                item_text = await item.inner_text()

                # Extract duration (format like "1:37")
                if ':' in item_text and any(char.isdigit() for char in item_text):
                    duration_match = re.search(r'(\d{1,2}:\d{2})', item_text)
                    if duration_match:
                        metadata["duration"] = duration_match.group(1)
                        if logger:
                            logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted duration: '{duration_match.group(1)}'")

                # Extract views (format like "1.4M (1,414,053)")
                if any(indicator in item_text.lower() for indicator in ['k', 'm', 'view', '(']):
                    # Try to extract the integer value inside parentheses first
                    views_int_match = re.search(r'\(([\d,]+)\)', item_text)
                    if views_int_match:
                        metadata["views"] = int(views_int_match.group(1).replace(',', ''))
                        if logger:
                            logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted views (integer): {metadata['views']}")
                    else:
                        # Fallback: extract main view count and convert to int if possible
                        views_match = re.search(r'([\d,.]+)([KMB]?)', item_text)
                        if views_match:
                            num, suffix = views_match.groups()
                            num = num.replace(',', '')
                            if suffix == 'M':
                                metadata["views"] = int(float(num) * 1_000_000)
                            elif suffix == 'K':
                                metadata["views"] = int(float(num) * 1_000)
                            elif suffix == 'B':
                                metadata["views"] = int(float(num) * 1_000_000_000)
                            else:
                                metadata["views"] = int(float(num))
                            if logger:
                                logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted views (converted): {metadata['views']}")

                # Extract upload date info (format like "6 years ago")
                if 'ago' in item_text.lower():
                    if logger:
                        logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Found upload date info: '{item_text.strip()}'")
                    # For now, keep current timestamp; could parse relative dates later
                    pass

        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting video info: {e}")

        # Extract uploaded_by
        try:
            uploaded_by_elem = await page.query_selector('div.col:has(.label:text("Uploaded by")) a.item_btn_link')
            if uploaded_by_elem:
                uploaded_by = await uploaded_by_elem.inner_text()
                if uploaded_by.strip():
                    metadata["uploaded_by"] = uploaded_by.strip()
                    if logger:
                        logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted uploaded_by: '{uploaded_by.strip()}'")
        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting uploaded_by: {e}")

        # Extract categories
        try:
            category_elements = await page.query_selector_all('div.col:has(.label:text("Categories")) a.item_btn_link span')
            categories = []
            for cat_elem in category_elements:
                cat_text = await cat_elem.inner_text()
                if cat_text.strip():
                    categories.append(cat_text.strip())
            metadata["categories"] = categories
            if logger:
                logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted {len(categories)} categories: {categories}")
        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting categories: {e}")

        # Extract artists
        try:
            artist_elements = await page.query_selector_all('div.col:has(.label:text("Artist")) a.item_btn_link span')
            artists = []
            for artist_elem in artist_elements:
                artist_text = await artist_elem.inner_text()
                if artist_text.strip():
                    artists.append(artist_text.strip())
            metadata["artists"] = artists
            if logger:
                logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted {len(artists)} artists: {artists}")
        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting artists: {e}")

        # Extract tags from Tags section
        try:
            tag_elements = await page.query_selector_all('div.row_spacer:has(.label:text("Tags")) a.tag_item')
            tags = []
            for tag_elem in tag_elements:
                tag_text = await tag_elem.inner_text()
                if tag_text.strip() and not tag_text.strip().startswith('+'):  # Skip "Suggest" button
                    tags.append(tag_text.strip())
            metadata["tags"] = tags[:20]  # Limit to reasonable number
            if logger:
                logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted {len(tags)} tags: {tags[:5]}{'...' if len(tags) > 5 else ''}")
        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting tags: {e}")

        # CRITICAL: Extract download sources with comprehensive error handling
        download_sources = await extract_download_sources(page, logger=logger, video_id=video_id, page_num=page_num, detail_url=detail_url)
        metadata["download_sources"] = download_sources

        # ROBUST: Select highest quality download source
        if download_sources:
            try:
                best_source = max(download_sources, key=lambda x: x.get('quality_score', 0))
                video_url = best_source["url"]

                # Remove &download=true from the URL (multiple methods for robustness)
                video_url = video_url.replace('&download=true', '').replace('download=true&', '').replace('download=true', '')
                metadata["video_src"] = video_url

                if logger:
                    logger.info(f"DOWNLOAD SUCCESS: video_id='{video_id}' page={page_num} - Selected best quality: {best_source.get('quality', 'unknown')} from {len(download_sources)} sources")
                    logger.debug(f"DOWNLOAD URL: video_id='{video_id}' - Final video_src: '{video_url[:100]}{'...' if len(video_url) > 100 else ''}'")

            except Exception as e:
                if logger:
                    logger.error(f"DOWNLOAD ERROR: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                               f"Exception while selecting best download source: {str(e)}")
                    logger.error(f"DOWNLOAD ERROR TRACEBACK: video_id='{video_id}'\n{traceback.format_exc()}")
        else:
            # CRITICAL: Log detailed failure when no download sources found
            if logger:
                logger.error(f"DOWNLOAD FAILURE: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                           f"Expected: at least one valid download source with get_file in URL; "
                           f"Found: 0 download sources extracted from page")

        # Extract description from author info if available
        try:
            desc_elem = await page.query_selector('.label em')
            if desc_elem:
                desc_text = await desc_elem.inner_text()
                if desc_text.strip():
                    metadata["description"] = desc_text.strip()
                    if logger:
                        logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted description: '{desc_text.strip()[:100]}{'...' if len(desc_text.strip()) > 100 else ''}'")
        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting description: {e}")

        # Extract additional metadata from JSON-LD if available
        try:
            json_ld = await page.query_selector('script[type="application/ld+json"]')
            if json_ld:
                json_text = await json_ld.inner_text()
                json_data = json.loads(json_text)

                # Extract duration in ISO format (PT0H1M37S -> 1:37)
                if 'duration' in json_data:
                    iso_duration = json_data['duration']
                    duration_match = re.search(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso_duration)
                    if duration_match:
                        hours, minutes, seconds = duration_match.groups()
                        hours = int(hours) if hours else 0
                        minutes = int(minutes) if minutes else 0
                        seconds = int(seconds) if seconds else 0

                        if hours > 0:
                            metadata["duration"] = f"{hours}:{minutes:02d}:{seconds:02d}"
                        else:
                            metadata["duration"] = f"{minutes}:{seconds:02d}"

                        if logger:
                            logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted duration from JSON-LD: '{metadata['duration']}'")

                # Extract view count from interactionStatistic
                if 'interactionStatistic' in json_data:
                    for stat in json_data['interactionStatistic']:
                        if stat.get('interactionType') == 'http://schema.org/WatchAction':
                            view_count = stat.get('userInteractionCount', '0')
                            try:
                                metadata["views"] = int(view_count)
                                if logger:
                                    logger.debug(f"DETAIL SCRAPER: video_id='{video_id}' - Extracted views from JSON-LD: {view_count}")
                            except Exception:
                                # If view_count is not an integer, fallback to 0
                                metadata["views"] = 0
                            break

        except Exception as e:
            if logger:
                logger.warning(f"DETAIL SCRAPER: video_id='{video_id}' - Error extracting JSON-LD metadata: {e}")

        # FINAL VALIDATION: Check if video_src was successfully extracted
        if not metadata.get("video_src"):
            # CRITICAL: Final error logging with complete context
            found_fields = [key for key, value in metadata.items() if value]
            download_sources_count = len(metadata.get('download_sources', []))

            if logger:
                logger.error(f"FINAL VALIDATION FAILURE: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                           f"Expected: valid video_src field containing direct download URL; "
                           f"Found: empty video_src after all extraction attempts; "
                           f"download_sources_count: {download_sources_count}; "
                           f"extracted_fields: {found_fields}; "
                           f"metadata_keys: {list(metadata.keys())}")

                # Log sample of download sources for debugging
                if download_sources:
                    for i, source in enumerate(download_sources[:3]):  # Log first 3 sources
                        logger.error(f"FINAL VALIDATION: video_id='{video_id}' - Source {i+1}: quality='{source.get('quality', 'unknown')}' url='{source.get('url', 'none')[:100]}{'...' if len(source.get('url', '')) > 100 else ''}'")
        else:
            if logger:
                logger.info(f"FINAL VALIDATION SUCCESS: video_id='{video_id}' page={page_num} - Complete metadata extracted with video_src length: {len(metadata['video_src'])} chars")

        return metadata

    except Exception as e:
        error_traceback = traceback.format_exc()
        if logger:
            logger.error(f"DETAIL SCRAPER EXCEPTION: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                        f"Unexpected exception during metadata extraction: {str(e)}")
            logger.error(f"DETAIL SCRAPER EXCEPTION TRACEBACK: video_id='{video_id}'\n{error_traceback}")
        else:
            print(f"Error extracting metadata from {detail_url}: {e}")

        return None
    finally:
        # Always close the page, even if an exception occurs
        if page:
            try:
                await page.close()
            except:
                pass


async def extract_download_sources(page, logger=None, video_id='unknown', page_num=None, detail_url='unknown'):
    """Extract download links from the Download section with comprehensive error logging"""
    sources = []

    try:
        if logger:
            logger.debug(f"DOWNLOAD EXTRACTION: video_id='{video_id}' page={page_num} - Starting download sources extraction")

        # Look for download section based on the exact HTML structure
        download_section = await page.query_selector('div.row_spacer:has(.label:text("Download"))')

        if download_section:
            if logger:
                logger.debug(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Found download section")

            # Get all download links - try multiple selectors for robustness
            download_selectors = [
                'a.tag_item[href*="get_file"]',
                'a.tag_item[href*="getfile"]', 
                'a[href*="get_file"]',
                'a[href*="getfile"]'
            ]

            download_links = []
            for selector in download_selectors:
                try:
                    links = await download_section.query_selector_all(selector)
                    if links:
                        download_links.extend(links)
                        if logger:
                            logger.debug(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Selector '{selector}' found {len(links)} links")
                        break
                except Exception as e:
                    if logger:
                        logger.debug(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Selector '{selector}' failed: {e}")
                    continue

            # Remove duplicates by URL
            seen_urls = set()
            unique_links = []
            for link in download_links:
                try:
                    url = await link.get_attribute('href')
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        unique_links.append(link)
                except:
                    continue

            download_links = unique_links

            if logger:
                logger.info(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Found {len(download_links)} unique download links")

            if not download_links:
                if logger:
                    logger.error(f"DOWNLOAD EXTRACTION FAILURE: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                               f"Expected: at least one download link with get_file/getfile in href attribute; "
                               f"Found: download section exists but 0 valid download links found with any selector")
                return sources

            for i, link in enumerate(download_links):
                try:
                    url = await link.get_attribute('href')
                    text = await link.inner_text()

                    if logger:
                        logger.debug(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Processing link {i+1}: '{text}' -> '{url[:100]}{'...' if len(url) > 100 else ''}'")

                    if url and ('get_file' in url or 'getfile' in url):
                        # Extract quality from text like "MP4 720p", "MP4 480p"
                        quality_match = re.search(r'(\d+)p', text)
                        quality = int(quality_match.group(1)) if quality_match else 0

                        # Calculate quality score for sorting
                        if quality >= 1080:
                            quality_score = 1080
                        elif quality >= 720:
                            quality_score = 720
                        elif quality >= 480:
                            quality_score = 480
                        elif quality >= 360:
                            quality_score = 360
                        else:
                            quality_score = 240  # Default for unknown quality

                        source = {
                            'url': url,
                            'quality': f"{quality}p" if quality else "unknown",
                            'quality_score': quality_score,
                            'format': 'mp4',
                            'text': text.strip()
                        }
                        sources.append(source)

                        if logger:
                            logger.debug(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Added source {i+1}: quality={source['quality']}, score={quality_score}")
                    else:
                        if logger:
                            logger.warning(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Link {i+1} rejected: URL does not contain get_file/getfile")

                except Exception as e:
                    if logger:
                        logger.warning(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Error processing link {i+1}: {e}")
                    continue

            # Sort by quality (highest first)
            sources.sort(key=lambda x: x['quality_score'], reverse=True)

            if logger:
                if sources:
                    logger.info(f"DOWNLOAD EXTRACTION SUCCESS: video_id='{video_id}' page={page_num} - Extracted {len(sources)} valid download sources")
                    for i, source in enumerate(sources):
                        logger.debug(f"DOWNLOAD EXTRACTION: video_id='{video_id}' - Final source {i+1}: {source['quality']} ({source['quality_score']}) - {source['url'][:100]}{'...' if len(source['url']) > 100 else ''}")
                else:
                    logger.error(f"DOWNLOAD EXTRACTION FAILURE: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                               f"Expected: at least one valid download source after processing {len(download_links)} links; "
                               f"Found: 0 valid sources extracted (all links rejected or failed processing)")
        else:
            if logger:
                logger.error(f"DOWNLOAD EXTRACTION FAILURE: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                           f"Expected: download section with class 'row_spacer' containing label text 'Download'; "
                           f"Found: no download section found on page")
            else:
                print(f"DEBUG: Download section not found for {video_id}")

    except Exception as e:
        error_traceback = traceback.format_exc()
        if logger:
            logger.error(f"DOWNLOAD EXTRACTION EXCEPTION: video_id='{video_id}' page={page_num} url='{detail_url}' - "
                        f"Unexpected exception while extracting download sources: {str(e)}")
            logger.error(f"DOWNLOAD EXTRACTION EXCEPTION TRACEBACK: video_id='{video_id}'\n{error_traceback}")
        else:
            print(f"Error extracting download sources for {video_id}: {e}")

    return sources
