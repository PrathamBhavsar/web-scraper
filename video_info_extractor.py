import re
import time
import json
import logging
import asyncio
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from urllib.parse import urljoin
from date_parser import DateParser

# Crawl4AI imports
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

class VideoInfoExtractor:
    def __init__(self, config, driver_manager, date_parser):
        self.config = config
        self.driver_manager = driver_manager
        self.date_parser = date_parser
        self.base_url = "https://rule34video.com"
        self.logger = logging.getLogger('Rule34Scraper')

        # Enhanced Crawl4AI schemas
        self.listing_schema = {
            "name": "rule34video.com Listing Schema",
            "baseSelector": "#custom_list_videos_most_recent_videos_items > div",
            "fields": [
                {"name": "upload_date", "selector": "a.th.js-open-popup > div.thumb_info > div.added:nth-of-type(1)", "type": "text"},
                {"name": "duration", "selector": "a.th.js-open-popup > div.img.wrap_image > div.time:nth-of-type(3)", "type": "text"},
                {"name": "title", "selector": "a.th.js-open-popup > div.thumb_title:nth-of-type(2)", "type": "text"},
                {"name": "video_link", "selector": "a.th.js-open-popup", "type": "attribute", "attribute": "href"},
                {"name": "thumbnail", "selector": "a.th.js-open-popup > div.img.wrap_image > img", "type": "attribute", "attribute": "data-src"}
            ]
        }

        # UPDATED: Better detail schema for views extraction
        self.detail_schema = {
            "name": "rule34video.com Detail Schema",
            "baseSelector": "div.fancybox-inner > div",
            "fields": [
                {"name": "info_details", "selector": "#tab_video_info > div.info.row", "type": "text"},
                {"name": "uploaded_by", "selector": "a.item.btn_link", "type": "text"},
                {"name": "tags", "selector": "div.wrap", "type": "text"},
                {"name": "video_source", "selector": "video", "type": "attribute", "attribute": "src"},
                {"name": "video_poster", "selector": "video", "type": "attribute", "attribute": "poster"},
                {"name": "views", "selector": "span", "type": "text"}  # NEW: Extract views from spans
            ]
        }

    @property
    def driver(self):
        return self.driver_manager.get_driver()

    async def extract_page_listings_crawl4ai(self, page_url):
        """Extract video listings from a page using Crawl4AI listing schema"""
        try:
            self.logger.info(f"Extracting video listings from: {page_url}")
            browser_config = BrowserConfig(headless=True, verbose=False)
            extraction_strategy = JsonCssExtractionStrategy(schema=self.listing_schema)
            crawler_config = CrawlerRunConfig(
                extraction_strategy=extraction_strategy,
                wait_for="css:#custom_list_videos_most_recent_videos_items",
                js_code="window.scrollTo(0, document.body.scrollHeight);"
            )

            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=page_url, config=crawler_config)

                if result.success and result.extracted_content:
                    data = json.loads(result.extracted_content)
                    self.logger.info(f"Successfully extracted {len(data)} video listings")
                    processed_listings = []
                    for item in data:
                        processed_item = self.process_listing_item(item)
                        if processed_item:
                            processed_listings.append(processed_item)
                    return processed_listings
                else:
                    self.logger.error(f"Listing extraction failed: {result.error_message}")
                    return []

        except Exception as e:
            self.logger.error(f"Error in Crawl4AI listing extraction: {e}")
            return []

    def process_listing_item(self, raw_item):
        """Process and clean a raw listing item"""
        try:
            processed = {
                "title": raw_item.get("title", "").strip(),
                "duration": raw_item.get("duration", "").strip(),
                "upload_date": raw_item.get("upload_date", "").strip(),
                "video_link": raw_item.get("video_link", "").strip(),
                "thumbnail": raw_item.get("thumbnail", "").strip()
            }

            # Clean and validate video link
            if processed["video_link"]:
                if not processed["video_link"].startswith('http'):
                    processed["video_link"] = urljoin(self.base_url, processed["video_link"])
                # Extract video ID
                processed["video_id"] = self.extract_video_id(processed["video_link"])

            # Clean thumbnail URL
            if processed["thumbnail"] and not processed["thumbnail"].startswith('http'):
                processed["thumbnail"] = urljoin(self.base_url, processed["thumbnail"])

            # Parse upload date to epoch if possible
            if processed["upload_date"]:
                epoch = self.date_parser.parse_upload_date_to_epoch(processed["upload_date"])
                processed["upload_date_epoch"] = epoch

            return processed

        except Exception as e:
            self.logger.error(f"Error processing listing item: {e}")
            return None

    async def extract_video_details_crawl4ai(self, video_url):
        """Extract detailed video information using Crawl4AI detail schema"""
        try:
            self.logger.info(f"Extracting video details from: {video_url}")
            browser_config = BrowserConfig(headless=True, verbose=False)
            extraction_strategy = JsonCssExtractionStrategy(schema=self.detail_schema)
            crawler_config = CrawlerRunConfig(
                extraction_strategy=extraction_strategy,
                wait_for="css:#tab_video_info",
                js_code="""
                // Wait for video player and info to load
                setTimeout(() => {
                const video = document.querySelector('video');
                if (video) {
                video.load();
                }
                // Wait for spans with view counts to load
                const spans = document.querySelectorAll('span');
                console.log('Found spans:', spans.length);
                }, 3000);
                """
            )

            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=video_url, config=crawler_config)

                if result.success and result.extracted_content:
                    data = json.loads(result.extracted_content)
                    detail_info = data[0] if data else {}
                    self.logger.info(f"Successfully extracted detailed info")
                    return detail_info
                else:
                    self.logger.error(f"Detail extraction failed: {result.error_message}")
                    return {}

        except Exception as e:
            self.logger.error(f"Error in Crawl4AI detail extraction: {e}")
            return {}

    async def parallel_extract_multiple_videos(self, video_urls):
        """Extract details from multiple video URLs in parallel using Crawl4AI"""
        try:
            self.logger.info(f"Starting parallel extraction for {len(video_urls)} videos")
            # Create tasks for parallel execution
            tasks = [self.extract_video_details_crawl4ai(url) for url in video_urls]
            
            # Execute all tasks in parallel with some concurrency control
            batch_size = min(self.config.get("processing", {}).get("parallel_batch_size", 5), len(tasks))
            results = []
            
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                batch_results = await asyncio.gather(*batch, return_exceptions=True)
                for result in batch_results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Parallel extraction failed: {result}")
                        results.append({})
                    else:
                        results.append(result)
                        
                # Small delay between batches
                if i + batch_size < len(tasks):
                    await asyncio.sleep(1)

            self.logger.info(f"Parallel extraction completed: {len(results)} results")
            return results

        except Exception as e:
            self.logger.error(f"Error in parallel video extraction: {e}")
            return [{}] * len(video_urls)

    def create_complete_video_info_from_schemas(self, listing_data, detail_data):
        """Create complete video info by combining listing and detail schema results"""
        try:
            video_info = {
                "video_id": listing_data.get("video_id", ""),
                "url": listing_data.get("video_link", ""),
                "title": listing_data.get("title", ""),
                "duration": listing_data.get("duration", ""),
                "views": "",
                "uploader": "",
                "upload_date": listing_data.get("upload_date", ""),
                "tags": [],
                "video_src": "",
                "thumbnail_src": listing_data.get("thumbnail", "")
            }

            # Merge detail data
            if detail_data:
                # Extract uploader
                if detail_data.get("uploaded_by"):
                    video_info["uploader"] = detail_data["uploaded_by"].strip()

                # Extract tags
                if detail_data.get("tags"):
                    tags = self.parse_tags_from_text(detail_data["tags"])
                    if tags:
                        video_info["tags"] = tags

                # Extract video source
                if detail_data.get("video_source"):
                    video_src = detail_data["video_source"]
                    if not video_src.startswith('http'):
                        video_src = urljoin(self.base_url, video_src)
                    video_info["video_src"] = video_src

                # Extract poster/thumbnail from video element
                if detail_data.get("video_poster") and not video_info["thumbnail_src"]:
                    poster = detail_data["video_poster"]
                    if not poster.startswith('http'):
                        poster = urljoin(self.base_url, poster)
                    video_info["thumbnail_src"] = poster

                # UPDATED: Extract views from Crawl4AI data
                if detail_data.get("views"):
                    views_num = self.extract_views_from_crawl4ai(detail_data["views"])
                    if views_num is not None:
                        video_info["views"] = str(views_num)

                # Parse info details for additional metadata
                if detail_data.get("info_details"):
                    self.parse_info_details_text(video_info, detail_data["info_details"])

            # Set defaults for missing values
            self.set_default_values(video_info)
            return video_info

        except Exception as e:
            self.logger.error(f"Error creating complete video info: {e}")
            return self.create_video_info_from_listing_only(listing_data)

    def extract_views_from_crawl4ai(self, views_data):
        """Extract views number from Crawl4AI span data, prioritizing bracket numbers"""
        try:
            if isinstance(views_data, str):
                views_text = views_data
            elif isinstance(views_data, list):
                # Join all span texts
                views_text = " ".join([str(v) for v in views_data if v])
            else:
                views_text = str(views_data)

            self.logger.debug(f"Processing Crawl4AI views data: '{views_text}'")

            # PRIORITY 1: Look for numbers in brackets/parentheses first: "2.4K (24,875)"
            bracket_patterns = [
                r'(\d+(?:,\d{3})*)\)',  # "24,875)" - number with commas before closing bracket
                r'\((\d+(?:,\d{3})*)\)',  # "(24,875)" - full bracket pattern
                r'(\d+(?:,\d{3})*)\]',    # "24,875]" - square bracket
            ]

            for pattern in bracket_patterns:
                bracket_match = re.search(pattern, views_text)
                if bracket_match:
                    num_str = bracket_match.group(1).replace(',', '')
                    try:
                        views_num = int(num_str)
                        self.logger.info(f"Views extracted from brackets: {views_num}")
                        return views_num
                    except ValueError:
                        continue

            # PRIORITY 2: Look for standard view patterns
            views_patterns = [
                r'(\d+(?:,\d{3})*)\s*views?',          # "24,875 views"
                r'(\d+(?:\.\d+)?[KkMmBb])\s*views?',   # "2.4K views"
                r'(\d+(?:,\d{3})*)',                   # Any number with commas
                r'(\d+)'                               # Any number
            ]

            for pattern in views_patterns:
                views_match = re.search(pattern, views_text, re.IGNORECASE)
                if views_match:
                    views_text_matched = views_match.group(1)
                    views_num = self.parse_views_number(views_text_matched)
                    if views_num is not None and views_num > 0:
                        self.logger.info(f"Views extracted from pattern: {views_num}")
                        return views_num

            return 0

        except Exception as e:
            self.logger.error(f"Error extracting views from Crawl4AI data: {e}")
            return 0

    def create_video_info_from_listing_only(self, listing_data):
        """Create video info structure from listing data only (fallback)"""
        try:
            video_info = {
                "video_id": listing_data.get("video_id", ""),
                "url": listing_data.get("video_link", ""),
                "title": listing_data.get("title", ""),
                "duration": listing_data.get("duration", ""),
                "views": "0",
                "uploader": "Unknown",
                "upload_date": listing_data.get("upload_date", ""),
                "tags": ["untagged"],
                "video_src": "",
                "thumbnail_src": listing_data.get("thumbnail", "")
            }

            self.set_default_values(video_info)
            return video_info

        except Exception as e:
            self.logger.error(f"Error creating video info from listing: {e}")
            return None

    def extract_video_info(self, video_url):
        """Main orchestrator method - enhanced with dual schema integration"""
        try:
            # Extract video ID first
            video_id = self.extract_video_id(video_url)
            if not video_id:
                return None

            # Try the new dual schema approach first
            loop = None
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Extract details using Crawl4AI detail schema
            crawl4ai_detail_data = loop.run_until_complete(
                self.extract_video_details_crawl4ai(video_url)
            )

            # Create basic listing data from URL (since we don't have the listing page context)
            basic_listing = {
                "video_id": video_id,
                "video_link": video_url,
                "title": "",
                "duration": "",
                "upload_date": "",
                "thumbnail": ""
            }

            # Create complete video info
            video_info = self.create_complete_video_info_from_schemas(
                basic_listing, crawl4ai_detail_data
            )

            # Fallback to Selenium extraction for missing critical data
            if not self.is_video_info_complete(video_info):
                self.logger.info("Crawl4AI data incomplete, supplementing with Selenium extraction")
                self.supplement_with_selenium(video_info, video_url)

            # Set defaults for missing values
            self.set_default_values(video_info)

            # Log extracted information
            self.log_extracted_info(video_info)
            return video_info

        except Exception as e:
            self.logger.error(f"Error extracting video info from {video_url}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None

    def parse_views_number(self, views_text):
        """Convert view text like '24K' to actual numbers, prioritizing parentheses numbers"""
        if not views_text:
            return 0

        # CHANGE: Prioritize number inside parentheses first: e.g. "2.4K (24875)"
        paren_match = re.search(r'\((\d+(?:,\d{3})*)\)', views_text)
        if paren_match:
            num = paren_match.group(1).replace(',', '')
            try:
                return int(num)
            except:
                pass

        # Parse K/M/B suffixes or plain numbers
        short_match = re.search(r'([\d,.]+)\s*([KkMmBb]?)', views_text)
        if short_match:
            num_str = short_match.group(1).replace(',', '')
            suffix = short_match.group(2).upper()
            try:
                value = float(num_str)
            except:
                # fallback: extract digits only
                digits = re.sub(r'\D', '', views_text)
                return int(digits) if digits else 0

            if suffix == 'K':
                return int(value * 1_000)
            if suffix == 'M':
                return int(value * 1_000_000)
            if suffix == 'B':
                return int(value * 1_000_000_000)
            return int(value)

        # Final fallback: extract digits
        digits = re.sub(r'\D', '', views_text)
        return int(digits) if digits else 0

    def extract_item_info_data(self, video_info):
        """Extract duration, views, and upload date from info row elements"""
        try:
            info_row_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'info row')]")
            self.logger.info(f"Found {len(info_row_elements)} info row elements")

            if not info_row_elements:
                self.logger.warning("No info row elements found - validation failed at element discovery")
                return

            for i, row in enumerate(info_row_elements):
                try:
                    row_text = row.get_attribute("textContent") or row.text.strip()
                    self.logger.debug(f"Processing row {i+1}: '{row_text}'")

                    if not row_text:
                        self.logger.warning(f"Row {i+1} validation failed: empty text content")
                        continue

                    # CHANGE: Check for views pattern in any row text - look for patterns like "2.4K (24875)"
                    if not video_info.get("views") or video_info["views"] == "0":
                        views_num = self.extract_views_from_crawl4ai(row_text)
                        if views_num and views_num > 0:
                            video_info["views"] = str(views_num)
                            self.logger.info(f"Views extracted: '{row_text}' -> {video_info['views']}")

                    # Check which type of icon this row contains
                    has_calendar = len(row.find_elements(By.XPATH, ".//svg[contains(@class,'custom-calender')]")) > 0
                    has_eye = len(row.find_elements(By.XPATH, ".//svg[contains(@class,'custom-eye')]")) > 0
                    has_time = len(row.find_elements(By.XPATH, ".//svg[contains(@class,'custom-time')]")) > 0

                    self.logger.debug(f"Row {i+1} icons - Calendar: {has_calendar}, Eye: {has_eye}, Time: {has_time}")

                    # Validate that exactly one icon type is found
                    icon_count = sum([has_calendar, has_eye, has_time])
                    if icon_count == 0:
                        self.logger.warning(f"Row {i+1} validation failed: no recognized icon found")
                        continue
                    elif icon_count > 1:
                        self.logger.warning(f"Row {i+1} validation failed: multiple icons found")
                        continue

                    # Process based on icon type
                    if has_calendar:
                        if not row_text:
                            continue
                        video_info["upload_date"] = row_text.strip()
                        epoch = self.date_parser.parse_upload_date_to_epoch(row_text.strip())
                        if epoch:
                            # CHANGE: Store as int upload_date instead of upload_date_epoch
                            video_info["upload_date"] = int(epoch)
                        self.logger.info(f"Upload date extracted: '{video_info['upload_date']}'")

                    elif has_eye:
                        if not row_text:
                            continue
                        views_num = self.extract_views_from_crawl4ai(row_text)
                        if views_num is not None:
                            video_info["views"] = str(views_num)
                        self.logger.info(f"Views extracted: '{row_text}' -> {video_info['views']}")

                    elif has_time:
                        if not row_text:
                            continue
                        time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', row_text)
                        if time_match:
                            video_info["duration"] = time_match.group(1)
                            self.logger.info(f"Duration extracted: '{video_info['duration']}'")
                        else:
                            if re.match(r'^\d{1,2}:\d{2}(?::\d{2})?$', row_text.strip()):
                                video_info["duration"] = row_text.strip()
                                self.logger.info(f"Duration extracted (fallback): '{video_info['duration']}'")

                except Exception as e:
                    self.logger.error(f"Error processing row {i+1}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Critical error in extract_item_info_data: {e}")

    def set_default_values(self, video_info):
        """Ensure all required fields have valid defaults"""
        if not video_info.get("title"):
            video_info["title"] = f"Video_{video_info.get('video_id', '')}"

        if not video_info.get("duration") or video_info.get("duration") in ["00:00", "0:00"]:
            # Try to get actual duration from video element one more time
            actual_duration = self.extract_duration()
            video_info["duration"] = actual_duration if actual_duration else "00:30"

        if not video_info.get("views"):
            video_info["views"] = "0"

        if not video_info.get("uploader"):
            video_info["uploader"] = "Unknown"

        # CHANGE: Use integer timestamp for upload_date
        if not video_info.get("upload_date") or video_info["upload_date"] == "Unknown":
            video_info["upload_date"] = int(datetime.now().timestamp() * 1000)
        elif "upload_date_epoch" in video_info and video_info["upload_date_epoch"]:
            # Convert epoch to upload_date if it exists
            video_info["upload_date"] = int(video_info["upload_date_epoch"])

        # Remove upload_date_epoch field if it exists
        if "upload_date_epoch" in video_info:
            del video_info["upload_date_epoch"]

        if not video_info.get("tags"):
            video_info["tags"] = ["untagged"]

        # CHANGE: Remove crawl4ai data fields
        for key in ["crawl4ai_data", "crawl4ai_listing_data", "crawl4ai_detail_data"]:
            if key in video_info:
                del video_info[key]

    # Keep all other existing methods unchanged for backward compatibility
    def extract_video_id(self, video_url):
        """Get video ID from URL using regex patterns"""
        video_id_match = re.search(r'/video/(\d+)/', video_url)
        if video_id_match:
            return video_id_match.group(1)
        else:
            return video_url.split('/')[-2] if video_url.endswith('/') else video_url.split('/')[-1]

    def extract_title(self):
        """Get video title from page elements"""
        try:
            title_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'thumb_title')]")
            title = title_element.text.strip()
            self.logger.info(f"Title extracted: {title}")
            return title
        except NoSuchElementException:
            try:
                title_element = self.driver.find_element(By.TAG_NAME, "h1")
                return title_element.text.strip()
            except:
                self.logger.warning("Could not extract title, will use default")
                return ""

    def parse_tags_from_text(self, tags_text):
        """Parse tags from the extracted tags text"""
        try:
            tags = []
            ignore_tags = {
                "+ | suggest", "+ | Suggest",
                "mp4 2160p", "mp4 1080p", "mp4 720p", "mp4 480p", "mp4 360p",
                "suggest"
            }

            # Split by common delimiters
            potential_tags = re.split(r'[,\n\r\t]+', tags_text)
            for tag in potential_tags:
                tag = tag.strip()
                if not tag:
                    continue
                if tag.lower() in {t.lower() for t in ignore_tags}:
                    continue
                # Remove any HTML-like content
                tag = re.sub(r'<[^>]+>', '', tag).strip()
                if tag and len(tag) > 1:  # Avoid single character tags
                    tags.append(tag)
            return tags[:20]  # Limit to first 20 tags

        except Exception as e:
            self.logger.error(f"Error parsing tags: {e}")
            return []

    def parse_info_details_text(self, video_info, info_text):
        """Parse info details text to extract duration, views, and upload date"""
        try:
            # Look for duration pattern
            duration_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', info_text)
            if duration_match and not video_info.get("duration"):
                video_info["duration"] = duration_match.group(1)

            # Look for views pattern - CHANGE: prioritize parentheses numbers
            if not video_info.get("views") or video_info["views"] == "0":
                views_num = self.extract_views_from_crawl4ai(info_text)
                if views_num and views_num > 0:
                    video_info["views"] = str(views_num)

            # Look for upload date patterns if not already set
            if not video_info.get("upload_date"):
                date_patterns = [
                    r'(?:uploaded|added|posted)(?:\s+on)?\s*:?\s*([^\n\r,]+)',
                    r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
                    r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4})',
                    r'(\d+\s+(?:days?|weeks?|months?|years?)\s+ago)'
                ]

                for pattern in date_patterns:
                    date_match = re.search(pattern, info_text, re.IGNORECASE)
                    if date_match:
                        date_text = date_match.group(1).strip()
                        video_info["upload_date"] = date_text
                        epoch = self.date_parser.parse_upload_date_to_epoch(date_text)
                        if epoch:
                            # CHANGE: Store as int upload_date
                            video_info["upload_date"] = int(epoch)
                        break

        except Exception as e:
            self.logger.error(f"Error parsing info details: {e}")

    def is_video_info_complete(self, video_info):
        """Check if video info has all essential fields populated"""
        essential_fields = ["title", "uploader"]
        for field in essential_fields:
            if not video_info.get(field):
                return False
        # Check if tags is empty list
        if isinstance(video_info.get("tags"), list) and not video_info["tags"]:
            return False
        return True

    def supplement_with_selenium(self, video_info, video_url):
        """Use Selenium to fill in missing information"""
        try:
            # Navigate to video page if needed
            if not self.driver_manager.navigate_to_page(video_url):
                return

            # Extract missing fields using existing Selenium methods
            if not video_info.get("title"):
                video_info["title"] = self.extract_title()

            if not video_info.get("duration") or not video_info.get("views") or not video_info.get("upload_date"):
                self.extract_item_info_data(video_info)

            if not video_info.get("uploader"):
                video_info["uploader"] = self.extract_uploader()

            if not video_info.get("tags") or (isinstance(video_info.get("tags"), list) and not video_info["tags"]):
                video_info["tags"] = self.extract_tags()

            if not video_info.get("thumbnail_src"):
                video_info["thumbnail_src"] = self.extract_thumbnail_url()

            if not video_info.get("video_src"):
                video_info["video_src"] = self.extract_video_source()

        except Exception as e:
            self.logger.error(f"Error in Selenium supplementation: {e}")

    def extract_uploader(self):
        """Get uploader name from page elements"""
        try:
            label_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class,'label')]")
            for lbl in label_elements:
                try:
                    lbl_text = lbl.text.strip()
                except:
                    lbl_text = ""

                if lbl_text and 'uploaded by' in lbl_text.lower():
                    try:
                        uploader_element = lbl.find_element(By.XPATH, ".//following-sibling::a[contains(@class,'btn_link')][1]")
                    except NoSuchElementException:
                        try:
                            uploader_element = lbl.find_element(By.XPATH, ".//a[contains(@class,'btn_link')][1]")
                        except NoSuchElementException:
                            uploader_element = None

                    if uploader_element:
                        uploader = uploader_element.text.strip()
                        self.logger.info(f"Uploader extracted: {uploader}")
                        return uploader

            self.logger.debug("Uploader not found, using default")
            return "Unknown"

        except Exception:
            self.logger.warning("Error while extracting uploader (non-fatal)")
            return "Unknown"

    def extract_tags(self):
        """Get all tags while filtering unwanted ones"""
        try:
            tag_elements = self.driver.find_elements(By.XPATH, "//a[contains(@class, 'tag_item')]")
            tags = []
            ignore_tags = {
                "+ | suggest", "+ | Suggest",
                "mp4 2160p", "mp4 1080p", "mp4 720p", "mp4 480p", "mp4 360p",
                "suggest"
            }

            for tag_element in tag_elements:
                tag_text = tag_element.text.strip()
                if not tag_text:
                    continue

                if tag_text.lower() in {t.lower() for t in ignore_tags}:
                    self.logger.debug(f"Ignoring tag: {tag_text}")
                    continue

                tags.append(tag_text)

            result_tags = tags if tags else ["untagged"]
            self.logger.info(f"Tags extracted: {len(result_tags)} tags - {result_tags[:5]}")
            return result_tags

        except NoSuchElementException:
            self.logger.warning("Could not find tag elements, using default")
            return ["untagged"]

    def extract_thumbnail_url(self):
        """Get thumbnail image URL from various sources"""
        try:
            thumbnail_selectors = [
                "//img[contains(@class, 'lazy-load')]",
                "//img[contains(@class, 'thumb')]",
                "//img[contains(@src, 'thumb')]",
                "//img[contains(@data-src, 'thumb')]",
                "//div[contains(@class, 'fp-player')]//img",
                "//video/@poster"
            ]

            thumb_src = None
            for selector in thumbnail_selectors:
                try:
                    if selector.endswith("/@poster"):
                        # Special case for video poster
                        video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")
                        thumb_src = video_element.get_attribute("poster")
                    else:
                        imgs = self.driver.find_elements(By.XPATH, selector)
                        for img in imgs:
                            candidate = (img.get_attribute("data-src") or
                                       img.get_attribute("src") or
                                       img.get_attribute("data-lazy"))
                            if candidate and candidate.strip() and ('thumb' in candidate or 'preview' in candidate):
                                thumb_src = candidate.strip()
                                break

                    if thumb_src:
                        break
                except:
                    continue

            if thumb_src:
                result = urljoin(self.base_url, thumb_src) if not thumb_src.startswith('http') else thumb_src
                self.logger.info(f"Thumbnail extracted: {result}")
                return result
            else:
                # Final fallback: search page source for image URLs
                return self.extract_thumbnail_from_source()

        except Exception as e:
            self.logger.warning(f"Thumbnail extraction failed: {e}")
            return ""

    def extract_thumbnail_from_source(self):
        """Extract thumbnail URL from page source as fallback"""
        try:
            page_source = self.driver.page_source
            img_patterns = [
                r'https?://[^\s"\'<>]+thumb[^\s"\'<>]*\.(?:jpg|jpeg|png|webp)',
                r'https?://[^\s"\'<>]+preview[^\s"\'<>]*\.(?:jpg|jpeg|png|webp)',
                r'"poster":\s*"([^"]+)"',
                r'data-src="([^"]*thumb[^"]*)"'
            ]

            for pattern in img_patterns:
                img_match = re.search(pattern, page_source, re.IGNORECASE)
                if img_match:
                    result = img_match.group(1) if pattern.startswith('"') else img_match.group(0)
                    self.logger.info(f"Thumbnail extracted from page source: {result}")
                    return result

            return ""
        except Exception as e:
            self.logger.warning(f"Thumbnail extraction from source failed: {e}")
            return ""

    def extract_video_source(self):
        """Get actual video file URL from player element"""
        try:
            video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")

            # Try to get source from video element
            video_src = video_element.get_attribute("src")
            if not video_src:
                # Try source elements within video
                source_elements = video_element.find_elements(By.TAG_NAME, "source")
                if source_elements:
                    for source in source_elements:
                        src = source.get_attribute("src")
                        if src:
                            video_src = src
                            # Prefer higher quality sources
                            if any(quality in src for quality in ['1080p', '2160p', '4k']):
                                break

            if video_src:
                result = video_src if video_src.startswith('http') else urljoin(self.base_url, video_src)
                self.logger.info(f"Video source extracted: {result}")
                return result
            else:
                self.logger.warning("Video element found but no source attribute")
                return self.extract_video_source_from_page()

        except NoSuchElementException:
            self.logger.warning("Could not find video player element")
            return self.extract_video_source_from_page()

    def extract_video_source_from_page(self):
        """Extract video URL from page source as fallback"""
        try:
            page_source = self.driver.page_source

            # Look for highest quality MP4 first
            quality_patterns = [
                r'https?://[^\s"\'<>]+(?:2160p|4k)[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+1080p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+720p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*'
            ]

            for pattern in quality_patterns:
                mp4_matches = re.findall(pattern, page_source)
                if mp4_matches:
                    result = mp4_matches[0]
                    self.logger.info(f"Video source found via regex: {result}")
                    return result

            return ""
        except:
            self.logger.error("Could not find video source with any method")
            return ""

    def extract_duration(self):
        """Get video duration from multiple possible sources"""
        try:
            video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")
            actual_duration = self.get_actual_duration_from_player(video_element)
            if actual_duration:
                self.logger.info(f"Duration extracted from video element: {actual_duration}")
                return actual_duration
        except Exception as e:
            self.logger.warning(f"Video element duration extraction failed: {e}")
        return None

    def get_actual_duration_from_player(self, video_element):
        """Extract real duration from video element using JavaScript"""
        try:
            # Wait for video to load metadata
            self.driver.execute_script("""
                var video = arguments[0];
                if (video.readyState === 0) {
                    video.load();
                }
            """, video_element)

            time.sleep(2)

            # Get duration via JavaScript
            duration_seconds = self.driver.execute_script("""
                var video = arguments[0];
                if (video.duration && !isNaN(video.duration) && video.duration > 0) {
                    return video.duration;
                }
                return null;
            """, video_element)

            if duration_seconds and duration_seconds > 0:
                hours = int(duration_seconds // 3600)
                minutes = int((duration_seconds % 3600) // 60)
                seconds = int(duration_seconds % 60)

                if hours > 0:
                    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                else:
                    return f"{minutes:02d}:{seconds:02d}"

            return None

        except Exception as e:
            self.logger.warning(f"Error getting video duration: {e}")
            return None

    def log_extracted_info(self, video_info):
        """Log summary of extracted video information"""
        video_id = video_info["video_id"]
        self.logger.info(f"Extracted info for {video_id}:")
        self.logger.info(f" Title: '{video_info['title'][:50]}...'")
        self.logger.info(f" Duration: {video_info['duration']}")
        self.logger.info(f" Views: {video_info['views']}")
        self.logger.info(f" Uploader: {video_info['uploader']}")
        self.logger.info(f" Upload date: {video_info['upload_date']}")
        self.logger.info(f" Tags: {len(video_info['tags'])} tags")
        self.logger.info(f" Video source: {'Found' if video_info['video_src'] else 'None'}")
        self.logger.info(f" Thumbnail: {'Found' if video_info['thumbnail_src'] else 'None'}")

    def save_video_info_to_json(self, video_info, output_dir):
        """Save extracted video information to JSON file"""
        try:
            import os
            os.makedirs(output_dir, exist_ok=True)
            video_id = video_info["video_id"]
            json_file_path = os.path.join(output_dir, f"{video_id}_info.json")

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Video info saved to: {json_file_path}")
            return json_file_path

        except Exception as e:
            self.logger.error(f"Error saving video info to JSON: {e}")
            return None
