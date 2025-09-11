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
                {"name": "views", "selector": "span", "type": "text"},
				{"name": "description", "selector": "#tab_video_info div.description, #tab_video_info div.desc, #tab_video_info p", "type": "text"},
				{"name": "categories", "selector": "#tab_video_info div[2]/div/div[1]//a[contains(@class, 'item btn_link')]", "type": "text"},
                {"name": "artists", "selector": "#tab_video_info div[2]/div/div[2]//a[contains(@class, 'item btn_link')]", "type": "text"}
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
            """Create complete video info by combining listing and detail schema results - UPDATED with quality selection"""
            try:
                video_info = {
                    "video_id": listing_data.get("video_id", ""),
                    "url": listing_data.get("video_link", ""),
                    "title": listing_data.get("title", ""),
                    "duration": listing_data.get("duration", ""),
                    "views": "",
                    "uploader": "",
                    "uploaded_by": "",  # NEW FIELD
                    "upload_date": listing_data.get("upload_date", ""),
                    "description": "No description available",  # NEW FIELD
                    "categories": [],   # NEW FIELD
                    "artists": [],      # NEW FIELD
                    "tags": [],
                    "video_src": "",
                    "thumbnail_src": listing_data.get("thumbnail", "")
                }

                # Merge detail data
                if detail_data:
                    # Extract uploader/uploaded_by
                    if detail_data.get("uploaded_by"):
                        uploaded_by = detail_data["uploaded_by"].strip()
                        video_info["uploader"] = uploaded_by
                        video_info["uploaded_by"] = uploaded_by

                    # Extract description
                    if detail_data.get("description"):
                        desc = detail_data["description"].strip()
                        if desc and len(desc) > 10:
                            video_info["description"] = desc

                    # Extract categories
                    if detail_data.get("categories"):
                        if isinstance(detail_data["categories"], list):
                            categories = [cat.strip() for cat in detail_data["categories"] if cat and cat.strip()]
                        elif isinstance(detail_data["categories"], str):
                            categories = [detail_data["categories"].strip()] if detail_data["categories"].strip() else []
                        else:
                            categories = []
                        
                        if categories:
                            video_info["categories"] = categories

                    # Extract artists
                    if detail_data.get("artists"):
                        if isinstance(detail_data["artists"], list):
                            artists = [art.strip() for art in detail_data["artists"] if art and art.strip()]
                        elif isinstance(detail_data["artists"], str):
                            artists = [detail_data["artists"].strip()] if detail_data["artists"].strip() else []
                        else:
                            artists = []
                        
                        if artists:
                            video_info["artists"] = artists

                    # Extract tags
                    if detail_data.get("tags"):
                        if isinstance(detail_data["tags"], list):
                            tags = [tag.strip() for tag in detail_data["tags"] if tag and tag.strip() and not any(ignore in tag.lower() for ignore in ['suggest', 'mp4'])]
                        else:
                            tags = self.parse_tags_from_text(str(detail_data["tags"]))
                        if tags:
                            video_info["tags"] = tags

                    # UPDATED: Extract video source with quality preference
                    if detail_data.get("video_source"):
                        video_src = detail_data["video_source"]
                        if not video_src.startswith('http'):
                            video_src = urljoin(self.base_url, video_src)
                        video_info["video_src"] = video_src
                        self.logger.info(f"Video source from Crawl4AI: {video_src}")

                    # Extract poster/thumbnail
                    if detail_data.get("video_poster") and not video_info["thumbnail_src"]:
                        poster = detail_data["video_poster"]
                        if not poster.startswith('http'):
                            poster = urljoin(self.base_url, poster)
                        video_info["thumbnail_src"] = poster

                    # Extract views
                    if detail_data.get("views"):
                        views_num = self.extract_views_from_crawl4ai(detail_data["views"])
                        if views_num is not None:
                            video_info["views"] = str(views_num)

                    # Extract duration
                    if detail_data.get("duration") and not video_info["duration"]:
                        video_info["duration"] = detail_data["duration"].strip()

                    # Extract upload date
                    if detail_data.get("upload_date") and not video_info["upload_date"]:
                        upload_date = detail_data["upload_date"].strip()
                        video_info["upload_date"] = upload_date
                        epoch = self.date_parser.parse_upload_date_to_epoch(upload_date)
                        if epoch:
                            video_info["upload_date"] = int(epoch)

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
        """Create video info structure from listing data only (fallback) - UPDATED"""
        try:
            video_info = {
                "video_id": listing_data.get("video_id", ""),
                "url": listing_data.get("video_link", ""),
                "title": listing_data.get("title", ""),
                "duration": listing_data.get("duration", ""),
                "views": "0",
                "uploader": "Unknown",
                "uploaded_by": "Unknown",  # NEW FIELD
                "upload_date": listing_data.get("upload_date", ""),
                "description": "No description available",  # NEW FIELD
                "categories": ["uncategorized"],  # NEW FIELD
                "artists": ["unknown_artist"],  # NEW FIELD
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
        """Main orchestrator method - UPDATED with comprehensive quality logging"""
        try:
            # Extract video ID first
            video_id = self.extract_video_id(video_url)
            if not video_id:
                return None

            self.logger.info(f"=== Starting video info extraction for {video_id} ===")

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

            # Final check for video source quality if still missing
            if not video_info.get("video_src"):
                self.logger.warning("No video source found, attempting direct quality extraction...")
                if self.driver_manager.navigate_to_page(video_url):
                    video_info["video_src"] = self.extract_video_source()

            # Set defaults for missing values
            self.set_default_values(video_info)

            # Log comprehensive extraction results
            self.log_extracted_info(video_info)
            
            # Additional quality logging if video source was found
            if video_info.get("video_src"):
                self.log_quality_extraction_summary(video_url, video_info["video_src"])

            self.logger.info(f"=== Completed video info extraction for {video_id} ===")
            return video_info

        except Exception as e:
            self.logger.error(f"Error extracting video info from {video_url}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
        

    def log_quality_extraction_summary(self, video_url, final_video_src):
        """Log a summary of quality extraction for debugging"""
        try:
            self.logger.info("=== QUALITY EXTRACTION SUMMARY ===")
            
            # Try to extract all available qualities for logging
            if self.driver_manager.navigate_to_page(video_url):
                all_qualities = self.extract_all_download_qualities()
                
                if all_qualities:
                    self.logger.info(f"All available download qualities:")
                    for i, quality in enumerate(all_qualities, 1):
                        self.logger.info(f"  {i}. {quality['text']} ({quality['quality']}p) - {quality['url'][:60]}...")
                    
                    # Find which quality was selected
                    selected_quality = None
                    for quality in all_qualities:
                        if quality['url'] == final_video_src:
                            selected_quality = quality
                            break
                    
                    if selected_quality:
                        self.logger.info(f"SELECTED QUALITY: {selected_quality['text']} ({selected_quality['quality']}p)")
                    else:
                        self.logger.info(f"SELECTED QUALITY: Not from download section (fallback method used)")
                else:
                    self.logger.info("No download qualities found in download section")
                
                self.logger.info(f"FINAL VIDEO URL: {final_video_src}")
            
            self.logger.info("=== END QUALITY EXTRACTION SUMMARY ===")
            
        except Exception as e:
            self.logger.error(f"Error in quality extraction summary: {e}")

    def get_quality_priority_list(self):
        """Get the quality priority list for reference"""
        return [
            {'quality': '4K/2160p', 'priority': 1},
            {'quality': '1440p', 'priority': 2},
            {'quality': '1080p', 'priority': 3},
            {'quality': '720p', 'priority': 4},
            {'quality': '480p', 'priority': 5},
            {'quality': '360p', 'priority': 6},
            {'quality': '240p', 'priority': 7},
        ]

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
        """Extract duration, views, and upload date from info row elements - FIXED with correct selectors"""
        try:
            # Extract duration using the specific XPath
            if not video_info.get("duration"):
                duration = self.extract_duration()
                if duration:
                    video_info["duration"] = duration

            # Extract views from the info row
            try:
                # Look for views in the second div of info row
                views_element = self.driver.find_element(By.XPATH, '//*[@id="tab_video_info"]/div[1]/div[2]/span')
                views_text = views_element.text.strip()
                if views_text:
                    views_num = self.extract_views_from_crawl4ai(views_text)
                    if views_num is not None:
                        video_info["views"] = str(views_num)
                        self.logger.info(f"Views extracted: {video_info['views']}")
            except NoSuchElementException:
                # Fallback: try to find any span with view-like text
                try:
                    info_spans = self.driver.find_elements(By.XPATH, '//*[@id="tab_video_info"]/div[1]//span')
                    for span in info_spans:
                        span_text = span.text.strip()
                        if span_text and any(keyword in span_text.lower() for keyword in ['view', 'k', 'm', '(']):
                            views_num = self.extract_views_from_crawl4ai(span_text)
                            if views_num is not None and views_num > 0:
                                video_info["views"] = str(views_num)
                                self.logger.info(f"Views extracted (fallback): {video_info['views']}")
                                break
                except:
                    pass

            # Extract upload date from the first div of info row
            try:
                upload_date_element = self.driver.find_element(By.XPATH, '//*[@id="tab_video_info"]/div[1]/div[1]/span')
                upload_date_text = upload_date_element.text.strip()
                if upload_date_text:
                    video_info["upload_date"] = upload_date_text
                    epoch = self.date_parser.parse_upload_date_to_epoch(upload_date_text)
                    if epoch:
                        video_info["upload_date"] = int(epoch)
                    self.logger.info(f"Upload date extracted: '{video_info['upload_date']}'")
            except NoSuchElementException:
                self.logger.warning("Could not extract upload date")

        except Exception as e:
            self.logger.error(f"Critical error in extract_item_info_data: {e}")

    def set_default_values(self, video_info):
        """Ensure all required fields have valid defaults - UPDATED to prioritize uploaded_by"""
        if not video_info.get("title"):
            video_info["title"] = f"Video_{video_info.get('video_id', '')}"

        if not video_info.get("duration") or video_info.get("duration") in ["00:00", "0:00"]:
            # Try to get actual duration from video element one more time
            actual_duration = self.extract_duration()
            video_info["duration"] = actual_duration if actual_duration else "00:30"

        if not video_info.get("views"):
            video_info["views"] = "0"

        # Prioritize uploaded_by over uploader
        if not video_info.get("uploaded_by"):
            video_info["uploaded_by"] = video_info.get("uploader", "Anonymous")
        
        # Set uploader to match uploaded_by for backward compatibility
        video_info["uploader"] = video_info.get("uploaded_by", "Anonymous")

        # Set default for description
        if not video_info.get("description"):
            video_info["description"] = "No description available"

        # Set default for categories
        if not video_info.get("categories") or (isinstance(video_info.get("categories"), list) and not video_info["categories"]):
            video_info["categories"] = ["uncategorized"]

        # Set default for artists
        if not video_info.get("artists") or (isinstance(video_info.get("artists"), list) and not video_info["artists"]):
            video_info["artists"] = ["unknown_artist"]

        # Use integer timestamp for upload_date
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

        # Remove crawl4ai data fields
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
        """Get video title from page elements using the correct selector"""
        try:
            # Using the provided XPath: /html/body/div/div[2]/div[2]/div[2]/div/div/div[1]/div[1]/h1
            title_element = self.driver.find_element(By.XPATH, "//body/div/div[2]/div[2]/div[2]/div/div/div[1]/div[1]/h1")
            title = title_element.text.strip()
            self.logger.info(f"Title extracted: {title}")
            return title
        except NoSuchElementException:
            try:
                # Alternative selector using the CSS path provided
                title_element = self.driver.find_element(By.CSS_SELECTOR, "body > div > div.wrapper > div.main > div.container > div > div > div:nth-child(1) > div.heading > h1")
                title = title_element.text.strip()
                self.logger.info(f"Title extracted (CSS): {title}")
                return title
            except NoSuchElementException:
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
        """Check if video info has all essential fields populated - UPDATED"""
        essential_fields = ["title", "uploader"]  # Reduced requirements
        
        for field in essential_fields:
            if not video_info.get(field):
                return False

        # Only check that arrays exist, not that they're populated (since defaults are acceptable)
        for array_field in ["categories", "artists", "tags"]:
            if not isinstance(video_info.get(array_field), list):
                return False

        return True

    def extract_uploaded_by(self):
        """
        Robust extraction of the 'Uploaded by' value.
        - Primary: find the column that has a label 'Uploaded by' and read its anchor/text.
        - Fallbacks: sibling anchor, any nearby .item.btn_link, page-source regex for 'Uploaded by'.
        Returns: string (e.g. 'Anonymous' or 'Unknown' if not found)
        """
        try:
            # 1) Primary XPath: the column whose .label text is 'Uploaded by'
            uploaded_by_col_xpath = "//div[@id='tab_video_info']//div[@class='col'][.//div[contains(@class,'label') and normalize-space(text())='Uploaded by']]"
            try:
                uploaded_by_col = self.driver.find_element(By.XPATH, uploaded_by_col_xpath)
            except NoSuchElementException:
                # Try more generic search (some pages vary slightly)
                uploaded_by_col = None

            if uploaded_by_col:
                # prefer anchor with btn_link
                try:
                    # anchor may contain span or direct text
                    uploaded_by_link = uploaded_by_col.find_element(By.XPATH, ".//a[contains(@class,'item') or contains(@class,'btn_link')][1]")
                    uploaded_by = uploaded_by_link.text.strip()
                    if uploaded_by:
                        self.logger.info(f"Uploaded by extracted (col -> a): {uploaded_by}")
                        return uploaded_by
                except NoSuchElementException:
                    # maybe text node directly after label
                    try:
                        # sibling text (direct text node following the label div)
                        text_node = uploaded_by_col.find_element(By.XPATH, ".//div[not(contains(@class,'label'))]")
                        txt = text_node.text.strip()
                        if txt:
                            self.logger.info(f"Uploaded by extracted (col -> div text): {txt}")
                            return txt
                    except Exception:
                        pass

            # 2) Generic: look for any label-like element containing 'Uploaded by' and grab following link/text
            try:
                label_el = self.driver.find_element(By.XPATH, "//div[@id='tab_video_info']//div[contains(@class,'label') and contains(normalize-space(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')),'uploaded by')]")
                # following-sibling anchor
                try:
                    uploader_anchor = label_el.find_element(By.XPATH, "following-sibling::a[contains(@class,'item') or contains(@class,'btn_link')][1]")
                    uploader_text = uploader_anchor.text.strip()
                    if uploader_text:
                        self.logger.info(f"Uploaded by extracted (label -> following a): {uploader_text}")
                        return uploader_text
                except NoSuchElementException:
                    # maybe within the same parent
                    try:
                        parent = label_el.find_element(By.XPATH, "..")
                        candidate = parent.find_element(By.XPATH, ".//a[contains(@class,'item') or contains(@class,'btn_link')][1]")
                        candidate_txt = candidate.text.strip()
                        if candidate_txt:
                            self.logger.info(f"Uploaded by extracted (label parent -> a): {candidate_txt}")
                            return candidate_txt
                    except Exception:
                        pass
            except NoSuchElementException:
                pass

            # 3) Fallback: any anchor under tab_video_info which visually matches the "Uploaded by" area
            try:
                anchors = self.driver.find_elements(By.XPATH, "//*[@id='tab_video_info']//a[contains(@class,'item') or contains(@class,'btn_link')]")
                # heuristics: pick the anchor that follows a small set of known labels or comes after Artist/Categories block
                for a in anchors:
                    txt = a.text.strip()
                    if not txt:
                        continue
                    # ignore tags like 'Suggest' etc
                    if any(ignore in txt.lower() for ignore in ['suggest', 'mp4', 'download']):
                        continue
                    # If it's the only author/uploader-like anchor or looks like a username, accept it
                    if len(txt) <= 60:  # small heuristic
                        self.logger.info(f"Uploaded by extracted (generic anchors): {txt}")
                        return txt
            except Exception:
                pass

            # 4) Final fallback: try page source regex (covers pages where structure is odd)
            try:
                page = self.driver.page_source
                m = re.search(r'Uploaded by[\s:\n]*<[^>]*>([^<]+)</', page, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if val:
                        self.logger.info(f"Uploaded by extracted (page-source regex): {val}")
                        return val
            except Exception:
                pass

            self.logger.warning("Could not extract uploaded_by, returning 'Unknown'")
            return "Unknown"

        except Exception as e:
            self.logger.error(f"Error extracting uploaded_by: {e}")
            return "Unknown"


    def extract_categories(self):
        """Extract categories using the correct structure from the HTML"""
        try:
            categories = []
            # Look for the col div that contains "Categories" label, then get all the spans
            category_col_xpath = "//div[@class='col'][.//div[@class='label' and text()='Categories']]"
            category_col = self.driver.find_element(By.XPATH, category_col_xpath)
            
            # Get all the anchor elements within this column
            category_links = category_col.find_elements(By.XPATH, ".//a[contains(@class, 'item btn_link')]")
            
            for link in category_links:
                try:
                    # Extract text from the span element
                    category_span = link.find_element(By.XPATH, ".//span")
                    category_text = category_span.text.strip()
                    if category_text:
                        categories.append(category_text)
                        self.logger.debug(f"Found category: {category_text}")
                except NoSuchElementException:
                    # Fallback to direct text extraction
                    category_text = link.text.strip()
                    if category_text:
                        categories.append(category_text)
                        self.logger.debug(f"Found category (fallback): {category_text}")

            result_categories = categories if categories else ["uncategorized"]
            self.logger.info(f"Categories extracted: {len(result_categories)} categories - {result_categories}")
            return result_categories

        except NoSuchElementException:
            self.logger.warning("Could not find categories column, using default")
            return ["uncategorized"]
        except Exception as e:
            self.logger.error(f"Error extracting categories: {e}")
            return ["uncategorized"]

    def extract_artists(self):
        """Extract artists using the correct structure from the HTML"""
        try:
            artists = []
            # Look for the col div that contains "Artist" label, then get all the spans with class "name"
            artist_col_xpath = "//div[@class='col'][.//div[@class='label' and text()='Artist']]"
            artist_col = self.driver.find_element(By.XPATH, artist_col_xpath)
            
            # Get all the anchor elements within this column
            artist_links = artist_col.find_elements(By.XPATH, ".//a[contains(@class, 'item btn_link')]")
            
            for link in artist_links:
                try:
                    # Extract text from span with class "name"
                    artist_span = link.find_element(By.XPATH, ".//span[@class='name']")
                    artist_text = artist_span.text.strip()
                    if artist_text:
                        artists.append(artist_text)
                        self.logger.debug(f"Found artist: {artist_text}")
                except NoSuchElementException:
                    # Fallback: try any span
                    try:
                        artist_span = link.find_element(By.XPATH, ".//span")
                        artist_text = artist_span.text.strip()
                        if artist_text:
                            artists.append(artist_text)
                            self.logger.debug(f"Found artist (span fallback): {artist_text}")
                    except NoSuchElementException:
                        # Final fallback to direct text extraction
                        artist_text = link.text.strip()
                        if artist_text:
                            artists.append(artist_text)
                            self.logger.debug(f"Found artist (text fallback): {artist_text}")

            result_artists = artists if artists else ["unknown_artist"]
            self.logger.info(f"Artists extracted: {len(result_artists)} artists - {result_artists}")
            return result_artists

        except NoSuchElementException:
            self.logger.warning("Could not find artists column, using default")
            return ["unknown_artist"]
        except Exception as e:
            self.logger.error(f"Error extracting artists: {e}")
            return ["unknown_artist"]

    def extract_uploaded_by(self):
        """Extract uploaded by using the correct structure from the HTML"""
        try:
            # Look for the col div that contains "Uploaded by" label, then get the anchor text
            uploaded_by_col_xpath = "//div[@class='col'][.//div[@class='label' and text()='Uploaded by']]"
            uploaded_by_col = self.driver.find_element(By.XPATH, uploaded_by_col_xpath)
            
            # Get the anchor element within this column
            uploaded_by_link = uploaded_by_col.find_element(By.XPATH, ".//a[contains(@class, 'item btn_link')]")
            
            # Extract the text content (it might be direct text, not in a span)
            uploaded_by = uploaded_by_link.text.strip()
            if uploaded_by:
                self.logger.info(f"Uploaded by extracted: {uploaded_by}")
                return uploaded_by
            else:
                # Fallback to existing method
                return self.extract_uploader()

        except NoSuchElementException:
            self.logger.warning("Could not find uploaded by column, trying fallback method")
            # Fallback to existing method
            try:
                return self.extract_uploader()
            except:
                self.logger.warning("Could not extract uploaded_by, using default")
                return "Unknown"
        except Exception as e:
            self.logger.error(f"Error extracting uploaded_by: {e}")
            return "Unknown"

    def extract_description(self):
        """
        Robust extraction of the video description / metadata block.
        Strategy:
        1) look for <div id="tab_video_info"> and within it check description-like blocks:
            - <p> tags
            - the second .info row (often used for longer text)
            - any div.wrap children that are not label elements
        2) Filter out UI lines/labels (categories, tags, download, mp4, views, uploaded by)
        3) Fallback: use page_source regex to capture lines like 'Patreon:', 'Author:', 'Source:'.
        Returns a cleaned description string or 'No description available'.
        """
        try:
            desc_text = ""

            try:
                root = self.driver.find_element(By.ID, "tab_video_info")
            except NoSuchElementException:
                root = None

            candidates = []

            if root:
                # 1) paragraphs
                try:
                    p_elems = root.find_elements(By.XPATH, ".//p")
                    candidates.extend([p.text.strip() for p in p_elems if p.text and len(p.text.strip()) > 10])
                except Exception:
                    pass

                # 2) info rows beyond the first (first info row usually has date/views/duration)
                try:
                    info_rows = root.find_elements(By.XPATH, ".//div[contains(@class,'info')]")
                    if len(info_rows) >= 2:
                        # take the text from subsequent info rows (join them)
                        for info in info_rows[1:]:
                            txt = info.text.strip()
                            if txt and len(txt) > 20:
                                candidates.append(txt)
                except Exception:
                    pass

                # 3) any wrap block (often holds the description) excluding small label elements
                try:
                    wrap_divs = root.find_elements(By.XPATH, ".//div[contains(@class,'wrap')]//div[not(contains(@class,'label'))]")
                    for w in wrap_divs:
                        txt = w.text.strip()
                        if txt and len(txt) > 15:
                            candidates.append(txt)
                except Exception:
                    pass

                # 4) direct sibling text nodes that are not labels: iterate root children
                try:
                    children = root.find_elements(By.XPATH, "./*")
                    for c in children:
                        try:
                            ctext = c.text.strip()
                            if ctext and len(ctext) > 20 and not any(skip in ctext.lower() for skip in ['categories', 'tags', 'download', 'mp4', 'uploaded by', 'views', 'duration', 'artist']):
                                candidates.append(ctext)
                        except Exception:
                            continue
                except Exception:
                    pass

            # Filter candidates: remove UI-ish lines and keep descriptive lines
            filtered = []
            for cand in candidates:
                # split into smaller lines and keep ones that look like real descriptions (avoid single-label lines)
                for line in cand.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    low = line.lower()
                    if len(line) < 12:
                        continue
                    if any(skip in low for skip in ['suggest', '+ |', 'mp4', 'download', 'views', 'added', 'uploaded by', 'duration', 'tags', 'categories']):
                        continue
                    filtered.append(line)

            if filtered:
                # Join unique lines preserving order
                seen = set()
                parts = []
                for s in filtered:
                    if s not in seen:
                        parts.append(s)
                        seen.add(s)
                desc_text = " ".join(parts).strip()

            # Fallback: page source regex for common metadata lines (Patreon, Author, Source)
            if not desc_text:
                try:
                    page = self.driver.page_source
                    parts = []
                    for key in ['Patreon', 'Author', 'Source', 'Source:']:
                        m = re.search(rf'({key}\s*[:\-]?\s*(?:https?://\S+|\S[^\n<]+))', page, re.IGNORECASE)
                        if m:
                            v = m.group(1).strip()
                            # clean html tags if any
                            v = re.sub(r'<[^>]+>', '', v).strip()
                            if v and v not in parts:
                                parts.append(v)
                    # Also try to capture lines like "Author: Name"
                    m_all = re.findall(r'(Patreon:\s*\S+|Author:\s*[^<\n]+|Source:\s*[^<\n]+)', page, re.IGNORECASE)
                    for mline in m_all:
                        cleaned = re.sub(r'<[^>]+>', '', mline).strip()
                        if cleaned and cleaned not in parts:
                            parts.append(cleaned)
                    if parts:
                        desc_text = " ".join(parts).strip()
                except Exception:
                    pass

            if not desc_text:
                self.logger.info("No substantive description found; returning default text.")
                return "No description available"

            # Final cleanup: reduce whitespace
            desc_text = re.sub(r'\s{2,}', ' ', desc_text).strip()
            self.logger.info(f"Description extracted: {desc_text[:200]}{'...' if len(desc_text)>200 else ''}")
            return desc_text

        except Exception as e:
            self.logger.error(f"Error extracting description: {e}")
            return "No description available"

          
    def supplement_with_selenium(self, video_info, video_url):
        """Use Selenium to fill in missing information - UPDATED with correct selectors"""
        try:
            # Navigate to video page if needed
            if not self.driver_manager.navigate_to_page(video_url):
                return

            self.logger.info(f"Starting Selenium supplementation for video {video_info.get('video_id', 'unknown')}")

            # Extract missing fields using corrected methods
            if not video_info.get("title"):
                self.logger.debug("Extracting title...")
                video_info["title"] = self.extract_title()

            if not video_info.get("duration") or not video_info.get("views") or not video_info.get("upload_date"):
                self.logger.debug("Extracting item info data...")
                self.extract_item_info_data(video_info)

            # Remove uploader field and use uploaded_by instead
            if not video_info.get("uploaded_by"):
                self.logger.debug("Extracting uploaded_by...")
                uploaded_by = self.extract_uploaded_by()
                video_info["uploaded_by"] = uploaded_by
                # Set uploader to same value for backward compatibility, but prioritize uploaded_by
                video_info["uploader"] = uploaded_by

            # Extract description
            if not video_info.get("description") or video_info["description"] == "No description available":
                self.logger.debug("Extracting description...")
                video_info["description"] = self.extract_description()

            # Extract categories (keep existing method)
            if not video_info.get("categories") or video_info["categories"] == ["uncategorized"]:
                self.logger.debug("Extracting categories...")
                video_info["categories"] = self.extract_categories()

            # Extract artists (keep existing method)
            if not video_info.get("artists") or video_info["artists"] == ["unknown_artist"]:
                self.logger.debug("Extracting artists...")
                video_info["artists"] = self.extract_artists()

            if not video_info.get("tags") or video_info["tags"] == ["untagged"]:
                self.logger.debug("Extracting tags...")
                video_info["tags"] = self.extract_tags()

            if not video_info.get("thumbnail_src"):
                self.logger.debug("Extracting thumbnail...")
                video_info["thumbnail_src"] = self.extract_thumbnail_url()

            # Extract video source with quality selection
            if not video_info.get("video_src"):
                self.logger.debug("Extracting video source with quality selection...")
                video_info["video_src"] = self.extract_video_source()

            self.logger.info(f"Completed Selenium supplementation for video {video_info.get('video_id', 'unknown')}")

        except Exception as e:
            self.logger.error(f"Error in Selenium supplementation: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

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
        """Get video duration using the correct XPath selector"""
        try:
            # Using the provided XPath: //*[@id="tab_video_info"]/div[1]/div[3]/span
            duration_element = self.driver.find_element(By.XPATH, '//*[@id="tab_video_info"]/div[1]/div[3]/span')
            duration = duration_element.text.strip()
            self.logger.info(f"Duration extracted: {duration}")
            return duration
        except NoSuchElementException:
            try:
                # Alternative using CSS selector
                duration_element = self.driver.find_element(By.CSS_SELECTOR, "#tab_video_info > div.info.row > div:nth-child(3) > span")
                duration = duration_element.text.strip()
                self.logger.info(f"Duration extracted (CSS): {duration}")
                return duration
            except NoSuchElementException:
                self.logger.warning("Could not extract duration")
                return ""

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
        """Log summary of extracted video information - UPDATED with quality info"""
        video_id = video_info["video_id"]
        self.logger.info(f"Extracted info for {video_id}:")
        self.logger.info(f" Title: '{video_info['title'][:50]}...'")
        self.logger.info(f" Duration: {video_info['duration']}")
        self.logger.info(f" Views: {video_info['views']}")
        self.logger.info(f" Uploader: {video_info['uploader']}")
        self.logger.info(f" Uploaded by: {video_info['uploaded_by']}")
        self.logger.info(f" Upload date: {video_info['upload_date']}")
        self.logger.info(f" Description: {video_info['description'][:50]}...")
        self.logger.info(f" Categories: {len(video_info['categories'])} categories - {video_info['categories']}")
        self.logger.info(f" Artists: {len(video_info['artists'])} artists - {video_info['artists']}")
        self.logger.info(f" Tags: {len(video_info['tags'])} tags")
        
        # NEW: Log video source with quality information
        if video_info.get('video_src'):
            self.logger.info(f" Video source: Found")
            
            # Try to determine quality from URL
            video_url = video_info['video_src']
            if '1080p' in video_url:
                self.logger.info(f" Video quality: 1080p (from URL)")
            elif '720p' in video_url:
                self.logger.info(f" Video quality: 720p (from URL)")
            elif '480p' in video_url:
                self.logger.info(f" Video quality: 480p (from URL)")
            elif '360p' in video_url:
                self.logger.info(f" Video quality: 360p (from URL)")
            elif '2160p' in video_url or '4k' in video_url.lower():
                self.logger.info(f" Video quality: 4K/2160p (from URL)")
            else:
                self.logger.info(f" Video quality: Unknown")
            
            self.logger.info(f" Video URL: {video_url[:100]}...")
        else:
            self.logger.info(f" Video source: None")
        
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

    def merge_crawl4ai_data(self, video_info, crawl4ai_result):
        """
        UPDATED METHOD: Merge Crawl4AI extraction results into video_info structure
        Now includes the new fields
        """
        try:
            if not crawl4ai_result:
                self.logger.debug("No Crawl4AI result to merge")
                return

            # If crawl4ai_result is already a dict with the extracted data, use it directly
            if isinstance(crawl4ai_result, dict):
                detail_data = crawl4ai_result
            else:
                detail_data = {}

            # Merge detail data into video_info
            if detail_data:
                # Extract uploader
                if detail_data.get("uploaded_by") and not video_info.get("uploader"):
                    video_info["uploader"] = detail_data["uploaded_by"].strip()
                    video_info["uploaded_by"] = detail_data["uploaded_by"].strip()  # NEW

                # Extract tags
                if detail_data.get("tags") and not video_info.get("tags"):
                    tags = self.parse_tags_from_text(detail_data["tags"])
                    if tags:
                        video_info["tags"] = tags

                # Extract video source
                if detail_data.get("video_source") and not video_info.get("video_src"):
                    video_src = detail_data["video_source"]
                    if not video_src.startswith('http'):
                        video_src = urljoin(self.base_url, video_src)
                    video_info["video_src"] = video_src

                # Extract poster/thumbnail from video element
                if detail_data.get("video_poster") and not video_info.get("thumbnail_src"):
                    poster = detail_data["video_poster"]
                    if not poster.startswith('http'):
                        poster = urljoin(self.base_url, poster)
                    video_info["thumbnail_src"] = poster

                # Extract views from Crawl4AI data
                if detail_data.get("views") and not video_info.get("views"):
                    views_num = self.extract_views_from_crawl4ai(detail_data["views"])
                    if views_num is not None:
                        video_info["views"] = str(views_num)

                # Parse info details for additional metadata
                if detail_data.get("info_details"):
                    self.parse_info_details_text(video_info, detail_data["info_details"])

            self.logger.debug(f"Merged Crawl4AI data for video {video_info.get('video_id', 'unknown')}")

        except Exception as e:
            self.logger.error(f"Error merging Crawl4AI data: {e}")

    def extract_video_source_with_quality_selection(self):
        """Get the best quality video URL from download section"""
        try:
            self.logger.info("Extracting video source with quality selection...")
            
            # Look for the download section
            download_section_xpath = "//div[@class='row row_spacer']//div[@class='wrap'][.//div[@class='label' and text()='Download']]"
            download_section = self.driver.find_element(By.XPATH, download_section_xpath)
            self.logger.debug("Found download section")
            
            # Get all download links
            download_links = download_section.find_elements(By.XPATH, ".//a[@class='tag_item']")
            self.logger.info(f"Found {len(download_links)} download options")
            
            if not download_links:
                self.logger.warning("No download links found in download section")
                return self.extract_video_source_fallback()
            
            # Extract all qualities with their URLs
            quality_options = []
            for link in download_links:
                try:
                    link_text = link.text.strip()
                    link_url = link.get_attribute('href')
                    
                    if link_url and link_text:
                        quality_info = self.parse_quality_from_text(link_text)
                        if quality_info:
                            quality_options.append({
                                'text': link_text,
                                'url': link_url,
                                'quality': quality_info['quality'],
                                'resolution': quality_info['resolution'],
                                'priority': quality_info['priority']
                            })
                            self.logger.debug(f"Found quality option: {link_text} -> {quality_info['quality']}p (priority: {quality_info['priority']})")
                
                except Exception as e:
                    self.logger.warning(f"Error processing download link: {e}")
                    continue
            
            if not quality_options:
                self.logger.warning("No valid quality options found")
                return self.extract_video_source_fallback()
            
            # Sort by priority (lower number = higher priority/better quality)
            quality_options.sort(key=lambda x: x['priority'])
            
            # Log all found qualities
            qualities_found = [f"{opt['quality']}p" for opt in quality_options]
            self.logger.info(f"Available qualities: {', '.join(qualities_found)}")
            
            # Select the best quality (first after sorting)
            best_quality = quality_options[0]
            self.logger.info(f"Selected best quality: {best_quality['text']} ({best_quality['quality']}p)")
            self.logger.info(f"Best quality URL: {best_quality['url']}")
            
            return best_quality['url']
            
        except NoSuchElementException:
            self.logger.warning("Download section not found, trying fallback methods")
            return self.extract_video_source_fallback()
        except Exception as e:
            self.logger.error(f"Error in quality selection: {e}")
            return self.extract_video_source_fallback()

    def parse_quality_from_text(self, text):
        """Parse quality information from link text like 'MP4 1080p' or '4K MP4'"""
        try:
            text_lower = text.lower()
            
            # Quality mappings with priorities (lower = better)
            quality_patterns = [
                # 4K/2160p patterns
                (r'(?:4k|2160p)', {'quality': 2160, 'resolution': '4K/2160p', 'priority': 1}),
                (r'(?:1440p)', {'quality': 1440, 'resolution': '1440p', 'priority': 2}),
                (r'(?:1080p)', {'quality': 1080, 'resolution': '1080p', 'priority': 3}),
                (r'(?:720p)', {'quality': 720, 'resolution': '720p', 'priority': 4}),
                (r'(?:480p)', {'quality': 480, 'resolution': '480p', 'priority': 5}),
                (r'(?:360p?)', {'quality': 360, 'resolution': '360p', 'priority': 6}),
                (r'(?:240p)', {'quality': 240, 'resolution': '240p', 'priority': 7}),
            ]
            
            for pattern, quality_info in quality_patterns:
                if re.search(pattern, text_lower):
                    return quality_info
            
            # If no pattern matches, try to extract numbers
            number_match = re.search(r'(\d{3,4})p?', text_lower)
            if number_match:
                quality_num = int(number_match.group(1))
                if quality_num >= 2160:
                    return {'quality': 2160, 'resolution': '4K/2160p', 'priority': 1}
                elif quality_num >= 1440:
                    return {'quality': 1440, 'resolution': '1440p', 'priority': 2}
                elif quality_num >= 1080:
                    return {'quality': 1080, 'resolution': '1080p', 'priority': 3}
                elif quality_num >= 720:
                    return {'quality': 720, 'resolution': '720p', 'priority': 4}
                elif quality_num >= 480:
                    return {'quality': 480, 'resolution': '480p', 'priority': 5}
                elif quality_num >= 360:
                    return {'quality': 360, 'resolution': '360p', 'priority': 6}
                else:
                    return {'quality': quality_num, 'resolution': f'{quality_num}p', 'priority': 8}
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error parsing quality from '{text}': {e}")
            return None

    def extract_video_source_fallback(self):
        """Fallback methods for video source extraction"""
        try:
            # Method 1: Try video player element
            self.logger.debug("Trying video player element extraction...")
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
                self.logger.info(f"Video source extracted from player: {result}")
                return result
            else:
                self.logger.warning("Video element found but no source attribute")
                
        except NoSuchElementException:
            self.logger.warning("Could not find video player element")
        
        # Method 2: Extract from page source
        try:
            self.logger.debug("Trying page source extraction...")
            page_source = self.driver.page_source
            
            # Look for highest quality MP4 first
            quality_patterns = [
                r'https?://[^\s"\'<>]+(?:2160p|4k)[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+1080p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+720p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+480p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+360p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*'
            ]
            
            for pattern in quality_patterns:
                mp4_matches = re.findall(pattern, page_source)
                if mp4_matches:
                    result = mp4_matches[0]
                    self.logger.info(f"Video source found via regex: {result}")
                    return result
            
        except Exception as e:
            self.logger.error(f"Page source extraction failed: {e}")
        
        self.logger.error("Could not find video source with any method")
        return ""

    def extract_video_source(self):
        """Updated main video source extraction method"""
        try:
            # First try the quality selection method
            quality_selected_url = self.extract_video_source_with_quality_selection()
            if quality_selected_url:
                return quality_selected_url
            
            # If that fails, use fallback methods
            self.logger.warning("Quality selection failed, using fallback methods")
            return self.extract_video_source_fallback()
            
        except Exception as e:
            self.logger.error(f"Error in extract_video_source: {e}")
            return ""

    def extract_all_download_qualities(self):
        """Extract all available download qualities for debugging/logging"""
        try:
            download_section_xpath = "//div[@class='row row_spacer']//div[@class='wrap'][.//div[@class='label' and text()='Download']]"
            download_section = self.driver.find_element(By.XPATH, download_section_xpath)
            
            download_links = download_section.find_elements(By.XPATH, ".//a[@class='tag_item']")
            
            qualities = []
            for link in download_links:
                try:
                    text = link.text.strip()
                    url = link.get_attribute('href')
                    if text and url:
                        quality_info = self.parse_quality_from_text(text)
                        if quality_info:
                            qualities.append({
                                'text': text,
                                'url': url,
                                'quality': quality_info['quality'],
                                'resolution': quality_info['resolution']
                            })
                except Exception as e:
                    continue
            
            return qualities
            
        except Exception as e:
            self.logger.error(f"Error extracting download qualities: {e}")
            return []