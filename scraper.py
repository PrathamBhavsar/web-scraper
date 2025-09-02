import os
import json
import time
import requests
import logging
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urljoin, urlparse
import re

class Rule34VideoScraper:
    def __init__(self):
        self.base_url = "https://rule34video.com"
        self.config = self.load_config()
        self.setup_logging()
        self.progress = self.load_progress()
        self.driver = None
        
    def load_config(self):
        """Load configuration from config.json"""
        config_path = Path("config.json")
        if not config_path.exists():
            self.create_default_config()
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def create_default_config(self):
        """Create default configuration file"""
        default_config = {
            "general": {
                "download_path": "C:\\scraper_downloads\\",
                "max_storage_gb": 100,
                "parallel_downloads": 3,
                "delay_between_requests": 2000,
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            "download": {
                "download_method": "hybrid",
                "idm_path": "C:\\Program Files (x86)\\Internet Download Manager\\idman.exe",
                "max_retries": 3,
                "timeout_seconds": 30,
                "chunk_size": 8192,
                "verify_downloads": True
            },
            "scraping": {
                "start_from_last_page": True,
                "pages_per_batch": 10,
                "wait_time_ms": 1500,
                "max_concurrent_pages": 5,
                "skip_existing_files": True
            },
            "storage": {
                "create_subdirectories": True,
                "compress_json": False,
                "backup_progress": True,
                "cleanup_incomplete": True
            },
            "logging": {
                "log_level": "INFO",
                "log_to_file": True,
                "log_file_path": "scraper.log",
                "max_log_size_mb": 50
            }
        }
        
        with open("config.json", 'w') as f:
            json.dump(default_config, f, indent=2)
        
        return default_config
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_level = getattr(logging, self.config["logging"]["log_level"].upper())
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Setup logger
        self.logger = logging.getLogger('Rule34Scraper')
        self.logger.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler if enabled
        if self.config["logging"]["log_to_file"]:
            file_handler = logging.FileHandler(self.config["logging"]["log_file_path"])
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def load_progress(self):
        """Load scraping progress"""
        progress_path = Path("progress.json")
        if progress_path.exists():
            try:
                with open(progress_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                self.logger.warning("Corrupted progress file, creating new one")
        return {
            "last_video_id": None,
            "last_page": None,
            "total_downloaded": 0,
            "total_size_mb": 0,
            "downloaded_videos": []
        }
    
    def save_progress(self):
        """Save current progress"""
        with open("progress.json", 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def setup_driver(self):
        """Setup Selenium WebDriver"""
        chrome_options = Options()
        # Don't run headless initially to see what's happening
        # chrome_options.add_argument("--headless")  # Comment out for debugging
        chrome_options.add_argument(f"--user-agent={self.config['general']['user_agent']}")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        
        # Add some preferences to handle downloads and popups
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2
            }
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)
        self.driver.maximize_window()  # Maximize for better element visibility
        self.logger.info("WebDriver initialized successfully")
    
    def handle_age_verification(self):
        """Handle age verification if present"""
        try:
            # Check if age verification is present
            if "age" in self.driver.current_url.lower() or "verify" in self.driver.page_source.lower():
                # Look for age verification button/link
                age_buttons = self.driver.find_elements(By.XPATH, "//a[contains(text(), '18') or contains(text(), 'Enter') or contains(text(), 'Yes')]")
                if age_buttons:
                    age_buttons[0].click()
                    time.sleep(3)
                    self.logger.info("Handled age verification")
        except Exception as e:
            self.logger.warning(f"Age verification handling failed: {e}")

    def get_last_page_number(self):
        """Navigate to the site and find the last page number"""
        try:
            self.driver.get(self.base_url)
            time.sleep(3)
            
            # Handle age verification
            self.handle_age_verification()
            
            # Navigate to a page to see pagination structure
            self.driver.get(f"{self.base_url}/newest/")
            time.sleep(3)
            
            # Look for pagination elements - try multiple selectors
            pagination_selectors = [
                ".pagination",
                ".pages", 
                ".page-navigation",
                "[class*='page']",
                "nav"
            ]
            
            pagination = None
            for selector in pagination_selectors:
                try:
                    pagination = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except TimeoutException:
                    continue
            
            if not pagination:
                self.logger.warning("No pagination found, defaulting to page 1")
                return 1
            
            # Find the last page link
            page_links = pagination.find_elements(By.TAG_NAME, "a")
            last_page = 1
            
            for link in page_links:
                try:
                    text = link.text.strip()
                    if text.isdigit():
                        page_num = int(text)
                        if page_num > last_page:
                            last_page = page_num
                    elif "last" in text.lower():
                        href = link.get_attribute("href")
                        if href and "page=" in href:
                            page_match = re.search(r'page=(\d+)', href)
                            if page_match:
                                last_page = int(page_match.group(1))
                except ValueError:
                    continue
            
            self.logger.info(f"Found last page: {last_page}")
            return last_page
            
        except Exception as e:
            self.logger.error(f"Error finding last page: {e}")
            return 1
    
    def get_video_links_from_page(self, page_num):
        """Get all video links from a specific page using exact XPaths"""
        try:
            # For first page, use the main URL
            if page_num == 1:
                page_url = f"{self.base_url}/"
            else:
                page_url = f"{self.base_url}/?page={page_num}"
            
            self.logger.info(f"Loading page: {page_url}")
            self.driver.get(page_url)
            time.sleep(self.config["scraping"]["wait_time_ms"] / 1000)
            
            # Handle age verification if needed
            self.handle_age_verification()
            
            # Use exact XPath to find video cards
            video_cards = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'video_')]"))
            )
            
            self.logger.info(f"Found {len(video_cards)} video cards on page {page_num}")
            
            video_links = []
            for i, card in enumerate(video_cards):
                try:
                    # Extract video link using exact XPath
                    link_element = card.find_element(By.XPATH, ".//a[contains(@class, 'js-open-popup')]")
                    video_url = link_element.get_attribute("href")
                    
                    if video_url and video_url not in video_links:
                        video_links.append(video_url)
                        self.logger.info(f"Video {i+1}: {video_url}")
                        
                except NoSuchElementException:
                    self.logger.warning(f"Could not find link in video card {i+1}")
                    continue
                except Exception as e:
                    self.logger.warning(f"Error processing video card {i+1}: {e}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(video_links)} video links from page {page_num}")
            return video_links
            
        except Exception as e:
            self.logger.error(f"Error getting video links from page {page_num}: {e}")
            return []
   
    def extract_video_info(self, video_url):
        """Extract video information using exact XPaths"""
        try:
            self.driver.get(video_url)
            time.sleep(3)

            self.handle_age_verification()

            # Extract video ID from URL
            video_id_match = re.search(r'/video/(\d+)/', video_url)
            if video_id_match:
                video_id = video_id_match.group(1)
            else:
                video_id = video_url.split('/')[-2] if video_url.endswith('/') else video_url.split('/')[-1]

            # Initialize video info structure
            video_info = {
                "video_id": video_id,
                "url": video_url,
                "title": "",
                "duration": "",
                "views": "",
                "uploader": "",
                "upload_date": "",
                "tags": [],
                "video_src": "",
                "thumbnail_src": ""
            }

            # ---- Helper: parse views text into integer ----
            def _parse_views_number(views_text: str) -> int:
                if not views_text:
                    return 0
                # Try to find number inside parentheses first: e.g. "24K (24,007)"
                paren_match = re.search(r'\(([\d,]+)\)', views_text)
                if paren_match:
                    num = paren_match.group(1).replace(',', '')
                    try:
                        return int(num)
                    except:
                        pass
                # Otherwise try to parse possible K/M/B suffixes or plain numbers (like "24K" or "1.2M")
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
                    # no suffix
                    return int(value)
                # Final fallback: extract digits
                digits = re.sub(r'\D', '', views_text)
                return int(digits) if digits else 0

            # Extract title using exact XPath
            try:
                title_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'thumb_title')]")
                video_info["title"] = title_element.text.strip()
                self.logger.info(f"Title extracted: {video_info['title']}")
            except NoSuchElementException:
                try:
                    title_element = self.driver.find_element(By.TAG_NAME, "h1")
                    video_info["title"] = title_element.text.strip()
                except:
                    video_info["title"] = f"Video_{video_id}"
                    self.logger.warning(f"Could not extract title, using default: {video_info['title']}")

            # Extract duration using exact XPath (legacy fallback kept)
            try:
                duration_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'time')]")
                if duration_element and duration_element.text.strip():
                    video_info["duration"] = duration_element.text.strip()
                    self.logger.info(f"Duration extracted (fallback): {video_info['duration']}")
            except NoSuchElementException:
                self.logger.debug("Could not find duration element using fallback XPath")

            # Extract views using exact XPath (legacy fallback kept)
            try:
                views_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'views')]")
                views_text = views_element.text.strip()
                views_number = None
                views_match = re.search(r'([\d,\.]+[KMBkmb]?)', views_text)
                if views_match:
                    views_number = _parse_views_number(views_text)
                else:
                    views_number = _parse_views_number(views_text)
                video_info["views"] = str(views_number)
                self.logger.info(f"Views extracted (fallback): {video_info['views']}")
            except NoSuchElementException:
                self.logger.debug("Could not find views element using fallback XPath")

            # Extract uploader (unchanged behavior but slightly more robust)
            try:
                # Find div with class 'label' that contains text 'Uploaded by' (case-insensitive)
                label_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class,'label')]")
                uploader_found = False
                for lbl in label_elements:
                    try:
                        lbl_text = lbl.text.strip()
                    except:
                        lbl_text = ""
                    if lbl_text and 'uploaded by' in lbl_text.lower():
                        # find following a.btn_link sibling (or descendant)
                        try:
                            uploader_element = lbl.find_element(By.XPATH, ".//following-sibling::a[contains(@class,'btn_link')][1]")
                        except NoSuchElementException:
                            try:
                                uploader_element = lbl.find_element(By.XPATH, ".//a[contains(@class,'btn_link')][1]")
                            except NoSuchElementException:
                                uploader_element = None
                        if uploader_element:
                            video_info["uploader"] = uploader_element.text.strip()
                            uploader_found = True
                            self.logger.info(f"Uploader extracted: {video_info['uploader']}")
                            break
                if not uploader_found:
                    self.logger.debug("Uploader label not found via label scan")
            except Exception:
                self.logger.warning("Error while extracting uploader (non-fatal)")

            # ---------- NEW: Extract the three item_info blocks (upload_date, views, duration) ----------
            try:
                item_info_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'item_info')]")
                # iterate through them and detect by svg class
                for item in item_info_elements:
                    try:
                        # Use the whole text of the item_info block as a starting point
                        item_text = item.text.strip()
                    except:
                        item_text = ""

                    # Try detection by presence of specific svg classes inside this item_info
                    is_calender = False
                    is_eye = False
                    is_time = False
                    try:
                        # check for svg presence; presence is enough to classify
                        if item.find_elements(By.XPATH, ".//svg[contains(@class,'custom-calender')]"):
                            is_calender = True
                        if item.find_elements(By.XPATH, ".//svg[contains(@class,'custom-eye')]"):
                            is_eye = True
                        if item.find_elements(By.XPATH, ".//svg[contains(@class,'custom-time')]"):
                            is_time = True
                    except Exception:
                        # fallback: try by aria-label or title on svg
                        try:
                            svg_elements = item.find_elements(By.TAG_NAME, "svg")
                            for sv in svg_elements:
                                try:
                                    cls = sv.get_attribute("class") or ""
                                    if "calender" in cls:
                                        is_calender = True
                                    if "eye" in cls:
                                        is_eye = True
                                    if "time" in cls:
                                        is_time = True
                                except:
                                    continue
                        except:
                            pass

                    # Now assign based on detection and cleaned text
                    if is_calender and not video_info.get("upload_date"):
                        # item_text likely like "1 week ago" or similar
                        if item_text:
                            # remove any label words and just keep the time text
                            video_info["upload_date"] = item_text
                        else:
                            # fallback: try sibling text node
                            try:
                                sibling = item.find_element(By.XPATH, ".//*[normalize-space(text())]")
                                video_info["upload_date"] = sibling.text.strip()
                            except:
                                pass
                        self.logger.info(f"Upload date extracted: {video_info['upload_date']}")

                    elif is_eye and not video_info.get("views"):
                        # item_text may be "24K (24,007)" or "24K" etc.
                        if item_text:
                            views_num = _parse_views_number(item_text)
                            video_info["views"] = str(views_num)
                            self.logger.info(f"Views extracted from item_info: {video_info['views']}")
                        else:
                            # fallback: try inner text nodes
                            try:
                                inner_text = item.get_attribute("textContent") or ""
                                views_num = _parse_views_number(inner_text)
                                video_info["views"] = str(views_num)
                                self.logger.info(f"Views extracted (fallback inner): {video_info['views']}")
                            except:
                                pass

                    elif is_time and (not video_info.get("duration") or video_info["duration"] == ""):
                        if item_text:
                            # prefer hh:mm:ss or m:ss style string
                            # item_text might contain extra label, so extract time pattern
                            time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', item_text)
                            if time_match:
                                video_info["duration"] = time_match.group(1)
                            else:
                                video_info["duration"] = item_text
                            self.logger.info(f"Duration extracted from item_info: {video_info['duration']}")
                        else:
                            try:
                                inner_text = item.get_attribute("textContent") or ""
                                time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', inner_text)
                                if time_match:
                                    video_info["duration"] = time_match.group(1)
                                    self.logger.info(f"Duration extracted (fallback inner): {video_info['duration']}")
                            except:
                                pass
                # If any of the fields still empty, they'll be filled by other fallbacks below
            except Exception:
                self.logger.warning("Could not parse item_info blocks for upload_date/views/duration")

            # Extract tags: exclude unwanted tags (case-insensitive)
            try:
                tag_elements = self.driver.find_elements(By.XPATH, "//a[contains(@class, 'tag_item')]")
                tags = []
                ignore_tags = {
                    "+ | suggest", "+ | Suggest",  # keep both forms but we'll do lower() comparison below
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
                video_info["tags"] = tags
                self.logger.info(f"Tags extracted: {len(tags)} tags - {tags[:5]}")
            except NoSuchElementException:
                self.logger.warning("Could not find tag elements")

            # Extract thumbnail using exact XPath (lazy-load before popup)
            try:
                # choose the first lazy-load image that has a src/data-src attribute
                lazy_imgs = self.driver.find_elements(By.XPATH, "//img[contains(@class, 'lazy-load')]")
                thumb_src = None
                for img in lazy_imgs:
                    try:
                        # prefer data-src, then src, then data-lazy
                        candidate = img.get_attribute("data-src") or img.get_attribute("src") or img.get_attribute("data-lazy")
                        if candidate and candidate.strip():
                            thumb_src = candidate.strip()
                            break
                    except:
                        continue
                if thumb_src:
                    video_info["thumbnail_src"] = urljoin(self.base_url, thumb_src)
                    self.logger.info(f"Thumbnail extracted (lazy-load): {video_info['thumbnail_src']}")
                else:
                    self.logger.debug("No lazy-load thumbnail found; will fallback to other selectors")
                    # fallback to previous selector
                    try:
                        thumbnail_element = self.driver.find_element(By.XPATH, "//img[contains(@class, 'thumb')]")
                        thumbnail_src = thumbnail_element.get_attribute("src")
                        if thumbnail_src:
                            video_info["thumbnail_src"] = urljoin(self.base_url, thumbnail_src)
                            self.logger.info(f"Thumbnail extracted (fallback): {video_info['thumbnail_src']}")
                    except NoSuchElementException:
                        self.logger.warning("Could not find thumbnail element")
            except Exception:
                self.logger.warning("Error while extracting lazy-load thumbnail")

            # Extract video source using exact XPath (unchanged)
            try:
                video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")

                # Try to get source from video element
                video_src = video_element.get_attribute("src")
                if not video_src:
                    # Try source elements within video
                    source_elements = video_element.find_elements(By.TAG_NAME, "source")
                    if source_elements:
                        video_src = source_elements[0].get_attribute("src")

                if video_src:
                    video_info["video_src"] = video_src if video_src.startswith('http') else urljoin(self.base_url, video_src)
                    self.logger.info(f"Video source extracted: {video_info['video_src']}")
                else:
                    self.logger.warning("Video element found but no source attribute")

            except NoSuchElementException:
                self.logger.warning("Could not find video player element")

                # Fallback: try to find any video element or MP4 links
                try:
                    page_source = self.driver.page_source
                    mp4_urls = re.findall(r'https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*', page_source)
                    if mp4_urls:
                        video_info["video_src"] = mp4_urls[0]
                        self.logger.info(f"Video source found via regex: {video_info['video_src']}")
                except:
                    self.logger.error("Could not find video source with any method")

            # Log what we found
            self.logger.info(f"Extracted info for {video_id}:")
            self.logger.info(f"  Title: '{video_info['title'][:50]}...'")
            self.logger.info(f"  Duration: {video_info['duration']}")
            self.logger.info(f"  Views: {video_info['views']}")
            self.logger.info(f"  Uploader: {video_info['uploader']}")
            self.logger.info(f"  Upload date: {video_info['upload_date']}")
            self.logger.info(f"  Tags: {len(video_info['tags'])} tags")
            self.logger.info(f"  Video source: {'Found' if video_info['video_src'] else 'None'}")
            self.logger.info(f"  Thumbnail: {'Found' if video_info['thumbnail_src'] else 'None'}")

            return video_info

        except Exception as e:
            self.logger.error(f"Error extracting video info from {video_url}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
    def download_file(self, url, filepath):
        """Download a file with retry logic"""
        max_retries = self.config["download"]["max_retries"]
        timeout = self.config["download"]["timeout_seconds"]
        chunk_size = self.config["download"]["chunk_size"]
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, stream=True, timeout=timeout)
                response.raise_for_status()
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                
                # Verify file was downloaded
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    self.logger.info(f"Successfully downloaded: {filepath}")
                    return True
                    
            except Exception as e:
                self.logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if os.path.exists(filepath):
                    os.remove(filepath)
                time.sleep(2)
        
        return False
    
    def process_video(self, video_info):
        """Process and download a single video with proper folder structure"""
        if not video_info or not video_info.get("video_src"):
            self.logger.error("No video info or video source provided")
            return False
        
        video_id = video_info["video_id"]
        
        # Check if already downloaded
        if video_id in self.progress["downloaded_videos"]:
            self.logger.info(f"Video {video_id} already downloaded, skipping")
            return True
        
        # Create main download directory
        download_path = Path(self.config["general"]["download_path"])
        download_path.mkdir(parents=True, exist_ok=True)
        
        # Create video-specific directory
        video_dir = download_path / video_id
        
        # Download video first (temporary location)
        temp_video_path = download_path / f"{video_id}_temp.mp4"
        
        self.logger.info(f"Starting download for video {video_id}")
        self.logger.info(f"Video URL: {video_info['video_src']}")
        
        if not self.download_file(video_info["video_src"], temp_video_path):
            self.logger.error(f"Failed to download video: {video_id}")
            return False
        
        # Verify video file
        if not self.verify_video_file(temp_video_path):
            self.logger.error(f"Video verification failed: {video_id}")
            if temp_video_path.exists():
                temp_video_path.unlink()
            return False
        
        # Create video directory only after successful download and verification
        video_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Created directory: {video_dir}")
        
        # Move video to final location
        final_video_path = video_dir / f"{video_id}.mp4"
        temp_video_path.rename(final_video_path)
        self.logger.info(f"Video moved to: {final_video_path}")
        
        # Download thumbnail if available
        if video_info.get("thumbnail_src"):
            thumbnail_path = video_dir / f"{video_id}.jpg"
            if self.download_file(video_info["thumbnail_src"], thumbnail_path):
                self.logger.info(f"Thumbnail downloaded: {thumbnail_path}")
            else:
                self.logger.warning(f"Failed to download thumbnail for {video_id}")
        
        # Save metadata JSON
        json_path = video_dir / f"{video_id}.json"
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Metadata saved: {json_path}")
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {video_id}: {e}")
        
        # Calculate file size for progress tracking
        try:
            file_size_mb = final_video_path.stat().st_size / (1024 * 1024)
        except:
            file_size_mb = 0
        
        # Update progress
        self.progress["downloaded_videos"].append(video_id)
        self.progress["last_video_id"] = video_id
        self.progress["total_downloaded"] += 1
        self.progress["total_size_mb"] = self.progress.get("total_size_mb", 0) + file_size_mb
        self.save_progress()
        
        self.logger.info(f"Successfully processed video: {video_id} ({file_size_mb:.2f} MB)")
        return True
    
    def verify_video_file(self, filepath):
        """Basic verification of video file"""
        try:
            if not os.path.exists(filepath):
                return False
            
            file_size = os.path.getsize(filepath)
            if file_size < 1024:  # Less than 1KB, probably not a valid video
                return False
            
            # Check if file starts with common MP4 headers
            with open(filepath, 'rb') as f:
                header = f.read(12)
                # MP4 files typically start with specific bytes
                if b'ftyp' in header:
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying video file {filepath}: {e}")
            return False
    
    def run(self):
        """Main scraping loop - process all videos on first page"""
        try:
            self.setup_driver()
            
            # Start with page 1 for testing
            current_page = 1
            self.logger.info(f"Starting scrape from page {current_page}")
            
            # Get all video links from the page
            video_links = self.get_video_links_from_page(current_page)
            
            if not video_links:
                self.logger.error(f"No video links found on page {current_page}")
                return
            
            self.logger.info(f"Found {len(video_links)} videos to process on page {current_page}")
            
            # Process each video
            successful_downloads = 0
            failed_downloads = 0
            
            for i, video_url in enumerate(video_links, 1):
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"Processing video {i}/{len(video_links)}")
                self.logger.info(f"URL: {video_url}")
                self.logger.info(f"{'='*50}")
                
                try:
                    # Extract video information
                    video_info = self.extract_video_info(video_url)
                    
                    if not video_info:
                        self.logger.error(f"Failed to extract info for video {i}")
                        failed_downloads += 1
                        continue
                    
                    # Log extracted information
                    self.logger.info(f"Video Info Extracted:")
                    self.logger.info(f"  ID: {video_info['video_id']}")
                    self.logger.info(f"  Title: {video_info['title']}")
                    self.logger.info(f"  Duration: {video_info['duration']}")
                    self.logger.info(f"  Views: {video_info['views']}")
                    self.logger.info(f"  Tags: {len(video_info['tags'])} tags")
                    self.logger.info(f"  Has video source: {'Yes' if video_info.get('video_src') else 'No'}")
                    self.logger.info(f"  Has thumbnail: {'Yes' if video_info.get('thumbnail_src') else 'No'}")
                    
                    # Process (download) the video
                    if video_info.get("video_src"):
                        if self.process_video(video_info):
                            successful_downloads += 1
                            self.logger.info(f"✅ Successfully processed video {i}/{len(video_links)}")
                        else:
                            failed_downloads += 1
                            self.logger.error(f"❌ Failed to process video {i}/{len(video_links)}")
                    else:
                        self.logger.warning(f"⚠️ No video source found for video {i}/{len(video_links)}")
                        failed_downloads += 1
                    
                    # Small delay between videos to be respectful
                    delay_seconds = self.config["general"]["delay_between_requests"] / 1000
                    self.logger.info(f"Waiting {delay_seconds} seconds before next video...")
                    time.sleep(delay_seconds)
                    
                except Exception as e:
                    self.logger.error(f"Unexpected error processing video {i}: {e}")
                    failed_downloads += 1
                    import traceback
                    self.logger.error(traceback.format_exc())
                    continue
            
            # Update page progress
            self.progress["last_page"] = current_page
            self.save_progress()
            
            # Final summary
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"PAGE {current_page} PROCESSING COMPLETE")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Total videos found: {len(video_links)}")
            self.logger.info(f"Successfully downloaded: {successful_downloads}")
            self.logger.info(f"Failed downloads: {failed_downloads}")
            self.logger.info(f"Success rate: {(successful_downloads/len(video_links)*100):.1f}%")
            self.logger.info(f"Total downloaded so far: {self.progress['total_downloaded']}")
            
            if self.progress.get("total_size_mb"):
                self.logger.info(f"Total size downloaded: {self.progress['total_size_mb']:.2f} MB")
            
            self.logger.info(f"{'='*60}")
                
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            if self.driver:
                self.logger.info("Keeping browser open for inspection...")
                input("Press Enter to close browser...")
                self.driver.quit()
            self.logger.info("Scraper finished")

# Usage
if __name__ == "__main__":
    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)
    
    scraper = Rule34VideoScraper()
    scraper.run()