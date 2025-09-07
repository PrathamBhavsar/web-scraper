import os
import json
import time
import dateutil
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
import shutil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class VideoScrapper:
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
            },
            "validation": {
                "min_video_size_bytes": 10240,  # 10KB minimum
                "min_thumbnail_size_bytes": 1024,  # 1KB minimum
                "required_json_fields": ["video_id", "title", "duration", "views", "upload_date_epoch"],
                "max_validation_retries": 3,
                "validation_delay_seconds": 2
            }
        }
        
        with open("config.json", 'w') as f:
            json.dump(default_config, f, indent=2)
        
        return default_config
    
    def setup_logging(self):
        """Configure logging system with UTF-8 support"""
        log_level = getattr(logging, self.config["logging"]["log_level"].upper())
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Setup logger
        self.logger = logging.getLogger('Rule34Scraper')
        self.logger.setLevel(log_level)
        
        # Console handler with UTF-8 support for Windows
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Fix encoding for Windows console
        if hasattr(console_handler.stream, 'buffer'):
            import sys
            
            class UTF8ConsoleHandler(logging.StreamHandler):
                def emit(self, record):
                    try:
                        msg = self.format(record)
                        # Use ASCII-safe replacement for problematic characters
                        msg = msg.replace('→', '->')
                        self.stream.write(msg + self.terminator)
                        self.flush()
                    except Exception:
                        self.handleError(record)
            
            console_handler = UTF8ConsoleHandler()
            console_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)
        
        # File handler if enabled
        if self.config["logging"]["log_to_file"]:
            file_handler = logging.FileHandler(
                self.config["logging"]["log_file_path"],
                encoding='utf-8'  # Ensure file logging uses UTF-8
            )
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

    def parse_upload_date_to_epoch(self, upload_date_text):
        """Parse upload date text and convert to milliseconds from epoch"""
        if not upload_date_text or not upload_date_text.strip():
            return None
        
        try:
            current_time = datetime.now()
            upload_date_text = upload_date_text.strip().lower()
            
            # Handle relative dates like "5 days ago", "2 weeks ago", "3 months ago", etc.
            relative_patterns = [
                (r'(\d+)\s*(?:days?|d)\s*ago', 'days'),
                (r'(\d+)\s*(?:weeks?|w)\s*ago', 'weeks'),
                (r'(\d+)\s*(?:months?|mon)\s*ago', 'months'),
                (r'(\d+)\s*(?:years?|y)\s*ago', 'years'),
                (r'(\d+)\s*(?:hours?|h)\s*ago', 'hours'),
                (r'(\d+)\s*(?:minutes?|min|m)\s*ago', 'minutes'),
                (r'yesterday', 'yesterday'),
                (r'today', 'today'),
            ]
            
            for pattern, time_unit in relative_patterns:
                if time_unit == 'yesterday':
                    if 'yesterday' in upload_date_text:
                        upload_date = current_time - timedelta(days=1)
                        return int(upload_date.timestamp() * 1000)
                elif time_unit == 'today':
                    if 'today' in upload_date_text:
                        return int(current_time.timestamp() * 1000)
                else:
                    match = re.search(pattern, upload_date_text)
                    if match:
                        amount = int(match.group(1))
                        if time_unit == 'days':
                            upload_date = current_time - timedelta(days=amount)
                        elif time_unit == 'weeks':
                            upload_date = current_time - timedelta(weeks=amount)
                        elif time_unit == 'months':
                            upload_date = current_time - relativedelta(months=amount)
                        elif time_unit == 'years':
                            upload_date = current_time - relativedelta(years=amount)
                        elif time_unit == 'hours':
                            upload_date = current_time - timedelta(hours=amount)
                        elif time_unit == 'minutes':
                            upload_date = current_time - timedelta(minutes=amount)
                        
                        return int(upload_date.timestamp() * 1000)
            
            # Try to parse as a direct date format
            try:
                # Handle various date formats
                date_formats = [
                    "%Y-%m-%d",
                    "%m/%d/%Y",
                    "%d/%m/%Y",
                    "%B %d, %Y",
                    "%b %d, %Y",
                    "%d %B %Y",
                    "%d %b %Y"
                ]
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(upload_date_text, fmt)
                        return int(parsed_date.timestamp() * 1000)
                    except ValueError:
                        continue
                
                # Use dateutil parser as fallback
                parsed_date = dateutil.parser.parse(upload_date_text, fuzzy=True)
                return int(parsed_date.timestamp() * 1000)
                
            except Exception as e:
                self.logger.warning(f"Could not parse date '{upload_date_text}': {e}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error parsing upload date '{upload_date_text}': {e}")
            return None

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

    def validate_video_info(self, video_info):
        """Comprehensive validation of video info JSON - ensures no empty fields"""
        if not video_info:
            self.logger.error("Video info is None")
            return False, ["Video info is None"]
        
        errors = []
        required_fields = self.config["validation"]["required_json_fields"]
        
        # Check required fields exist and are not empty
        for field in required_fields:
            if field not in video_info:
                errors.append(f"Missing required field: {field}")
            elif not video_info[field] or str(video_info[field]).strip() == "":
                errors.append(f"Empty required field: {field}")
        
        # Specific validations
        if "duration" in video_info:
            duration = str(video_info["duration"]).strip()
            if duration in ["", "00:00", "0:00", "N/A", "null", "undefined"]:
                errors.append(f"Invalid duration: '{duration}' - must be actual video duration")
            elif not re.match(r'^(?:(\d{1,2}):)?(\d{1,2}):(\d{2})$', duration):
                errors.append(f"Duration format invalid: '{duration}' - should be MM:SS or HH:MM:SS")
        
        if "upload_date_epoch" in video_info:
            epoch = video_info["upload_date_epoch"]
            if not epoch or not isinstance(epoch, (int, float)) or epoch <= 0:
                errors.append(f"Invalid upload_date_epoch: {epoch} - must be positive integer timestamp")
        
        if "views" in video_info:
            views = str(video_info["views"]).strip()
            if not views.isdigit() or int(views) < 0:
                errors.append(f"Invalid views: '{views}' - must be positive integer")
        
        if "title" in video_info:
            title = str(video_info["title"]).strip()
            if len(title) < 3:
                errors.append(f"Title too short: '{title}' - must be at least 3 characters")
        
        if "video_src" in video_info:
            video_src = str(video_info["video_src"]).strip()
            if not video_src.startswith('http'):
                errors.append(f"Invalid video_src: '{video_src}' - must be valid URL")
        
        # Check tags array
        if "tags" in video_info:
            tags = video_info["tags"]
            if not isinstance(tags, list):
                errors.append(f"Tags must be a list, got: {type(tags)}")
            elif len(tags) == 0:
                errors.append("Tags list is empty - should have at least some tags")
        
        is_valid = len(errors) == 0
        if not is_valid:
            self.logger.error(f"Video info validation failed: {errors}")
        else:
            self.logger.info("Video info validation passed")
            
        return is_valid, errors

    def validate_video_folder(self, video_id):
        """Enhanced validation that video folder contains all required files with proper content"""
        try:
            download_path = Path(self.config["general"]["download_path"])
            video_dir = download_path / video_id
            
            if not video_dir.exists():
                self.logger.debug(f"Video folder does not exist: {video_dir}")
                return False
            
            # Check for required files
            json_file = video_dir / f"{video_id}.json"
            video_file = video_dir / f"{video_id}.mp4"
            thumbnail_files = list(video_dir.glob(f"{video_id}.*"))
            thumbnail_files = [f for f in thumbnail_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']]
            
            validation_errors = []
            
            # Validate JSON file
            if not json_file.exists():
                validation_errors.append("JSON file missing")
            elif json_file.stat().st_size == 0:
                validation_errors.append("JSON file is empty")
            else:
                # Validate JSON content
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        video_info = json.load(f)
                    
                    is_valid, json_errors = self.validate_video_info(video_info)
                    if not is_valid:
                        validation_errors.extend([f"JSON validation: {err}" for err in json_errors])
                except json.JSONDecodeError as e:
                    validation_errors.append(f"JSON file corrupted: {e}")
                except Exception as e:
                    validation_errors.append(f"JSON file error: {e}")
            
            # Validate video file
            if not video_file.exists():
                validation_errors.append("Video file missing")
            elif video_file.stat().st_size < self.config["validation"]["min_video_size_bytes"]:
                validation_errors.append(f"Video file too small: {video_file.stat().st_size} bytes")
            else:
                # Verify it's a valid video file
                if not self.verify_video_file(video_file):
                    validation_errors.append("Video file appears corrupted")
            
            # Validate thumbnail file
            if len(thumbnail_files) == 0:
                validation_errors.append("Thumbnail file missing")
            else:
                thumb_file = thumbnail_files[0]
                if thumb_file.stat().st_size < self.config["validation"]["min_thumbnail_size_bytes"]:
                    validation_errors.append(f"Thumbnail file too small: {thumb_file.stat().st_size} bytes")
            
            if validation_errors:
                self.logger.warning(f"Video {video_id} folder validation failed: {validation_errors}")
                return False
            else:
                self.logger.info(f"Video {video_id} folder validation passed")
                return True
                
        except Exception as e:
            self.logger.error(f"Error validating video folder {video_id}: {e}")
            return False

    def cleanup_empty_folder(self, video_id):
        """Remove empty or incomplete video folders"""
        try:
            download_path = Path(self.config["general"]["download_path"])
            video_dir = download_path / video_id
            
            if video_dir.exists():
                shutil.rmtree(video_dir)
                self.logger.info(f"Removed incomplete folder: {video_dir}")
        except Exception as e:
            self.logger.error(f"Error removing folder {video_id}: {e}")

    def get_actual_video_duration(self, video_element):
        """Get the actual duration from video element using JavaScript"""
        try:
            # Wait for video to load metadata
            self.driver.execute_script("""
                var video = arguments[0];
                if (video.readyState === 0) {
                    video.load();
                }
            """, video_element)
            
            # Wait a bit for metadata to load
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

    def extract_video_info(self, video_url):
        """Extract video information with enhanced validation and duration extraction"""
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
                "upload_date_epoch": None,
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

            # EXTRACT ALL THREE ITEM_INFO VALUES (upload_date, views, duration)
            try:
                item_info_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'item_info')]")
                self.logger.info(f"Found {len(item_info_elements)} item_info elements")
                
                for i, item in enumerate(item_info_elements):
                    try:
                        # Get the text content of this item_info div
                        item_text = item.get_attribute("textContent") or item.text.strip()
                        self.logger.debug(f"Item_info {i+1} text: '{item_text}'")
                        
                        # Check which type of icon this item contains
                        has_calendar = len(item.find_elements(By.XPATH, ".//svg[contains(@class,'custom-calender')]")) > 0
                        has_eye = len(item.find_elements(By.XPATH, ".//svg[contains(@class,'custom-eye')]")) > 0
                        has_time = len(item.find_elements(By.XPATH, ".//svg[contains(@class,'custom-time')]")) > 0
                        
                        self.logger.debug(f"Item_info {i+1} icons - Calendar: {has_calendar}, Eye: {has_eye}, Time: {has_time}")
                        
                        if has_calendar and item_text:
                            video_info["upload_date"] = item_text.strip()
                            # Parse to epoch
                            epoch = self.parse_upload_date_to_epoch(item_text.strip())
                            if epoch:
                                video_info["upload_date_epoch"] = epoch
                            self.logger.info(f"Upload date extracted: '{video_info['upload_date']}' -> {video_info['upload_date_epoch']}")
                            
                        elif has_eye and item_text:
                            views_num = _parse_views_number(item_text)
                            video_info["views"] = str(views_num)
                            self.logger.info(f"Views extracted: '{item_text}' -> {video_info['views']}")
                            
                        elif has_time and item_text:
                            # Extract time pattern from the text
                            time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', item_text)
                            if time_match:
                                video_info["duration"] = time_match.group(1)
                            else:
                                video_info["duration"] = item_text.strip()
                            self.logger.info(f"Duration extracted: '{video_info['duration']}'")
                            
                    except Exception as e:
                        self.logger.warning(f"Error processing item_info {i+1}: {e}")
                        continue
                
            except Exception as e:
                self.logger.error(f"Error extracting item_info data: {e}")

            # Enhanced duration extraction from video element if still empty or invalid
            if not video_info["duration"] or video_info["duration"] in ["00:00", "0:00"]:
                try:
                    video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")
                    actual_duration = self.get_actual_video_duration(video_element)
                    if actual_duration:
                        video_info["duration"] = actual_duration
                        self.logger.info(f"Duration extracted from video element: {video_info['duration']}")
                except Exception as e:
                    self.logger.warning(f"Video element duration extraction failed: {e}")

            # Extract uploader
            try:
                label_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class,'label')]")
                uploader_found = False
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
                            video_info["uploader"] = uploader_element.text.strip()
                            uploader_found = True
                            self.logger.info(f"Uploader extracted: {video_info['uploader']}")
                            break
                if not uploader_found:
                    video_info["uploader"] = "Unknown"
                    self.logger.debug("Uploader not found, using default")
            except Exception:
                video_info["uploader"] = "Unknown"
                self.logger.warning("Error while extracting uploader (non-fatal)")

            # Extract tags: exclude unwanted tags (case-insensitive)
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
                video_info["tags"] = tags if tags else ["untagged"]  # Ensure not empty
                self.logger.info(f"Tags extracted: {len(video_info['tags'])} tags - {video_info['tags'][:5]}")
            except NoSuchElementException:
                video_info["tags"] = ["untagged"]
                self.logger.warning("Could not find tag elements, using default")

            # IMPROVED THUMBNAIL EXTRACTION
            try:
                # Try multiple approaches for thumbnail extraction
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
                    video_info["thumbnail_src"] = urljoin(self.base_url, thumb_src) if not thumb_src.startswith('http') else thumb_src
                    self.logger.info(f"Thumbnail extracted: {video_info['thumbnail_src']}")
                else:
                    # Final fallback: search page source for image URLs
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
                            video_info["thumbnail_src"] = img_match.group(1) if pattern.startswith('"') else img_match.group(0)
                            self.logger.info(f"Thumbnail extracted from page source: {video_info['thumbnail_src']}")
                            break
                            
            except Exception as e:
                self.logger.warning(f"Thumbnail extraction failed: {e}")

            # Extract video source using exact XPath
            try:
                video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")

                # Get actual duration from video element
                actual_duration = self.get_actual_video_duration(video_element)
                if actual_duration and actual_duration not in ["00:00", "0:00"]:
                    video_info["duration"] = actual_duration
                    self.logger.info(f"Actual duration from video element: {video_info['duration']}")

                # Try to get source from video element
                video_src = video_element.get_attribute("src")
                if not video_src:
                    # Try source elements within video
                    source_elements = video_element.find_elements(By.TAG_NAME, "source")
                    if source_elements:
                        # Get the highest quality source (usually the first or last)
                        for source in source_elements:
                            src = source.get_attribute("src")
                            if src:
                                video_src = src
                                # If we find a 1080p or higher quality, prefer it
                                if any(quality in src for quality in ['1080p', '2160p', '4k']):
                                    break

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
                            video_info["video_src"] = mp4_matches[0]
                            self.logger.info(f"Video source found via regex: {video_info['video_src']}")
                            break
                except:
                    self.logger.error("Could not find video source with any method")

            # Ensure all fields have valid defaults
            if not video_info["title"]:
                video_info["title"] = f"Video_{video_id}"
            if not video_info["duration"]:
                video_info["duration"] = "00:30"  # Default 30 seconds if can't determine
            if not video_info["views"]:
                video_info["views"] = "0"
            if not video_info["uploader"]:
                video_info["uploader"] = "Unknown"
            if not video_info["upload_date"]:
                video_info["upload_date"] = "Unknown"
            if not video_info["upload_date_epoch"]:
                video_info["upload_date_epoch"] = int(datetime.now().timestamp() * 1000)  # Current time as fallback
            if not video_info["tags"]:
                video_info["tags"] = ["untagged"]

            # Log what we found
            self.logger.info(f"Extracted info for {video_id}:")
            self.logger.info(f"  Title: '{video_info['title'][:50]}...'")
            self.logger.info(f"  Duration: {video_info['duration']}")
            self.logger.info(f"  Views: {video_info['views']}")
            self.logger.info(f"  Uploader: {video_info['uploader']}")
            self.logger.info(f"  Upload date: {video_info['upload_date']} (Epoch: {video_info['upload_date_epoch']})")
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
                headers = {
                    'User-Agent': self.config["general"]["user_agent"],
                    'Referer': self.base_url
                }
                
                response = requests.get(url, stream=True, timeout=timeout, headers=headers)
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
    
    def download_thumbnail(self, thumbnail_url, video_id, video_dir):
        """Download thumbnail with multiple format support and validation"""
        if not thumbnail_url:
            return False
            
        # Determine file extension from URL or default to jpg
        parsed_url = urlparse(thumbnail_url)
        url_ext = os.path.splitext(parsed_url.path)[1].lower()
        if url_ext in ['.jpg', '.jpeg', '.png', '.webp']:
            ext = url_ext
        else:
            ext = '.jpg'  # Default
        
        thumbnail_path = video_dir / f"{video_id}{ext}"
        
        if self.download_file(thumbnail_url, thumbnail_path):
            # Validate thumbnail size
            if thumbnail_path.stat().st_size >= self.config["validation"]["min_thumbnail_size_bytes"]:
                self.logger.info(f"Thumbnail downloaded and validated: {thumbnail_path}")
                return True
            else:
                self.logger.warning(f"Thumbnail too small, removing: {thumbnail_path}")
                thumbnail_path.unlink()
                return False
        else:
            self.logger.warning(f"Failed to download thumbnail for {video_id}")
            return False

    def validate_complete_download(self, video_info, video_dir):
        """Validate that all files are properly downloaded and contain valid data"""
        video_id = video_info["video_id"]
        
        try:
            # Check all required files exist
            json_file = video_dir / f"{video_id}.json"
            video_file = video_dir / f"{video_id}.mp4"
            thumbnail_files = list(video_dir.glob(f"{video_id}.*"))
            thumbnail_files = [f for f in thumbnail_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']]
            
            validation_errors = []
            
            # Validate JSON file and content
            if not json_file.exists():
                validation_errors.append("JSON file missing")
            else:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        saved_info = json.load(f)
                    
                    is_valid, json_errors = self.validate_video_info(saved_info)
                    if not is_valid:
                        validation_errors.extend([f"JSON validation: {err}" for err in json_errors])
                except Exception as e:
                    validation_errors.append(f"JSON file error: {e}")
            
            # Validate video file
            if not video_file.exists():
                validation_errors.append("Video file missing")
            elif video_file.stat().st_size < self.config["validation"]["min_video_size_bytes"]:
                validation_errors.append(f"Video file too small: {video_file.stat().st_size} bytes")
            elif not self.verify_video_file(video_file):
                validation_errors.append("Video file appears corrupted")
            
            # Validate thumbnail file
            if len(thumbnail_files) == 0:
                validation_errors.append("Thumbnail file missing")
            else:
                thumb_file = thumbnail_files[0]
                if thumb_file.stat().st_size < self.config["validation"]["min_thumbnail_size_bytes"]:
                    validation_errors.append(f"Thumbnail file too small: {thumb_file.stat().st_size} bytes")
            
            if validation_errors:
                self.logger.error(f"Complete download validation failed for {video_id}: {validation_errors}")
                return False, validation_errors
            else:
                self.logger.info(f"Complete download validation passed for {video_id}")
                return True, []
                
        except Exception as e:
            self.logger.error(f"Error in complete download validation for {video_id}: {e}")
            return False, [f"Validation error: {e}"]
    
    def process_video(self, video_info, max_retries=3):
        """Process and download a single video with comprehensive validation and retry logic"""
        if not video_info or not video_info.get("video_src"):
            self.logger.error("No video info or video source provided")
            return False
        
        video_id = video_info["video_id"]
        download_path = Path(self.config["general"]["download_path"])
        video_dir = download_path / video_id
        
        # Check if video already exists and is completely valid
        if self.validate_video_folder(video_id):
            self.logger.info(f"Video {video_id} already exists and is valid, skipping")
            if video_id not in self.progress["downloaded_videos"]:
                self.progress["downloaded_videos"].append(video_id)
                self.save_progress()
            return True
        
        # Clean up any existing incomplete folder before starting
        if video_dir.exists():
            self.cleanup_empty_folder(video_id)
        
        # Retry logic for the entire video processing
        for retry in range(max_retries):
            try:
                self.logger.info(f"Processing video {video_id} (attempt {retry + 1}/{max_retries})")
                
                # Create main download directory
                download_path.mkdir(parents=True, exist_ok=True)
                
                # Validate video info before proceeding
                is_info_valid, info_errors = self.validate_video_info(video_info)
                if not is_info_valid:
                    self.logger.error(f"Video info validation failed for {video_id}: {info_errors}")
                    if retry == max_retries - 1:
                        return False
                    
                    # Try to re-extract video info
                    self.logger.info(f"Re-extracting video info for {video_id}")
                    video_info = self.extract_video_info(video_info["url"])
                    if not video_info:
                        continue
                    time.sleep(self.config["validation"]["validation_delay_seconds"])
                    continue
                
                # Create video directory
                video_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Created directory: {video_dir}")
                
                # Download video file
                video_file_path = video_dir / f"{video_id}.mp4"
                self.logger.info(f"Starting video download for {video_id}")
                
                if not self.download_file(video_info["video_src"], video_file_path):
                    self.logger.error(f"Failed to download video: {video_id} (attempt {retry + 1})")
                    self.cleanup_empty_folder(video_id)
                    if retry == max_retries - 1:
                        return False
                    time.sleep(self.config["validation"]["validation_delay_seconds"])
                    continue
                
                # Verify video file immediately after download
                if not self.verify_video_file(video_file_path):
                    self.logger.error(f"Video verification failed: {video_id} (attempt {retry + 1})")
                    self.cleanup_empty_folder(video_id)
                    if retry == max_retries - 1:
                        return False
                    time.sleep(self.config["validation"]["validation_delay_seconds"])
                    continue
                
                # Download thumbnail - required for validation
                thumbnail_downloaded = False
                if video_info.get("thumbnail_src"):
                    thumbnail_downloaded = self.download_thumbnail(video_info["thumbnail_src"], video_id, video_dir)
                
                if not thumbnail_downloaded:
                    self.logger.error(f"Thumbnail download failed for {video_id} (attempt {retry + 1})")
                    self.cleanup_empty_folder(video_id)
                    if retry == max_retries - 1:
                        return False
                    time.sleep(self.config["validation"]["validation_delay_seconds"])
                    continue
                
                # Save metadata JSON
                json_path = video_dir / f"{video_id}.json"
                try:
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(video_info, f, indent=2, ensure_ascii=False)
                    self.logger.info(f"Metadata saved: {json_path}")
                except Exception as e:
                    self.logger.error(f"Failed to save metadata for {video_id}: {e}")
                    self.cleanup_empty_folder(video_id)
                    if retry == max_retries - 1:
                        return False
                    time.sleep(self.config["validation"]["validation_delay_seconds"])
                    continue
                
                # Final comprehensive validation - ensure all files are valid
                is_valid, validation_errors = self.validate_complete_download(video_info, video_dir)
                if not is_valid:
                    self.logger.error(f"Final validation failed for {video_id} (attempt {retry + 1}): {validation_errors}")
                    self.cleanup_empty_folder(video_id)
                    if retry == max_retries - 1:
                        self.logger.error(f"Max retries exceeded for {video_id}, skipping permanently")
                        return False
                    time.sleep(self.config["validation"]["validation_delay_seconds"])
                    continue
                
                # Calculate file size for progress tracking
                try:
                    file_size_mb = video_file_path.stat().st_size / (1024 * 1024)
                except:
                    file_size_mb = 0
                
                # Update progress
                self.progress["downloaded_videos"].append(video_id)
                self.progress["last_video_id"] = video_id
                self.progress["total_downloaded"] += 1
                self.progress["total_size_mb"] = self.progress.get("total_size_mb", 0) + file_size_mb
                self.save_progress()
                
                self.logger.info(f"Successfully processed and validated video: {video_id} ({file_size_mb:.2f} MB)")
                return True
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing video {video_id} (attempt {retry + 1}): {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.cleanup_empty_folder(video_id)
                if retry == max_retries - 1:
                    return False
                time.sleep(self.config["validation"]["validation_delay_seconds"])
                
        return False
    
    def verify_video_file(self, filepath):
        """Enhanced verification of video file"""
        try:
            if not os.path.exists(filepath):
                return False
            
            file_size = os.path.getsize(filepath)
            if file_size < self.config["validation"]["min_video_size_bytes"]:
                return False
            
            # Check if file starts with common MP4 headers
            with open(filepath, 'rb') as f:
                header = f.read(32)  # Read more bytes for better detection
                
                # MP4 files typically start with specific bytes
                mp4_signatures = [b'ftyp', b'\x00\x00\x00\x18ftypmp4', b'\x00\x00\x00\x1cftypmp4']
                
                for signature in mp4_signatures:
                    if signature in header:
                        self.logger.debug(f"Valid MP4 signature found in {filepath}")
                        return True
                
                # Additional check for common video file patterns
                if any(pattern in header for pattern in [b'mp4', b'MP4', b'ftyp']):
                    return True
            
            self.logger.warning(f"Video file {filepath} does not have valid MP4 signature")
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying video file {filepath}: {e}")
            return False
    
    def smart_retry_video_extraction(self, video_url, max_attempts=3):
        """Smart retry logic for video information extraction"""
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"Extracting video info (attempt {attempt + 1}/{max_attempts})")
                
                # Add longer wait time for subsequent attempts
                if attempt > 0:
                    wait_time = 5 + (attempt * 3)  # Increasing wait time
                    self.logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                
                # Re-navigate to the page
                self.driver.get(video_url)
                time.sleep(3)
                
                # Handle age verification
                self.handle_age_verification()
                
                # Extract video info
                video_info = self.extract_video_info(video_url)
                
                if video_info:
                    # Validate the extracted info
                    is_valid, errors = self.validate_video_info(video_info)
                    if is_valid:
                        self.logger.info(f"Successfully extracted and validated video info on attempt {attempt + 1}")
                        return video_info
                    else:
                        self.logger.warning(f"Video info validation failed on attempt {attempt + 1}: {errors}")
                else:
                    self.logger.warning(f"Video info extraction returned None on attempt {attempt + 1}")
                
            except Exception as e:
                self.logger.error(f"Error during video info extraction attempt {attempt + 1}: {e}")
        
        self.logger.error(f"Failed to extract valid video info after {max_attempts} attempts")
        return None
    
    def run(self):
        """Main scraping loop - process all videos on first page with enhanced validation"""
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
            skipped_downloads = 0
            
            for i, video_url in enumerate(video_links, 1):
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"Processing video {i}/{len(video_links)}")
                self.logger.info(f"URL: {video_url}")
                self.logger.info(f"{'='*50}")
                
                try:
                    # Extract video ID for pre-check
                    video_id_match = re.search(r'/video/(\d+)/', video_url)
                    if video_id_match:
                        video_id = video_id_match.group(1)
                    else:
                        video_id = video_url.split('/')[-2] if video_url.endswith('/') else video_url.split('/')[-1]
                    
                    # Check if already processed and valid
                    if self.validate_video_folder(video_id):
                        self.logger.info(f"Video {video_id} already exists and is valid, skipping")
                        if video_id not in self.progress["downloaded_videos"]:
                            self.progress["downloaded_videos"].append(video_id)
                            self.save_progress()
                        skipped_downloads += 1
                        continue
                    
                    # Smart retry extraction of video information
                    video_info = self.smart_retry_video_extraction(video_url)
                    
                    if not video_info:
                        self.logger.error(f"Failed to extract valid info for video {i} after all retries")
                        failed_downloads += 1
                        continue
                    
                    # Log extracted information
                    self.logger.info(f"Video Info Extracted and Validated:")
                    self.logger.info(f"  ID: {video_info['video_id']}")
                    self.logger.info(f"  Title: {video_info['title']}")
                    self.logger.info(f"  Duration: {video_info['duration']}")
                    self.logger.info(f"  Views: {video_info['views']}")
                    self.logger.info(f"  Upload Date: {video_info['upload_date']} (Epoch: {video_info.get('upload_date_epoch')})")
                    self.logger.info(f"  Tags: {len(video_info['tags'])} tags")
                    self.logger.info(f"  Has video source: {'Yes' if video_info.get('video_src') else 'No'}")
                    self.logger.info(f"  Has thumbnail: {'Yes' if video_info.get('thumbnail_src') else 'No'}")
                    
                    # Process (download) the video with validation
                    if video_info.get("video_src"):
                        if self.process_video(video_info):
                            successful_downloads += 1
                            self.logger.info(f"✅ Successfully processed and validated video {i}/{len(video_links)}")
                        else:
                            failed_downloads += 1
                            self.logger.error(f"❌ Failed to process video {i}/{len(video_links)} after all retries")
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
            self.logger.info(f"Already existed (skipped): {skipped_downloads}")
            self.logger.info(f"Failed downloads: {failed_downloads}")
            total_processed = successful_downloads + skipped_downloads
            self.logger.info(f"Success rate: {(total_processed/len(video_links)*100):.1f}%")
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
    
    scraper = VideoScrapper()
    scraper.run()