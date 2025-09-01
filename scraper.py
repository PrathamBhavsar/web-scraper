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
            with open(progress_path, 'r') as f:
                return json.load(f)
        return {
            "last_video_id": None,
            "last_page": None,
            "total_downloaded": 0,
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
    
    def extract_video_info(self, video_url: str):
            """Extract video information using exact XPaths with fallbacks"""
            try:
                self.driver.get(video_url)
                time.sleep(3)

                # Handle age verification if present
                self.handle_age_verification()

                # Extract video ID
                video_id_match = re.search(r'/video/(\d+)/', video_url)
                if video_id_match:
                    video_id = video_id_match.group(1)
                else:
                    video_id = video_url.rstrip("/").split("/")[-1]

                # Initialize info
                video_info = {
                    "video_id": video_id,
                    "url": video_url,
                    "title": f"Video_{video_id}",
                    "duration": "",
                    "views": "",
                    "uploader": "",
                    "upload_date": "",
                    "tags": [],
                    "video_src": "",
                    "thumbnail_src": ""
                }

                # --- Title ---
                try:
                    title_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'thumb_title')]")
                    video_info["title"] = title_element.text.strip()
                except NoSuchElementException:
                    try:
                        title_element = self.driver.find_element(By.TAG_NAME, "h1")
                        video_info["title"] = title_element.text.strip()
                    except NoSuchElementException:
                        self.logger.warning(f"No title found, fallback used: {video_info['title']}")

                # --- Duration ---
                try:
                    duration_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'time')]")
                    video_info["duration"] = duration_element.text.strip()
                except NoSuchElementException:
                    self.logger.warning("Could not find duration element")

                # --- Views ---
                try:
                    views_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'views')]")
                    views_text = views_element.text.strip()
                    views_match = re.search(r'([\d,\.]+[KMB]?)', views_text)
                    video_info["views"] = views_match.group(1) if views_match else views_text
                except NoSuchElementException:
                    self.logger.warning("Could not find views element")

                # --- Tags ---
                try:
                    tag_elements = self.driver.find_elements(By.XPATH, "//a[contains(@class, 'tag_item')]")
                    tags = [t.text.strip() for t in tag_elements if t.text.strip()]
                    video_info["tags"] = tags
                except NoSuchElementException:
                    self.logger.warning("Could not find tags")

                # --- Thumbnail ---
                try:
                    thumbnail_element = self.driver.find_element(By.XPATH, "//img[contains(@class, 'thumb')]")
                    src = thumbnail_element.get_attribute("src")
                    if src:
                        video_info["thumbnail_src"] = urljoin(self.base_url, src)
                except NoSuchElementException:
                    # Fallback: find any "poster/preview" img
                    img_elements = self.driver.find_elements(By.TAG_NAME, "img")
                    for img in img_elements:
                        src = img.get_attribute("src")
                        if src and any(x in src.lower() for x in ["thumb", "preview", "poster"]):
                            video_info["thumbnail_src"] = urljoin(self.base_url, src)
                            break

                # --- Video Source ---
                try:
                    video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")
                    video_src = video_element.get_attribute("src")
                    if not video_src:
                        sources = video_element.find_elements(By.TAG_NAME, "source")
                        if sources:
                            video_src = sources[0].get_attribute("src")
                    if video_src:
                        video_info["video_src"] = video_src if video_src.startswith("http") else urljoin(self.base_url, video_src)
                except NoSuchElementException:
                    # Fallback regex search in page source
                    page_source = self.driver.page_source
                    mp4_urls = re.findall(r'https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*', page_source)
                    if mp4_urls:
                        video_info["video_src"] = mp4_urls[0]

                # --- Log summary ---
                self.logger.info(f"Extracted info for {video_id}:")
                self.logger.info(f"  Title: {video_info['title'][:50]}...")
                self.logger.info(f"  Duration: {video_info['duration']}")
                self.logger.info(f"  Views: {video_info['views']}")
                self.logger.info(f"  Tags: {len(video_info['tags'])} found")
                self.logger.info(f"  Video source: {'Yes' if video_info['video_src'] else 'No'}")
                self.logger.info(f"  Thumbnail: {'Yes' if video_info['thumbnail_src'] else 'No'}")

                return video_info

            except Exception as e:
                self.logger.error(f"Error extracting video info from {video_url}: {e}")
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
        """Process and download a single video"""
        if not video_info or not video_info.get("video_src"):
            return False
        
        video_id = video_info["video_id"]
        
        # Check if already downloaded
        if video_id in self.progress["downloaded_videos"]:
            self.logger.info(f"Video {video_id} already downloaded, skipping")
            return True
        
        # Create download directory
        download_path = Path(self.config["general"]["download_path"])
        video_dir = download_path / video_id
        
        # Download video first (temporary location)
        temp_video_path = download_path / f"{video_id}_temp.mp4"
        
        if not self.download_file(video_info["video_src"], temp_video_path):
            self.logger.error(f"Failed to download video: {video_id}")
            return False
        
        # Verify video file
        if not self.verify_video_file(temp_video_path):
            self.logger.error(f"Video verification failed: {video_id}")
            if temp_video_path.exists():
                temp_video_path.unlink()
            return False
        
        # Create video directory and move files
        video_dir.mkdir(exist_ok=True)
        final_video_path = video_dir / f"{video_id}.mp4"
        temp_video_path.rename(final_video_path)
        
        # Download thumbnail
        if video_info.get("thumbnail_src"):
            thumbnail_path = video_dir / f"{video_id}.jpg"
            self.download_file(video_info["thumbnail_src"], thumbnail_path)
        
        # Save metadata
        json_path = video_dir / f"{video_id}.json"
        with open(json_path, 'w') as f:
            json.dump(video_info, f, indent=2)
        
        # Update progress
        self.progress["downloaded_videos"].append(video_id)
        self.progress["last_video_id"] = video_id
        self.progress["total_downloaded"] += 1
        self.save_progress()
        
        self.logger.info(f"Successfully processed video: {video_id}")
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
        """Main scraping loop"""
        try:
            self.setup_driver()
            
            # Get starting page - for testing, let's start with a smaller number
            if self.config["scraping"]["start_from_last_page"]:
                start_page = min(self.get_last_page_number(), 5)  # Limit to 5 for testing
            else:
                start_page = self.progress.get("last_page", 1)
            
            self.logger.info(f"Starting scrape from page {start_page}")
            
            # Process pages in reverse order (or just start with page 1 for testing)
            test_pages = [1, 2] if start_page > 2 else [start_page]
            
            for page_num in test_pages:
                self.logger.info(f"Processing page {page_num}")
                
                video_links = self.get_video_links_from_page(page_num)
                
                if not video_links:
                    self.logger.warning(f"No video links found on page {page_num}")
                    continue
                
                # Process only first 2 videos for testing
                for video_url in video_links[:2]:
                    self.logger.info(f"Processing video: {video_url}")
                    video_info = self.extract_video_info(video_url)
                    
                    if video_info:
                        if video_info.get("video_src"):
                            self.process_video(video_info)
                        else:
                            self.logger.warning(f"No video source found for {video_url}")
                    
                    # Small delay between videos
                    time.sleep(self.config["general"]["delay_between_requests"] / 1000)
                
                # Update page progress
                self.progress["last_page"] = page_num
                self.save_progress()
                
                # For testing, break after first page
                break
                
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            if self.driver:
                input("Press Enter to close browser...")  # Keep browser open for debugging
                self.driver.quit()
            self.logger.info("Scraper finished")

# Usage
if __name__ == "__main__":
    # Create downloads directory
    os.makedirs("C:\\scraper_downloads", exist_ok=True)
    
    scraper = Rule34VideoScraper()
    scraper.run()