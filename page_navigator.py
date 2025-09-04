import time
import re
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

class PageNavigator:
    def __init__(self, config, driver_manager):
        self.config = config
        self.driver_manager = driver_manager
        self.base_url = "https://rule34video.com"
        self.logger = logging.getLogger('Rule34Scraper')
    
    @property
    def driver(self):
        return self.driver_manager.get_driver()
    
    def get_last_page_number(self):
        """Find the highest page number from pagination"""
        try:
            # Navigate to main page first
            if not self.driver_manager.navigate_to_page(self.base_url):
                return 1
            
            # Navigate to newest page to see pagination
            newest_url = f"{self.base_url}/newest/"
            if not self.driver_manager.navigate_to_page(newest_url):
                return 1
            
            # Extract pagination info
            pagination = self.extract_pagination_info()
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
    
    def extract_pagination_info(self):
        """Get pagination structure and available pages"""
        pagination_selectors = [
            ".pagination",
            ".pages", 
            ".page-navigation",
            "[class*='page']",
            "nav"
        ]
        
        for selector in pagination_selectors:
            try:
                pagination = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                return pagination
            except TimeoutException:
                continue
        
        return None
    
    def get_video_links_from_page(self, page_num):
        """Extract all video URLs from a specific page"""
        try:
            # Navigate to the specific page
            if not self.handle_page_navigation(page_num):
                return []
            
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
                        
                except Exception as e:
                    self.logger.warning(f"Error processing video card {i+1}: {e}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(video_links)} video links from page {page_num}")
            return video_links
            
        except Exception as e:
            self.logger.error(f"Error getting video links from page {page_num}: {e}")
            return []
    
    def handle_page_navigation(self, page_num):
        """Navigate between pages with proper waiting"""
        try:
            # For first page, use the main URL
            if page_num == 1:
                page_url = f"{self.base_url}/"
            else:
                page_url = f"{self.base_url}/?page={page_num}"
            
            self.logger.info(f"Loading page: {page_url}")
            
            if not self.driver_manager.navigate_to_page(page_url):
                return False
            
            time.sleep(self.config["scraping"]["wait_time_ms"] / 1000)
            return True
            
        except Exception as e:
            self.logger.error(f"Error navigating to page {page_num}: {e}")
            return False