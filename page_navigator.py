import time
import re
import logging
import traceback
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
        """
        RELIABLE: Directly find the 'Last' link via known XPath and extract its page number.
        """
        self.logger.info("Fetching last page number via direct XPath...")

        try:
            # Navigate to videos listing
            videos_url = f"{self.base_url}/videos"
            self.driver.get(videos_url)
            time.sleep(3)

            # Bypass age gate
            self.driver_manager.handle_age_verification()

            # Ensure pagination container is loaded
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Use the exact XPath you provided
            xpath = '//*[@id="custom_list_videos_most_recent_videos_pagination"]/div[11]/a'
            self.logger.info(f"Looking for Last button at XPath: {xpath}")

            element = self.driver.find_element(By.XPATH, xpath)
            href = element.get_attribute("href")
            if not href:
                raise RuntimeError("Last button has no href")

            # Extract the page number from URL
            match = re.search(r'/latest-updates/(\d+)/', href)
            if not match:
                raise RuntimeError(f"Could not parse page number from href: {href}")

            last = int(match.group(1))
            self.logger.info(f"Determined last page = {last} (from Last button href)")
            return last

        except Exception as e:
            self.logger.error(f"Error determining last page number: {e}")
            # Fallback to sensible default
            return 1000


    def _find_last_page_by_navigation(self):
        """
        SIMPLIFIED: Just look for the "Last" button and extract href
        """
        self.logger.info("Looking for 'Last' button to extract page number...")
        
        try:
            # Scroll to bottom first
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Look for "Last" button
            last_selectors = [
                "//a[contains(text(), 'Last')]",
                "//a[@data-action='ajax'][contains(text(), 'Last')]"
            ]
            
            for selector in last_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    if elements:
                        element = elements[0]
                        href = element.get_attribute("href")
                        
                        if href:
                            self.logger.info(f"Found 'Last' button href: {href}")
                            page_numbers = re.findall(r'/(\d+)/', href)
                            if page_numbers:
                                last_page = int(page_numbers[-1])
                                return last_page
                                
                except Exception as e:
                    self.logger.debug(f"Selector failed: {e}")
                    continue
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error in navigation method: {e}")
            return None

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