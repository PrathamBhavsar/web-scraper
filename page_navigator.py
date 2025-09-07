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
        FIXED: Get the actual last page number by finding the "Last" button in footer
        """
        self.logger.info("Starting to fetch last page number from website...")
        
        try:
            # Step 1: Navigate to the main page
            self.logger.info("Step 1: Navigating to main page...")
            self.driver.get("https://rule34video.com/")
            time.sleep(3)
            
            # Step 2: Handle age verification if present
            self.logger.info("Step 2: Checking for age verification...")
            age_verify_selectors = [
                "button[type='submit']",
                ".age-verify-btn",
                "#age-verify",
                "input[value='Enter']",
                "button.btn-primary",
                "form button[type='submit']"
            ]
            
            age_verified = False
            for selector in age_verify_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        element = elements[0]
                        self.logger.info(f"Found age verification element: {selector}")
                        self.driver.execute_script("arguments[0].click();", element)
                        self.logger.info("Age verification clicked successfully")
                        age_verified = True
                        break
                except Exception as e:
                    self.logger.debug(f"Selector {selector} not found: {e}")
                    continue
            
            if age_verified:
                self.logger.info("Age verification completed, waiting for page load...")
                time.sleep(5)
            else:
                self.logger.info("No age verification found or already bypassed")
            
            # Step 3: Navigate to videos page
            self.logger.info("Step 3: Ensuring we're on the videos listing page...")
            if "videos" not in self.driver.current_url.lower():
                videos_url = "https://rule34video.com/videos"
                self.driver.get(videos_url)
                time.sleep(3)
                self.logger.info(f"Navigated to videos page: {videos_url}")
            
            # Step 4: Scroll to bottom to reveal pagination
            self.logger.info("Step 4: Scrolling to bottom to reveal pagination...")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            
            # Step 5: Find the specific "Last" button using the exact XPath you provided
            self.logger.info("Step 5: Looking for 'Last' button in pagination...")
            
            # Try the exact XPath first
            exact_xpath = "/html/body/div/div[2]/div[2]/div[2]/div/div/div[4]/div[11]/a"
            
            try:
                self.logger.info(f"Trying exact XPath: {exact_xpath}")
                last_button = self.driver.find_element(By.XPATH, exact_xpath)
                
                if last_button:
                    href = last_button.get_attribute("href")
                    text = last_button.text
                    self.logger.info(f"Found 'Last' button with href: {href}")
                    self.logger.info(f"Button text: '{text}'")
                    
                    # Extract page number from href
                    if href:
                        # Extract the page number from URL like: https://rule34video.com/latest-updates/6540/
                        page_numbers = re.findall(r'/(\d+)/', href)
                        if page_numbers:
                            last_page = int(page_numbers[-1])
                            self.logger.info(f"FOUND LAST PAGE: {last_page}")
                            return last_page
                            
            except Exception as e:
                self.logger.warning(f"Exact XPath failed: {e}")
            
            # Fallback: Look for "Last" button with more generic selectors
            self.logger.info("Step 6: Trying fallback selectors for 'Last' button...")
            
            fallback_selectors = [
                "//a[contains(text(), 'Last')]",
                "//a[@data-action='ajax'][contains(text(), 'Last')]",
                "//a[contains(@href, 'latest-updates')][contains(text(), 'Last')]",
                "//div[@id='custom_list_videos_most_recent_videos_pagination']//a[contains(text(), 'Last')]",
                "//div[contains(@class, 'pagination')]//a[contains(text(), 'Last')]"
            ]
            
            for selector in fallback_selectors:
                try:
                    self.logger.info(f"Trying fallback selector: {selector}")
                    elements = self.driver.find_elements(By.XPATH, selector)
                    
                    if elements:
                        for element in elements:
                            href = element.get_attribute("href")
                            text = element.text.strip()
                            
                            if href and text.lower() == "last":
                                self.logger.info(f"Found 'Last' button with href: {href}")
                                
                                # Extract page number from href
                                page_numbers = re.findall(r'/(\d+)/', href)
                                if page_numbers:
                                    last_page = int(page_numbers[-1])
                                    self.logger.info(f"FOUND LAST PAGE: {last_page}")
                                    return last_page
                                    
                except Exception as e:
                    self.logger.debug(f"Fallback selector {selector} failed: {e}")
                    continue
            
            # Final fallback
            self.logger.warning("Could not find 'Last' button, using fallback value")
            return 6000
            
        except Exception as e:
            self.logger.error(f"Error getting last page number: {e}")
            traceback.print_exc()
            return 6000

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