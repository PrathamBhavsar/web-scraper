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
        CORRECTED: Find the 'Last' button and extract page number from its href
        """
        self.logger.info("Fetching last page number from pagination...")

        try:
            # Navigate to the main videos listing page (first page)
            videos_url = f"{self.base_url}/latest-updates/"
            self.logger.info(f"Navigating to: {videos_url}")
            self.driver.get(videos_url)
            time.sleep(3)

            # Bypass age gate if present
            self.driver_manager.handle_age_verification()

            # Ensure pagination container is loaded by scrolling to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # CORRECTED: Use the correct XPath based on your HTML structure
            # The pagination container is: custom_list_videos_latest_videos_list_pagination
            # The Last button is in div[11] within this container
            xpath = '//*[@id="custom_list_videos_latest_videos_list_pagination"]/div[11]/a'
            self.logger.info(f"Looking for Last button at XPath: {xpath}")

            # Wait for the element to be present
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            
            href = element.get_attribute("href")
            if not href:
                raise RuntimeError("Last button has no href attribute")

            self.logger.info(f"Found Last button href: {href}")

            # Extract the page number from URL like "/latest-updates/9547/"
            match = re.search(r'/latest-updates/(\d+)/', href)
            if not match:
                raise RuntimeError(f"Could not parse page number from href: {href}")

            last_page = int(match.group(1))
            self.logger.info(f"Successfully determined last page = {last_page}")
            
            # IMPORTANT: Navigate to the last page to verify it exists and start scraping from there
            self.logger.info(f"Verifying last page by navigating to: {href}")
            self.driver.get(href)
            time.sleep(3)
            
            # Check if we successfully loaded the last page
            current_url = self.driver.current_url
            if f"/{last_page}/" in current_url or current_url.endswith(f"/{last_page}"):
                self.logger.info(f"Successfully verified last page {last_page} - ready to start scraping")
                return last_page
            else:
                self.logger.warning(f"Page verification failed. Current URL: {current_url}")
                return last_page  # Return anyway, might still work

        except Exception as e:
            self.logger.error(f"Error determining last page number: {e}")
            traceback.print_exc()
            # Fallback to reasonable default
            self.logger.warning("Using fallback page number: 1000")
            return 1000

    def get_video_links_from_page(self, page_num):
        """Extract all video URLs from a specific page"""
        try:
            # Navigate to the specific page
            if not self.handle_page_navigation(page_num):
                return []
            
            # Wait a moment for the page to fully load
            time.sleep(2)
            
            # Use exact XPath to find video cards - updated to match the actual structure
            video_selectors = [
                "//div[contains(@class, 'video_')]",  # Original selector
                "//div[@class='item']//a[contains(@href, '/video/')]",  # Alternative selector
                "//a[contains(@class, 'js-open-popup')]",  # Direct popup links
                "//div[@class='item']//a[@data-action='ajax']"  # Data-action links
            ]
            
            video_links = []
            
            for selector in video_selectors:
                try:
                    elements = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.XPATH, selector))
                    )
                    
                    self.logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    
                    for i, element in enumerate(elements):
                        try:
                            if element.tag_name == 'a':
                                video_url = element.get_attribute("href")
                            else:
                                # If it's a div, find the link inside it
                                link_element = element.find_element(By.XPATH, ".//a[contains(@href, '/video/') or contains(@class, 'js-open-popup')]")
                                video_url = link_element.get_attribute("href")
                            
                            # Validate the URL contains '/video/' to ensure it's a video link
                            if video_url and '/video/' in video_url and video_url not in video_links:
                                video_links.append(video_url)
                                self.logger.debug(f"Video {len(video_links)}: {video_url}")
                                
                        except Exception as e:
                            self.logger.debug(f"Error processing element {i+1} with selector {selector}: {e}")
                            continue
                    
                    # If we found videos with this selector, break
                    if video_links:
                        break
                        
                except Exception as e:
                    self.logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(video_links)} video links from page {page_num}")
            
            # Log first few URLs for debugging
            for i, url in enumerate(video_links[:3]):
                self.logger.info(f"Sample video {i+1}: {url}")
            
            return video_links
            
        except Exception as e:
            self.logger.error(f"Error getting video links from page {page_num}: {e}")
            traceback.print_exc()
            return []
    
    def handle_page_navigation(self, page_num):
        """Navigate between pages with proper waiting"""
        try:
            # CORRECTED: Use the proper URL format based on your structure
            if page_num == 1:
                page_url = f"{self.base_url}/latest-updates/"
            else:
                page_url = f"{self.base_url}/latest-updates/{page_num}/"

            self.logger.info(f"Navigating to page: {page_url}")

            if not self.driver_manager.navigate_to_page(page_url):
                self.logger.error(f"Failed to navigate to page {page_num}")
                return False

            # Wait for page to load
            wait_time = self.config.get("scraping", {}).get("wait_time_ms", 3000) / 1000
            time.sleep(wait_time)
            
            # Verify we're on the correct page
            current_url = self.driver.current_url
            if page_num == 1:
                expected_in_url = "/latest-updates/"
            else:
                expected_in_url = f"/latest-updates/{page_num}/"
            
            if expected_in_url in current_url:
                self.logger.info(f"Successfully navigated to page {page_num}")
                return True
            else:
                self.logger.warning(f"Page navigation may have failed. Expected: {expected_in_url}, Got: {current_url}")
                return True  # Continue anyway, might still work

        except Exception as e:
            self.logger.error(f"Error navigating to page {page_num}: {e}")
            traceback.print_exc()
            return False

    def _find_last_page_by_navigation(self):
        """
        Fallback method: Look for the "Last" button using multiple selectors
        """
        self.logger.info("Fallback: Looking for 'Last' button to extract page number...")
        
        try:
            # Scroll to bottom to ensure pagination is visible
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Multiple selectors to find the "Last" button
            last_selectors = [
                "//a[contains(text(), 'Last')]",
                "//a[@data-action='ajax' and contains(text(), 'Last')]",
                "//*[@id='custom_list_videos_latest_videos_list_pagination']//a[contains(text(), 'Last')]",
                "//div[@class='item']//a[contains(text(), 'Last')]"
            ]
            
            for selector in last_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    if elements:
                        element = elements[0]
                        href = element.get_attribute("href")
                        
                        if href:
                            self.logger.info(f"Found 'Last' button href with selector {selector}: {href}")
                            # Extract page number from href
                            match = re.search(r'/latest-updates/(\d+)/', href)
                            if match:
                                last_page = int(match.group(1))
                                self.logger.info(f"Extracted last page number: {last_page}")
                                return last_page
                                
                except Exception as e:
                    self.logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            self.logger.warning("Could not find Last button with any selector")
            return None
            
        except Exception as e:
            self.logger.error(f"Error in fallback navigation method: {e}")
            return None

    def extract_pagination_info(self):
        """Get pagination structure and available pages"""
        pagination_selectors = [
            "#custom_list_videos_latest_videos_list_pagination",
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
                self.logger.info(f"Found pagination with selector: {selector}")
                return pagination
            except TimeoutException:
                self.logger.debug(f"Pagination selector failed: {selector}")
                continue
        
        self.logger.warning("No pagination container found")
        return None