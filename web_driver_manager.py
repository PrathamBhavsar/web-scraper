import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

class WebDriverManager:
    def __init__(self, config):
        self.config = config
        self.driver = None
        self.base_url = "https://rule34video.com"
        self.logger = logging.getLogger('Rule34Scraper')
    
    def setup_driver(self):
        """Initialize Selenium WebDriver with Chrome options"""
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # Comment out for debugging
        chrome_options.add_argument(f"--user-agent={self.config['general']['user_agent']}")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")

        # 🔧 Fix WebGL fallback warning
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-webgl")
        chrome_options.add_argument("--disable-webgl2")

        # Add preferences to handle downloads and popups
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
        return self.driver
    
    def handle_age_verification(self):
        """Handle age verification popups if present"""
        try:
            # Check if age verification is present
            if "age" in self.driver.current_url.lower() or "verify" in self.driver.page_source.lower():
                # Look for age verification button/link
                age_buttons = self.driver.find_elements(
                    By.XPATH, 
                    "//a[contains(text(), '18') or contains(text(), 'Enter') or contains(text(), 'Yes')]"
                )
                if age_buttons:
                    age_buttons[0].click()
                    time.sleep(3)
                    self.logger.info("Handled age verification")
        except Exception as e:
            self.logger.warning(f"Age verification handling failed: {e}")
    
    def navigate_to_page(self, url):
        """Navigate to specific page URL with error handling"""
        try:
            self.driver.get(url)
            time.sleep(3)
            self.handle_age_verification()
            return True
        except Exception as e:
            self.logger.error(f"Error navigating to {url}: {e}")
            return False
    
    def close_driver(self):
        """Properly close and cleanup WebDriver instance"""
        if self.driver:
            self.logger.info("Keeping browser open for inspection...")
            input("Press Enter to close browser...")
            self.driver.quit()
            self.logger.info("WebDriver closed")
    
    def get_driver(self):
        """Get the current driver instance"""
        return self.driver