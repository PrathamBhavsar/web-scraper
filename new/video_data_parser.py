import asyncio
import json
import re
import logging
from datetime import datetime
from html import unescape
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
from lxml import html
from typing import Optional, List, Dict, Any
from video_extractor import VideoExtractor

class OptimizedVideoDataParser:
    """Main video data parser class with delegation to VideoExtractor."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.video_urls = []
        self.parsed_video_data = []
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Initialize extractor
        self.extractor = VideoExtractor(self.logger)
        
        self.logger.info("Initialized parser", extra={"base_url": base_url})
    
    async def handle_age_verification(self, page) -> bool:
        """Handle age verification popup if it appears - FROM NEW PARSER"""
        try:
            self.logger.info("Checking for age verification popup")
            await page.wait_for_timeout(2000)
            
            popup_selector = '.popup.popup_access'
            popup = await page.query_selector(popup_selector)
            
            if popup:
                is_visible = await page.evaluate("""(element) => {
                    const style = window.getComputedStyle(element);
                    return style.display !== 'none' && style.visibility !== 'hidden';
                }""", popup)
                
                if is_visible:
                    self.logger.warning("Age verification popup detected")
                    continue_button = await page.query_selector('input[name="continue"]')
                    if continue_button:
                        self.logger.info("Clicking Continue button")
                        await continue_button.click()
                        await page.wait_for_timeout(3000)
                        
                        try:
                            await page.wait_for_selector('#custom_list_videos_most_recent_videos_items', timeout=10000)
                            self.logger.info("Age verification bypassed successfully")
                            return True
                        except:
                            self.logger.warning("Content took longer to load, continuing anyway")
                            return True
                    else:
                        self.logger.error("Continue button not found")
                        return False
                else:
                    self.logger.info("Age popup exists but is hidden")
                    return True
            else:
                self.logger.info("No age verification popup found")
                return True
                
        except Exception as e:
            self.logger.error("Error handling age verification", extra={"error": str(e)})
            return True
    
    async def extract_video_urls(self) -> List[str]:
        """Extract video URLs from main page - FROM OLD PARSER WITH NEW ENHANCEMENTS"""
        self.logger.info("Starting video URL extraction", extra={"base_url": self.base_url})
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                self.logger.info("Loading main page", extra={"url": self.base_url})
                await page.goto(self.base_url, wait_until='domcontentloaded')
                
                # Handle age verification
                await self.handle_age_verification(page)
                await page.wait_for_timeout(3000)
                
                # Extract using XPath
                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'
                video_elements = await page.query_selector_all(f'xpath={xpath}')
                
                # Try alternative selectors if primary fails
                if len(video_elements) == 0:
                    self.logger.warning("No videos found with primary selector, trying alternatives")
                    alternative_selectors = ['.video-item', '[class*="video"]', '.th.js-open-popup', 'a[href*="/video"]']
                    
                    for selector in alternative_selectors:
                        video_elements = await page.query_selector_all(selector)
                        if video_elements:
                            self.logger.info("Found videos with alternative selector", 
                                           extra={"selector": selector, "count": len(video_elements)})
                            break
                
                self.logger.info("Found video elements", extra={"count": len(video_elements)})
                
                video_urls = []
                for i, element in enumerate(video_elements):
                    try:
                        # Primary link selector
                        link_element = await element.query_selector('a.th.js-open-popup')
                        if not link_element:
                            # Alternative selectors
                            link_element = await element.query_selector('a[href*="/video"]')
                            if not link_element:
                                link_element = await element.query_selector('a')
                        
                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                full_url = urljoin(self.base_url, href)
                                video_urls.append(full_url)
                        
                    except Exception as e:
                        self.logger.warning("Error extracting URL from video element", 
                                          extra={"element_index": i, "error": str(e)})
                
                self.video_urls = video_urls
                self.logger.info("Video URL extraction completed", extra={"count": len(self.video_urls)})
                
                if self.video_urls:
                    sample_urls = self.video_urls[:3]
                    self.logger.info("Sample URLs", extra={"sample": sample_urls})
                
            except Exception as e:
                self.logger.error("Error loading main page", extra={"error": str(e)})
            finally:
                await browser.close()
        
        return self.video_urls
    
    def extract_json_ld_data(self, html_content: str) -> Optional[Dict[str, Any]]:
        """Extract data from JSON-LD script tag - FROM OLD PARSER"""
        try:
            json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>([^<]*)</script>'
            match = re.search(json_ld_pattern, html_content, re.DOTALL | re.IGNORECASE)
            
            if match:
                json_str = match.group(1)
                json_str = json_str.replace(r'\\/', '/')
                json_data = json.loads(json_str)
                
                if json_data.get('@type') == 'VideoObject':
                    self.logger.info("JSON-LD VideoObject found")
                    return json_data
                else:
                    self.logger.warning("JSON-LD found but not VideoObject type")
                    return None
            else:
                self.logger.warning("No JSON-LD script tag found")
                return None
                
        except json.JSONDecodeError as e:
            self.logger.error("JSON parsing error", extra={"error": str(e)})
            return None
        except Exception as e:
            self.logger.error("Error extracting JSON-LD", extra={"error": str(e)})
            return None
    
    # Delegated extractor methods
    def extract_video_id(self, video_url: str) -> str:
        return self.extractor.extract_video_id(video_url)
    
    def extract_title(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        return self.extractor.extract_title(json_ld_data, html_content)
    
    def extract_description(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        return self.extractor.extract_description(json_ld_data, html_content)
    
    def extract_tags(self, html_content: str) -> List[str]:
        return self.extractor.extract_tags(html_content)
    
    def extract_categories(self, html_content: str) -> List[str]:
        return self.extractor.extract_categories(html_content)
    
    def extract_uploaded_by(self, html_content: str) -> str:
        return self.extractor.extract_uploaded_by(html_content)
    
    def extract_artists(self, html_content: str) -> List[str]:
        return self.extractor.extract_artists(html_content)
    
    def extract_duration(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        return self.extractor.extract_duration(json_ld_data, html_content)
    
    def extract_views(self, json_ld_data: Optional[Dict], html_content: str) -> int:
        return self.extractor.extract_views(json_ld_data, html_content)
    
    def extract_likes(self, html_content: str) -> int:
        return self.extractor.extract_likes(html_content)
    
    def extract_upload_date(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        return self.extractor.extract_upload_date(json_ld_data, html_content)
    
    def extract_thumbnail_src(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        return self.extractor.extract_thumbnail_src(json_ld_data, html_content)
    
    def extract_video_src(self, html_content: str) -> str:
        return self.extractor.extract_video_src(html_content)
    
    # Legacy method names for backward compatibility
    def extract_tags_from_html(self, html_content: str) -> List[str]:
        return self.extractor.extract_tags_from_html(html_content)
    
    def extract_categories_from_html(self, html_content: str) -> List[str]:
        return self.extractor.extract_categories_from_html(html_content)
    
    def extract_video_id_from_url(self, video_url: str) -> str:
        return self.extractor.extract_video_id_from_url(video_url)
    
    def extract_artist_and_uploader(self, html_content: str) -> Dict[str, Any]:
        return self.extractor.extract_artist_and_uploader(html_content)
    
    def extract_thumbnail_url(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        return self.extractor.extract_thumbnail_url(json_ld_data, html_content)
    
    def convert_iso_duration_to_readable(self, iso_duration: str) -> str:
        return self.extractor.convert_iso_duration_to_readable(iso_duration)
    
    def get_download_links_from_html(self, html_content: str) -> List[Dict[str, str]]:
        return self.extractor.get_download_links_from_html(html_content)
    
    def find_highest_quality_download_url(self, html_content: str) -> str:
        return self.extractor.find_highest_quality_download_url(html_content)
    
    def extract_player_data(self, html_content: str) -> Optional[Dict[str, Any]]:
        """Extract player configuration data from HTML - FROM OLD PARSER"""
        try:
            # Look for player data in various formats
            player_patterns = [
                r'playerConfig\s*=\s*({[^;]+});',
                r'var\s+player\s*=\s*({[^;]+});',
                r'jwplayer\([^)]*\)\.setup\(({[^}]+})\)',
                r'"sources"\s*:\s*(\[[^\]]+\])'
            ]
            
            for pattern in player_patterns:
                match = re.search(pattern, html_content, re.DOTALL)
                if match:
                    try:
                        player_str = match.group(1)
                        # Clean up the string for JSON parsing
                        player_str = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', player_str)
                        player_data = json.loads(player_str)
                        self.logger.info("Extracted player data")
                        return player_data
                    except json.JSONDecodeError:
                        continue
            
            self.logger.warning("No player data found")
            return None
        except Exception as e:
            self.logger.error("Error extracting player data", extra={"error": str(e)})
            return None
    
    async def parse_individual_video(self, video_url: str) -> Dict[str, Any]:
        """Parse individual video from URL - FROM OLD PARSER"""
        self.logger.info("Parsing individual video", extra={"url": video_url})
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                self.logger.info("Loading video page", extra={"url": video_url})
                await page.goto(video_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(3000)
                
                html_content = await page.content()
                json_ld_data = self.extract_json_ld_data(html_content)
                
                # Extract all video data
                video_data = {
                    'video_id': self.extract_video_id(video_url),
                    'title': self.extract_title(json_ld_data, html_content),
                    'description': self.extract_description(json_ld_data, html_content),
                    'tags': self.extract_tags(html_content),
                    'categories': self.extract_categories(html_content),
                    'uploaded_by': self.extract_uploaded_by(html_content),
                    'artists': self.extract_artists(html_content),
                    'duration': self.extract_duration(json_ld_data, html_content),
                    'views': self.extract_views(json_ld_data, html_content),
                    'likes': self.extract_likes(html_content),
                    'upload_date': self.extract_upload_date(json_ld_data, html_content),
                    'thumbnail_url': self.extract_thumbnail_src(json_ld_data, html_content),
                    'video_url': self.extract_video_src(html_content),
                    'source_url': video_url
                }
                
                self.logger.info("Video parsing completed", extra={"video_id": video_data['video_id']})
                return video_data
                
            except Exception as e:
                self.logger.error("Error parsing individual video", extra={"url": video_url, "error": str(e)})
                return {}
            finally:
                await browser.close()
    
    async def parse_single_video(self, video_url: str) -> Dict[str, Any]:
        """Parse single video - alias for parse_individual_video"""
        return await self.parse_individual_video(video_url)
    
    async def parse_all_videos(self) -> List[Dict[str, Any]]:
        """Parse all extracted video URLs"""
        if not self.video_urls:
            self.logger.warning("No video URLs to parse")
            return []
        
        self.logger.info("Starting to parse all videos", extra={"count": len(self.video_urls)})
        parsed_data = []
        
        for i, video_url in enumerate(self.video_urls):
            try:
                self.logger.info("Parsing video", extra={"index": i+1, "total": len(self.video_urls), "url": video_url})
                video_data = await self.parse_individual_video(video_url)
                if video_data:
                    parsed_data.append(video_data)
                    
                # Add delay to avoid overwhelming the server
                if i < len(self.video_urls) - 1:
                    await asyncio.sleep(2)
                    
            except Exception as e:
                self.logger.error("Error in parse_all_videos loop", extra={"index": i, "url": video_url, "error": str(e)})
        
        self.parsed_video_data = parsed_data
        self.logger.info("Completed parsing all videos", extra={"successful": len(parsed_data)})
        return self.parsed_video_data
    
    def save_data(self, filename: str = 'video_data.json') -> bool:
        """Save parsed data to JSON file"""
        try:
            if not self.parsed_video_data:
                self.logger.warning("No data to save")
                return False
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.parsed_video_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info("Data saved successfully", extra={"filename": filename, "count": len(self.parsed_video_data)})
            return True
            
        except Exception as e:
            self.logger.error("Error saving data", extra={"filename": filename, "error": str(e)})
            return False