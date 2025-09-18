#!/usr/bin/env python3
# Enhanced Video Data Parser with Comprehensive Debugging
# This version adds detailed logging for debugging failed video processing issues.

import asyncio
import json
import re
from datetime import datetime
from html import unescape
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
import re
from lxml import html

class OptimizedVideoDataParser:
    def __init__(self, base_url):
        self.base_url = base_url
        self.video_urls = []
        self.parsed_video_data = []

        # DEBUG: Enhanced logging
        print(f"🐛 DEBUG: Initializing parser for URL: {base_url}")

    async def handle_age_verification(self, page):
        # Handle age verification popup if it appears
        try:
            print("🔍 Checking for age verification popup...")
            await page.wait_for_timeout(2000)

            popup_selector = '.popup.popup_access'
            popup = await page.query_selector(popup_selector)

            if popup:
                is_visible = await page.evaluate('''(element) => {
                    const style = window.getComputedStyle(element);
                    return style.display !== 'none' && style.visibility !== 'hidden';
                }''', popup)

                if is_visible:
                    print("⚠️ Age verification popup detected!")
                    continue_button = await page.query_selector('input[name="continue"]')
                    if continue_button:
                        print("🖱️ Clicking Continue button...")
                        await continue_button.click()
                        await page.wait_for_timeout(3000)

                        try:
                            await page.wait_for_selector('#custom_list_videos_most_recent_videos_items', timeout=10000)
                            print("✅ Age verification bypassed successfully!")
                            return True
                        except:
                            print("⚠️ Content took longer to load, continuing anyway...")
                            return True
                    else:
                        print("❌ Continue button not found!")
                        return False
                else:
                    print("ℹ️ Age popup exists but is hidden")
                    return True
            else:
                print("ℹ️ No age verification popup found")
                return True

        except Exception as e:
            print(f"⚠️ Error handling age verification: {e}")
            return True

    async def extract_video_urls(self):
        # Extract video URLs from main page with enhanced debugging
        print(f"🐛 DEBUG: Starting video URL extraction from {self.base_url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                print(f"🚀 Loading main page: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')

                # DEBUG: Log page title and basic info
                page_title = await page.title()
                current_url = page.url
                print(f"🐛 DEBUG: Page loaded - Title: '{page_title}', URL: {current_url}")

                # Handle age verification
                await self.handle_age_verification(page)

                # DEBUG: Check page content after age verification
                await page.wait_for_timeout(3000)
                page_content_length = len(await page.content())
                print(f"🐛 DEBUG: Page content length after age verification: {page_content_length} characters")

                # Extract using primary XPath
                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'
                print(f"🐛 DEBUG: Trying primary selector: {xpath}")
                video_elements = await page.query_selector_all(f'xpath={xpath}')

                if len(video_elements) == 0:
                    print("⚠️ No videos found with primary selector, trying alternatives...")
                    alternative_selectors = [
                        '.video-item',
                        '[class*="video"]',
                        '.th.js-open-popup',
                        'a[href*="/video"]'
                    ]

                    for selector in alternative_selectors:
                        print(f"🐛 DEBUG: Trying alternative selector: {selector}")
                        video_elements = await page.query_selector_all(selector)
                        if video_elements:
                            print(f"✅ Found {len(video_elements)} videos with selector: {selector}")
                            break
                        else:
                            print(f"🐛 DEBUG: No videos found with selector: {selector}")

                print(f"📹 Found {len(video_elements)} video elements")
                print(f"🐛 DEBUG: Video elements count verification: {len(video_elements)}")

                video_urls = []
                for i, element in enumerate(video_elements):
                    try:
                        print(f"🐛 DEBUG: Processing video element {i+1}/{len(video_elements)}")

                        # Try primary link selector
                        link_element = await element.query_selector('a.th.js-open-popup')
                        if not link_element:
                            # Try alternative link selectors
                            link_element = await element.query_selector('a[href*="/video"]')
                            if not link_element:
                                link_element = await element.query_selector('a')

                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                full_url = urljoin(self.base_url, href)
                                video_urls.append(full_url)
                                print(f"🐛 DEBUG: Video {i+1} URL: {full_url}")
                            else:
                                print(f"🐛 DEBUG: Video {i+1} - No href attribute found")
                        else:
                            print(f"🐛 DEBUG: Video {i+1} - No link element found")

                    except Exception as e:
                        print(f"⚠️ Error extracting URL from video {i+1}: {e}")

                self.video_urls = video_urls
                print(f"🎯 Extracted {len(self.video_urls)} video URLs")
                print(f"🐛 DEBUG: Final video URLs count: {len(self.video_urls)}")

                # DEBUG: Log first few URLs for verification
                if self.video_urls:
                    print(f"🐛 DEBUG: Sample URLs (first 3):")
                    for i, url in enumerate(self.video_urls[:3]):
                        print(f"  {i+1}: {url}")

            except Exception as e:
                print(f"❌ Error loading main page: {e}")
                print(f"🐛 DEBUG: Exception details: {type(e).__name__}: {str(e)}")
            finally:
                await browser.close()

        return self.video_urls

    def extract_json_ld_data(self, html_content):
        # Extract data from JSON-LD script tag with robust error handling
        try:
            json_ld_pattern = r'<script[^>]*type=[\'"]application/ld\+json[\'"][^>]*>(.*?)</script>'
            match = re.search(json_ld_pattern, html_content, re.DOTALL | re.IGNORECASE)

            if match:
                json_str = match.group(1)
                json_str = json_str.replace(r'\/', '/')
                json_data = json.loads(json_str)

                if json_data.get('@type') == 'VideoObject':
                    print(f"🐛 DEBUG: Found JSON-LD VideoObject data")
                    return json_data
                else:
                    print(f"⚠️ JSON-LD found but not VideoObject type: {json_data.get('@type')}")
                    return None
            else:
                print("⚠️ No JSON-LD script tag found")
                return None

        except json.JSONDecodeError as e:
            print(f"⚠️ JSON parsing error: {e}")
            return None
        except Exception as e:
            print(f"⚠️ Error extracting JSON-LD: {e}")
            return None

    async def parse_all_videos(self):
        # Parse all video URLs with comprehensive debugging - SIMPLIFIED VERSION
        print(f"🐛 DEBUG: Starting to parse {len(self.video_urls)} videos")

        if not self.video_urls:
            print("❌ No video URLs to parse!")
            return []

        # For debugging, let's return mock data to test the IDM integration
        parsed_videos = []

        for i, video_url in enumerate(self.video_urls, 1):
            video_id = video_url.split('/')[-1] if '/' in video_url else f"video_{i}"

            video_data = {
                "video_id": video_id,
                "title": f"Test Video {i}",
                "video_src": f"https://example.com/video_{i}.mp4", 
                "thumbnail_src": f"https://example.com/thumb_{i}.jpg",
                "source_url": video_url,
                "parse_status": "success",
                "extracted_at": datetime.now().isoformat()
            }

            parsed_videos.append(video_data)
            print(f"🐛 DEBUG: Mock parsed video {i}: {video_id}")

        self.parsed_video_data = parsed_videos

        print(f"\n🎯 PARSING SUMMARY:")
        print(f"  📹 Total videos found: {len(self.video_urls)}")
        print(f"  ✅ Videos parsed: {len(parsed_videos)}")
        print(f"  🐛 DEBUG: Using mock data for testing IDM integration")

        return parsed_videos

if __name__ == "__main__":
    # Test the enhanced parser
    test_url = "https://rule34video.com/latest-updates/987"
    parser = OptimizedVideoDataParser(test_url)

    async def test_parser():
        print("Testing enhanced video parser with debugging...")
        urls = await parser.extract_video_urls()
        if urls:
            videos = await parser.parse_all_videos()
            print(f"\nFinal result: {len(videos)} videos parsed")
        else:
            print("No URLs extracted to parse")

    # Uncomment to test: asyncio.run(test_parser())
