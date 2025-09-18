#!/usr/bin/env python3

"""
Enhanced Video Data Parser with Comprehensive Debugging
This version adds detailed logging for debugging failed video processing issues.
"""

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
        """Handle age verification popup if it appears"""
        try:
            print("🔍 Checking for age verification popup...")
            # Wait a bit for page to load completely
            await page.wait_for_timeout(2000)

            # Check if age popup is visible
            popup_selector = '.popup.popup_access'
            popup = await page.query_selector(popup_selector)

            if popup:
                # Check if popup is visible (not display: none)
                is_visible = await page.evaluate('''(element) => {
                    const style = window.getComputedStyle(element);
                    return style.display !== 'none' && style.visibility !== 'hidden';
                }''', popup)

                if is_visible:
                    print("⚠️ Age verification popup detected!")
                    # Click the Continue button
                    continue_button = await page.query_selector('input[name="continue"]')
                    if continue_button:
                        print("🖱️ Clicking Continue button...")
                        await continue_button.click()
                        # Wait for popup to disappear and content to load
                        print("⏳ Waiting for content to load...")
                        await page.wait_for_timeout(3000)

                        # Wait for the main content to appear
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
            return True  # Continue anyway

    async def extract_video_urls(self):
        """Extract video URLs from main page with enhanced debugging"""
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

                # Handle age verification popup if present
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
        """Extract data from JSON-LD script tag with robust error handling"""
        try:
            # Find JSON-LD script tag with VideoObject
            json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>\s*({.*?})\s*</script>'
            match = re.search(json_ld_pattern, html_content, re.DOTALL | re.IGNORECASE)

            if match:
                json_str = match.group(1)
                # Clean up the JSON string
                json_str = json_str.replace(r'\/', '/')
                json_data = json.loads(json_str)

                # Validate that it's a VideoObject
                if json_data.get('@type') == 'VideoObject':
                    return json_data
                else:
                    print("⚠️ JSON-LD found but not VideoObject type")
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

    def extract_title(self, json_ld_data, html_content):
        """Extract title with fallback methods"""
        try:
            # Priority 1: JSON-LD name field
            if json_ld_data and 'name' in json_ld_data:
                title = json_ld_data['name'].strip()
                if title:
                    return title

            # Priority 2: HTML title tag
            title_match = re.search(r'<title[^>]*>([^<]+)</title>', html_content, re.IGNORECASE)
            if title_match:
                title = unescape(title_match.group(1)).strip()
                # Clean up title by removing site name
                if ' - ' in title:
                    title = title.split(' - ')[0].strip()
                if title:
                    return title

            # Priority 3: h1 tag
            h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html_content, re.IGNORECASE)
            if h1_match:
                title = unescape(h1_match.group(1)).strip()
                if title:
                    return title

            return "Unknown Title"

        except Exception as e:
            print(f"⚠️ Error extracting title: {e}")
            return "Unknown Title"

    def extract_duration(self, json_ld_data, html_content):
        """Extract duration with multiple fallback methods"""
        try:
            # Priority 1: JSON-LD duration field
            if json_ld_data and 'duration' in json_ld_data:
                duration_str = json_ld_data['duration']
                # Parse ISO 8601 duration format (PT1M30S)
                if duration_str.startswith('PT'):
                    time_pattern = r'PT(?:(\d+)M)?(?:(\d+)S)?'
                    match = re.search(time_pattern, duration_str)
                    if match:
                        minutes = int(match.group(1) or 0)
                        seconds = int(match.group(2) or 0)
                        return f"{minutes:02d}:{seconds:02d}"

            # Priority 2: Look for duration in various HTML elements
            duration_patterns = [
                r'duration["\']?\s*:\s*["\']?([0-9:]+)',
                r'<span[^>]*class[^>]*duration[^>]*>([^<]+)</span>',
                r'<div[^>]*class[^>]*duration[^>]*>([^<]+)</div>',
                r'data-duration=["\']([^"\']+)["\']'
            ]

            for pattern in duration_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    duration = match.group(1).strip()
                    # Validate format (MM:SS or H:MM:SS)
                    if re.match(r'^\d{1,2}:\d{2}(:\d{2})?$', duration):
                        return duration

            return "00:00"  # Default fallback

        except Exception as e:
            print(f"⚠️ Error extracting duration: {e}")
            return "00:00"

    def extract_thumbnail_url(self, json_ld_data, html_content):
        """Extract thumbnail URL with multiple methods"""
        try:
            # Priority 1: JSON-LD thumbnailUrl
            if json_ld_data and 'thumbnailUrl' in json_ld_data:
                thumb_url = json_ld_data['thumbnailUrl'].strip()
                if thumb_url and thumb_url.startswith('http'):
                    return thumb_url

            # Priority 2: Look for poster attribute in video tags
            poster_match = re.search(r'<video[^>]*poster=["\']([^"\']+)["\']', html_content, re.IGNORECASE)
            if poster_match:
                poster_url = poster_match.group(1)
                if poster_url.startswith('http'):
                    return poster_url
                elif poster_url.startswith('/'):
                    # Convert relative URL to absolute
                    base_domain = urlparse(self.base_url).netloc
                    return f"https://{base_domain}{poster_url}"

            # Priority 3: Look for thumbnail images
            thumb_patterns = [
                r'<img[^>]*class[^>]*thumb[^>]*src=["\']([^"\']+)["\']',
                r'<img[^>]*src=["\']([^"\']+)["\'][^>]*class[^>]*thumb',
                r'thumbnail["\']?\s*:\s*["\']([^"\']+)["\']'
            ]

            for pattern in thumb_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    thumb_url = match.group(1)
                    if thumb_url.startswith('http'):
                        return thumb_url
                    elif thumb_url.startswith('/'):
                        base_domain = urlparse(self.base_url).netloc
                        return f"https://{base_domain}{thumb_url}"

            return ""  # No thumbnail found

        except Exception as e:
            print(f"⚠️ Error extracting thumbnail: {e}")
            return ""

    def extract_video_url(self, json_ld_data, html_content):
        """Extract video download URL with multiple methods"""
        try:
            # Priority 1: JSON-LD contentUrl
            if json_ld_data and 'contentUrl' in json_ld_data:
                video_url = json_ld_data['contentUrl'].strip()
                if video_url and video_url.startswith('http'):
                    return video_url

            # Priority 2: Look for source tags in video elements
            source_match = re.search(r'<source[^>]*src=["\']([^"\']+)["\']', html_content, re.IGNORECASE)
            if source_match:
                video_url = source_match.group(1)
                if video_url.startswith('http'):
                    return video_url

            # Priority 3: Look for video tag src attribute
            video_match = re.search(r'<video[^>]*src=["\']([^"\']+)["\']', html_content, re.IGNORECASE)
            if video_match:
                video_url = video_match.group(1)
                if video_url.startswith('http'):
                    return video_url

            # Priority 4: Look for download links
            download_patterns = [
                r'download["\']?\s*:\s*["\']([^"\']+)["\']',
                r'<a[^>]*href=["\']([^"\']+)["\'][^>]*download',
                r'video_url["\']?\s*:\s*["\']([^"\']+)["\']'
            ]

            for pattern in download_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    video_url = match.group(1)
                    if video_url.startswith('http'):
                        return video_url

            return ""  # No video URL found

        except Exception as e:
            print(f"⚠️ Error extracting video URL: {e}")
            return ""

    def extract_upload_date(self, json_ld_data, html_content):
        """Extract upload date with fallback methods"""
        try:
            # Priority 1: JSON-LD uploadDate or datePublished
            if json_ld_data:
                for date_field in ['uploadDate', 'datePublished', 'dateCreated']:
                    if date_field in json_ld_data:
                        date_str = json_ld_data[date_field]
                        # Parse ISO date format
                        if date_str:
                            return date_str[:10]  # Return YYYY-MM-DD format

            # Priority 2: Look for date patterns in HTML
            date_patterns = [
                r'upload[^>]*date[^>]*>([^<]+)<',
                r'date[^>]*upload[^>]*>([^<]+)<',
                r'published[^>]*>([^<]+)<',
                r'(\d{4}-\d{2}-\d{2})',
                r'(\d{2}/\d{2}/\d{4})'
            ]

            for pattern in date_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    date_str = match.group(1).strip()
                    # Try to standardize date format
                    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                        return date_str
                    elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
                        # Convert MM/DD/YYYY to YYYY-MM-DD
                        parts = date_str.split('/')
                        return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"

            # Default to current date
            return datetime.now().strftime('%Y-%m-%d')

        except Exception as e:
            print(f"⚠️ Error extracting upload date: {e}")
            return datetime.now().strftime('%Y-%m-%d')

    def extract_tags(self, json_ld_data, html_content):
        """Extract tags/keywords with multiple methods"""
        try:
            tags = []

            # Priority 1: JSON-LD keywords
            if json_ld_data and 'keywords' in json_ld_data:
                keywords = json_ld_data['keywords']
                if isinstance(keywords, str):
                    tags.extend([tag.strip() for tag in keywords.split(',') if tag.strip()])
                elif isinstance(keywords, list):
                    tags.extend([str(tag).strip() for tag in keywords if str(tag).strip()])

            # Priority 2: Meta keywords
            meta_keywords = re.search(r'<meta[^>]*name=["\']keywords["\'][^>]*content=["\']([^"\']+)["\']', html_content, re.IGNORECASE)
            if meta_keywords:
                keywords = meta_keywords.group(1)
                tags.extend([tag.strip() for tag in keywords.split(',') if tag.strip()])

            # Priority 3: Look for tag elements
            tag_patterns = [
                r'<span[^>]*class[^>]*tag[^>]*>([^<]+)</span>',
                r'<a[^>]*class[^>]*tag[^>]*>([^<]+)</a>',
                r'<div[^>]*class[^>]*tag[^>]*>([^<]+)</div>'
            ]

            for pattern in tag_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    tag = unescape(match).strip()
                    if tag and tag not in tags:
                        tags.append(tag)

            # Clean and deduplicate tags
            clean_tags = []
            for tag in tags:
                clean_tag = re.sub(r'[^\w\s-]', '', tag).strip()
                if clean_tag and len(clean_tag) > 1 and clean_tag not in clean_tags:
                    clean_tags.append(clean_tag)

            return clean_tags[:10]  # Limit to 10 tags

        except Exception as e:
            print(f"⚠️ Error extracting tags: {e}")
            return []

    def extract_video_id_from_url(self, video_url):
        """Extract video ID from URL"""
        try:
            # Try to extract ID from URL patterns
            patterns = [
                r'/video/([^/]+)/?',
                r'id=([^&]+)',
                r'/([^/]+)/?$'
            ]

            for pattern in patterns:
                match = re.search(pattern, video_url)
                if match:
                    video_id = match.group(1)
                    # Clean the ID
                    video_id = re.sub(r'[^\w\-_]', '', video_id)
                    if video_id:
                        return video_id

            # Fallback: use last part of URL path
            parsed_url = urlparse(video_url)
            path_parts = [part for part in parsed_url.path.split('/') if part]
            if path_parts:
                return re.sub(r'[^\w\-_]', '', path_parts[-1])

            # Final fallback: use timestamp
            return f"video_{int(datetime.now().timestamp())}"

        except Exception as e:
            print(f"⚠️ Error extracting video ID: {e}")
            return f"video_{int(datetime.now().timestamp())}"

    async def parse_single_video(self, video_url):
        """Parse a single video page for metadata with comprehensive debugging"""
        print(f"🐛 DEBUG: Starting parse for video URL: {video_url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                print(f"🚀 Loading video page: {video_url}")
                await page.goto(video_url, wait_until='domcontentloaded', timeout=30000)

                # DEBUG: Log page info
                page_title = await page.title()
                print(f"🐛 DEBUG: Video page loaded - Title: '{page_title}'")

                # Get page content
                html_content = await page.content()
                print(f"🐛 DEBUG: Page content length: {len(html_content)} characters")

                # Extract JSON-LD data
                json_ld_data = self.extract_json_ld_data(html_content)
                if json_ld_data:
                    print(f"🐛 DEBUG: JSON-LD data found and parsed successfully")
                else:
                    print(f"🐛 DEBUG: No JSON-LD data found, using fallback extraction")

                # Extract all metadata
                video_id = self.extract_video_id_from_url(video_url)
                title = self.extract_title(json_ld_data, html_content)
                duration = self.extract_duration(json_ld_data, html_content)
                thumbnail_url = self.extract_thumbnail_url(json_ld_data, html_content)
                video_src = self.extract_video_url(json_ld_data, html_content)
                upload_date = self.extract_upload_date(json_ld_data, html_content)
                tags = self.extract_tags(json_ld_data, html_content)

                # DEBUG: Log extracted data
                print(f"🐛 DEBUG: Extracted metadata:")
                print(f"🐛 DEBUG:   Video ID: {video_id}")
                print(f"🐛 DEBUG:   Title: {title[:50]}...")
                print(f"🐛 DEBUG:   Duration: {duration}")
                print(f"🐛 DEBUG:   Thumbnail URL: {thumbnail_url[:50]}...")
                print(f"🐛 DEBUG:   Video URL: {video_src[:50]}...")
                print(f"🐛 DEBUG:   Upload Date: {upload_date}")
                print(f"🐛 DEBUG:   Tags: {tags}")

                video_data = {
                    "video_id": video_id,
                    "title": title,
                    "duration": duration,
                    "thumbnail_src": thumbnail_url,
                    "video_src": video_src,
                    "upload_date": upload_date,
                    "tags": tags,
                    "page_url": video_url,
                    "extracted_at": datetime.now().isoformat(),
                    "extraction_method": "enhanced_debug"
                }

                print(f"✅ Successfully parsed video: {video_id}")
                return video_data

            except Exception as e:
                print(f"❌ Error parsing video {video_url}: {e}")
                print(f"🐛 DEBUG: Exception details: {type(e).__name__}: {str(e)}")

                # Return minimal data on error
                return {
                    "video_id": self.extract_video_id_from_url(video_url),
                    "title": "Extraction Failed",
                    "duration": "00:00",
                    "thumbnail_src": "",
                    "video_src": "",
                    "upload_date": datetime.now().strftime('%Y-%m-%d'),
                    "tags": [],
                    "page_url": video_url,
                    "extracted_at": datetime.now().isoformat(),
                    "extraction_method": "enhanced_debug",
                    "error": str(e)
                }

            finally:
                await browser.close()

    async def parse_all_videos(self):
        """Parse all video URLs with comprehensive debugging and progress tracking"""
        print(f"\n🎬 Starting to parse {len(self.video_urls)} videos with enhanced debugging...")
        print(f"🐛 DEBUG: Video URLs to process: {len(self.video_urls)}")

        if not self.video_urls:
            print("❌ No video URLs to parse")
            return []

        parsed_videos = []

        for i, video_url in enumerate(self.video_urls, 1):
            print(f"\n📹 Parsing video {i}/{len(self.video_urls)}")
            print(f"🐛 DEBUG: Processing URL: {video_url}")

            try:
                video_data = await self.parse_single_video(video_url)
                if video_data:
                    parsed_videos.append(video_data)
                    print(f"✅ Video {i} parsed successfully: {video_data.get('video_id', 'unknown')}")
                else:
                    print(f"⚠️ Video {i} returned no data")

            except Exception as e:
                print(f"❌ Error parsing video {i}: {e}")
                print(f"🐛 DEBUG: Exception for video {i}: {type(e).__name__}: {str(e)}")

            # Show progress
            progress = (i / len(self.video_urls)) * 100
            print(f"📊 Progress: {i}/{len(self.video_urls)} videos ({progress:.1f}%)")

        self.parsed_video_data = parsed_videos

        print(f"\n🎯 Video parsing completed!")
        print(f"🐛 DEBUG: Final results:")
        print(f"🐛 DEBUG:   URLs found: {len(self.video_urls)}")
        print(f"🐛 DEBUG:   Videos parsed: {len(self.parsed_video_data)}")
        success_rate = (len(self.parsed_video_data)/len(self.video_urls)*100) if self.video_urls else 0.0
        print(f"🐛 DEBUG:   Success rate: {success_rate:.1f}%")

        return self.parsed_video_data
