
import asyncio
import json
import re
from datetime import datetime
from html import unescape
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
from lxml import html
from typing import Optional, List, Dict, Any

class OptimizedVideoDataParser:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.video_urls = []
        self.parsed_video_data = []
        print(f"ðŸ› DEBUG: Initializing parser for URL: {base_url}")

    async def handle_age_verification(self, page) -> bool:
        """Handle age verification popup if it appears - FROM NEW PARSER"""
        try:
            print("ðŸ” Checking for age verification popup...")
            await page.wait_for_timeout(2000)

            popup_selector = '.popup.popup_access'
            popup = await page.query_selector(popup_selector)

            if popup:
                is_visible = await page.evaluate("""(element) => {
                    const style = window.getComputedStyle(element);
                    return style.display !== 'none' && style.visibility !== 'hidden';
                }""", popup)

                if is_visible:
                    print("âš ï¸ Age verification popup detected!")
                    continue_button = await page.query_selector('input[name="continue"]')
                    if continue_button:
                        print("ðŸ–±ï¸ Clicking Continue button...")
                        await continue_button.click()
                        await page.wait_for_timeout(3000)
                        try:
                            await page.wait_for_selector('#custom_list_videos_most_recent_videos_items', timeout=10000)
                            print("âœ… Age verification bypassed successfully!")
                            return True
                        except:
                            print("âš ï¸ Content took longer to load, continuing anyway...")
                            return True
                    else:
                        print("âŒ Continue button not found!")
                        return False
                else:
                    print("â„¹ï¸ Age popup exists but is hidden")
                    return True
            else:
                print("â„¹ï¸ No age verification popup found")
                return True

        except Exception as e:
            print(f"âš ï¸ Error handling age verification: {e}")
            return True

    async def extract_video_urls(self) -> List[str]:
        """Extract video URLs from main page - FROM OLD PARSER WITH NEW ENHANCEMENTS"""
        print(f"ðŸ› DEBUG: Starting video URL extraction from {self.base_url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                print(f"ðŸš€ Loading main page: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')

                # NEW FEATURE: Handle age verification
                await self.handle_age_verification(page)
                await page.wait_for_timeout(3000)

                # OLD PARSER: Extract using XPath
                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'
                print(f"ðŸ› DEBUG: Trying primary selector: {xpath}")
                video_elements = await page.query_selector_all(f'xpath={xpath}')

                # NEW FEATURE: Try alternative selectors if primary fails
                if len(video_elements) == 0:
                    print("âš ï¸ No videos found with primary selector, trying alternatives...")
                    alternative_selectors = ['.video-item', '[class*="video"]', '.th.js-open-popup', 'a[href*="/video"]']
                    for selector in alternative_selectors:
                        print(f"ðŸ› DEBUG: Trying alternative selector: {selector}")
                        video_elements = await page.query_selector_all(selector)
                        if video_elements:
                            print(f"âœ… Found {len(video_elements)} videos with selector: {selector}")
                            break

                print(f"ðŸ“¹ Found {len(video_elements)} video elements")

                video_urls = []
                for i, element in enumerate(video_elements):
                    try:
                        print(f"ðŸ› DEBUG: Processing video element {i+1}/{len(video_elements)}")

                        # OLD PARSER: Primary link selector
                        link_element = await element.query_selector('a.th.js-open-popup')
                        if not link_element:
                            # NEW FEATURE: Alternative selectors
                            link_element = await element.query_selector('a[href*="/video"]')
                        if not link_element:
                            link_element = await element.query_selector('a')

                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                full_url = urljoin(self.base_url, href)
                                video_urls.append(full_url)
                                print(f"ðŸ› DEBUG: Video {i+1} URL: {full_url}")
                            else:
                                print(f"ðŸ› DEBUG: Video {i+1} - No href attribute found")
                        else:
                            print(f"ðŸ› DEBUG: Video {i+1} - No link element found")
                    except Exception as e:
                        print(f"âš ï¸ Error extracting URL from video {i+1}: {e}")

                self.video_urls = video_urls
                print(f"ðŸŽ¯ Extracted {len(self.video_urls)} video URLs")

                # Enhanced debugging from NEW parser
                if self.video_urls:
                    print(f"ðŸ› DEBUG: Sample URLs (first 3):")
                    for i, url in enumerate(self.video_urls[:3]):
                        print(f"  {i+1}: {url}")

            except Exception as e:
                print(f"âŒ Error loading main page: {e}")
                print(f"ðŸ› DEBUG: Exception details: {type(e).__name__}: {str(e)}")
            finally:
                await browser.close()

        return self.video_urls

    def extract_json_ld_data(self, html_content: str) -> Optional[Dict[str, Any]]:
        """Extract data from JSON-LD script tag - FROM OLD PARSER"""
        try:
            json_ld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
            match = re.search(json_ld_pattern, html_content, re.DOTALL | re.IGNORECASE)

            if match:
                json_str = match.group(1)
                json_str = json_str.replace(r'\/', '/')
                json_data = json.loads(json_str)

                if json_data.get('@type') == 'VideoObject':
                    return json_data
                else:
                    print("âš ï¸ JSON-LD found but not VideoObject type")
                    return None
            else:
                print("âš ï¸ No JSON-LD script tag found")
                return None

        except json.JSONDecodeError as e:
            print(f"âš ï¸ JSON parsing error: {e}")
            return None
        except Exception as e:
            print(f"âš ï¸ Error extracting JSON-LD: {e}")
            return None

    def extract_title(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract title with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD name field
            if json_ld_data and 'name' in json_ld_data:
                title = json_ld_data['name'].strip()
                if title:
                    return title

            # Priority 2: HTML title tag
            title_match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.IGNORECASE)
            if title_match:
                title = unescape(title_match.group(1)).strip()
                if title and title != "":
                    return title

            # Priority 3: Meta tags
            meta_title_match = re.search(
                r'<meta\s+(?:property=["\']og:title["\']\s+content|name=["\']title["\']\s+content)=["\']([^"\']*)["\']',
                html_content,
                re.IGNORECASE
            )
            if meta_title_match:
                title = unescape(meta_title_match.group(1)).strip()
                if title:
                    return title

            print("âš ï¸ Title not found, using default")
            return "Unknown Title"

        except Exception as e:
            print(f"âš ï¸ Error extracting title: {e}")
            return "Unknown Title"
    def extract_description(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract description with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD description field
            if json_ld_data and 'description' in json_ld_data:
                description = json_ld_data['description'].strip()
                if description:
                    return description

            # Priority 2: Meta description tags
            meta_desc_match = re.search(
                r'<meta\s+(?:name=["\']description["\']\s+content|property=["\']og:description["\']\s+content)=["\']([^"\']*)["\']',
                html_content,
                re.IGNORECASE
            )
            if meta_desc_match:
                description = unescape(meta_desc_match.group(1)).strip()
                if description:
                    return description

            # Priority 3: Generic description patterns
            desc_patterns = [
                r'<div[^>]*class="[^"]*description[^"]*"[^>]*>([^<]*)</div>',
                r'<p[^>]*class="[^"]*desc[^"]*"[^>]*>([^<]*)</p>'
            ]

            for pattern in desc_patterns:
                desc_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if desc_match:
                    description = unescape(desc_match.group(1)).strip()
                    if description and len(description) > 10:
                        return description

            print("âš ï¸ Description not found, setting empty")
            return ""

        except Exception as e:
            print(f"âš ï¸ Error extracting description: {e}")
            return ""

    def extract_thumbnail_url(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract thumbnail URL with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD thumbnailUrl field
            if json_ld_data and 'thumbnailUrl' in json_ld_data:
                thumbnail_url = json_ld_data['thumbnailUrl']
                if thumbnail_url and thumbnail_url.startswith('http'):
                    return thumbnail_url

            # Priority 2: Video poster attribute
            poster_match = re.search(r'poster="([^"]*)"', html_content)
            if poster_match:
                poster_url = poster_match.group(1)
                if poster_url and poster_url.startswith('http'):
                    return poster_url

            # Priority 3: Meta image tags
            meta_image_match = re.search(r'<meta[^>]*property="og:image"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
            if meta_image_match:
                image_url = meta_image_match.group(1)
                if image_url and image_url.startswith('http'):
                    return image_url

            # Priority 4: Link rel image_src
            link_image_match = re.search(r'<link[^>]*rel="image_src"[^>]*href="([^"]*)"', html_content)
            if link_image_match:
                image_url = link_image_match.group(1)
                if image_url and image_url.startswith('http'):
                    return image_url

            print("âš ï¸ Thumbnail not found, setting empty")
            return ""

        except Exception as e:
            print(f"âš ï¸ Error extracting thumbnail: {e}")
            return ""

    def extract_views(self, json_ld_data: Optional[Dict], html_content: str) -> int:
        """Extract view count with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD interactionStatistic
            if json_ld_data and 'interactionStatistic' in json_ld_data:
                interaction_stats = json_ld_data['interactionStatistic']
                if isinstance(interaction_stats, list):
                    for stat in interaction_stats:
                        if stat.get('interactionType') == 'http://schema.org/WatchAction':
                            view_count = stat.get('userInteractionCount')
                            if view_count:
                                return int(view_count)

            # Priority 2: HTML patterns like "1.2K (1,194)"
            views_pattern = r'([0-9.,KM]+)\s*\(([0-9,]+)\)'
            views_match = re.search(views_pattern, html_content)
            if views_match:
                full_number = views_match.group(2).replace(',', '')
                if full_number.isdigit():
                    return int(full_number)

            # Priority 3: Simple number patterns
            simple_views_patterns = [
                r'([0-9,]+)\s*views?',
                r'Views?:\s*([0-9,]+)',
                r'([0-9,]+)\s*Views?'
            ]

            for pattern in simple_views_patterns:
                views_match = re.search(pattern, html_content, re.IGNORECASE)
                if views_match:
                    views_str = views_match.group(1).replace(',', '')
                    if views_str.isdigit():
                        return int(views_str)

            print("âš ï¸ Views not found, setting to 0")
            return 0

        except Exception as e:
            print(f"âš ï¸ Error extracting views: {e}")
            return 0

    def extract_duration(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract duration with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD duration field
            if json_ld_data and 'duration' in json_ld_data:
                duration = json_ld_data['duration']
                if duration:
                    return self.convert_iso_duration_to_readable(duration)

            # Priority 2: HTML duration patterns
            duration_patterns = [
                r'duration="([^"]*)"',
                r"duration='([^']*)'",
                r'<time[^>]*duration[^>]*>([^<]*)</time>',
                r'([0-9]+:[0-9]+(?::[0-9]+)?)'  # MM:SS or HH:MM:SS format
            ]

            for pattern in duration_patterns:
                duration_match = re.search(pattern, html_content, re.IGNORECASE)
                if duration_match:
                    duration_str = duration_match.group(1)
                    # If it's already in readable format, return it
                    if re.match(r'^[0-9]+:[0-9]+(?::[0-9]+)?$', duration_str):
                        return duration_str
                    # If it's ISO format, convert it
                    elif duration_str.startswith('PT'):
                        return self.convert_iso_duration_to_readable(duration_str)

            print("âš ï¸ Duration not found, setting to Unknown")
            return "Unknown"

        except Exception as e:
            print(f"âš ï¸ Error extracting duration: {e}")
            return "Unknown"

    def extract_upload_date(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract upload date with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD uploadDate or datePublished
            if json_ld_data:
                for date_field in ['uploadDate', 'datePublished', 'dateCreated']:
                    if date_field in json_ld_data:
                        date_str = json_ld_data[date_field]
                        if date_str:
                            try:
                                # Parse ISO format date
                                parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                return parsed_date.strftime('%Y-%m-%d')
                            except:
                                return date_str

            # Priority 2: HTML meta tags
            date_patterns = [
                r'<meta[^>]*property="article:published_time"[^>]*content="([^"]*)"',
                r'<meta[^>]*name="publish_date"[^>]*content="([^"]*)"',
                r'<time[^>]*datetime="([^"]*)"[^>]*>',
                r'datePublished="([^"]*)"'
            ]

            for pattern in date_patterns:
                date_match = re.search(pattern, html_content, re.IGNORECASE)
                if date_match:
                    date_str = date_match.group(1)
                    try:
                        parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        return parsed_date.strftime('%Y-%m-%d')
                    except:
                        # Try other common formats
                        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                            try:
                                parsed_date = datetime.strptime(date_str, fmt)
                                return parsed_date.strftime('%Y-%m-%d')
                            except:
                                continue

            print("âš ï¸ Upload date not found, setting to Unknown")
            return "Unknown"

        except Exception as e:
            print(f"âš ï¸ Error extracting upload date: {e}")
            return "Unknown"

    def convert_iso_duration_to_readable(self, iso_duration: str) -> str:
        """Convert ISO 8601 duration to readable format - FROM OLD PARSER"""
        try:
            # Parse ISO 8601 duration (e.g., "PT4M33S" -> "4:33")
            match = re.match(r'PT(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?', iso_duration)
            if match:
                hours = int(match.group(1)) if match.group(1) else 0
                minutes = int(match.group(2)) if match.group(2) else 0
                seconds = int(match.group(3)) if match.group(3) else 0

                if hours > 0:
                    return f"{hours}:{minutes:02d}:{seconds:02d}"
                else:
                    return f"{minutes}:{seconds:02d}"

            return iso_duration

        except Exception as e:
            print(f"âš ï¸ Error converting duration: {e}")
            return iso_duration

    def extract_player_data(self, html_content: str) -> str:
        """Extract video source URL - FROM OLD PARSER"""
        try:
            # Pattern 1: Direct video URL in various contexts
            video_patterns = [
                r'videoUrl="([^"]*)"',
                r'video_url="([^"]*)"',
                r'src="([^"]*.(?:mp4|webm|ogg|avi|mov))"',
                r'<video[^>]*src="([^"]*)"',
                r'<source[^>]*src="([^"]*)"'
            ]

            for pattern in video_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    video_url = match.group(1)
                    if video_url and (video_url.startswith('http') or video_url.startswith('//')):
                        return video_url

            # Pattern 2: Look in JavaScript variables
            js_patterns = [
                r'var\s+videoUrl\s*=\s*"([^"]*)"',
                r'let\s+videoUrl\s*=\s*"([^"]*)"',
                r'const\s+videoUrl\s*=\s*"([^"]*)"'
            ]

            for pattern in js_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    video_url = match.group(1)
                    if video_url and (video_url.startswith('http') or video_url.startswith('//')):
                        return video_url

            print("âš ï¸ Video source not found")
            return ""

        except Exception as e:
            print(f"âš ï¸ Error extracting video source: {e}")
            return ""

    def extract_tags_from_html(self, html_content):
        """Extract tags from the HTML content"""
        try:
            tags = []
            # Find tags in the tag items
            tag_pattern = r'<a class="tag_item" href="[^"]*">([^<]+)</a>'
            tag_matches = re.findall(tag_pattern, html_content)
            tags = [tag.strip() for tag in tag_matches if tag.strip()]
            return tags
        except Exception as e:
            print(f"⚠️ Error extracting tags: {e}")
            return []

    def extract_categories_from_html(self, html_content):
        """Extract categories from the HTML content - FIXED VERSION"""
        try:
            categories = []
            
            # Find the cols div using the same method as artist extraction
            cols_start = html_content.find('<div class="cols">')
            if cols_start == -1:
                return categories
            
            # Find the content after the opening tag
            content_start = cols_start + len('<div class="cols">')
            
            # Count opening and closing div tags to find the matching closing tag
            div_count = 1
            pos = content_start
            cols_end = -1
            
            while pos < len(html_content) and div_count > 0:
                # Find next div tag (opening or closing)
                next_open = html_content.find('<div', pos)
                next_close = html_content.find('</div>', pos)
                
                if next_close == -1:
                    break
                
                if next_open != -1 and next_open < next_close:
                    # Found opening div first
                    div_count += 1
                    pos = next_open + 4
                else:
                    # Found closing div first  
                    div_count -= 1
                    if div_count == 0:
                        cols_end = next_close
                        break
                    pos = next_close + 6
            
            if cols_end == -1:
                return categories
            
            # Extract the full cols content
            cols_content = html_content[content_start:cols_end]
            
            # Look for Categories section
            category_match = re.search(r'<div class="label">Categories</div>(.*?)(?=<div class="col">|</div>\s*$)', cols_content, re.DOTALL | re.IGNORECASE)
            if category_match:
                categories_section = category_match.group(1)
                
                # Extract category names from span tags within the categories section
                # Pattern: <span>Category Name</span>
                category_spans = re.findall(r'<span>([^<]+)</span>', categories_section, re.IGNORECASE)
                categories = [cat.strip() for cat in category_spans if cat.strip()]
            
            return categories
            
        except Exception as e:
            print(f"⚠️ Error extracting categories: {e}")
            return []
        



    def get_download_links_from_html(self, html_content: str) -> List[str]:
        """Extract download links - FROM OLD PARSER"""
        try:
            download_links = []

            # Pattern 1: Direct download links
            download_patterns = [
                r'href="([^"]*download[^"]*)"',
                r'download_url="([^"]*)"',
                r'src="([^"]*.(?:mp4|webm|avi|mov|flv)[^"]*)"'
            ]

            for pattern in download_patterns:
                matches = re.finditer(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    url = match.group(1)
                    if url and (url.startswith('http') or url.startswith('//')):
                        download_links.append(url)

            return download_links

        except Exception as e:
            print(f"âš ï¸ Error extracting download links: {e}")
            return []

    def find_highest_quality_download_url(self, html_content: str) -> str:
        """Find the highest quality video download URL - FROM OLD PARSER"""
        try:
            # First try player data extraction
            player_url = self.extract_player_data(html_content)
            if player_url:
                return player_url

            # Then try download links
            download_links = self.get_download_links_from_html(html_content)
            if download_links:
                # Prefer mp4 format and highest quality indicators
                quality_priorities = ['1080p', '720p', '480p', '360p', 'high', 'medium', 'low']
                format_priorities = ['.mp4', '.webm', '.avi', '.mov']

                best_link = None
                best_score = -1

                for link in download_links:
                    score = 0
                    link_lower = link.lower()

                    # Quality scoring
                    for i, quality in enumerate(quality_priorities):
                        if quality in link_lower:
                            score += (len(quality_priorities) - i) * 10
                            break

                    # Format scoring
                    for i, fmt in enumerate(format_priorities):
                        if fmt in link_lower:
                            score += (len(format_priorities) - i)
                            break

                    if score > best_score:
                        best_score = score
                        best_link = link

                return best_link if best_link else download_links[0]

            print("âš ï¸ Video source not found, setting empty")
            return ""

        except Exception as e:
            print(f"âš ï¸ Error finding highest quality download URL: {e}")
            return ""

    def extract_video_id_from_url(self, video_url: str) -> str:
        """Extract video ID from URL - ENHANCED FROM BOTH PARSERS"""
        try:
            # OLD PARSER: Primary pattern for /video/{id}/
            video_id_match = re.search(r'/video/(\d+)/', video_url)
            if video_id_match:
                return video_id_match.group(1)

            # NEW FEATURE: Fallback patterns
            # Try to extract from end of URL
            parsed_url = urlparse(video_url)
            path_parts = [part for part in parsed_url.path.split('/') if part]

            # Look for numeric parts
            for part in reversed(path_parts):
                if part.isdigit():
                    return part

            # Fallback: use last part of URL path
            if path_parts:
                return re.sub(r'[^\w\-_]', '', path_parts[-1])

            print("âš ï¸ Could not extract video_id, using 'unknown'")
            return "unknown"

        except Exception as e:
            print(f"âš ï¸ Error extracting video ID: {e}")
            return "unknown"


    def extract_artist_and_uploader(self, html_content):
        """Extract artist and uploader from the cols structure - FIXED VERSION"""
        try:
            artist_name = "Anonymous"
            uploader_name = "Anonymous"

            # Find the cols div using a more robust method
            cols_start = html_content.find('<div class="cols">')
            if cols_start == -1:
                return artist_name, uploader_name

            # Find the content after the opening tag
            content_start = cols_start + len('<div class="cols">')

            # Count opening and closing div tags to find the matching closing tag
            div_count = 1
            pos = content_start
            cols_end = -1

            while pos < len(html_content) and div_count > 0:
                # Find next div tag (opening or closing)
                next_open = html_content.find('<div', pos)
                next_close = html_content.find('</div>', pos)

                if next_close == -1:
                    break

                if next_open != -1 and next_open < next_close:
                    # Found opening div first
                    div_count += 1
                    pos = next_open + 4
                else:
                    # Found closing div first  
                    div_count -= 1
                    if div_count == 0:
                        cols_end = next_close
                        break
                    pos = next_close + 6

            if cols_end == -1:
                return artist_name, uploader_name

            # Extract the full cols content
            cols_content = html_content[content_start:cols_end]

            # Look for Artist section
            artist_match = re.search(r'<div class="label">Artist</div>(.*?)(?=<div class="col">|</div>\s*$)', cols_content, re.DOTALL | re.IGNORECASE)
            if artist_match:
                artist_section = artist_match.group(1)

                # Look for span with class "name"
                artist_name_match = re.search(r'<span class="name">([^<]+)</span>', artist_section, re.IGNORECASE)
                if artist_name_match:
                    artist_name = artist_name_match.group(1).strip()
                else:
                    # Fallback: look for alt attribute
                    artist_alt_match = re.search(r'alt="([^"]+)"', artist_section)
                    if artist_alt_match:
                        artist_name = artist_alt_match.group(1).strip()

            # Look for Uploaded by section
            uploader_match = re.search(r'<div class="label">Uploaded by</div>(.*?)(?=<div class="col">|</div>\s*$)', cols_content, re.DOTALL | re.IGNORECASE)
            if uploader_match:
                uploader_section = uploader_match.group(1)

                # Remove image tags and verified status divs first
                clean_section = re.sub(r'<img[^>]*>', '', uploader_section)
                clean_section = re.sub(r'<div class="verified-status">.*?</div>', '', clean_section, flags=re.DOTALL)

                # Look for text content within the anchor tag
                uploader_text_match = re.search(r'<a[^>]*>(.*?)</a>', clean_section, re.DOTALL | re.IGNORECASE)
                if uploader_text_match:
                    uploader_text = uploader_text_match.group(1)
                    # Clean up whitespace, newlines, and remaining HTML
                    uploader_text = re.sub(r'<[^>]*>', '', uploader_text)  # Remove any remaining HTML
                    uploader_name = re.sub(r'\s+', ' ', uploader_text).strip()

                    # If still empty, try alt attribute
                    if not uploader_name:
                        uploader_alt_match = re.search(r'alt="([^"]+)"', uploader_section)
                        if uploader_alt_match:
                            uploader_name = uploader_alt_match.group(1).strip()

            return artist_name, uploader_name

        except Exception as e:
            print(f"⚠️ Error extracting artist/uploader: {e}")
            return "Anonymous", "Anonymous"


    async def parse_individual_video(self, video_url):
        """Parse individual video page using optimized JSON-LD extraction"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                print(f"🔍 Parsing: {video_url}")
                await page.goto(video_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(2000)
                
                html_content = await page.content()
                
                # Extract video ID from URL
                video_id_match = re.search(r'/video/(\d+)/', video_url)
                video_id = video_id_match.group(1) if video_id_match else "unknown"
                
                # Extract JSON-LD data first (most reliable source)
                json_ld_data = self.extract_json_ld_data(html_content)
                
                # Extract all data fields with fallbacks
                title = self.extract_title(json_ld_data, html_content)
                description = self.extract_description(json_ld_data, html_content)
                thumbnail_src = self.extract_thumbnail_url(json_ld_data, html_content)
                views = self.extract_views(json_ld_data, html_content)
                duration = self.extract_duration(json_ld_data, html_content)
                upload_date = self.extract_upload_date(json_ld_data, html_content)
                
                # Extract other data (unchanged from original)
                video_src = self.find_highest_quality_download_url(html_content)

                artist_name, uploader_name = self.extract_artist_and_uploader(html_content)
                tags = self.extract_tags_from_html(html_content)
                categories = self.extract_categories_from_html(html_content)
                
                video_data = {
                    "video_id": video_id,
                    "url": video_url,
                    "title": title,
                    "duration": duration,
                    "views": views,
                    "tags": tags,
                    "categories": categories,
                    "description": description,
                    "thumbnail_src": thumbnail_src,
                    "video_src": video_src or "",
                    "uploaded_by": uploader_name,
                    "artists": [artist_name] if artist_name != "Anonymous" else [],
                    "upload_date": upload_date
                }
                
                print(f"✅ Successfully parsed: {title}")
                return video_data
                
            except Exception as e:
                print(f"❌ Error parsing video {video_url}: {e}")
                return None
            finally:
                await browser.close()



    async def parse_single_video(self, video_url: str) -> Optional[Dict[str, Any]]:
        """Parse single video - COMPATIBILITY METHOD FOR PAGE_PARSER"""
        return await self.parse_individual_video(video_url)

    async def parse_all_videos(self, max_videos: Optional[int] = None) -> List[Dict[str, Any]]:
        """Parse all extracted video URLs - FROM OLD PARSER"""
        if not self.video_urls:
            print("âš ï¸ No video URLs found. Run extract_video_urls() first.")
            return []

        videos_to_parse = self.video_urls[:max_videos] if max_videos else self.video_urls
        print(f"ðŸŽ¯ Starting to parse {len(videos_to_parse)} videos...")

        parsed_videos = []

        for i, video_url in enumerate(videos_to_parse):
            try:
                print(f"\nðŸ“¹ Processing video {i+1}/{len(videos_to_parse)}: {video_url}")
                video_data = await self.parse_individual_video(video_url)

                if video_data:
                    parsed_videos.append(video_data)
                    print(f"âœ… Successfully parsed video {i+1}: {video_data.get('title', 'Unknown')}")
                else:
                    print(f"âŒ Failed to parse video {i+1}")

            except Exception as e:
                print(f"âŒ Error processing video {i+1} ({video_url}): {e}")
                continue

        self.parsed_video_data = parsed_videos
        print(f"\nðŸŽ‰ Completed! Successfully parsed {len(parsed_videos)} out of {len(videos_to_parse)} videos")

        return parsed_videos

    def save_data(self, filename: str = "video_data.json") -> bool:
        """Save parsed video data to JSON file - FROM OLD PARSER"""
        try:
            if not self.parsed_video_data:
                print("âš ï¸ No parsed video data to save.")
                return False

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.parsed_video_data, f, indent=2, ensure_ascii=False)

            print(f"ðŸ’¾ Saved {len(self.parsed_video_data)} videos to {filename}")
            print(f"ðŸ› DEBUG: Folder structure should be: downloads/page_{{page}}/{'{video_id}'}/metadata.json")
            return True

        except Exception as e:
            print(f"âŒ Error saving data: {e}")
            return False

# Example usage and main function - FROM OLD PARSER
async def main():
    """Main function to demonstrate usage"""
    # Replace with your target URL
    base_url = "https://rule34video.com/latest-updates"

    parser = OptimizedVideoDataParser(base_url)

    try:
        # Step 1: Extract video URLs from main page
        print("ðŸš€ Step 1: Extracting video URLs...")
        video_urls = await parser.extract_video_urls()

        if not video_urls:
            print("âŒ No video URLs found!")
            return

        print(f"âœ… Found {len(video_urls)} video URLs")

        # Step 2: Parse individual videos (limit to first 5 for testing)
        print("\nðŸ” Step 2: Parsing individual videos...")
        parsed_videos = await parser.parse_all_videos(max_videos=5)

        if parsed_videos:
            # Step 3: Save data
            print("\nðŸ’¾ Step 3: Saving data...")
            parser.save_data("parsed_videos.json")

            # Display summary with OLD PARSER structure
            print("\nðŸ“Š Summary:")
            for i, video in enumerate(parsed_videos[:3], 1):
                print(f"  {i}. {video.get('title', 'Unknown')}")
                print(f"     Video ID: {video.get('video_id', 'Unknown')} (folder name)")
                print(f"     Views: {video.get('views', 'Unknown')}")
                print(f"     Duration: {video.get('duration', 'Unknown')}")
                print(f"     Categories: {video.get('categories', [])}")
                print(f"     Description: {video.get('description', 'Unknown')[:50]}...")
                print(f"     Upload Date: {video.get('upload_date', 'Unknown')}")
                print(f"     Folder: downloads/page_X/{video.get('video_id', 'unknown')}/")

    except Exception as e:
        print(f"âŒ Error in main execution: {e}")

if __name__ == "__main__":
    asyncio.run(main())