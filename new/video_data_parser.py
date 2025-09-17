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

    async def extract_video_urls(self):
        """Extract video URLs from main page"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                print(f"🚀 Loading main page: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(3000)

                # Extract using XPath
                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'
                video_elements = await page.query_selector_all(f'xpath={xpath}')
                print(f"📹 Found {len(video_elements)} video elements")

                video_urls = []
                for i, element in enumerate(video_elements):
                    try:
                        link_element = await element.query_selector('a.th.js-open-popup')
                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                full_url = urljoin(self.base_url, href)
                                video_urls.append(full_url)
                    except Exception as e:
                        print(f"⚠️ Error extracting URL from video {i+1}: {e}")

                self.video_urls = video_urls
                print(f"🎯 Extracted {len(self.video_urls)} video URLs")
            except Exception as e:
                print(f"❌ Error loading main page: {e}")
            finally:
                await browser.close()

        return self.video_urls

    def extract_json_ld_data(self, html_content):
        """Extract data from JSON-LD script tag with robust error handling"""
        try:
            # Find JSON-LD script tag with VideoObject
            json_ld_pattern = r'<script type="application/ld\+json">\s*({.*?})\s*</script>'
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
            title_match = re.search(r'<title>(.*?)</title>', html_content, re.IGNORECASE)
            if title_match:
                title = title_match.group(1).strip()
                if title and title != 'Unknown':
                    return title

            # Priority 3: H1 tag
            h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html_content, re.DOTALL | re.IGNORECASE)
            if h1_match:
                title = re.sub(r'<[^>]+>', '', h1_match.group(1)).strip()
                if title:
                    return title

            return "Unknown"
        except Exception as e:
            print(f"⚠️ Error extracting title: {e}")
            return "Unknown"

    def extract_description(self, json_ld_data, html_content):
        """Extract description with fallback methods"""
        try:
            # Priority 1: JSON-LD description field
            if json_ld_data and 'description' in json_ld_data:
                description = json_ld_data['description']
                if description:
                    # Clean up HTML entities and formatting
                    description = unescape(description)
                    description = description.replace('\\r\\n', '\n').replace('\r\n', '\n')
                    description = description.replace('\\n', '\n')
                    return description.strip()

            # Priority 2: Meta description tag
            meta_desc_match = re.search(
                r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']*)["\']', 
                html_content, 
                re.IGNORECASE
            )
            if meta_desc_match:
                description = meta_desc_match.group(1).strip()
                if description:
                    return unescape(description)

            return ""
        except Exception as e:
            print(f"⚠️ Error extracting description: {e}")
            return ""

    def extract_thumbnail_url(self, json_ld_data, html_content):
        """Extract thumbnail URL with fallback methods"""
        try:
            # Priority 1: JSON-LD thumbnailUrl field
            if json_ld_data and 'thumbnailUrl' in json_ld_data:
                thumbnail_url = json_ld_data['thumbnailUrl']
                if thumbnail_url and thumbnail_url.startswith('http'):
                    return thumbnail_url

            # Priority 2: Video poster attribute
            poster_match = re.search(r'poster=["\']([^"\']*)["\']', html_content)
            if poster_match:
                poster_url = poster_match.group(1)
                if poster_url and poster_url.startswith('http'):
                    return poster_url

            # Priority 3: Meta image tags
            meta_image_match = re.search(
                r'<meta\s+(?:property=["\']og:image["\']\s+content|name=["\']twitter:image["\']\s+content)=["\']([^"\']*)["\']',
                html_content,
                re.IGNORECASE
            )
            if meta_image_match:
                image_url = meta_image_match.group(1)
                if image_url and image_url.startswith('http'):
                    return image_url

            return ""
        except Exception as e:
            print(f"⚠️ Error extracting thumbnail: {e}")
            return ""

    def extract_views(self, json_ld_data, html_content):
        """Extract view count with fallback methods"""
        try:
            # Priority 1: JSON-LD interactionStatistic
            if json_ld_data and 'interactionStatistic' in json_ld_data:
                interaction_stats = json_ld_data['interactionStatistic']
                for stat in interaction_stats:
                    if stat.get('interactionType') == 'http://schema.org/WatchAction':
                        view_count = stat.get('userInteractionCount')
                        if view_count:
                            return int(view_count)

            # Priority 2: HTML patterns like "1.2K (1,194)"
            views_pattern = r'([0-9.,KM]+)\s*\(([0-9,]+)\)'
            views_match = re.search(views_pattern, html_content)
            if views_match:
                # Use the number in parentheses (more accurate)
                views_str = views_match.group(2).replace(',', '')
                return int(views_str)

            # Priority 3: Simple parentheses pattern
            fallback_pattern = r'\(([0-9,]+)\)'
            fallback_match = re.search(fallback_pattern, html_content)
            if fallback_match:
                views_str = fallback_match.group(1).replace(',', '')
                return int(views_str)

            return 0
        except Exception as e:
            print(f"⚠️ Error extracting views: {e}")
            return 0

    def extract_duration(self, json_ld_data, html_content):
        """Extract duration with fallback methods"""
        try:
            # Priority 1: JSON-LD duration field (ISO 8601 format)
            if json_ld_data and 'duration' in json_ld_data:
                iso_duration = json_ld_data['duration']
                if iso_duration:
                    readable_duration = self.convert_iso_duration_to_readable(iso_duration)
                    if readable_duration != "Unknown":
                        return readable_duration

            # Priority 2: HTML patterns like "6:48"
            duration_pattern = r'(\d+:\d{2})'
            duration_match = re.search(duration_pattern, html_content)
            if duration_match:
                return duration_match.group(1)

            # Priority 3: Time elements with custom classes
            time_element_pattern = r'<[^>]*(?:class|id)=["\'][^"\']*(?:duration|time)[^"\']*["\'][^>]*>(\d+:\d{2})</[^>]*>'
            time_element_match = re.search(time_element_pattern, html_content, re.IGNORECASE)
            if time_element_match:
                return time_element_match.group(1)

            return "Unknown"
        except Exception as e:
            print(f"⚠️ Error extracting duration: {e}")
            return "Unknown"

    def extract_upload_date(self, json_ld_data, html_content):
        """Extract upload date"""
        try:
            # JSON-LD uploadDate field
            if json_ld_data and 'uploadDate' in json_ld_data:
                upload_date = json_ld_data['uploadDate']
                if upload_date:
                    return upload_date
            return None
        except Exception as e:
            print(f"⚠️ Error extracting upload date: {e}")
            return None

    def convert_iso_duration_to_readable(self, iso_duration):
        """Convert ISO 8601 duration (PT0H6M48S) to readable format (6:48)"""
        try:
            if not iso_duration or not iso_duration.startswith('PT'):
                return "Unknown"

            # Remove PT prefix
            duration_str = iso_duration[2:]
            hours = 0
            minutes = 0
            seconds = 0

            # Extract hours
            hours_match = re.search(r'(\d+)H', duration_str)
            if hours_match:
                hours = int(hours_match.group(1))

            # Extract minutes
            minutes_match = re.search(r'(\d+)M', duration_str)
            if minutes_match:
                minutes = int(minutes_match.group(1))

            # Extract seconds
            seconds_match = re.search(r'(\d+)S', duration_str)
            if seconds_match:
                seconds = int(seconds_match.group(1))

            # Format as H:MM:SS or M:SS
            if hours > 0:
                return f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                return f"{minutes}:{seconds:02d}"
        except Exception as e:
            print(f"⚠️ Error converting duration: {e}")
            return "Unknown"

    def extract_player_data(self, html_content):
        """Extract video URLs from player flashvars - UNCHANGED FROM ORIGINAL"""
        try:
            # Find the flashvars object in the player script
            flashvars_pattern = r'var flashvars\s*=\s*\{([^}]+)\}'
            match = re.search(flashvars_pattern, html_content, re.DOTALL)

            if match:
                flashvars_content = match.group(1)
                # Extract video URLs with different qualities
                video_urls = {}

                # Extract video_url (usually 360p)
                video_url_match = re.search(r"video_url\s*:\s*['\"]([^'\"]+)['\"]", flashvars_content)
                if video_url_match:
                    url = video_url_match.group(1)
                    if url.startswith('function/0/'):
                        url = url[11:]
                    video_urls['360p'] = url

                # Extract video_alt_url (usually 480p)
                alt_url_match = re.search(r"video_alt_url\s*:\s*['\"]([^'\"]+)['\"]", flashvars_content)
                if alt_url_match:
                    url = alt_url_match.group(1)
                    if url.startswith('function/0/'):
                        url = url[11:]
                    video_urls['480p'] = url

                # Extract video_alt_url2 (usually 720p)
                alt_url2_match = re.search(r"video_alt_url2\s*:\s*['\"]([^'\"]+)['\"]", flashvars_content)
                if alt_url2_match:
                    url = alt_url2_match.group(1)
                    if url.startswith('function/0/'):
                        url = url[11:]
                    video_urls['720p'] = url

                # Return the highest quality available
                for quality in ['720p', '480p', '360p']:
                    if quality in video_urls and video_urls[quality]:
                        return video_urls[quality]

            return None
        except Exception as e:
            print(f"⚠️ Error parsing player data: {e}")
            return None

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

    def get_download_links_from_html(self, html_content):
        """
        Parse all download links inside <div class="label">Download</div> and their qualities.
        Returns a list of dict: [{'url': ..., 'quality': 1080}, ...]
        """
        tree = html.fromstring(html_content)
        download_divs = tree.xpath("//div[contains(@class,'label') and text()='Download']")
        links = []
        for d in download_divs:
            parent = d.getparent()
            if parent is None:
                continue
            anchor_tags = parent.xpath(".//a[contains(@href, '.mp4')]")
            for a in anchor_tags:
                url = a.attrib.get('href')
                if not url:
                    continue
                quality_match = re.search(r'_(\d+)p\.mp4', url)
                quality = int(quality_match.group(1)) if quality_match else 0
                links.append({'url': url, 'quality': quality})
        return links


    def find_highest_quality_download_url(self, html_content):
        """
        Returns the highest quality mp4 download url from the Download section,
        with &download=true removed.
        """
        links = self.get_download_links_from_html(html_content)
        if not links:
            return None
        # Sort by quality descending
        links_sorted = sorted(links, key=lambda x: x['quality'], reverse=True)
        best = links_sorted[0]['url']
        # Remove "&download=true", if present
        best_cleaned = re.sub(r"(\&|\?)download=true(\&)?", r"\1", best).rstrip("&?")
        return best_cleaned

    async def parse_all_videos(self):
        """Parse all videos in the list"""
        print(f"🚀 Starting to parse {len(self.video_urls)} videos...")
        
        for i, video_url in enumerate(self.video_urls):
            print(f"\n📹 Progress: {i+1}/{len(self.video_urls)}")
            video_data = await self.parse_individual_video(video_url)
            if video_data:
                self.parsed_video_data.append(video_data)
        
        print(f"\n🎯 Finished parsing! Successfully extracted {len(self.parsed_video_data)} videos")
        return self.parsed_video_data

    def save_data(self, filename="optimized_videos.json"):
        """Save parsed data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.parsed_video_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Data saved to {filename}")
        except Exception as e:
            print(f"❌ Error saving data: {e}")

# Usage example
async def main():
    parser = OptimizedVideoDataParser("https://rule34video.com")
    
    # Extract video URLs from main page
    await parser.extract_video_urls()
    
    # Parse all videos
    await parser.parse_all_videos()
    
    # Save the results
    parser.save_data("optimized_videos.json")

if __name__ == "__main__":
    asyncio.run(main())