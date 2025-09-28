import re
import json
from urllib.parse import urljoin, urlparse
from datetime import datetime

async def extract_complete_metadata(browser, detail_url: str, video_data: dict):
    """Extract complete metadata from video detail page based on example.html structure"""
    page = None
    try:
        # Create a new page from the browser
        page = await browser.new_page()

        await page.goto(detail_url, wait_until='networkidle')
        
        # Initialize metadata structure as specified
        metadata = {
            "video_id": video_data.get("video_id", ""),
            "url": detail_url,
            "title": video_data.get("title", ""),
            "duration": "",
            "views": "0",
            "upload_date": int(datetime.now().timestamp() * 1000),
            "tags": [],
            "video_src": "",
            "thumbnail_src": video_data.get("thumbnail", ""),
            "uploaded_by": "Anonymous",
            "description": "No description available",
            "categories": [],
            "artists": [],
            "download_sources": []
        }
        
        # Extract title from page (h1.title_video)
        try:
            title_elem = await page.query_selector('h1.title_video')
            if title_elem:
                title = await title_elem.inner_text()
                if title.strip():
                    metadata["title"] = title.strip()
        except Exception as e:
            print(f"Error extracting title: {e}")
        
        # Extract video info (duration, views, upload date) from .item_info elements
        try:
            info_items = await page.query_selector_all('.info.row .item_info')
            for item in info_items:
                item_text = await item.inner_text()
                
                # Extract duration (format like "1:37")
                if ':' in item_text and any(char.isdigit() for char in item_text):
                    duration_match = re.search(r'(\d{1,2}:\d{2})', item_text)
                    if duration_match:
                        metadata["duration"] = duration_match.group(1)
                
                # Extract views (format like "1.4M (1,414,053)")
                if any(indicator in item_text.lower() for indicator in ['k', 'm', 'view', '(']):
                    # Try to extract the integer value inside parentheses first
                    views_int_match = re.search(r'\(([\d,]+)\)', item_text)
                    if views_int_match:
                        metadata["views"] = int(views_int_match.group(1).replace(',', ''))
                    else:
                        # Fallback: extract main view count and convert to int if possible
                        views_match = re.search(r'([\d,.]+)([KMB]?)', item_text)
                        if views_match:
                            num, suffix = views_match.groups()
                            num = num.replace(',', '')
                            if suffix == 'M':
                                metadata["views"] = int(float(num) * 1_000_000)
                            elif suffix == 'K':
                                metadata["views"] = int(float(num) * 1_000)
                            elif suffix == 'B':
                                metadata["views"] = int(float(num) * 1_000_000_000)
                            else:
                                metadata["views"] = int(float(num))
                
                # Extract upload date info (format like "6 years ago")
                if 'ago' in item_text.lower():
                    # For now, keep current timestamp; could parse relative dates later
                    pass
        
        except Exception as e:
            print(f"Error extracting video info: {e}")
        
        # Extract uploaded_by - XPath: /html/body/div[1]/div[2]/div[2]/div[2]/div/div/div[1]/div[4]/div[2]/div[3]/div/div[3]/a
        try:
            uploaded_by_elem = await page.query_selector('div.col:has(.label:text("Uploaded by")) a.item.btn_link')
            if uploaded_by_elem:
                uploaded_by = await uploaded_by_elem.inner_text()
                if uploaded_by.strip():
                    metadata["uploaded_by"] = uploaded_by.strip()
        except Exception as e:
            print(f"Error extracting uploaded_by: {e}")
        
        # Extract categories - XPath: /html/body/div[1]/div[2]/div[2]/div[2]/div/div/div[1]/div[4]/div[2]/div[3]/div/div[1]/a[*]/span
        try:
            category_elements = await page.query_selector_all('div.col:has(.label:text("Categories")) a.item.btn_link span')
            categories = []
            for cat_elem in category_elements:
                cat_text = await cat_elem.inner_text()
                if cat_text.strip():
                    categories.append(cat_text.strip())
            metadata["categories"] = categories
        except Exception as e:
            print(f"Error extracting categories: {e}")
        
        # Extract artists - XPath: /html/body/div[1]/div[2]/div[2]/div[2]/div/div/div[1]/div[4]/div[2]/div[3]/div/div[2]/a/span
        try:
            artist_elements = await page.query_selector_all('div.col:has(.label:text("Artist")) a.item.btn_link span')
            artists = []
            for artist_elem in artist_elements:
                artist_text = await artist_elem.inner_text()
                if artist_text.strip():
                    artists.append(artist_text.strip())
            metadata["artists"] = artists
        except Exception as e:
            print(f"Error extracting artists: {e}")
        
        # Extract tags from Tags section
        try:
            tag_elements = await page.query_selector_all('div.row_spacer:has(.label:text("Tags")) a.tag_item')
            tags = []
            for tag_elem in tag_elements:
                tag_text = await tag_elem.inner_text()
                if tag_text.strip() and not tag_text.strip().startswith('+'):  # Skip "Suggest" button
                    tags.append(tag_text.strip())
            metadata["tags"] = tags[:20]  # Limit to reasonable number
        except Exception as e:
            print(f"Error extracting tags: {e}")
        
        # Extract download sources (MOST IMPORTANT)
        download_sources = await extract_download_sources(page)
        metadata["download_sources"] = download_sources
        
        # Set the highest quality video source
        if download_sources:
            best_source = max(download_sources, key=lambda x: x.get('quality_score', 0))
            metadata["video_src"] = best_source["url"]
        
        # Extract description from author info if available
        try:
            desc_elem = await page.query_selector('.label em')  # "Author: Lvl3toaster\nSource: https://twitter.com/Lvl3toaster_"
            if desc_elem:
                desc_text = await desc_elem.inner_text()
                if desc_text.strip():
                    metadata["description"] = desc_text.strip()
        except Exception as e:
            print(f"Error extracting description: {e}")
        
        # Extract additional metadata from JSON-LD if available (from example.html)
        try:
            json_ld = await page.query_selector('script[type="application/ld+json"]')
            if json_ld:
                json_text = await json_ld.inner_text()
                json_data = json.loads(json_text)
                
                # Extract duration in ISO format (PT0H1M37S -> 1:37)
                if 'duration' in json_data:
                    iso_duration = json_data['duration']
                    duration_match = re.search(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso_duration)
                    if duration_match:
                        hours, minutes, seconds = duration_match.groups()
                        hours = int(hours) if hours else 0
                        minutes = int(minutes) if minutes else 0
                        seconds = int(seconds) if seconds else 0
                        
                        if hours > 0:
                            metadata["duration"] = f"{hours}:{minutes:02d}:{seconds:02d}"
                        else:
                            metadata["duration"] = f"{minutes}:{seconds:02d}"
                
                # Extract view count from interactionStatistic
                if 'interactionStatistic' in json_data:
                    for stat in json_data['interactionStatistic']:
                        if stat.get('interactionType') == 'http://schema.org/WatchAction':
                            view_count = stat.get('userInteractionCount', '0')
                            try:
                                metadata["views"] = int(view_count)
                            except Exception:
                                # If view_count is not an integer, fallback to 0
                                metadata["views"] = 0
                            break
        
        except Exception as e:
            print(f"Error extracting JSON-LD metadata: {e}")
        
        return metadata

    except Exception as e:
        print(f"Error extracting metadata from {detail_url}: {e}")
        return None

    finally:
        # Always close the page, even if an exception occurs
        if page:
            try:
                await page.close()
            except:
                pass

async def extract_download_sources(page):
    """Extract download links from the Download section"""
    sources = []
    
    try:
        # Look for download section based on the exact HTML structure from example.html
        download_section = await page.query_selector('div.row_spacer:has(.label:text("Download"))')
        
        if download_section:
            # Get all download links
            download_links = await download_section.query_selector_all('a.tag_item[href*="get_file"]')
            
            print(f"DEBUG: Found {len(download_links)} download links")
            
            for i, link in enumerate(download_links):
                try:
                    url = await link.get_attribute('href')
                    text = await link.inner_text()
                    
                    print(f"DEBUG: Download link {i+1}: {text} -> {url}")
                    
                    if url and 'get_file' in url:
                        # Extract quality from text like "MP4 720p", "MP4 480p"
                        quality_match = re.search(r'(\d+)p', text)
                        quality = int(quality_match.group(1)) if quality_match else 0
                        
                        # Calculate quality score for sorting
                        quality_score = quality
                        if quality >= 1080:
                            quality_score = 1080
                        elif quality >= 720:
                            quality_score = 720
                        elif quality >= 480:
                            quality_score = 480
                        elif quality >= 360:
                            quality_score = 360
                        else:
                            quality_score = 240  # Default for unknown quality
                        
                        sources.append({
                            'url': url,
                            'quality': f"{quality}p" if quality else "unknown",
                            'quality_score': quality_score,
                            'format': 'mp4',
                            'text': text.strip()
                        })
                
                except Exception as e:
                    print(f"Error processing download link {i+1}: {e}")
                    continue
        
        else:
            print("DEBUG: Download section not found")
        
        # Sort by quality (highest first)
        sources.sort(key=lambda x: x['quality_score'], reverse=True)
        
        print(f"DEBUG: Extracted {len(sources)} download sources")
        for i, source in enumerate(sources):
            print(f"  Source {i+1}: {source['quality']} - {source['url'][:100]}...")
    
    except Exception as e:
        print(f"Error extracting download sources: {e}")
    
    return sources