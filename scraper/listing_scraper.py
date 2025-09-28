import re
from urllib.parse import urljoin
from playwright.async_api import Page

async def find_last_page_number(page: Page, base_url: str):
    """Find the last page number by looking for the 'Last' link"""
    try:
        # Navigate to base URL first
        await page.goto(base_url, wait_until='networkidle')
        
        # Look for the Last link in pagination - based on base.html structure
        last_link = await page.query_selector('#custom_list_videos_latest_videos_list_pagination a:has-text("Last")')
        
        if last_link:
            href = await last_link.get_attribute('href')
            if href:
                print(f"DEBUG: Found Last link href: {href}")
                # Extract page number from href like "/latest-updates/9686/"
                match = re.search(r'/latest-updates/(\d+)/', href)
                if match:
                    page_num = int(match.group(1))
                    print(f"DEBUG: Extracted last page number: {page_num}")
                    return page_num
        
        # Fallback: look for highest numbered pagination link
        pagination_links = await page.query_selector_all('#custom_list_videos_latest_videos_list_pagination a[href*="/latest-updates/"]')
        max_page = 1
        
        print(f"DEBUG: Found {len(pagination_links)} pagination links")
        
        for link in pagination_links:
            href = await link.get_attribute('href') or ''
            print(f"DEBUG: Checking pagination link: {href}")
            match = re.search(r'/latest-updates/(\d+)/', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
                print(f"DEBUG: Found page number: {page_num}, current max: {max_page}")
        
        return max_page
        
    except Exception as e:
        print(f"ERROR: Error finding last page: {e}")
        return 1

async def scrape_video_links_from_page(page: Page, page_url: str):
    """Extract all video page URLs from a listing page (NOT download URLs)"""
    print(f"DEBUG: Navigating to: {page_url}")
    await page.goto(page_url, wait_until='networkidle')
    
    # Handle age verification modal if present
    try:
        age_continue_btn = await page.wait_for_selector('button:has-text("Continue")', timeout=3000)
        if age_continue_btn:
            print("DEBUG: Clicking age verification 'Continue' button")
            await age_continue_btn.click()
            await page.wait_for_load_state('networkidle')
    except Exception:
        print("DEBUG: No age verification modal found or already handled")
    
    results = []
    
    try:
        # Wait for the video container to load with multiple attempts
        container_found = False
        
        # Try multiple selectors for the container
        container_selectors = [
            '#custom_list_videos_latest_videos_list_items',
            '.thumbs.clearfix',
            'div.thumbs'
        ]
        
        for selector in container_selectors:
            try:
                await page.wait_for_selector(selector, timeout=5000)
                print(f"DEBUG: Found container with selector: {selector}")
                container_found = True
                break
            except Exception:
                continue
        
        if not container_found:
            print("DEBUG: Container not found with any selector, trying to extract from page source")
        
        # Get all video items using multiple selector strategies
        video_items = []
        
        # Strategy 1: Try the specific container selector
        if container_found:
            video_items = await page.query_selector_all('#custom_list_videos_latest_videos_list_items div.item.thumb[class*="video_"]')
        
        # Strategy 2: If no items found, try broader selectors
        if not video_items:
            selectors_to_try = [
                'div.thumbs div.item.thumb[class*="video_"]',
                '.thumbs div.item.thumb',
                'div.item.thumb',
                'a[href*="/video/"]'  # Direct video links
            ]
            
            for selector in selectors_to_try:
                video_items = await page.query_selector_all(selector)
                if video_items:
                    print(f"DEBUG: Found {len(video_items)} video items using selector: {selector}")
                    break
        
        print(f"DEBUG: Found {len(video_items)} video items on page")
        
        for i, item in enumerate(video_items):
            try:
                video_link = None
                
                # Multiple strategies to find video links
                link_selectors = [
                    'a.th.js-open-popup',  # Primary selector
                    'a.th',                # Fallback
                    'a[href*="/video/"]'   # Any link containing /video/
                ]
                
                for selector in link_selectors:
                    video_link = await item.query_selector(selector)
                    if video_link:
                        break
                
                # If item itself is a video link
                if not video_link and await item.get_attribute('href'):
                    video_link = item
                
                if video_link:
                    href = await video_link.get_attribute('href')
                    title = await video_link.get_attribute('title')
                    
                    if href:
                        print(f"DEBUG: Item {i+1} - Found href: {href}")
                        
                        # Extract video ID from URL like "/video/4039646/rouge-titty-fuck-highlandr34/"
                        video_id_match = re.search(r'/video/(\d+)/', href)
                        video_id = video_id_match.group(1) if video_id_match else None
                        
                        if not video_id:
                            # Try alternative pattern
                            video_id_match = re.search(r'video/(\d+)', href)
                            video_id = video_id_match.group(1) if video_id_match else f"unknown_{i}"
                        
                        # Get thumbnail URL
                        thumbnail = None
                        img = await item.query_selector('img.thumb')
                        if img:
                            thumbnail = await img.get_attribute('data-original')
                            if not thumbnail:
                                thumbnail = await img.get_attribute('src')
                        
                        # Ensure absolute URL
                        if href.startswith('/'):
                            href = urljoin(page_url, href)
                        
                        video_data = {
                            'video_id': video_id,
                            'detail_url': href,  # This is the page URL, NOT download URL
                            'title': title or f'Video {video_id}',
                            'thumbnail': thumbnail or ''
                        }
                        
                        results.append(video_data)
                        print(f"DEBUG: Successfully extracted video {video_id}: {title}")
                
                else:
                    print(f"DEBUG: Item {i+1} - No video link found")
            
            except Exception as e:
                print(f"ERROR: Error extracting video from item {i+1}: {e}")
                continue
    
    except Exception as e:
        print(f"ERROR: Error in scrape_video_links_from_page: {e}")
        
        # Emergency fallback - try to get page source for debugging
        try:
            content = await page.content()
            if 'custom_list_videos_latest_videos_list_items' in content:
                print("DEBUG: Container div found in page source")
            else:
                print("DEBUG: Container div NOT found - page structure may have changed")
                
            # Count video items in source
            video_count = content.count('class="item thumb video_')
            print(f"DEBUG: Found {video_count} video items in page source")
            
        except Exception as debug_e:
            print(f"ERROR: Debug inspection failed: {debug_e}")
    
    print(f"DEBUG: Returning {len(results)} video page URLs")
    return results