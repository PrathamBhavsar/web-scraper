import re
import time
import logging
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from urllib.parse import urljoin
from date_parser import DateParser


class VideoInfoExtractor:
    def __init__(self, config, driver_manager, date_parser):
        self.config = config
        self.driver_manager = driver_manager
        self.date_parser = date_parser
        self.base_url = "https://rule34video.com"
        self.logger = logging.getLogger('Rule34Scraper')
    
    @property
    def driver(self):
        return self.driver_manager.get_driver()
    
    def extract_video_info(self, video_url):
        """Main orchestrator method for extracting video information"""
        try:
            # Navigate to video page
            if not self.driver_manager.navigate_to_page(video_url):
                return None
            
            # Extract video ID
            video_id = self.extract_video_id(video_url)
            if not video_id:
                return None
            
            # Initialize video info structure
            video_info = {
                "video_id": video_id,
                "url": video_url,
                "title": "",
                "duration": "",
                "views": "",
                "uploader": "",
                "upload_date": "",
                "upload_date_epoch": None,
                "tags": [],
                "video_src": "",
                "thumbnail_src": ""
            }
            
            # Extract all information
            video_info["title"] = self.extract_title()
            self.extract_item_info_data(video_info)  # Gets duration, views, upload_date
            video_info["uploader"] = self.extract_uploader()
            video_info["tags"] = self.extract_tags()
            video_info["thumbnail_src"] = self.extract_thumbnail_url()
            video_info["video_src"] = self.extract_video_source()
            
            # Set defaults for missing values
            self.set_default_values(video_info)
            
            # Log extracted information
            self.log_extracted_info(video_info)
            
            return video_info
            
        except Exception as e:
            self.logger.error(f"Error extracting video info from {video_url}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
    def extract_video_id(self, video_url):
        """Get video ID from URL using regex patterns"""
        video_id_match = re.search(r'/video/(\d+)/', video_url)
        if video_id_match:
            return video_id_match.group(1)
        else:
            return video_url.split('/')[-2] if video_url.endswith('/') else video_url.split('/')[-1]
    
    def extract_title(self):
        """Get video title from page elements"""
        try:
            title_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'thumb_title')]")
            title = title_element.text.strip()
            self.logger.info(f"Title extracted: {title}")
            return title
        except NoSuchElementException:
            try:
                title_element = self.driver.find_element(By.TAG_NAME, "h1")
                return title_element.text.strip()
            except:
                self.logger.warning("Could not extract title, will use default")
                return ""
    
    def extract_item_info_data(self, video_info):
        """Extract duration, views, and upload date from info row elements"""
        try:
            info_row_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'info row')]")
            self.logger.info(f"Found {len(info_row_elements)} info row elements")
            
            if not info_row_elements:
                self.logger.warning("No info row elements found - validation failed at element discovery")
                return
            
            for i, row in enumerate(info_row_elements):
                try:
                    row_text = row.get_attribute("textContent") or row.text.strip()
                    self.logger.debug(f"Processing row {i+1}: '{row_text}'")
                    
                    if not row_text:
                        self.logger.warning(f"Row {i+1} validation failed: empty text content")
                        continue
                    
                    # Check which type of icon this row contains
                    has_calendar = len(row.find_elements(By.XPATH, ".//svg[contains(@class,'custom-calender')]")) > 0
                    has_eye = len(row.find_elements(By.XPATH, ".//svg[contains(@class,'custom-eye')]")) > 0
                    has_time = len(row.find_elements(By.XPATH, ".//svg[contains(@class,'custom-time')]")) > 0
                    
                    self.logger.debug(f"Row {i+1} icons - Calendar: {has_calendar}, Eye: {has_eye}, Time: {has_time}")
                    
                    # Validate that exactly one icon type is found
                    icon_count = sum([has_calendar, has_eye, has_time])
                    if icon_count == 0:
                        self.logger.warning(f"Row {i+1} validation failed: no recognized icon found")
                        continue
                    elif icon_count > 1:
                        self.logger.warning(f"Row {i+1} validation failed: multiple icons found (Calendar: {has_calendar}, Eye: {has_eye}, Time: {has_time})")
                        continue
                    
                    # Process based on icon type
                    if has_calendar:
                        if not row_text:
                            self.logger.warning(f"Row {i+1} (calendar) validation failed: empty text after icon detection")
                            continue
                        
                        video_info["upload_date"] = row_text.strip()
                        epoch = self.date_parser.parse_upload_date_to_epoch(row_text.strip())
                        if epoch:
                            video_info["upload_date_epoch"] = epoch
                            self.logger.info(f"Upload date extracted successfully: '{video_info['upload_date']}' -> {video_info['upload_date_epoch']}")
                        else:
                            self.logger.warning(f"Row {i+1} (calendar) validation failed: could not parse date '{row_text}' to epoch")
                    
                    elif has_eye:
                        if not row_text:
                            self.logger.warning(f"Row {i+1} (eye) validation failed: empty text after icon detection")
                            continue
                        
                        views_num = self.parse_views_number(row_text)
                        if views_num is not None:
                            video_info["views"] = str(views_num)
                            self.logger.info(f"Views extracted successfully: '{row_text}' -> {video_info['views']}")
                        else:
                            self.logger.warning(f"Row {i+1} (eye) validation failed: could not parse views number from '{row_text}'")
                    
                    elif has_time:
                        if not row_text:
                            self.logger.warning(f"Row {i+1} (time) validation failed: empty text after icon detection")
                            continue
                        
                        time_match = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', row_text)
                        if time_match:
                            video_info["duration"] = time_match.group(1)
                            self.logger.info(f"Duration extracted successfully: '{video_info['duration']}' from text '{row_text}'")
                        else:
                            # Fallback: use the entire text if it looks like a duration
                            if re.match(r'^\d{1,2}:\d{2}(?::\d{2})?$', row_text.strip()):
                                video_info["duration"] = row_text.strip()
                                self.logger.info(f"Duration extracted (fallback): '{video_info['duration']}'")
                            else:
                                self.logger.warning(f"Row {i+1} (time) validation failed: no valid time format found in '{row_text}'")
                
                except Exception as e:
                    self.logger.error(f"Error processing row {i+1}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Critical error in extract_item_info_data: {e}")
            self.logger.error("Validation failed at method level - could not process info rows")
    
    def extract_duration(self):
        """Get video duration from multiple possible sources"""
        try:
            video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")
            actual_duration = self.get_actual_duration_from_player(video_element)
            if actual_duration:
                self.logger.info(f"Duration extracted from video element: {actual_duration}")
                return actual_duration
        except Exception as e:
            self.logger.warning(f"Video element duration extraction failed: {e}")
        
        return None
    
    def get_actual_duration_from_player(self, video_element):
        """Extract real duration from video element using JavaScript"""
        try:
            # Wait for video to load metadata
            self.driver.execute_script("""
                var video = arguments[0];
                if (video.readyState === 0) {
                    video.load();
                }
            """, video_element)
            
            time.sleep(2)
            
            # Get duration via JavaScript
            duration_seconds = self.driver.execute_script("""
                var video = arguments[0];
                if (video.duration && !isNaN(video.duration) && video.duration > 0) {
                    return video.duration;
                }
                return null;
            """, video_element)
            
            if duration_seconds and duration_seconds > 0:
                hours = int(duration_seconds // 3600)
                minutes = int((duration_seconds % 3600) // 60)
                seconds = int(duration_seconds % 60)
                
                if hours > 0:
                    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                else:
                    return f"{minutes:02d}:{seconds:02d}"
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting video duration: {e}")
            return None
    
    def parse_views_number(self, views_text):
        """Convert view text like '24K' to actual numbers"""
        if not views_text:
            return 0
        
        # Try to find number inside parentheses first: e.g. "24K (24,007)"
        paren_match = re.search(r'\(([\d,]+)\)', views_text)
        if paren_match:
            num = paren_match.group(1).replace(',', '')
            try:
                return int(num)
            except:
                pass
        
        # Parse K/M/B suffixes or plain numbers
        short_match = re.search(r'([\d,.]+)\s*([KkMmBb]?)', views_text)
        if short_match:
            num_str = short_match.group(1).replace(',', '')
            suffix = short_match.group(2).upper()
            
            try:
                value = float(num_str)
            except:
                # fallback: extract digits only
                digits = re.sub(r'\D', '', views_text)
                return int(digits) if digits else 0
            
            if suffix == 'K':
                return int(value * 1_000)
            if suffix == 'M':
                return int(value * 1_000_000)
            if suffix == 'B':
                return int(value * 1_000_000_000)
            
            return int(value)
        
        # Final fallback: extract digits
        digits = re.sub(r'\D', '', views_text)
        return int(digits) if digits else 0
    
    def extract_uploader(self):
        """Get uploader name from page elements"""
        try:
            label_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class,'label')]")
            
            for lbl in label_elements:
                try:
                    lbl_text = lbl.text.strip()
                except:
                    lbl_text = ""
                
                if lbl_text and 'uploaded by' in lbl_text.lower():
                    try:
                        uploader_element = lbl.find_element(By.XPATH, ".//following-sibling::a[contains(@class,'btn_link')][1]")
                    except NoSuchElementException:
                        try:
                            uploader_element = lbl.find_element(By.XPATH, ".//a[contains(@class,'btn_link')][1]")
                        except NoSuchElementException:
                            uploader_element = None
                    
                    if uploader_element:
                        uploader = uploader_element.text.strip()
                        self.logger.info(f"Uploader extracted: {uploader}")
                        return uploader
            
            self.logger.debug("Uploader not found, using default")
            return "Unknown"
            
        except Exception:
            self.logger.warning("Error while extracting uploader (non-fatal)")
            return "Unknown"
    
    def extract_tags(self):
        """Get all tags while filtering unwanted ones"""
        try:
            tag_elements = self.driver.find_elements(By.XPATH, "//a[contains(@class, 'tag_item')]")
            tags = []
            
            ignore_tags = {
                "+ | suggest", "+ | Suggest",
                "mp4 2160p", "mp4 1080p", "mp4 720p", "mp4 480p", "mp4 360p",
                "suggest"
            }
            
            for tag_element in tag_elements:
                tag_text = tag_element.text.strip()
                if not tag_text:
                    continue
                
                if tag_text.lower() in {t.lower() for t in ignore_tags}:
                    self.logger.debug(f"Ignoring tag: {tag_text}")
                    continue
                
                tags.append(tag_text)
            
            result_tags = tags if tags else ["untagged"]
            self.logger.info(f"Tags extracted: {len(result_tags)} tags - {result_tags[:5]}")
            return result_tags
            
        except NoSuchElementException:
            self.logger.warning("Could not find tag elements, using default")
            return ["untagged"]
    
    def extract_thumbnail_url(self):
        """Get thumbnail image URL from various sources"""
        try:
            thumbnail_selectors = [
                "//img[contains(@class, 'lazy-load')]",
                "//img[contains(@class, 'thumb')]",
                "//img[contains(@src, 'thumb')]",
                "//img[contains(@data-src, 'thumb')]",
                "//div[contains(@class, 'fp-player')]//img",
                "//video/@poster"
            ]
            
            thumb_src = None
            for selector in thumbnail_selectors:
                try:
                    if selector.endswith("/@poster"):
                        # Special case for video poster
                        video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")
                        thumb_src = video_element.get_attribute("poster")
                    else:
                        imgs = self.driver.find_elements(By.XPATH, selector)
                        for img in imgs:
                            candidate = (img.get_attribute("data-src") or
                                       img.get_attribute("src") or
                                       img.get_attribute("data-lazy"))
                            if candidate and candidate.strip() and ('thumb' in candidate or 'preview' in candidate):
                                thumb_src = candidate.strip()
                                break
                    
                    if thumb_src:
                        break
                except:
                    continue
            
            if thumb_src:
                result = urljoin(self.base_url, thumb_src) if not thumb_src.startswith('http') else thumb_src
                self.logger.info(f"Thumbnail extracted: {result}")
                return result
            else:
                # Final fallback: search page source for image URLs
                return self.extract_thumbnail_from_source()
                
        except Exception as e:
            self.logger.warning(f"Thumbnail extraction failed: {e}")
            return ""
    
    def extract_thumbnail_from_source(self):
        """Extract thumbnail URL from page source as fallback"""
        try:
            page_source = self.driver.page_source
            img_patterns = [
                r'https?://[^\s"\'<>]+thumb[^\s"\'<>]*\.(?:jpg|jpeg|png|webp)',
                r'https?://[^\s"\'<>]+preview[^\s"\'<>]*\.(?:jpg|jpeg|png|webp)',
                r'"poster":\s*"([^"]+)"',
                r'data-src="([^"]*thumb[^"]*)"'
            ]
            
            for pattern in img_patterns:
                img_match = re.search(pattern, page_source, re.IGNORECASE)
                if img_match:
                    result = img_match.group(1) if pattern.startswith('"') else img_match.group(0)
                    self.logger.info(f"Thumbnail extracted from page source: {result}")
                    return result
            
            return ""
        except Exception as e:
            self.logger.warning(f"Thumbnail extraction from source failed: {e}")
            return ""
    
    def extract_video_source(self):
        """Get actual video file URL from player element"""
        try:
            video_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'fp-player')]//video")
            
            # Try to get source from video element
            video_src = video_element.get_attribute("src")
            if not video_src:
                # Try source elements within video
                source_elements = video_element.find_elements(By.TAG_NAME, "source")
                if source_elements:
                    for source in source_elements:
                        src = source.get_attribute("src")
                        if src:
                            video_src = src
                            # Prefer higher quality sources
                            if any(quality in src for quality in ['1080p', '2160p', '4k']):
                                break
            
            if video_src:
                result = video_src if video_src.startswith('http') else urljoin(self.base_url, video_src)
                self.logger.info(f"Video source extracted: {result}")
                return result
            else:
                self.logger.warning("Video element found but no source attribute")
                return self.extract_video_source_from_page()
                
        except NoSuchElementException:
            self.logger.warning("Could not find video player element")
            return self.extract_video_source_from_page()
    
    def extract_video_source_from_page(self):
        """Extract video URL from page source as fallback"""
        try:
            page_source = self.driver.page_source
            
            # Look for highest quality MP4 first
            quality_patterns = [
                r'https?://[^\s"\'<>]+(?:2160p|4k)[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+1080p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+720p[^\s"\'<>]*\.mp4[^\s"\'<>]*',
                r'https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*'
            ]
            
            for pattern in quality_patterns:
                mp4_matches = re.findall(pattern, page_source)
                if mp4_matches:
                    result = mp4_matches[0]
                    self.logger.info(f"Video source found via regex: {result}")
                    return result
            
            return ""
        except:
            self.logger.error("Could not find video source with any method")
            return ""
    
    def set_default_values(self, video_info):
        """Ensure all required fields have valid defaults"""
        if not video_info["title"]:
            video_info["title"] = f"Video_{video_info['video_id']}"
        
        if not video_info["duration"] or video_info["duration"] in ["00:00", "0:00"]:
            # Try to get actual duration from video element one more time
            actual_duration = self.extract_duration()
            video_info["duration"] = actual_duration if actual_duration else "00:30"
        
        if not video_info["views"]:
            video_info["views"] = "0"
        
        if not video_info["uploader"]:
            video_info["uploader"] = "Unknown"
        
        if not video_info["upload_date"]:
            video_info["upload_date"] = "Unknown"
        
        if not video_info["upload_date_epoch"]:
            video_info["upload_date_epoch"] = int(datetime.now().timestamp() * 1000)
        
        if not video_info["tags"]:
            video_info["tags"] = ["untagged"]
    
    def log_extracted_info(self, video_info):
        """Log summary of extracted video information"""
        video_id = video_info["video_id"]
        self.logger.info(f"Extracted info for {video_id}:")
        self.logger.info(f" Title: '{video_info['title'][:50]}...'")
        self.logger.info(f" Duration: {video_info['duration']}")
        self.logger.info(f" Views: {video_info['views']}")
        self.logger.info(f" Uploader: {video_info['uploader']}")
        self.logger.info(f" Upload date: {video_info['upload_date']} (Epoch: {video_info['upload_date_epoch']})")
        self.logger.info(f" Tags: {len(video_info['tags'])} tags")
        self.logger.info(f" Video source: {'Found' if video_info['video_src'] else 'None'}")
        self.logger.info(f" Thumbnail: {'Found' if video_info['thumbnail_src'] else 'None'}")