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



class VideoExtractor:
    """Class responsible for extracting video data from HTML content and JSON-LD."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def extract_video_id(self, video_url: str) -> str:
        """Extract video ID from URL."""
        try:
            video_id = self.extract_video_id_from_url(video_url)
            if video_id:
                self.logger.info("Extracted video ID", extra={"video_id": video_id})
                return video_id
            else:
                self.logger.warning("No video ID found", extra={"url": video_url})
                return ""
        except Exception as e:
            self.logger.error("Error extracting video ID", extra={"url": video_url, "error": str(e)})
            return ""
    
    def extract_video_id_from_url(self, video_url: str) -> str:
        """Extract video ID from video URL - FROM OLD PARSER"""
        try:
            # Pattern for common video ID extraction
            patterns = [
                r'/video/([^/]+)/?',
                r'[?&]v=([^&]+)',
                r'/v/([^/]+)/?',
                r'/embed/([^/]+)/?'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, video_url)
                if match:
                    return match.group(1)
            
            return ""
        except Exception as e:
            self.logger.error("Error extracting video ID from URL", extra={"url": video_url, "error": str(e)})
            return ""
    
    def extract_title(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract title with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD name field
            if json_ld_data and 'name' in json_ld_data:
                title = json_ld_data['name'].strip()
                if title:
                    self.logger.info("Extracted title from JSON-LD", extra={"title": title})
                    return title
            
            # Priority 2: HTML title tag
            title_match = re.search(r'<title[^>]*>([^<]*)</title>', html_content, re.IGNORECASE)
            if title_match:
                title = unescape(title_match.group(1)).strip()
                if title and title != "404 Not Found":
                    self.logger.info("Extracted title from HTML", extra={"title": title})
                    return title
            
            # Priority 3: H1 tag
            h1_match = re.search(r'<h1[^>]*>([^<]*)</h1>', html_content, re.IGNORECASE)
            if h1_match:
                title = unescape(h1_match.group(1)).strip()
                if title:
                    self.logger.info("Extracted title from H1", extra={"title": title})
                    return title
            
            # Priority 4: Meta property og:title
            og_title_match = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
            if og_title_match:
                title = unescape(og_title_match.group(1)).strip()
                if title:
                    self.logger.info("Extracted title from OG meta", extra={"title": title})
                    return title
            
            self.logger.warning("No title found, setting default")
            return "Unknown Title"
        except Exception as e:
            self.logger.error("Error extracting title", extra={"error": str(e)})
            return "Unknown Title"
    
    def extract_description(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract description with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD description field
            if json_ld_data and 'description' in json_ld_data:
                description = json_ld_data['description'].strip()
                if description and len(description) > 5:
                    self.logger.info("Extracted description from JSON-LD", extra={"length": len(description)})
                    return description
            
            # Priority 2: Meta description
            meta_desc_match = re.search(r'<meta[^>]*name="description"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
            if meta_desc_match:
                description = unescape(meta_desc_match.group(1)).strip()
                if description and len(description) > 5:
                    self.logger.info("Extracted description from meta", extra={"length": len(description)})
                    return description
            
            # Priority 3: OG description
            og_desc_match = re.search(r'<meta[^>]*property="og:description"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
            if og_desc_match:
                description = unescape(og_desc_match.group(1)).strip()
                if description and len(description) > 5:
                    self.logger.info("Extracted description from OG meta", extra={"length": len(description)})
                    return description
            
            # Priority 4: Various description patterns
            desc_patterns = [
                r'<div[^>]*class="[^"]*description[^"]*"[^>]*>([^<]*)',
                r'<p[^>]*class="[^"]*desc[^"]*"[^>]*>([^<]*)'
            ]
            
            for pattern in desc_patterns:
                desc_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if desc_match:
                    description = unescape(desc_match.group(1)).strip()
                    if description and len(description) > 10:
                        self.logger.info("Extracted description from HTML", extra={"length": len(description)})
                        return description
            
            self.logger.warning("No description found")
            return ""
        except Exception as e:
            self.logger.error("Error extracting description", extra={"error": str(e)})
            return ""
    
    def extract_tags(self, html_content: str) -> List[str]:
        """Extract tags from HTML content."""
        try:
            tags = self.extract_tags_from_html(html_content)
            if tags:
                self.logger.info("Extracted tags", extra={"count": len(tags)})
                return tags
            else:
                self.logger.warning("No tags found")
                return []
        except Exception as e:
            self.logger.error("Error extracting tags", extra={"error": str(e)})
            return []
    
    def extract_tags_from_html(self, html_content: str) -> List[str]:
        """Extract tags from HTML content - FROM OLD PARSER"""
        try:
            tags = []
            
            # Look for meta keywords
            keywords_match = re.search(r'<meta[^>]*name="keywords"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
            if keywords_match:
                keywords = keywords_match.group(1)
                tags.extend([tag.strip() for tag in keywords.split(',') if tag.strip()])
            
            # Look for hashtags in content
            hashtag_matches = re.findall(r'#(\w+)', html_content)
            tags.extend(hashtag_matches)
            
            # Look for tag-related class content
            tag_patterns = [
                r'<[^>]*class="[^"]*tag[^"]*"[^>]*>([^<]+)',
                r'<[^>]*class="[^"]*keyword[^"]*"[^>]*>([^<]+)',
                r'<span[^>]*class="[^"]*category[^"]*"[^>]*>([^<]+)'
            ]
            
            for pattern in tag_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    clean_tag = unescape(match).strip()
                    if clean_tag and len(clean_tag) < 50:  # Reasonable tag length
                        tags.append(clean_tag)
            
            # Remove duplicates while preserving order
            unique_tags = []
            for tag in tags:
                if tag not in unique_tags:
                    unique_tags.append(tag)
            
            return unique_tags[:20]  # Limit to 20 tags
        except Exception as e:
            self.logger.error("Error extracting tags from HTML", extra={"error": str(e)})
            return []
    
    def extract_categories(self, html_content: str) -> List[str]:
        """Extract categories from HTML content."""
        try:
            categories = self.extract_categories_from_html(html_content)
            if categories:
                self.logger.info("Extracted categories", extra={"count": len(categories)})
                return categories
            else:
                self.logger.warning("No categories found")
                return []
        except Exception as e:
            self.logger.error("Error extracting categories", extra={"error": str(e)})
            return []
    
    def extract_categories_from_html(self, html_content: str) -> List[str]:
        """Extract categories from HTML content - FROM OLD PARSER"""
        try:
            categories = []
            
            # Look for category-related patterns
            category_patterns = [
                r'<[^>]*class="[^"]*category[^"]*"[^>]*>([^<]+)',
                r'<[^>]*class="[^"]*genre[^"]*"[^>]*>([^<]+)',
                r'Category:\s*([^\n<]+)',
                r'Genre:\s*([^\n<]+)'
            ]
            
            for pattern in category_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    clean_category = unescape(match).strip()
                    if clean_category and len(clean_category) < 100:
                        categories.append(clean_category)
            
            # Remove duplicates while preserving order
            unique_categories = []
            for category in categories:
                if category not in unique_categories:
                    unique_categories.append(category)
            
            return unique_categories[:10]  # Limit to 10 categories
        except Exception as e:
            self.logger.error("Error extracting categories from HTML", extra={"error": str(e)})
            return []
    
    def extract_uploaded_by(self, html_content: str) -> str:
        """Extract uploader information."""
        try:
            artist_info = self.extract_artist_and_uploader(html_content)
            uploader = artist_info.get('uploader', '')
            if uploader:
                self.logger.info("Extracted uploader", extra={"uploader": uploader})
                return uploader
            else:
                self.logger.warning("No uploader found")
                return ""
        except Exception as e:
            self.logger.error("Error extracting uploader", extra={"error": str(e)})
            return ""
    
    def extract_artists(self, html_content: str) -> List[str]:
        """Extract artist information."""
        try:
            artist_info = self.extract_artist_and_uploader(html_content)
            artists = artist_info.get('artists', [])
            if artists:
                self.logger.info("Extracted artists", extra={"count": len(artists)})
                return artists
            else:
                self.logger.warning("No artists found")
                return []
        except Exception as e:
            self.logger.error("Error extracting artists", extra={"error": str(e)})
            return []
    
    def extract_artist_and_uploader(self, html_content: str) -> Dict[str, Any]:
        """Extract artist and uploader info from HTML - FROM OLD PARSER"""
        try:
            result = {
                'artists': [],
                'uploader': ''
            }
            
            # Look for uploader patterns
            uploader_patterns = [
                r'Uploaded by:\s*([^\n<]+)',
                r'<[^>]*class="[^"]*uploader[^"]*"[^>]*>([^<]+)',
                r'<[^>]*class="[^"]*author[^"]*"[^>]*>([^<]+)',
                r'By:\s*([^\n<]+)'
            ]
            
            for pattern in uploader_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    uploader = unescape(match.group(1)).strip()
                    if uploader:
                        result['uploader'] = uploader
                        break
            
            # Look for artist patterns
            artist_patterns = [
                r'Artist:\s*([^\n<]+)',
                r'<[^>]*class="[^"]*artist[^"]*"[^>]*>([^<]+)',
                r'Performer:\s*([^\n<]+)'
            ]
            
            for pattern in artist_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    artist = unescape(match).strip()
                    if artist and artist not in result['artists']:
                        result['artists'].append(artist)
            
            return result
        except Exception as e:
            self.logger.error("Error extracting artist and uploader", extra={"error": str(e)})
            return {'artists': [], 'uploader': ''}
    
    def extract_duration(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract duration with fallback methods - FROM OLD PARSER"""
        try:
            # Priority 1: JSON-LD duration field
            if json_ld_data and 'duration' in json_ld_data:
                duration = json_ld_data['duration']
                if duration:
                    readable_duration = self.convert_iso_duration_to_readable(duration)
                    self.logger.info("Extracted duration from JSON-LD", extra={"duration": readable_duration})
                    return readable_duration
            
            # Priority 2: HTML duration patterns
            duration_patterns = [
                r'duration="([^"]*)"',
                r"duration='([^']*)'",
                r'<meta[^>]*property="video:duration"[^>]*content="([^"]*)"',
                r'([0-9]+:[0-9]+(?::[0-9]+)?)'  # MM:SS or HH:MM:SS format
            ]
            
            for pattern in duration_patterns:
                duration_match = re.search(pattern, html_content, re.IGNORECASE)
                if duration_match:
                    duration_str = duration_match.group(1)
                    # If it's already in readable format, return it
                    if re.match(r'^[0-9]+:[0-9]+(?::[0-9]+)?$', duration_str):
                        self.logger.info("Extracted duration from HTML", extra={"duration": duration_str})
                        return duration_str
                    # If it's ISO format, convert it
                    elif duration_str.startswith('PT'):
                        readable_duration = self.convert_iso_duration_to_readable(duration_str)
                        self.logger.info("Extracted duration from HTML (ISO)", extra={"duration": readable_duration})
                        return readable_duration
            
            self.logger.warning("No duration found")
            return "Unknown"
        except Exception as e:
            self.logger.error("Error extracting duration", extra={"error": str(e)})
            return "Unknown"
    
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
                                views = int(view_count)
                                self.logger.info("Extracted views from JSON-LD", extra={"views": views})
                                return views
            
            # Priority 2: HTML patterns like "1.2K (1,194)" views
            views_pattern = r'([0-9.,KM]+)\s*\(([0-9,]+)\)'
            views_match = re.search(views_pattern, html_content)
            if views_match:
                full_number = views_match.group(2).replace(',', '')
                if full_number.isdigit():
                    views = int(full_number)
                    self.logger.info("Extracted views from HTML (parentheses)", extra={"views": views})
                    return views
            
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
                        views = int(views_str)
                        self.logger.info("Extracted views from HTML", extra={"views": views})
                        return views
            
            self.logger.warning("No views found")
            return 0
        except Exception as e:
            self.logger.error("Error extracting views", extra={"error": str(e)})
            return 0
    
    def extract_likes(self, html_content: str) -> int:
        """Extract like count from HTML content."""
        try:
            # Look for like patterns
            like_patterns = [
                r'([0-9,]+)\s*likes?',
                r'Likes?:\s*([0-9,]+)',
                r'<[^>]*class="[^"]*like[^"]*"[^>]*>([0-9,]+)',
                r'👍\s*([0-9,]+)',
                r'♥\s*([0-9,]+)'
            ]
            
            for pattern in like_patterns:
                like_match = re.search(pattern, html_content, re.IGNORECASE)
                if like_match:
                    likes_str = like_match.group(1).replace(',', '')
                    if likes_str.isdigit():
                        likes = int(likes_str)
                        self.logger.info("Extracted likes", extra={"likes": likes})
                        return likes
            
            self.logger.warning("No likes found")
            return 0
        except Exception as e:
            self.logger.error("Error extracting likes", extra={"error": str(e)})
            return 0
    
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
                                formatted_date = parsed_date.strftime('%Y-%m-%d')
                                self.logger.info("Extracted upload date from JSON-LD", extra={"date": formatted_date})
                                return formatted_date
                            except:
                                self.logger.info("Extracted upload date from JSON-LD", extra={"date": date_str})
                                return date_str
            
            # Priority 2: HTML meta tags
            date_patterns = [
                r'<meta[^>]*property="article:published_time"[^>]*content="([^"]*)"',
                r'<meta[^>]*name="publish_date"[^>]*content="([^"]*)"',
                r'<meta[^>]*property="video:release_date"[^>]*content="([^"]*)"',
                r'Upload(?:ed)?\s*(?:on|date)?:?\s*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})',
                r'([0-9]{4}-[0-9]{2}-[0-9]{2})'
            ]
            
            for pattern in date_patterns:
                date_match = re.search(pattern, html_content, re.IGNORECASE)
                if date_match:
                    date_str = date_match.group(1)
                    try:
                        # Try to parse and format the date
                        if 'T' in date_str:  # ISO format
                            parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            formatted_date = parsed_date.strftime('%Y-%m-%d')
                        else:
                            formatted_date = date_str
                        self.logger.info("Extracted upload date from HTML", extra={"date": formatted_date})
                        return formatted_date
                    except:
                        self.logger.info("Extracted upload date from HTML", extra={"date": date_str})
                        return date_str
            
            self.logger.warning("No upload date found")
            return "Unknown"
        except Exception as e:
            self.logger.error("Error extracting upload date", extra={"error": str(e)})
            return "Unknown"
    
    def extract_thumbnail_src(self, json_ld_data: Optional[Dict], html_content: str) -> str:
        """Extract thumbnail URL with fallback methods."""
        try:
            thumbnail_url = self.extract_thumbnail_url(json_ld_data, html_content)
            if thumbnail_url:
                self.logger.info("Extracted thumbnail", extra={"url": thumbnail_url})
                return thumbnail_url
            else:
                self.logger.warning("No thumbnail found")
                return ""
        except Exception as e:
            self.logger.error("Error extracting thumbnail", extra={"error": str(e)})
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
            
            return ""
        except Exception as e:
            self.logger.error("Error extracting thumbnail URL", extra={"error": str(e)})
            return ""
    
    def extract_video_src(self, html_content: str) -> str:
        """Extract video source URL."""
        try:
            download_url = self.find_highest_quality_download_url(html_content)
            if download_url:
                self.logger.info("Extracted video source", extra={"url": download_url})
                return download_url
            else:
                self.logger.warning("No video source found")
                return ""
        except Exception as e:
            self.logger.error("Error extracting video source", extra={"error": str(e)})
            return ""
    
    def convert_iso_duration_to_readable(self, iso_duration: str) -> str:
        """Convert ISO 8601 duration to readable format - FROM OLD PARSER"""
        try:
            if not iso_duration or not iso_duration.startswith('PT'):
                return iso_duration
            
            # Remove PT prefix
            duration_str = iso_duration[2:]
            
            hours = 0
            minutes = 0
            seconds = 0
            
            # Extract hours
            if 'H' in duration_str:
                hours_match = re.search(r'(\d+)H', duration_str)
                if hours_match:
                    hours = int(hours_match.group(1))
            
            # Extract minutes
            if 'M' in duration_str:
                minutes_match = re.search(r'(\d+)M', duration_str)
                if minutes_match:
                    minutes = int(minutes_match.group(1))
            
            # Extract seconds
            if 'S' in duration_str:
                seconds_match = re.search(r'(\d+(?:\.\d+)?)S', duration_str)
                if seconds_match:
                    seconds = int(float(seconds_match.group(1)))
            
            # Format as readable string
            if hours > 0:
                return f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                return f"{minutes}:{seconds:02d}"
        except Exception as e:
            self.logger.error("Error converting ISO duration", extra={"duration": iso_duration, "error": str(e)})
            return iso_duration
    
    def find_highest_quality_download_url(self, html_content: str) -> str:
        """Find highest quality download URL - FROM OLD PARSER"""
        try:
            download_links = self.get_download_links_from_html(html_content)
            if not download_links:
                return ""
            
            # Quality priority (higher number = better quality)
            quality_order = {
                '4K': 10, '2160p': 10,
                '1440p': 9, '2K': 9,
                '1080p': 8, 'Full HD': 8,
                '720p': 7, 'HD': 7,
                '480p': 6,
                '360p': 5,
                '240p': 4,
                '144p': 3
            }
            
            best_quality = 0
            best_url = ""
            
            for link in download_links:
                url = link.get('url', '')
                quality = link.get('quality', '').lower()
                
                # Find quality score
                quality_score = 0
                for q, score in quality_order.items():
                    if q.lower() in quality:
                        quality_score = score
                        break
                
                # If no quality found, check URL for quality indicators
                if quality_score == 0:
                    for q, score in quality_order.items():
                        if q.lower() in url.lower():
                            quality_score = score
                            break
                
                # Default score for unspecified quality
                if quality_score == 0:
                    quality_score = 1
                
                if quality_score > best_quality:
                    best_quality = quality_score
                    best_url = url
            
            return best_url
        except Exception as e:
            self.logger.error("Error finding highest quality download URL", extra={"error": str(e)})
            return ""
    
    def get_download_links_from_html(self, html_content: str) -> List[Dict[str, str]]:
        """Extract download links from HTML content - FROM OLD PARSER"""
        try:
            download_links = []
            
            # Look for various download link patterns
            link_patterns = [
                r'<a[^>]*href="([^"]*)"[^>]*download[^>]*>.*?([0-9]+p|HD|Full HD|4K|2K)',
                r'<a[^>]*download[^>]*href="([^"]*)"[^>]*>.*?([0-9]+p|HD|Full HD|4K|2K)',
                r'href="([^"]*\.mp4[^"]*)"[^>]*>.*?([0-9]+p|HD|Full HD|4K|2K|MP4)',
                r'"url":\s*"([^"]*\.mp4[^"]*)".*?"quality":\s*"([^"]*)"'
            ]
            
            for pattern in link_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    url, quality = match
                    if url.startswith('http'):
                        download_links.append({
                            'url': url,
                            'quality': quality
                        })
            
            # Look for video source tags
            video_sources = re.findall(r'<source[^>]*src="([^"]*)"[^>]*>', html_content, re.IGNORECASE)
            for src in video_sources:
                if src.startswith('http') and ('.mp4' in src or '.webm' in src or '.avi' in src):
                    download_links.append({
                        'url': src,
                        'quality': 'Unknown'
                    })
            
            return download_links
        except Exception as e:
            self.logger.error("Error getting download links from HTML", extra={"error": str(e)})
            return []
