import re
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import dateutil.parser

class DateParser:
    def __init__(self):
        self.logger = logging.getLogger('Rule34Scraper')
    
    def parse_upload_date_to_epoch(self, upload_date_text):
        """Convert various date formats to epoch milliseconds"""
        if not upload_date_text or not upload_date_text.strip():
            return None
        
        try:
            current_time = datetime.now()
            upload_date_text = upload_date_text.strip().lower()
            
            # Try relative date parsing first
            epoch_time = self.parse_relative_date(upload_date_text, current_time)
            if epoch_time:
                return epoch_time
            
            # Try absolute date parsing
            epoch_time = self.parse_absolute_date(upload_date_text)
            if epoch_time:
                return epoch_time
            
            # Use dateutil parser as fallback
            parsed_date = dateutil.parser.parse(upload_date_text, fuzzy=True)
            return int(parsed_date.timestamp() * 1000)
            
        except Exception as e:
            self.logger.error(f"Error parsing upload date '{upload_date_text}': {e}")
            return None
    
    def parse_relative_date(self, upload_date_text, current_time):
        """Handle relative dates like '5 days ago', '2 weeks ago'"""
        try:
            relative_patterns = [
                (r'(\d+)\s*(?:days?|d)\s*ago', 'days'),
                (r'(\d+)\s*(?:weeks?|w)\s*ago', 'weeks'),
                (r'(\d+)\s*(?:months?|mon)\s*ago', 'months'),
                (r'(\d+)\s*(?:years?|y)\s*ago', 'years'),
                (r'(\d+)\s*(?:hours?|h)\s*ago', 'hours'),
                (r'(\d+)\s*(?:minutes?|min|m)\s*ago', 'minutes'),
                (r'yesterday', 'yesterday'),
                (r'today', 'today'),
            ]
            
            for pattern, time_unit in relative_patterns:
                if time_unit == 'yesterday':
                    if 'yesterday' in upload_date_text:
                        upload_date = current_time - timedelta(days=1)
                        return int(upload_date.timestamp() * 1000)
                elif time_unit == 'today':
                    if 'today' in upload_date_text:
                        return int(current_time.timestamp() * 1000)
                else:
                    match = re.search(pattern, upload_date_text)
                    if match:
                        amount = int(match.group(1))
                        if time_unit == 'days':
                            upload_date = current_time - timedelta(days=amount)
                        elif time_unit == 'weeks':
                            upload_date = current_time - timedelta(weeks=amount)
                        elif time_unit == 'months':
                            upload_date = current_time - relativedelta(months=amount)
                        elif time_unit == 'years':
                            upload_date = current_time - relativedelta(years=amount)
                        elif time_unit == 'hours':
                            upload_date = current_time - timedelta(hours=amount)
                        elif time_unit == 'minutes':
                            upload_date = current_time - timedelta(minutes=amount)
                        return int(upload_date.timestamp() * 1000)
            
            return None
        except Exception as e:
            self.logger.warning(f"Error parsing relative date '{upload_date_text}': {e}")
            return None
    
    def parse_absolute_date(self, upload_date_text):
        """Handle absolute date formats like '2023-01-15'"""
        try:
            date_formats = [
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%B %d, %Y",
                "%b %d, %Y",
                "%d %B %Y",
                "%d %b %Y"
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(upload_date_text, fmt)
                    return int(parsed_date.timestamp() * 1000)
                except ValueError:
                    continue
            
            return None
        except Exception as e:
            self.logger.warning(f"Error parsing absolute date '{upload_date_text}': {e}")
            return None
    
    def validate_parsed_date(self, epoch_timestamp):
        """Ensure parsed date is reasonable and valid"""
        if not epoch_timestamp:
            return False
        
        try:
            # Convert back to datetime for validation
            date = datetime.fromtimestamp(epoch_timestamp / 1000)
            current_time = datetime.now()
            
            # Should not be in the future
            if date > current_time:
                return False
            
            # Should not be older than 20 years
            twenty_years_ago = current_time - relativedelta(years=20)
            if date < twenty_years_ago:
                return False
            
            return True
        except Exception:
            return False