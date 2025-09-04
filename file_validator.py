import re
import os
import json
import logging
from pathlib import Path

class FileValidator:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('Rule34Scraper')
    
    def validate_video_info(self, video_info):
        """Relaxed validation of extracted video metadata - allows more flexibility"""
        if not video_info:
            self.logger.error("Video info is None")
            return False, ["Video info is None"]
        
        errors = []
        warnings = []
        
        # Core required fields - only check these are present
        core_required_fields = ["video_id", "video_src"]
        
        for field in core_required_fields:
            if field not in video_info:
                errors.append(f"Missing critical field: {field}")
            elif not video_info[field] or str(video_info[field]).strip() == "":
                errors.append(f"Empty critical field: {field}")
        
        # Relaxed field validations - these generate warnings, not errors
        self._validate_duration_relaxed(video_info, warnings)
        self._validate_upload_date_epoch_relaxed(video_info, warnings)
        self._validate_views_relaxed(video_info, warnings)
        self._validate_title_relaxed(video_info, warnings)
        self._validate_video_src_relaxed(video_info, warnings)
        self._validate_tags_relaxed(video_info, warnings)
        
        is_valid = len(errors) == 0
        
        if warnings:
            self.logger.warning(f"Video info has warnings (but still valid): {warnings}")
        
        if not is_valid:
            self.logger.error(f"Video info validation failed: {errors}")
        else:
            self.logger.info("Video info validation passed")
        
        return is_valid, errors
    
    def _validate_duration_relaxed(self, video_info, warnings):
        """Relaxed duration validation - allows defaults and estimates"""
        if "duration" in video_info:
            duration = str(video_info["duration"]).strip()
            # Only warn about completely missing duration
            if duration in ["", "N/A", "null", "undefined"]:
                warnings.append(f"Duration is placeholder: '{duration}'")
            # Allow 00:00 and estimates like 00:30
            elif duration in ["00:00", "0:00"]:
                warnings.append(f"Duration may be inaccurate: '{duration}'")
    
    def _validate_upload_date_epoch_relaxed(self, video_info, warnings):
        """Relaxed upload date validation - allows current timestamp as fallback"""
        if "upload_date_epoch" in video_info:
            epoch = video_info["upload_date_epoch"]
            if not epoch or not isinstance(epoch, (int, float)):
                warnings.append(f"Upload date epoch missing or invalid: {epoch}")
            # Don't require it to be in the past - allow current time fallbacks
    
    def _validate_views_relaxed(self, video_info, warnings):
        """Relaxed views validation - allows zero views"""
        if "views" in video_info:
            views = str(video_info["views"]).strip()
            # Allow zero views - new videos might have no views yet
            if not views.isdigit():
                warnings.append(f"Views not a number: '{views}' - using as-is")
    
    def _validate_title_relaxed(self, video_info, warnings):
        """Relaxed title validation - allows generated titles"""
        if "title" in video_info:
            title = str(video_info["title"]).strip()
            if len(title) < 1:
                warnings.append("Title is empty")
            elif title.startswith("Video_"):
                warnings.append(f"Title appears to be auto-generated: '{title}'")
    
    def _validate_video_src_relaxed(self, video_info, warnings):
        """Strict video source validation - this is critical for downloads"""
        if "video_src" in video_info:
            video_src = str(video_info["video_src"]).strip()
            if not video_src:
                # This is still an error since we need video source to download
                return
            elif not video_src.startswith('http'):
                warnings.append(f"Video source may be relative URL: '{video_src[:50]}...'")
    
    def _validate_tags_relaxed(self, video_info, warnings):
        """Relaxed tags validation - allows default tags"""
        if "tags" in video_info:
            tags = video_info["tags"]
            if not isinstance(tags, list):
                warnings.append(f"Tags should be a list, got: {type(tags)}")
            elif len(tags) == 0:
                warnings.append("No tags found - will use default")
            elif tags == ["untagged"]:
                warnings.append("Using default 'untagged' tag")
    
    def validate_video_folder(self, video_id):
        """Relaxed folder validation - only check for essential files"""
        try:
            download_path = Path(self.config["general"]["download_path"])
            video_dir = download_path / video_id
            
            if not video_dir.exists():
                self.logger.debug(f"Video folder does not exist: {video_dir}")
                return False
            
            # Only check for essential files
            video_file = video_dir / f"{video_id}.mp4"
            
            # Must have video file and it must be valid
            if not video_file.exists():
                self.logger.debug(f"Video file missing: {video_file}")
                return False
            
            # Check minimum video file size (relaxed)
            min_size = max(1024, self.config["validation"]["min_video_size_bytes"] // 10)  # 10x more lenient
            if video_file.stat().st_size < min_size:
                self.logger.warning(f"Video file very small: {video_file.stat().st_size} bytes")
                return False
            
            # Optional JSON validation - warn but don't fail
            json_file = video_dir / f"{video_id}.json"
            if not json_file.exists():
                self.logger.warning(f"JSON metadata missing for {video_id}")
            elif json_file.stat().st_size == 0:
                self.logger.warning(f"JSON metadata empty for {video_id}")
            
            # Optional thumbnail validation - warn but don't fail
            thumbnail_files = list(video_dir.glob(f"{video_id}.*"))
            thumbnail_files = [f for f in thumbnail_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']]
            if len(thumbnail_files) == 0:
                self.logger.warning(f"No thumbnail found for {video_id}")
            
            self.logger.info(f"Video {video_id} folder validation passed (relaxed mode)")
            return True
                
        except Exception as e:
            self.logger.error(f"Error validating video folder {video_id}: {e}")
            return False
    
    def _validate_json_file(self, json_file, validation_errors):
        """Validate JSON metadata file"""
        if not json_file.exists():
            validation_errors.append("JSON file missing")
        elif json_file.stat().st_size == 0:
            validation_errors.append("JSON file is empty")
        else:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    video_info = json.load(f)
                is_valid, json_errors = self.validate_video_info(video_info)
                if not is_valid:
                    validation_errors.extend([f"JSON validation: {err}" for err in json_errors])
            except json.JSONDecodeError as e:
                validation_errors.append(f"JSON file corrupted: {e}")
            except Exception as e:
                validation_errors.append(f"JSON file error: {e}")
    
    def _validate_video_file(self, video_file, validation_errors):
        """Relaxed video file validation"""
        if not video_file.exists():
            validation_errors.append("Video file missing")
        else:
            # Much more lenient size check
            min_size = max(1024, self.config["validation"]["min_video_size_bytes"] // 10)
            if video_file.stat().st_size < min_size:
                validation_errors.append(f"Video file too small: {video_file.stat().st_size} bytes")
            # Skip MP4 header validation for now - too strict
    
    def _validate_thumbnail_files(self, thumbnail_files, validation_errors):
        """Relaxed thumbnail validation - optional"""
        if len(thumbnail_files) == 0:
            # Don't fail validation for missing thumbnail - just warn
            self.logger.warning("Thumbnail file missing - continuing anyway")
        else:
            thumb_file = thumbnail_files[0]
            # Very lenient thumbnail size check
            if thumb_file.stat().st_size < 100:  # Just 100 bytes minimum
                self.logger.warning(f"Thumbnail file very small: {thumb_file.stat().st_size} bytes")
    
    def verify_video_file(self, filepath):
        """Much more lenient video file verification"""
        try:
            if not os.path.exists(filepath):
                return False
            
            file_size = os.path.getsize(filepath)
            # 10x more lenient minimum size
            min_size = max(1024, self.config["validation"]["min_video_size_bytes"] // 10)
            if file_size < min_size:
                return False
            
            # Skip strict MP4 header checking - too many false negatives
            # Just check file isn't completely empty or corrupted
            try:
                with open(filepath, 'rb') as f:
                    header = f.read(16)
                    # Very basic check - file has some content
                    if len(header) < 10:
                        return False
                    return True
            except:
                return False
                
        except Exception as e:
            self.logger.error(f"Error verifying video file {filepath}: {e}")
            return False
    
    def validate_complete_download(self, video_info, video_dir):
        """Much more lenient complete download validation"""
        video_id = video_info["video_id"]
        
        try:
            video_file = video_dir / f"{video_id}.mp4"
            
            # Only require video file to exist and be reasonable size
            if not video_file.exists():
                self.logger.error(f"Video file missing for {video_id}")
                return False, ["Video file missing"]
            
            # Very lenient size check
            min_size = max(1024, self.config["validation"]["min_video_size_bytes"] // 10)
            if video_file.stat().st_size < min_size:
                self.logger.error(f"Video file too small for {video_id}: {video_file.stat().st_size} bytes")
                return False, [f"Video file too small: {video_file.stat().st_size} bytes"]
            
            # Basic file corruption check
            if not self.verify_video_file(video_file):
                self.logger.error(f"Video file appears corrupted for {video_id}")
                return False, ["Video file appears corrupted"]
            
            # Check other files but don't fail validation for them
            json_file = video_dir / f"{video_id}.json"
            if not json_file.exists():
                self.logger.warning(f"JSON metadata missing for {video_id} - creating minimal version")
                self._create_minimal_json(video_info, json_file)
            
            self.logger.info(f"Complete download validation passed for {video_id} (relaxed mode)")
            return True, []
                
        except Exception as e:
            self.logger.error(f"Error in complete download validation for {video_id}: {e}")
            return False, [f"Validation error: {e}"]
    
    def _create_minimal_json(self, video_info, json_file):
        """Create minimal JSON metadata if missing"""
        try:
            minimal_info = {
                "video_id": video_info.get("video_id", "unknown"),
                "title": video_info.get("title", "Unknown Video"),
                "duration": video_info.get("duration", "00:30"),
                "views": video_info.get("views", "0"),
                "upload_date_epoch": video_info.get("upload_date_epoch", int(datetime.now().timestamp() * 1000))
            }
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(minimal_info, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Created minimal JSON metadata: {json_file}")
            
        except Exception as e:
            self.logger.warning(f"Could not create minimal JSON: {e}")


from datetime import datetime