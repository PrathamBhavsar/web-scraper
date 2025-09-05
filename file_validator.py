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
        """Comprehensive validation of extracted video metadata with safe config access"""
        if not video_info:
            self.logger.error("Video info is None")
            return False, ["Video info is None"]

        errors = []
        
        # Safe config access with defaults
        validation_config = self.config.get("validation", {})
        required_fields = validation_config.get("required_json_fields", ["video_id"])

        # Check required fields exist and are not empty
        for field in required_fields:
            if field not in video_info:
                errors.append(f"Missing required field: {field}")
            elif not video_info[field] or str(video_info[field]).strip() == "":
                errors.append(f"Empty required field: {field}")

        # Specific field validations
        self._validate_duration(video_info, errors)
        self._validate_upload_date_epoch(video_info, errors)
        self._validate_views(video_info, errors)
        self._validate_title(video_info, errors)
        self._validate_video_src(video_info, errors)
        self._validate_tags(video_info, errors)

        is_valid = len(errors) == 0

        if not is_valid:
            self.logger.error(f"Video info validation failed: {errors}")
        else:
            self.logger.info("Video info validation passed")

        return is_valid, errors

    def _validate_duration(self, video_info, errors):
        """Validate duration field format and value"""
        if "duration" in video_info:
            duration = str(video_info["duration"]).strip()
            if duration in ["", "00:00", "0:00", "N/A", "null", "undefined"]:
                errors.append(f"Invalid duration: '{duration}' - must be actual video duration")
            elif not re.match(r'^(?:(\d{1,2}):)?(\d{1,2}):(\d{2})$', duration):
                errors.append(f"Duration format invalid: '{duration}' - should be MM:SS or HH:MM:SS")

    def _validate_upload_date_epoch(self, video_info, errors):
        """Validate upload date epoch timestamp"""
        if "upload_date_epoch" in video_info:
            epoch = video_info["upload_date_epoch"]
            if not epoch or not isinstance(epoch, (int, float)) or epoch <= 0:
                errors.append(f"Invalid upload_date_epoch: {epoch} - must be positive integer timestamp")

    def _validate_views(self, video_info, errors):
        """Validate views count is a valid positive integer"""
        if "views" in video_info:
            views = str(video_info["views"]).strip()
            if not views.isdigit() or int(views) < 0:
                errors.append(f"Invalid views: '{views}' - must be positive integer")

    def _validate_title(self, video_info, errors):
        """Validate title meets minimum length requirements"""
        if "title" in video_info:
            title = str(video_info["title"]).strip()
            if len(title) < 3:
                errors.append(f"Title too short: '{title}' - must be at least 3 characters")

    def _validate_video_src(self, video_info, errors):
        """Validate video source URL format"""
        if "video_src" in video_info:
            video_src = str(video_info["video_src"]).strip()
            if not video_src.startswith('http'):
                errors.append(f"Invalid video_src: '{video_src}' - must be valid URL")

    def _validate_tags(self, video_info, errors):
        """Validate tags array structure and content"""
        if "tags" in video_info:
            tags = video_info["tags"]
            if not isinstance(tags, list):
                errors.append(f"Tags must be a list, got: {type(tags)}")
            elif len(tags) == 0:
                errors.append("Tags list is empty - should have at least some tags")

    def validate_video_folder(self, video_id):
        """Check if video folder contains all required files with proper content"""
        try:
            # Safe config access
            general_config = self.config.get("general", {})
            download_path = Path(general_config.get("download_path", "C:\\scraper_downloads\\"))
            video_dir = download_path / video_id

            if not video_dir.exists():
                self.logger.debug(f"Video folder does not exist: {video_dir}")
                return False

            # Check for required files
            json_file = video_dir / f"{video_id}.json"
            video_file = video_dir / f"{video_id}.mp4"
            thumbnail_files = list(video_dir.glob(f"{video_id}.*"))
            thumbnail_files = [f for f in thumbnail_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']]

            validation_errors = []

            # Validate each file type
            self._validate_json_file(json_file, validation_errors)
            self._validate_video_file(video_file, validation_errors)
            self._validate_thumbnail_files(thumbnail_files, validation_errors)

            if validation_errors:
                self.logger.warning(f"Video {video_id} folder validation failed: {validation_errors}")
                return False
            else:
                self.logger.info(f"Video {video_id} folder validation passed")
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
        """Validate video file existence and size"""
        if not video_file.exists():
            validation_errors.append("Video file missing")
        else:
            # Safe config access with default
            validation_config = self.config.get("validation", {})
            min_video_size = validation_config.get("min_video_size_bytes", 1024)
            
            if video_file.stat().st_size < min_video_size:
                validation_errors.append(f"Video file too small: {video_file.stat().st_size} bytes")
            elif not self.verify_video_file(video_file):
                validation_errors.append("Video file appears corrupted")

    def _validate_thumbnail_files(self, thumbnail_files, validation_errors):
        """Validate thumbnail file existence and size"""
        if len(thumbnail_files) == 0:
            validation_errors.append("Thumbnail file missing")
        else:
            thumb_file = thumbnail_files[0]
            # Safe config access with default
            validation_config = self.config.get("validation", {})
            min_thumbnail_size = validation_config.get("min_thumbnail_size_bytes", 100)
            
            if thumb_file.stat().st_size < min_thumbnail_size:
                validation_errors.append(f"Thumbnail file too small: {thumb_file.stat().st_size} bytes")

    def verify_video_file(self, filepath):
        """Check if video file has valid MP4 headers"""
        try:
            if not os.path.exists(filepath):
                return False

            file_size = os.path.getsize(filepath)
            # Safe config access with default
            validation_config = self.config.get("validation", {})
            min_video_size = validation_config.get("min_video_size_bytes", 1024)
            
            if file_size < min_video_size:
                return False

            # Check if file starts with common MP4 headers
            with open(filepath, 'rb') as f:
                header = f.read(32)

            # MP4 files typically start with specific bytes
            mp4_signatures = [b'ftyp', b'\x00\x00\x00\x18ftypmp4', b'\x00\x00\x00\x1cftypmp4']

            for signature in mp4_signatures:
                if signature in header:
                    self.logger.debug(f"Valid MP4 signature found in {filepath}")
                    return True

            # Additional check for common video file patterns
            if any(pattern in header for pattern in [b'mp4', b'MP4', b'ftyp']):
                return True

            self.logger.warning(f"Video file {filepath} does not have valid MP4 signature")
            return False

        except Exception as e:
            self.logger.error(f"Error verifying video file {filepath}: {e}")
            return False

    def check_file_sizes(self, video_dir, video_id):
        """Ensure files meet minimum size requirements"""
        json_file = video_dir / f"{video_id}.json"
        video_file = video_dir / f"{video_id}.mp4"

        # Check JSON file size
        if json_file.exists() and json_file.stat().st_size == 0:
            return False, "JSON file is empty"

        # Safe config access with default
        validation_config = self.config.get("validation", {})
        min_video_size = validation_config.get("min_video_size_bytes", 1024)

        # Check video file size
        if video_file.exists() and video_file.stat().st_size < min_video_size:
            return False, f"Video file too small: {video_file.stat().st_size} bytes"

        return True, "File sizes are valid"

    def validate_complete_download(self, video_info, video_dir):
        """Verify all files are properly downloaded and contain valid data"""
        video_id = video_info["video_id"]

        try:
            json_file = video_dir / f"{video_id}.json"
            video_file = video_dir / f"{video_id}.mp4"
            thumbnail_files = list(video_dir.glob(f"{video_id}.*"))
            thumbnail_files = [f for f in thumbnail_files if f.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']]

            validation_errors = []

            # Validate each component
            self._validate_json_file(json_file, validation_errors)
            self._validate_video_file(video_file, validation_errors)
            self._validate_thumbnail_files(thumbnail_files, validation_errors)

            if validation_errors:
                self.logger.error(f"Complete download validation failed for {video_id}: {validation_errors}")
                return False, validation_errors
            else:
                self.logger.info(f"Complete download validation passed for {video_id}")
                return True, []

        except Exception as e:
            self.logger.error(f"Error in complete download validation for {video_id}: {e}")
            return False, [f"Validation error: {e}"]
