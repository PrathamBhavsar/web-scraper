import json
import os
import logging

class VideoInfoValidator:
	def __init__(self):
		self.logger = logging.getLogger('Rule34Scraper')
		self.required_fields = [
			"video_id",
			"url", 
			"title",
			"duration",
			"views",
			"uploader",
			"uploaded_by",    # NEW FIELD
			"upload_date",
			"description",    # NEW FIELD
			"categories",     # NEW FIELD - should be list
			"artists",        # NEW FIELD - should be list
			"tags",           # should be list
			"video_src",
			"thumbnail_src"
		]
		
		self.list_fields = ["categories", "artists", "tags"]
		
	def validate_video_info(self, video_info):
		"""Validate a single video info dictionary"""
		if not isinstance(video_info, dict):
			return False, "video_info must be a dictionary"
			
		# Check required fields exist
		missing_fields = []
		for field in self.required_fields:
			if field not in video_info:
				missing_fields.append(field)
				
		if missing_fields:
			return False, f"Missing required fields: {missing_fields}"
			
		# Check list fields are actually lists
		for field in self.list_fields:
			if field in video_info and not isinstance(video_info[field], list):
				return False, f"Field '{field}' must be a list, got {type(video_info[field])}"
				
		# Check essential fields are not empty
		essential_checks = {
			"video_id": lambda x: x and str(x).strip(),
			"url": lambda x: x and str(x).strip(),
			"title": lambda x: x and str(x).strip(),
			"uploaded_by": lambda x: x and str(x).strip(),
			"categories": lambda x: isinstance(x, list) and len(x) > 0,
			"artists": lambda x: isinstance(x, list) and len(x) > 0,
			"tags": lambda x: isinstance(x, list) and len(x) > 0
		}
		
		for field, check_func in essential_checks.items():
			if not check_func(video_info.get(field)):
				return False, f"Field '{field}' is empty or invalid: {video_info.get(field)}"
				
		return True, "Valid video info"
		
	def validate_video_file(self, json_file_path):
		"""Validate a single JSON video file"""
		try:
			if not os.path.exists(json_file_path):
				return False, f"File does not exist: {json_file_path}"
				
			with open(json_file_path, 'r', encoding='utf-8') as f:
				video_info = json.load(f)
				
			return self.validate_video_info(video_info)
			
		except json.JSONDecodeError as e:
			return False, f"Invalid JSON in file {json_file_path}: {e}"
		except Exception as e:
			return False, f"Error reading file {json_file_path}: {e}"
			
	def validate_directory(self, directory_path):
		"""Validate all JSON files in a directory"""
		if not os.path.exists(directory_path):
			self.logger.error(f"Directory does not exist: {directory_path}")
			return {}
			
		results = {
			"total_files": 0,
			"valid_files": 0,
			"invalid_files": 0,
			"errors": []
		}
		
		json_files = [f for f in os.listdir(directory_path) if f.endswith('.json')]
		results["total_files"] = len(json_files)
		
		for json_file in json_files:
			file_path = os.path.join(directory_path, json_file)
			is_valid, message = self.validate_video_file(file_path)
			
			if is_valid:
				results["valid_files"] += 1
				self.logger.info(f"✓ {json_file}: {message}")
			else:
				results["invalid_files"] += 1
				results["errors"].append({
					"file": json_file,
					"error": message
				})
				self.logger.error(f"✗ {json_file}: {message}")
				
		# Log summary
		self.logger.info(f"\nValidation Summary:")
		self.logger.info(f"Total files: {results['total_files']}")
		self.logger.info(f"Valid files: {results['valid_files']}")
		self.logger.info(f"Invalid files: {results['invalid_files']}")
		
		if results["errors"]:
			self.logger.info(f"\nErrors found:")
			for error in results["errors"]:
				self.logger.info(f"  {error['file']}: {error['error']}")
				
		return results
		
	def fix_video_info_defaults(self, video_info):
		"""Fix common issues in video_info by setting proper defaults"""
		fixed_info = video_info.copy()
		
		# Ensure all required fields exist
		defaults = {
			"video_id": "",
			"url": "",
			"title": "",
			"duration": "00:30",
			"views": "0",
			"uploader": "Unknown",
			"uploaded_by": "Unknown",
			"upload_date": 0,
			"description": "No description available",
			"categories": ["uncategorized"],
			"artists": ["unknown_artist"],
			"tags": ["untagged"],
			"video_src": "",
			"thumbnail_src": ""
		}
		
		for field, default_value in defaults.items():
			if field not in fixed_info or not fixed_info[field]:
				fixed_info[field] = default_value
				
		# Ensure list fields are lists
		for field in self.list_fields:
			if field in fixed_info and not isinstance(fixed_info[field], list):
				if isinstance(fixed_info[field], str):
					fixed_info[field] = [fixed_info[field]] if fixed_info[field] else defaults[field]
				else:
					fixed_info[field] = defaults[field]
					
		return fixed_info
		
	def repair_video_file(self, json_file_path):
		"""Attempt to repair an invalid video file"""
		try:
			with open(json_file_path, 'r', encoding='utf-8') as f:
				video_info = json.load(f)
				
			# Check if it needs fixing
			is_valid, _ = self.validate_video_info(video_info)
			if is_valid:
				return True, "File is already valid"
				
			# Apply fixes
			fixed_info = self.fix_video_info_defaults(video_info)
			
			# Validate fixed version
			is_valid, message = self.validate_video_info(fixed_info)
			if not is_valid:
				return False, f"Could not repair file: {message}"
				
			# Create backup
			backup_path = json_file_path + ".backup"
			os.rename(json_file_path, backup_path)
			
			# Save repaired version
			with open(json_file_path, 'w', encoding='utf-8') as f:
				json.dump(fixed_info, f, indent=2, ensure_ascii=False)
				
			return True, f"File repaired successfully (backup saved as {backup_path})"
			
		except Exception as e:
			return False, f"Error repairing file: {e}"

# Usage example
if __name__ == "__main__":
	validator = VideoInfoValidator()
	
	# Validate a single file
	# is_valid, message = validator.validate_video_file("video_123_info.json")
	# print(f"Validation result: {is_valid} - {message}")
	
	# Validate all files in a directory
	# results = validator.validate_directory("./video_info_output")
	
	# Repair a specific file
	# success, message = validator.repair_video_file("video_123_info.json")
	# print(f"Repair result: {success} - {message}")