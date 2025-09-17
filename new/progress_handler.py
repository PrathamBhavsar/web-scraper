
"""
Progress Handler for Rule34Video Scraper

This class manages the progress tracking by reading the last_page from progress.json
and providing the proper page URL to the IDM manager for continued scraping.

Features:
- Reads progress.json to get the last processed page
- Constructs proper URL with page number
- Integrates with existing IDM manager without modifications
- Console logging for tracking progress

Author: AI Assistant
Version: 1.0
"""

import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
import asyncio

class ProgressHandler:
    """
    Handles progress tracking and URL construction for the Rule34Video scraper.

    This class reads the last processed page from progress.json and constructs
    the proper URL for the IDM manager to continue scraping from where it left off.
    """

    def __init__(self, progress_file: str = "progress.json"):
        """
        Initialize Progress Handler.

        Args:
            progress_file: Path to the progress.json file
        """
        self.progress_file = Path(progress_file)
        self.base_url = "https://rule34video.com"

        print(f"📊 Progress Handler Initialized")
        print(f"📄 Progress file: {self.progress_file}")
        print(f"🌐 Base URL: {self.base_url}")

    def read_progress(self) -> Optional[Dict[str, Any]]:
        """
        Read progress data from progress.json file.

        Returns:
            Dictionary containing progress data or None if file doesn't exist/is invalid
        """
        try:
            if not self.progress_file.exists():
                print(f"⚠️ Progress file not found: {self.progress_file}")
                print("💡 Starting from page 1...")
                return {"last_page": 1}

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)

            print(f"✅ Progress file loaded successfully")
            print(f"📄 Last processed page: {progress_data.get('last_page', 1)}")
            print(f"📥 Total downloaded: {progress_data.get('total_downloaded', 0)}")
            print(f"🎬 Downloaded videos: {len(progress_data.get('downloaded_videos', []))}")

            return progress_data

        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in progress file: {e}")
            print("💡 Starting from page 1...")
            return {"last_page": 1}
        except Exception as e:
            print(f"❌ Error reading progress file: {e}")
            print("💡 Starting from page 1...")
            return {"last_page": 1}

    def get_last_page(self) -> int:
        """
        Get the last processed page number from progress.json.

        Returns:
            Last processed page number (defaults to 1 if not found)
        """
        progress_data = self.read_progress()
        if progress_data:
            last_page = progress_data.get('last_page', 1)
            print(f"🔍 Retrieved last page: {last_page}")
            return last_page
        else:
            print(f"🔍 No progress found, starting from page: 1")
            return 1

    def construct_url(self, page: Optional[int] = None) -> str:
        """
        Construct the full URL with page number.

        Args:
            page: Specific page number (uses last_page from progress if None)

        Returns:
            Full URL with page number
        """
        if page is None:
            page = self.get_last_page()

        # Construct URL in the format: https://rule34video.com/latest-updates/{page}
        url = f"{self.base_url}/{page}"

        print(f"🌐 Constructed URL: {url}")
        return url

    def get_progress_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current progress.

        Returns:
            Dictionary containing progress summary
        """
        progress_data = self.read_progress()
        if not progress_data:
            return {
                "last_page": 1,
                "total_downloaded": 0,
                "downloaded_videos": [],
                "failed_videos": [],
                "status": "No progress file found"
            }

        summary = {
            "last_page": progress_data.get('last_page', 1),
            "total_downloaded": progress_data.get('total_downloaded', 0),
            "downloaded_videos": len(progress_data.get('downloaded_videos', [])),
            "failed_videos": len(progress_data.get('failed_videos', [])),
            "last_updated": progress_data.get('last_updated', 'Unknown'),
            "status": "Progress loaded successfully"
        }

        return summary

    def start_idm_process(self, download_dir: str = "downloads", idm_path: str = None) -> Dict[str, Any]:
        """
        Start the IDM manager process with the proper URL from progress.

        Args:
            download_dir: Directory for downloads
            idm_path: Path to IDM executable

        Returns:
            Results from the IDM processing
        """
        print("\n" + "="*80)
        print("🚀 STARTING IDM PROCESS WITH PROGRESS HANDLER")
        print("="*80)

        # Get the URL from progress
        current_url = self.construct_url()

        # Display progress summary
        summary = self.get_progress_summary()
        print(f"📊 PROGRESS SUMMARY:")
        print(f"   📄 Current page: {summary['last_page']}")
        print(f"   📥 Total downloaded: {summary['total_downloaded']}")
        print(f"   🎬 Downloaded videos: {summary['downloaded_videos']}")
        print(f"   ❌ Failed videos: {summary['failed_videos']}")
        print(f"   🕒 Last updated: {summary['last_updated']}")
        print("="*80)

        try:
            # Import and initialize the IDM processor
            from idm_manager import FixedIDMManager

            processor = FixedIDMManager(
                base_url=current_url,
                download_dir=download_dir,
                idm_path=idm_path
            )

            print(f"✅ IDM Processor initialized with URL: {current_url}")

            # Start the async process
            print("🎬 Starting video processing workflow...")
            results = asyncio.run(processor.process_all_videos())

            print("\n" + "="*80)
            print("🎯 IDM PROCESS COMPLETED")
            print("="*80)

            return {
                "success": True,
                "url_used": current_url,
                "page_processed": summary['last_page'],
                "idm_results": results,
                "progress_summary": summary
            }

        except ImportError as e:
            print(f"❌ Could not import IDM manager: {e}")
            print("💡 Make sure idm_manager.py is in the same directory")
            return {
                "success": False,
                "error": f"Import error: {e}",
                "url_used": current_url,
                "page_processed": summary['last_page']
            }
        except Exception as e:
            print(f"❌ Error starting IDM process: {e}")
            return {
                "success": False,
                "error": str(e),
                "url_used": current_url,
                "page_processed": summary['last_page']
            }

# Example usage function
def main():
    """
    Main function to demonstrate usage of ProgressHandler.
    """
    print("📊 Rule34Video Progress Handler")
    print("=" * 60)
    print("🔧 Features:")
    print("   - Reads last_page from progress.json")
    print("   - Constructs proper URL for IDM manager")
    print("   - Integrates seamlessly with existing IDM manager")
    print("   - No changes needed to existing files")
    print("=" * 60)

    # Initialize progress handler
    handler = ProgressHandler()

    # Show current progress
    print("\n📊 Current Progress Status:")
    summary = handler.get_progress_summary()
    for key, value in summary.items():
        print(f"   {key}: {value}")

    # Get the URL that will be used
    url = handler.construct_url()
    print(f"\n🌐 URL that will be processed: {url}")

    print("\n💡 To start the IDM process, call:")
    print("   handler.start_idm_process()")

    # Optionally start the process (commented out for safety)
    # results = handler.start_idm_process()
    # print("\nResults:", results)

if __name__ == "__main__":
    main()
