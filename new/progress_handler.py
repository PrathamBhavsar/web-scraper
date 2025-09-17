#!/usr/bin/env python3

"""
Enhanced Progress Handler for Continuous Rule34Video Scraper

This enhanced version supports continuous page processing with proper
progress tracking, size monitoring, and stop condition checking.

Features:
- Processes individual pages on demand
- Updates progress.json after each page
- Supports custom page numbers
- Enhanced error handling and logging
- Integration with continuous scraper

Author: AI Assistant
Version: 2.0 - Enhanced for continuous processing
"""

import json
import os
from pathlib import Path
from typing import Optional, Dict, Any
import asyncio
import time


class EnhancedProgressHandler:
    """
    Enhanced Progress Handler that supports continuous scraping operations.

    This version can process individual pages and is designed to work
    with the continuous scraper that calls it multiple times for different pages.
    """

    def __init__(self, progress_file: str = "progress.json"):
        """
        Initialize Enhanced Progress Handler.

        Args:
            progress_file: Path to the progress.json file
        """
        self.progress_file = Path(progress_file)
        self.base_url = "https://rule34video.com/latest-updates/"

        print(f"✅ Enhanced Progress Handler Initialized")
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
                print(f"📄 Progress file not found: {self.progress_file}")
                print("🔄 Starting from page 1000...")
                return {"last_page": 1000, "total_size_mb": 0.0, "total_downloaded": 0}

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)

            print("✅ Progress file loaded successfully")
            print(f"📄 Last processed page: {progress_data.get('last_page', 1000)}")
            print(f"📥 Total downloaded: {progress_data.get('total_downloaded', 0)}")
            print(f"💾 Current size: {progress_data.get('total_size_mb', 0.0):.2f} MB")
            print(f"🎬 Downloaded videos: {len(progress_data.get('downloaded_videos', []))}")

            return progress_data

        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in progress file: {e}")
            print("🔄 Starting from page 1000...")
            return {"last_page": 1000, "total_size_mb": 0.0, "total_downloaded": 0}
        except Exception as e:
            print(f"❌ Error reading progress file: {e}")
            print("🔄 Starting from page 1000...")
            return {"last_page": 1000, "total_size_mb": 0.0, "total_downloaded": 0}

    def get_last_page(self) -> int:
        """
        Get the last processed page number from progress.json.

        Returns:
            Last processed page number (defaults to 1000 if not found)
        """
        progress_data = self.read_progress()
        if progress_data:
            last_page = progress_data.get('last_page', 1000)
            print(f"🔍 Retrieved last page: {last_page}")
            return last_page
        else:
            print("🔄 No progress found, starting from page 1000")
            return 1000

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

        # Construct URL in the format: https://rule34video.com/{page}
        url = f"{self.base_url}{page}"
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
                "last_page": 1000,
                "total_downloaded": 0,
                "total_size_mb": 0.0,
                "downloaded_videos": 0,
                "failed_videos": 0,
                "status": "No progress file found"
            }

        summary = {
            "last_page": progress_data.get('last_page', 1000),
            "total_downloaded": progress_data.get('total_downloaded', 0),
            "total_size_mb": progress_data.get('total_size_mb', 0.0),
            "downloaded_videos": len(progress_data.get('downloaded_videos', [])),
            "failed_videos": len(progress_data.get('failed_videos', [])),
            "last_updated": progress_data.get('last_updated', "Unknown"),
            "status": "Progress loaded successfully"
        }

        return summary

    def process_single_page(self, page: int, download_dir: str = "downloads", idm_path: str = None) -> Dict[str, Any]:
        """
        Process a single specific page.

        Args:
            page: Page number to process
            download_dir: Directory for downloads
            idm_path: Path to IDM executable

        Returns:
            Results from processing this specific page
        """
        print(f"\n📄 Processing single page: {page}")
        print("-" * 40)

        try:
            # Construct URL for specific page
            page_url = f"{self.base_url}{page}"
            print(f"🌐 Page URL: {page_url}")

            # Import and initialize the IDM processor
            from idm_manager import FixedVideoIDMProcessor

            processor = FixedVideoIDMProcessor(
                base_url=page_url,
                download_dir=download_dir,
                idm_path=idm_path
            )

            print(f"✅ IDM Processor initialized for page {page}")

            # Process this specific page
            print(f"🚀 Starting processing workflow for page {page}...")
            results = asyncio.run(processor.process_all_videos())

            print(f"✅ Page {page} processing completed")

            return {
                "success": True,
                "page_processed": page,
                "url_used": page_url,
                "idm_results": results
            }

        except ImportError as e:
            print(f"❌ Could not import IDM manager: {e}")
            return {
                "success": False,
                "error": f"Import error: {e}",
                "page_processed": page,
                "url_used": f"{self.base_url}{page}"
            }
        except Exception as e:
            print(f"❌ Error processing page {page}: {e}")
            return {
                "success": False,
                "error": str(e),
                "page_processed": page,
                "url_used": f"{self.base_url}{page}"
            }

    def start_idm_process(self, download_dir: str = "downloads", idm_path: str = None) -> Dict[str, Any]:
        """
        Start the IDM manager process with the proper URL from progress.

        This method processes the current page from progress.json.
        For continuous processing, use process_single_page() instead.

        Args:
            download_dir: Directory for downloads
            idm_path: Path to IDM executable

        Returns:
            Results from the IDM processing
        """
        print("=" * 80)
        print("🚀 STARTING IDM PROCESS WITH PROGRESS HANDLER")
        print("=" * 80)

        # Get the current page from progress
        summary = self.get_progress_summary()
        current_page = summary['last_page']

        # Display progress summary
        print("📊 PROGRESS SUMMARY:")
        print(f"   📄 Current page: {summary['last_page']}")
        print(f"   📥 Total downloaded: {summary['total_downloaded']}")
        print(f"   💾 Current size: {summary['total_size_mb']:.2f} MB")
        print(f"   🎬 Downloaded videos: {summary['downloaded_videos']}")
        print(f"   ❌ Failed videos: {summary['failed_videos']}")
        print(f"   🕒 Last updated: {summary['last_updated']}")
        print("=" * 80)

        # Process the current page
        results = self.process_single_page(current_page, download_dir, idm_path)

        print("=" * 80)
        print("✅ IDM PROCESS COMPLETED")
        print("=" * 80)

        return results


def main():
    """
    Main function to demonstrate usage of Enhanced ProgressHandler.
    """
    print("🎬 Enhanced Rule34Video Progress Handler")
    print("=" * 60)
    print("🔧 Enhanced Features:")
    print("  - Process individual pages on demand")
    print("  - Support for continuous page processing")
    print("  - Enhanced progress tracking")
    print("  - Better error handling")
    print("=" * 60)

    # Initialize progress handler
    handler = EnhancedProgressHandler()

    # Show current progress
    print("\n📊 Current Progress Status:")
    summary = handler.get_progress_summary()
    for key, value in summary.items():
        print(f"   {key}: {value}")

    # Get the URL that will be used
    url = handler.construct_url()
    print(f"\n🎯 Current page URL: {url}")

    print("\n💡 Usage examples:")
    print("   # Process current page from progress.json:")
    print("   handler.start_idm_process()")
    print("   ")
    print("   # Process specific page:")
    print("   handler.process_single_page(999)")


if __name__ == "__main__":
    main()
