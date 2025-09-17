
"""
Main Video Scraper with IDM Integration

This module serves as the main entry point for the video scraping project.
It integrates the IDM manager with progress tracking and configuration management.

Features:
- Progress tracking with resume capability
- Storage limit monitoring
- IDM integration for downloads  
- Configuration management
- Starting strategy determination

Author: AI Assistant
Date: September 2025
"""

import os
import sys
import time
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import our modules
from progress_tracker import ProgressTracker


class VideoScraperMain:
    """
    Main video scraper class that orchestrates the entire scraping process.
    """

    def __init__(self):
        """Initialize the main scraper with all components."""
        print("🎬 Video Scraper with IDM Integration")
        print("=" * 60)

        # Initialize components
        self.progress_tracker = ProgressTracker()

        # Try to import IDM manager
        try:
            # Read the idm_manager.py file and import it
            sys.path.append('.')

            print("📦 Loading IDM Manager...")

            # Read the idm_manager file content to understand its structure
            with open('idm_manager.py', 'r', encoding='utf-8') as f:
                idm_content = f.read()

            # Check if the IDM manager classes are available
            if 'class FixedIDMManager' in idm_content:
                print("✅ Found FixedIDMManager class")
                # We'll import it dynamically
                from idm_manager import FixedIDMManager

                download_path = self.progress_tracker.get_download_path()
                self.idm_manager = FixedIDMManager(base_download_dir=download_path)
                print("✅ IDM Manager initialized")
            else:
                print("❌ IDM Manager class not found")
                self.idm_manager = None

        except Exception as e:
            print(f"⚠️ Could not load IDM Manager: {e}")
            self.idm_manager = None

        # Configuration
        self.base_url = "https://rule34video.com"
        self.max_storage_gb = self.progress_tracker.get_max_storage_gb()

        print(f"🌐 Base URL: {self.base_url}")
        print(f"📊 Max storage: {self.max_storage_gb} GB")
        print("=" * 60)

    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met for scraping."""
        print("🔍 Checking prerequisites...")

        issues = []

        # Check IDM availability
        if not self.idm_manager:
            issues.append("IDM Manager not available")

        # Check storage limit
        limit_exceeded, current_gb, max_gb = self.progress_tracker.check_storage_limit()
        if limit_exceeded:
            issues.append(f"Storage limit exceeded ({current_gb:.1f}/{max_gb:.1f} GB)")

        # Check configuration
        if not self.base_url:
            issues.append("Base URL not configured")

        if issues:
            print("❌ Prerequisites check failed:")
            for issue in issues:
                print(f"   - {issue}")
            return False
        else:
            print("✅ All prerequisites met")
            return True

    def determine_starting_strategy(self) -> Tuple[int, str]:
        """
        Determine the starting strategy based on progress.json.

        Returns:
            Tuple of (starting_page, strategy_description)
        """
        return self.progress_tracker.determine_starting_page()

    def simulate_page_processing(self, page_num: int) -> Dict:
        """
        Simulate processing a single page (placeholder for actual implementation).

        Args:
            page_num: Page number to process

        Returns:
            Dictionary with processing results
        """
        print(f"\n📄 Processing page {page_num}...")

        # Simulate finding videos on the page
        # In real implementation, this would scrape the website
        simulated_videos = [
            {
                'video_id': f'video_{page_num}_001',
                'title': f'Sample Video 1 from Page {page_num}',
                'video_src': f'https://example.com/video_{page_num}_001.mp4',
                'thumbnail_src': f'https://example.com/thumb_{page_num}_001.jpg'
            },
            {
                'video_id': f'video_{page_num}_002', 
                'title': f'Sample Video 2 from Page {page_num}',
                'video_src': f'https://example.com/video_{page_num}_002.mp4',
                'thumbnail_src': f'https://example.com/thumb_{page_num}_002.jpg'
            }
        ]

        # Filter out already downloaded videos
        videos_to_process = []
        for video in simulated_videos:
            if not self.progress_tracker.is_video_downloaded(video['video_id']):
                videos_to_process.append(video)
            else:
                print(f"⏭️ Skipping {video['video_id']} - already downloaded")

        print(f"🎯 Found {len(videos_to_process)} new videos to process")

        # Process videos with IDM
        results = {'successful': 0, 'failed': 0, 'skipped': len(simulated_videos) - len(videos_to_process)}

        if self.idm_manager and videos_to_process:
            print(f"📥 Adding {len(videos_to_process)} videos to IDM queue...")

            for video in videos_to_process:
                try:
                    # Add to IDM queue using the existing method
                    idm_result = self.idm_manager.add_video_to_idm_queue(video)

                    if idm_result.get('video', False):  # If video was added successfully
                        # Simulate download completion and add to progress
                        # In real implementation, you'd wait for download to complete
                        simulated_size_mb = 25.5  # Simulated video size
                        self.progress_tracker.add_downloaded_video(video['video_id'], simulated_size_mb)
                        results['successful'] += 1
                        print(f"✅ Successfully added {video['video_id']} to IDM")
                    else:
                        results['failed'] += 1
                        self.progress_tracker.add_failed_video(video['video_id'], "IDM addition failed")

                except Exception as e:
                    print(f"❌ Error processing {video['video_id']}: {e}")
                    results['failed'] += 1
                    self.progress_tracker.add_failed_video(video['video_id'], str(e))

        # Update last processed page
        self.progress_tracker.update_last_page(page_num)

        return results

    def run_scraping_loop(self, start_page: int, max_pages: int = 5):
        """
        Main scraping loop that processes pages backwards.

        Args:
            start_page: Page to start from
            max_pages: Maximum number of pages to process (for demo)
        """
        print(f"\n🚀 Starting scraping loop from page {start_page}")
        print(f"📖 Will process up to {max_pages} pages (going backwards)")
        print("=" * 60)

        current_page = start_page
        pages_processed = 0

        while current_page > 0 and pages_processed < max_pages:
            print(f"\n📑 Processing page {current_page} ({pages_processed + 1}/{max_pages})")

            # Check storage limit before processing
            limit_exceeded, current_gb, max_gb = self.progress_tracker.check_storage_limit()
            if limit_exceeded:
                print("🚫 Storage limit exceeded! Stopping scraper.")
                break

            try:
                # Process the current page
                results = self.simulate_page_processing(current_page)

                print(f"📊 Page {current_page} results:")
                print(f"   ✅ Successful: {results['successful']}")
                print(f"   ❌ Failed: {results['failed']}")
                print(f"   ⏭️ Skipped: {results['skipped']}")

                # Move to previous page (backwards scraping)
                current_page -= 1
                pages_processed += 1

                # Brief delay between pages
                if pages_processed < max_pages:
                    print("⏱️ Waiting 2 seconds before next page...")
                    time.sleep(2)

            except Exception as e:
                print(f"❌ Error processing page {current_page}: {e}")
                current_page -= 1
                pages_processed += 1
                continue

        print(f"\n🏁 Scraping loop completed!")
        print(f"📈 Processed {pages_processed} pages")
        print(f"🔚 Last page processed: {current_page + 1}")

    def run(self, max_pages: int = 5):
        """
        Main entry point to run the scraper.

        Args:
            max_pages: Maximum pages to process (for demo purposes)
        """
        print("🎬 Starting Video Scraper with IDM Integration")
        print("=" * 80)

        # Show current progress summary
        self.progress_tracker.print_summary()

        # Check prerequisites
        if not self.check_prerequisites():
            print("❌ Cannot start scraping due to prerequisite failures")
            return False

        # Determine starting strategy
        starting_page, strategy = self.determine_starting_strategy()

        print(f"\n🚀 Starting Strategy: {strategy}")
        print(f"📄 Starting from page: {starting_page}")

        # Run the scraping loop
        try:
            self.run_scraping_loop(starting_page, max_pages)

            # Final summary
            print("\n" + "=" * 80)
            print("📊 FINAL SUMMARY")
            print("=" * 80)
            self.progress_tracker.print_summary()

            return True

        except KeyboardInterrupt:
            print("\n⚠️ Scraping interrupted by user (Ctrl+C)")
            return False
        except Exception as e:
            print(f"\n❌ Unexpected error during scraping: {e}")
            return False


def main():
    """Main function to run the scraper."""
    print("🎯 Video Scraper with IDM Integration - Demo Mode")
    print("=" * 80)
    print("📝 Note: This is a demonstration that shows the flow without")
    print("   actually downloading files or opening Chrome browser.")
    print("=" * 80)

    # Create and run scraper
    scraper = VideoScraperMain()

    # Run with limited pages for demo
    success = scraper.run(max_pages=3)  # Only process 3 pages for demo

    if success:
        print("\n✅ Scraper completed successfully!")
    else:
        print("\n❌ Scraper finished with errors")


if __name__ == "__main__":
    main()
