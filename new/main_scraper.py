#!/usr/bin/env python3

"""
Final Continuous Rule34Video Scraper

This is the complete continuous scraping solution that implements:

1. Backward page parsing loop (last_page → last_page-1 → ... → 1)
2. Size limit monitoring (progress.json total_size_mb vs config.json max_storage_gb) 
3. Three stop conditions:
   - Size limit approached (95% of max_storage_gb)
   - User interrupts with Ctrl+C
   - Reaches page 1 (end of pages)
4. Progress saved after each page
5. Graceful interrupt handling

Usage:
    python continuous_main_scraper.py

Author: AI Assistant  
Version: 3.0 - Final continuous implementation
"""

from progress_handler import EnhancedProgressHandler
import json
import sys
import os
import time
import signal
from pathlib import Path
from typing import Dict, Any


class ContinuousScraperController:
    """
    Controls the continuous scraping process with proper stop conditions.
    """

    def __init__(self, progress_file: str = "progress.json", config_file: str = "config.json"):
        """
        Initialize the continuous scraper controller.
        """
        self.progress_file = progress_file
        self.config_file = config_file
        self.config_data = self.load_config()
        self.should_stop = False
        self.stop_reason = ""

        # Setup signal handler for graceful Ctrl+C
        signal.signal(signal.SIGINT, self.signal_handler)

        print("🚀 Continuous Rule34Video Scraper")
        print("=" * 80)
        print("🎯 Stop Conditions:")
        print("   1. Size limit: 95% of max_storage_gb from config.json")
        print("   2. User interrupt: Ctrl+C")
        print("   3. End reached: Page 1")
        print("=" * 80)

    def load_config(self) -> Dict[str, Any]:
        """Load configuration from config.json."""
        try:
            if not os.path.exists(self.config_file):
                print(f"⚠️  Config file not found: {self.config_file}")
                default_config = {"general": {"max_storage_gb": 940}}
                print(f"🔧 Using default: max_storage_gb = 940 GB")
                return default_config

            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            max_storage_gb = config.get('general', {}).get('max_storage_gb', 940)
            print(f"✅ Config loaded: max_storage_gb = {max_storage_gb} GB")
            return config

        except Exception as e:
            print(f"❌ Error loading config: {e}")
            print("🔧 Using default: max_storage_gb = 940 GB")
            return {"general": {"max_storage_gb": 940}}

    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\n\n⚠️  Interrupt received (Ctrl+C)")
        print("🛑 Preparing to stop after current page...")
        self.should_stop = True
        self.stop_reason = "User interrupted with Ctrl+C"

    def get_current_progress(self) -> Dict[str, Any]:
        """Get current progress data."""
        try:
            if not os.path.exists(self.progress_file):
                return {"last_page": 1000, "total_size_mb": 0.0, "total_downloaded": 0}

            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        except Exception as e:
            print(f"❌ Error reading progress: {e}")
            return {"last_page": 1000, "total_size_mb": 0.0, "total_downloaded": 0}

    def update_progress_page(self, new_page: int):
        """Update the last_page in progress.json."""
        try:
            progress_data = self.get_current_progress()
            progress_data['last_page'] = new_page

            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2)

            print(f"💾 Progress updated: last_page = {new_page}")

        except Exception as e:
            print(f"⚠️  Failed to update progress: {e}")

    def check_size_limit(self) -> bool:
        """
        Check if current size is approaching the configured limit.

        Returns:
            True if should stop due to size limit
        """
        progress_data = self.get_current_progress()
        current_size_mb = progress_data.get('total_size_mb', 0.0)
        max_storage_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)

        # Convert GB to MB for comparison
        max_storage_mb = max_storage_gb * 1024

        # Calculate usage percentage
        usage_percent = (current_size_mb / max_storage_mb) * 100 if max_storage_mb > 0 else 0

        print(f"💾 Storage Status:")
        print(f"   📥 Current: {current_size_mb:.2f} MB")
        print(f"   🎯 Limit: {max_storage_gb} GB ({max_storage_mb:.0f} MB)")
        print(f"   📊 Usage: {usage_percent:.2f}%")

        # Stop if approaching 95% to leave buffer
        if usage_percent >= 95.0:
            print("🛑 SIZE LIMIT REACHED!")
            print(f"   ⚠️  Usage ({usage_percent:.2f}%) >= 95% threshold")
            self.stop_reason = f"Size limit reached: {current_size_mb:.2f} MB / {max_storage_gb} GB ({usage_percent:.2f}%)"
            return True

        return False

    def should_continue(self, current_page: int) -> bool:
        """
        Check all stop conditions.

        Returns:
            True if should continue parsing
        """
        # Check user interrupt
        if self.should_stop:
            return False

        # Check if reached the end (page 1)
        if current_page <= 1:
            self.stop_reason = "Reached page 1 (end of available pages)"
            return False

        # Check size limit  
        if self.check_size_limit():
            return False

        return True

    def run_continuous_loop(self) -> Dict[str, Any]:
        """
        Main continuous scraping loop.

        Returns:
            Results dictionary with statistics
        """
        print("\n🔄 Starting continuous parsing loop...")

        # Initialize enhanced progress handler
        handler = EnhancedProgressHandler(progress_file=self.progress_file)

        # Get starting page
        progress_data = self.get_current_progress()
        current_page = progress_data.get('last_page', 1000)
        starting_page = current_page

        print(f"\n🎯 Starting Configuration:")
        print(f"   📄 Starting page: {current_page}")
        print(f"   📊 Current size: {progress_data.get('total_size_mb', 0):.2f} MB")
        print(f"   🎯 Size limit: {self.config_data.get('general', {}).get('max_storage_gb', 940)} GB")
        print(f"   🔄 Parse direction: {current_page} → {current_page-1} → ... → 1")
        print("\n" + "="*80)

        pages_processed = 0
        total_videos_found = 0
        successful_pages = 0
        failed_pages = 0

        try:
            # Main continuous loop
            while self.should_continue(current_page):
                print(f"\n📄 PROCESSING PAGE {current_page}")
                print("-" * 50)

                try:
                    # Process current page using enhanced handler
                    page_results = handler.process_single_page(
                        page=current_page,
                        download_dir="downloads",
                        idm_path=None
                    )

                    pages_processed += 1

                    if page_results.get("success"):
                        successful_pages += 1

                        # Extract video statistics
                        idm_results = page_results.get('idm_results', {})
                        videos_found = idm_results.get('videos_parsed', 0)
                        total_videos_found += videos_found

                        print(f"✅ Page {current_page} completed successfully")
                        print(f"   🎬 Videos found: {videos_found}")
                        print(f"   📊 Total videos so far: {total_videos_found}")

                        # Show download statistics if available
                        if 'idm_results' in idm_results:
                            idm_stats = idm_results['idm_results']
                            successful_adds = idm_stats.get('successful_additions', 0)
                            failed_adds = idm_stats.get('failed_additions', 0)
                            print(f"   ✅ IDM additions: {successful_adds} successful, {failed_adds} failed")

                    else:
                        failed_pages += 1
                        error_msg = page_results.get('error', 'Unknown error')
                        print(f"⚠️  Page {current_page} failed: {error_msg}")
                        # Continue to next page even if current fails

                except Exception as page_error:
                    failed_pages += 1
                    print(f"❌ Exception processing page {current_page}: {page_error}")
                    # Continue to next page even if current fails

                # Move to previous page (backward parsing)
                current_page -= 1

                # Update progress.json with new current page
                self.update_progress_page(current_page)

                # Show progress summary
                progress_data = self.get_current_progress()
                current_size = progress_data.get('total_size_mb', 0.0)
                max_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)
                usage_percent = (current_size / (max_gb * 1024)) * 100

                print(f"📊 Progress: Page {current_page} | Size: {current_size:.2f}MB ({usage_percent:.1f}%) | Processed: {pages_processed}")
                print("-" * 50)

                # Brief pause to prevent server overload
                time.sleep(0.5)

            # Determine final stop reason
            if not self.stop_reason:
                if current_page <= 1:
                    self.stop_reason = "Reached the end (page 1)"
                else:
                    self.stop_reason = "Unknown stop condition"

            # Final statistics
            final_progress = self.get_current_progress()
            final_size_mb = final_progress.get('total_size_mb', 0.0)
            max_storage_gb = self.config_data.get('general', {}).get('max_storage_gb', 940)
            final_usage_percent = (final_size_mb / (max_storage_gb * 1024)) * 100

            print("\n" + "="*80)
            print("🎯 CONTINUOUS SCRAPING COMPLETED")
            print("="*80)
            print(f"🛑 Stop Reason: {self.stop_reason}")
            print(f"📄 Starting page: {starting_page}")
            print(f"📄 Final page: {current_page}")
            print(f"📊 Pages processed: {pages_processed}")
            print(f"✅ Successful pages: {successful_pages}")
            print(f"❌ Failed pages: {failed_pages}")
            print(f"🎬 Total videos found: {total_videos_found}")
            print(f"💾 Final size: {final_size_mb:.2f} MB / {max_storage_gb} GB ({final_usage_percent:.2f}%)")
            print("="*80)

            return {
                "success": True,
                "stop_reason": self.stop_reason,
                "starting_page": starting_page,
                "final_page": current_page,
                "pages_processed": pages_processed,
                "successful_pages": successful_pages,
                "failed_pages": failed_pages,
                "total_videos_found": total_videos_found,
                "final_size_mb": final_size_mb,
                "final_usage_percent": final_usage_percent
            }

        except KeyboardInterrupt:
            print("\n\n⚠️  Keyboard interrupt detected")
            self.stop_reason = "User interrupted with Ctrl+C"
            self.update_progress_page(current_page)

            return {
                "success": False,
                "stop_reason": self.stop_reason,
                "starting_page": starting_page,
                "final_page": current_page,
                "pages_processed": pages_processed,
                "successful_pages": successful_pages,
                "failed_pages": failed_pages,
                "total_videos_found": total_videos_found
            }


def main():
    """
    Main entry point for the continuous scraper.
    """
    print("🎬 Continuous Rule34Video Scraper")
    print("=" * 70)
    print("🔧 This scraper will:")
    print("   1. Read starting page from progress.json")
    print("   2. Parse pages backwards continuously")  
    print("   3. Monitor size limits from config.json")
    print("   4. Stop when conditions are met")
    print("   5. Save progress after each page")
    print("=" * 70)

    try:
        # Check required files
        required_files = ["progress_handler.py", "idm_manager.py", "video_data_parser.py"]
        missing_files = [f for f in required_files if not os.path.exists(f)]

        if missing_files:
            print(f"\n❌ Missing required files: {missing_files}")
            print("💡 Ensure all files are in the same directory")
            return 1

        # Check optional config files
        if not os.path.exists("config.json"):
            print("⚠️  config.json not found - using default max_storage_gb: 940")

        if not os.path.exists("progress.json"):
            print("⚠️  progress.json not found - will start from page 1000")

        print("✅ Required files found")

        # Initialize controller
        controller = ContinuousScraperController()

        # Show initial status
        progress_data = controller.get_current_progress()
        max_gb = controller.config_data.get('general', {}).get('max_storage_gb', 940)
        current_mb = progress_data.get('total_size_mb', 0.0)
        usage_percent = (current_mb / (max_gb * 1024)) * 100

        print("\n📋 CURRENT STATUS:")
        print(f"   📄 Starting page: {progress_data.get('last_page', 1000)}")
        print(f"   💾 Current size: {current_mb:.2f} MB")
        print(f"   🎯 Size limit: {max_gb} GB ({max_gb * 1024} MB)")
        print(f"   📊 Current usage: {usage_percent:.2f}%")
        print(f"   🛑 Stop threshold: 95% ({max_gb * 1024 * 0.95:.0f} MB)")

        # Confirm start
        print("\n" + "="*60)
        print("⚠️  READY TO START CONTINUOUS PARSING")
        print("="*60)
        print("💡 Press Ctrl+C anytime to stop gracefully")
        print("💾 Progress saved after each page")
        print("🔄 Parsing will continue until stop condition met")
        print("="*60)

        response = input("\n🤔 Start continuous parsing? (y/n): ").strip().lower()

        if response in ['y', 'yes']:
            print("\n🚀 Starting continuous scraper...")

            # Run the continuous loop
            results = controller.run_continuous_loop()

            # Display final results
            print("\n📋 FINAL RESULTS:")
            print(f"   Success: {results.get('success')}")
            print(f"   Stop reason: {results.get('stop_reason')}")
            print(f"   Pages processed: {results.get('pages_processed', 0)}")
            print(f"   Videos found: {results.get('total_videos_found', 0)}")

            if results.get("success"):
                print("\n✅ Scraping completed successfully!")
            else:
                print("\n⚠️  Scraping stopped early")

            print("\n💡 Next steps:")
            print("   - Check IDM for active downloads")
            print("   - Downloads saved to downloads/ directory")
            print("   - Run again to continue from current position")

        else:
            print("\n🛑 Cancelled by user")
            print("💡 Run anytime to start scraping")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("\n🔧 Check:")
        print("   - All required files present")
        print("   - IDM installed and accessible")
        print("   - Valid JSON files")
        print("   - File permissions")
        return 1

    return 0


if __name__ == "__main__":
    """
    Entry point for continuous scraper.

    Files needed:
    - progress_handler.py (enhanced version)
    - idm_manager.py (your working version)
    - video_data_parser.py (your working version)  
    - config.json (optional - contains max_storage_gb)
    - progress.json (optional - tracks progress)
    """
    print("\n🎯 Continuous Rule34Video Scraper v3.0")
    print("📁 Working directory:", os.getcwd())

    exit_code = main()

    print("\n👋 Scraper finished")
    input("Press Enter to close...")
    sys.exit(exit_code)
