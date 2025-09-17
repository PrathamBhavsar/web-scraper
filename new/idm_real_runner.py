
"""
Real Infinite IDM Runner with Page Progression and Storage Limits

Features:
- Starts at https://rule34video.com/latest-updates/1000/
- Decreases page number after each loop (1000 → 999 → 998...)  
- Checks storage limits and stops when exceeded
- Integrates with progress tracker for storage monitoring

Author: AI Assistant
Date: September 2025
"""

import asyncio
import time
import sys
import os
from pathlib import Path

# Import IDM processor
try:
    from idm_manager import FixedVideoIDMProcessor
    print("✅ Successfully imported FixedVideoIDMProcessor")
except ImportError as e:
    print(f"❌ Could not import FixedVideoIDMProcessor from idm_manager: {e}")
    sys.exit(1)

# Import progress tracker for storage monitoring
try:
    from progress_tracker import ProgressTracker
    print("✅ Successfully imported ProgressTracker")
except ImportError as e:
    print(f"⚠️ Could not import ProgressTracker: {e}")
    print("💡 Will continue without storage limit checking")

class PageProgressiveIDMRunner:
    """
    IDM Runner that processes pages in descending order with storage limit checking.
    """

    def __init__(self, starting_page: int = 1000):
        """
        Initialize the runner.

        Args:
            starting_page: Page number to start from (default: 1000)
        """
        self.base_url = "https://rule34video.com"
        self.download_dir = "E:/scraper_downloads/"
        self.current_page = starting_page
        self.loop_count = 0
        self.total_processed = 0

        print("🎬 Page Progressive IDM Runner")
        print("=" * 60)
        print(f"🌐 Base URL: {self.base_url}")
        print(f"📁 Download Dir: {self.download_dir}")
        print(f"📄 Starting Page: {self.current_page}")
        print("=" * 60)

        # Initialize progress tracker for storage monitoring
        try:
            self.progress_tracker = ProgressTracker()
            self.max_storage_gb = self.progress_tracker.get_max_storage_gb()
            print(f"📊 Storage limit: {self.max_storage_gb} GB")
        except:
            self.progress_tracker = None
            self.max_storage_gb = 940  # Default fallback
            print(f"⚠️ Using default storage limit: {self.max_storage_gb} GB")

        # Initialize IDM processor
        try:
            self.processor = FixedVideoIDMProcessor(
                base_url=self.base_url,
                download_dir=self.download_dir,
                idm_path=None
            )
            print("✅ IDM Processor initialized")
        except Exception as e:
            print(f"❌ Failed to initialize IDM processor: {e}")
            raise

    def get_current_page_url(self) -> str:
        """Get the current page URL."""
        return f"{self.base_url}/latest-updates/{self.current_page}/"

    def check_storage_limit(self) -> tuple:
        """
        Check if storage limit has been exceeded.

        Returns:
            Tuple of (limit_exceeded, current_gb, max_gb)
        """
        if self.progress_tracker:
            return self.progress_tracker.check_storage_limit()
        else:
            # Fallback: check actual disk usage if possible
            try:
                # Get folder size
                total_size = 0
                download_path = Path(self.download_dir)
                if download_path.exists():
                    for item in download_path.rglob('*'):
                        if item.is_file():
                            total_size += item.stat().st_size

                current_gb = total_size / (1024 ** 3)  # Convert bytes to GB
                limit_exceeded = current_gb >= self.max_storage_gb

                return limit_exceeded, current_gb, self.max_storage_gb
            except:
                return False, 0, self.max_storage_gb

    async def process_current_page(self) -> dict:
        """
        Process the current page with the IDM processor.

        Returns:
            Dictionary with processing results
        """
        page_url = self.get_current_page_url()

        print(f"\n📄 Processing page {self.current_page}")
        print(f"🔗 URL: {page_url}")
        print("-" * 60)

        try:
            # Update the processor's base URL to current page
            self.processor.base_url = page_url

            # Process all videos on current page
            results = await self.processor.process_all_videos()

            if results.get('success'):
                urls_found = results.get('urls_found', 0)
                videos_parsed = results.get('videos_parsed', 0)

                print(f"✅ Page {self.current_page} processed successfully!")
                print(f"🔍 URLs found: {urls_found}")
                print(f"📝 Videos parsed: {videos_parsed}")

                # Get IDM results if available
                idm_results = results.get('idm_results', {})
                successful = idm_results.get('successful_additions', 0)
                failed = idm_results.get('failed_additions', 0)

                print(f"✅ IDM successful: {successful}")
                print(f"❌ IDM failed: {failed}")

                self.total_processed += videos_parsed

                return {
                    'success': True,
                    'urls_found': urls_found,
                    'videos_parsed': videos_parsed,
                    'idm_successful': successful,
                    'idm_failed': failed
                }
            else:
                error = results.get('error', 'Unknown error')
                print(f"❌ Page {self.current_page} failed: {error}")
                return {'success': False, 'error': error}

        except Exception as e:
            print(f"❌ Error processing page {self.current_page}: {e}")
            return {'success': False, 'error': str(e)}

    async def run_infinite_loop(self):
        """
        Run the infinite loop with page progression and storage checking.
        """
        print(f"\n🚀 Starting infinite loop from page {self.current_page}")
        print("📈 Will process pages in descending order (1000 → 999 → 998...)")
        print("💾 Will check storage limits after each page")
        print("⏹️ Press Ctrl+C to stop")
        print("=" * 80)

        try:
            while self.current_page > 0:
                self.loop_count += 1

                # Check storage limit before processing
                limit_exceeded, current_gb, max_gb = self.check_storage_limit()

                print(f"\n💾 Storage Check (Loop {self.loop_count}):")
                print(f"📊 Current: {current_gb:.2f} GB / {max_gb:.0f} GB")
                print(f"📉 Usage: {(current_gb/max_gb*100):.1f}%")

                if limit_exceeded:
                    print("\n🚫 STORAGE LIMIT EXCEEDED!")
                    print(f"📊 Current size: {current_gb:.2f} GB")
                    print(f"📈 Maximum allowed: {max_gb:.2f} GB")
                    print("⛔ Stopping parser to prevent disk overflow!")
                    break

                # Process current page
                print(f"\n🔄 LOOP {self.loop_count} - Page {self.current_page}")
                results = await self.process_current_page()

                if results['success']:
                    print(f"\n✅ Loop {self.loop_count} completed successfully!")
                    print(f"📊 Total videos processed so far: {self.total_processed}")
                else:
                    print(f"\n⚠️ Loop {self.loop_count} had issues: {results.get('error')}")

                # Move to next page (decrease page number)
                self.current_page -= 1
                next_page = self.current_page

                if next_page > 0:
                    print(f"\n📄 Next page will be: {next_page}")
                    print(f"🔗 Next URL: {self.base_url}/latest-updates/{next_page}/")
                    print("⏱️ Sleeping 10 seconds before next page...")
                    await asyncio.sleep(10)
                else:
                    print("\n🏁 Reached page 0 - stopping loop")
                    break

        except KeyboardInterrupt:
            print(f"\n⏹️ Stopping after {self.loop_count} loops")
            print("✅ Loop stopped by user (Ctrl+C)")
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")

        # Final summary
        print("\n" + "=" * 80)
        print("🏁 FINAL SUMMARY")
        print("=" * 80)
        print(f"📄 Started at page: 1000")
        print(f"📄 Ended at page: {self.current_page + 1}")
        print(f"🔄 Total loops: {self.loop_count}")
        print(f"📊 Total videos processed: {self.total_processed}")

        # Final storage check
        limit_exceeded, current_gb, max_gb = self.check_storage_limit()
        print(f"💾 Final storage: {current_gb:.2f} GB / {max_gb:.0f} GB ({(current_gb/max_gb*100):.1f}%)")

        if limit_exceeded:
            print("🚫 Stopped due to storage limit")
        else:
            print("✅ Storage limit OK")

        print("=" * 80)

async def main():
    """Main entry point."""
    print("🎯 Page Progressive IDM Runner")
    print("=" * 80)
    print("📋 Features:")
    print("  - Starts at https://rule34video.com/latest-updates/1000/")
    print("  - Processes pages in descending order (1000 → 999 → 998...)")
    print("  - Monitors storage usage and stops when limit exceeded")
    print("  - 10 second delay between pages")
    print("=" * 80)

    try:
        runner = PageProgressiveIDMRunner(starting_page=1000)
        await runner.run_infinite_loop()
    except Exception as e:
        print(f"❌ Failed to start runner: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Stopped by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")

    print("\n✅ Program finished!")
