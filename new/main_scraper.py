#!/usr/bin/env python3
"""
Main Script for Rule34Video Scraper with Progress Handler

This script demonstrates how to use the ProgressHandler to continue scraping
from where it left off by reading the progress.json file and starting the
IDM manager with the proper page URL.

Usage:
    python main_scraper.py

Features:
- Automatically reads progress.json for last processed page
- Constructs proper URL for continued scraping
- Starts IDM manager without any modifications needed
- Comprehensive console logging

Author: AI Assistant
Version: 1.0
"""

from progress_handler import ProgressHandler
import sys
import os

def main():
    """
    Main function to start the scraping process with progress tracking.
    """
    print("🎬 Rule34Video Scraper with Progress Handler")
    print("=" * 70)
    print("🔧 This script will:")
    print("   1. Read the last_page from progress.json")
    print("   2. Construct the proper URL (https://rule34video.com/latest-updates/{last_page})")
    print("   3. Start the IDM manager with that URL")
    print("   4. Continue scraping from where it left off")
    print("=" * 70)

    try:
        # Initialize the progress handler
        print("\n🔍 Initializing Progress Handler...")
        handler = ProgressHandler(progress_file="progress.json")

        # Show current progress status
        print("\n📊 Reading current progress...")
        summary = handler.get_progress_summary()

        print("\n📋 CURRENT PROGRESS STATUS:")
        print(f"   📄 Last processed page: {summary['last_page']}")
        print(f"   📥 Total downloaded: {summary['total_downloaded']}")
        print(f"   🎬 Video count in progress: {summary['downloaded_videos']}")
        print(f"   ❌ Failed videos: {summary['failed_videos']}")
        print(f"   🕒 Last updated: {summary['last_updated']}")
        print(f"   📊 Status: {summary['status']}")

        # Get the URL that will be processed
        print("\n🌐 Constructing URL...")
        target_url = handler.construct_url()
        print(f"🎯 Target URL: {target_url}")

        # Confirm before starting
        print("\n" + "="*50)
        print("⚠️  READY TO START SCRAPING")
        print("="*50)
        print(f"📄 Will process page: {summary['last_page']}")
        print(f"🌐 URL: {target_url}")
        print(f"📁 Downloads will go to: downloads/ directory")
        print("="*50)

        # Ask for confirmation (you can remove this for automated runs)
        response = input("\n🤔 Do you want to start the scraping process? (y/n): ").strip().lower()

        if response in ['y', 'yes']:
            print("\n🚀 Starting IDM process...")

            # Start the IDM process
            results = handler.start_idm_process(
                download_dir="downloads",  # You can change this
                idm_path=None  # Auto-detect IDM path
            )

            # Display final results
            print("\n" + "="*80)
            print("🎯 SCRAPING PROCESS COMPLETED")
            print("="*80)

            if results.get("success"):
                print("✅ Process completed successfully!")
                print(f"🌐 URL processed: {results.get('url_used')}")
                print(f"📄 Page processed: {results.get('page_processed')}")

                idm_results = results.get('idm_results', {})
                if idm_results:
                    print(f"🔍 URLs found: {idm_results.get('urls_found', 0)}")
                    print(f"📝 Videos parsed: {idm_results.get('videos_parsed', 0)}")

                    if 'idm_results' in idm_results:
                        idm_stats = idm_results['idm_results']
                        print(f"✅ Successful IDM additions: {idm_stats.get('successful_additions', 0)}")
                        print(f"❌ Failed IDM additions: {idm_stats.get('failed_additions', 0)}")
                        print(f"📂 Directories created: {idm_stats.get('directories_created', 0)}")
                        print(f"📥 Items in queue: {idm_stats.get('download_queue_size', 0)}")
                        print(f"🚀 Queue started: {idm_stats.get('queue_started', False)}")

                print("\n💡 Next steps:")
                print("   - Check IDM for download progress")
                print("   - Files will be organized in the downloads/ directory")
                print("   - Progress will be updated in progress.json")

            else:
                print("❌ Process completed with errors!")
                print(f"🌐 URL that was attempted: {results.get('url_used')}")
                print(f"📄 Page that was attempted: {results.get('page_processed')}")
                print(f"❌ Error: {results.get('error', 'Unknown error')}")

                print("\n🔧 Troubleshooting tips:")
                print("   - Make sure idm_manager.py is in the same directory")
                print("   - Ensure IDM is properly installed")
                print("   - Check your internet connection")
                print("   - Verify the progress.json file is not corrupted")

        else:
            print("\n🛑 Scraping cancelled by user")
            print("💡 You can run this script again anytime to start scraping")

    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user (Ctrl+C)")
        print("💡 Progress has been saved and you can resume later")

    except Exception as e:
        print(f"\n❌ Unexpected error occurred: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Check that all required files are present:")
        print("     - progress.json (will be created if missing)")
        print("     - idm_manager.py")
        print("     - progress_handler.py")
        print("   - Ensure IDM is installed and accessible")
        print("   - Check file permissions")

        return 1

    return 0

if __name__ == "__main__":
    """
    Entry point for the scraper.

    Requirements:
    - progress_handler.py (created by this setup)
    - idm_manager.py (your existing file)
    - progress.json (will be created if missing)
    - Internet Download Manager installed
    """
    print("\n🎯 Rule34Video Scraper - Progress Handler Integration")
    print("📁 Working directory:", os.getcwd())
    print("📄 Required files:", ["progress_handler.py", "idm_manager.py", "progress.json"])

    # Check if required files exist
    required_files = ["progress_handler.py", "idm_manager.py"]
    missing_files = [f for f in required_files if not os.path.exists(f)]

    if missing_files:
        print(f"\n❌ Missing required files: {missing_files}")
        print("💡 Make sure all files are in the same directory")
        sys.exit(1)
    else:
        print("✅ All required files found")

    # Run the main function
    exit_code = main()
    sys.exit(exit_code)
