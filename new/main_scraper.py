#!/usr/bin/env python3
"""
Main Scraper - Refactored Entry Point

Updated main entry point that uses the new batch processing orchestrator
while maintaining backward compatibility with existing interfaces.

Author: AI Assistant
Version: 2.0 - Refactored for batch processing
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from typing import Optional

from scrape_orchestrator import ScrapeOrchestrator
from config_manager import ConfigManager
from utils import LoggingConfig


def setup_logging(config: dict) -> None:
    """Setup logging based on configuration."""
    log_config = config.get("logging", {})

    log_level = log_config.get("log_level", "INFO")
    log_file = log_config.get("log_file_path") if log_config.get("log_to_file", False) else None

    LoggingConfig.setup_logging(log_level, log_file)


def validate_requirements() -> bool:
    """
    Validate that all required dependencies and files are available.

    Returns:
        True if all requirements are met
    """
    required_modules = [
        'config_manager', 'progress_manager', 'scrape_orchestrator',
        'page_parser', 'idm_manager', 'validator', 'utils'
    ]

    missing_modules = []
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)

    if missing_modules:
        print(f"❌ Missing required modules: {missing_modules}")
        return False

    # Check for video_data_parser (legacy dependency)
    try:
        import video_data_parser
        print("✅ Video data parser available")
    except ImportError:
        print("⚠️  video_data_parser.py not found - parsing may fail")
        return False

    print("✅ All required modules available")
    return True


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Web Parser with Batch Processing and Retry Logic",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run with default settings
  %(prog)s --start-page 1000        # Start from specific page
  %(prog)s --max-pages 10           # Limit to 10 pages
  %(prog)s --batch-pages 5          # Process 5 pages per batch
  %(prog)s --dry-run                # Simulate without downloads
  %(prog)s --max-retries 5          # Set max retries per page
        """
    )

    # Basic operation
    parser.add_argument(
        '--start-page', type=int, default=None,
        help='Starting page number (uses progress.json if not specified)'
    )
    parser.add_argument(
        '--max-pages', type=int, default=None,
        help='Maximum number of pages to process (unlimited if not specified)'
    )

    # Batch configuration
    parser.add_argument(
        '--batch-pages', type=int, default=None,
        help='Number of pages to process per batch (overrides config)'
    )
    parser.add_argument(
        '--batch-wait', type=int, default=None,
        help='Initial wait time after batch enqueue in seconds (overrides config)'
    )
    parser.add_argument(
        '--max-retries', type=int, default=None,
        help='Maximum retry attempts per page (overrides config)'
    )

    # Operation modes
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Simulate operations without actual downloads or file changes'
    )

    # Configuration
    parser.add_argument(
        '--config', default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    parser.add_argument(
        '--progress', default='progress.json', 
        help='Path to progress file (default: progress.json)'
    )
    parser.add_argument(
        '--downloads-dir', default=None,
        help='Downloads directory (overrides config)'
    )

    # Logging
    parser.add_argument(
        '--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Set logging level (overrides config)'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Enable verbose logging (sets DEBUG level)'
    )

    return parser


def update_config_from_args(config_manager: ConfigManager, args: argparse.Namespace) -> bool:
    """
    Update configuration based on command line arguments.

    Args:
        config_manager: Configuration manager instance
        args: Parsed command line arguments

    Returns:
        True if config was updated successfully
    """
    config_updated = False

    # Update batch configuration
    if args.batch_pages is not None:
        config_manager.update_config_value("batch", "batch_pages", args.batch_pages)
        config_updated = True

    if args.batch_wait is not None:
        config_manager.update_config_value("batch", "batch_initial_wait_seconds", args.batch_wait)
        config_updated = True

    if args.max_retries is not None:
        config_manager.update_config_value("batch", "max_failed_retries_per_page", args.max_retries)
        config_updated = True

    # Update downloads directory
    if args.downloads_dir is not None:
        config_manager.update_config_value("general", "download_path", args.downloads_dir)
        config_updated = True

    # Update logging configuration
    if args.log_level is not None:
        config_manager.update_config_value("logging", "log_level", args.log_level)
        config_updated = True
    elif args.verbose:
        config_manager.update_config_value("logging", "log_level", "DEBUG")
        config_updated = True

    if config_updated:
        logging.info("Configuration updated based on command line arguments")

    return config_updated


def display_startup_info(config_manager: ConfigManager, args: argparse.Namespace) -> None:
    """Display startup information and configuration."""
    config = config_manager.load_config()

    print("=" * 80)
    print("🚀 Web Parser - Batch Processing with Retry Logic")
    print("=" * 80)

    # Display operation mode
    if args.dry_run:
        print("🧪 DRY RUN MODE - No actual downloads or file changes")
    else:
        print("🔥 LIVE MODE - Downloads and file changes enabled")

    print()
    print("📋 Configuration:")

    # General settings
    general = config.get("general", {})
    print(f"  📁 Download Directory: {general.get('download_path', 'downloads')}")
    print(f"  💾 Storage Limit: {general.get('max_storage_gb', 940)} GB")

    # Batch settings
    batch = config.get("batch", {})
    print(f"  📦 Batch Size: {batch.get('batch_pages', 3)} pages")
    print(f"  ⏱️  Initial Wait: {batch.get('batch_initial_wait_seconds', 240)}s")
    print(f"  🔄 Max Retries: {batch.get('max_failed_retries_per_page', 3)} per page")
    print(f"  ⏲️  Retry Wait: {batch.get('per_page_idm_wait_seconds', 120)}s")

    # Operation parameters
    print()
    print("🎯 Operation Parameters:")
    print(f"  🏁 Start Page: {args.start_page or 'From progress.json'}")
    print(f"  🏁 Max Pages: {args.max_pages or 'Unlimited'}")
    print(f"  📊 Log Level: {config.get('logging', {}).get('log_level', 'INFO')}")

    print("=" * 80)


def main() -> int:
    """
    Main entry point for the scraper.

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Parse command line arguments
    parser = create_argument_parser()
    args = parser.parse_args()

    try:
        # Validate requirements
        if not validate_requirements():
            print("❌ Requirements validation failed")
            return 1

        # Initialize configuration manager
        config_manager = ConfigManager(args.config)

        # Update config from command line arguments
        update_config_from_args(config_manager, args)

        # Setup logging
        config = config_manager.load_config()
        setup_logging(config)

        logger = logging.getLogger(__name__)
        logger.info("Main scraper starting")

        # Display startup information
        display_startup_info(config_manager, args)

        # Determine downloads directory
        downloads_dir = args.downloads_dir or config.get("general", {}).get("download_path", "downloads")

        # Initialize orchestrator
        orchestrator = ScrapeOrchestrator(
            config_file=args.config,
            progress_file=args.progress,
            downloads_dir=downloads_dir,
            dry_run=args.dry_run
        )

        logger.info(f"Orchestrator initialized (dry_run={args.dry_run})")

        # Display pre-run information
        print("\n🔍 Pre-run Status Check:")
        state = orchestrator.get_current_state()
        progress_stats = state.get("progress_stats", {})

        print(f"  📄 Current Page: {progress_stats.get('current_page', 'Unknown')}")
        print(f"  📥 Downloaded Videos: {progress_stats.get('total_downloaded', 0)}")
        print(f"  💾 Total Size: {progress_stats.get('total_size_mb', 0):.1f} MB")
        print(f"  ❌ Failed Videos: {progress_stats.get('failed_video_count', 0)}")
        print(f"  🚫 Permanent Failed Pages: {progress_stats.get('permanent_failed_pages', 0)}")

        print("\n🚀 Starting batch processing workflow...")
        print("Press Ctrl+C to stop gracefully at any time")
        print("-" * 80)

        # Run the orchestrator
        results = orchestrator.run(
            start_page=args.start_page,
            max_pages=args.max_pages
        )

        # Display results
        print("\n" + "=" * 80)
        print("📊 FINAL RESULTS")
        print("=" * 80)

        if results.get("success"):
            print("✅ Scraping completed successfully!")
        elif results.get("interrupted"):
            print("⚠️ Scraping interrupted by user")
        else:
            print("❌ Scraping failed")
            if "error" in results:
                print(f"Error: {results['error']}")

        print()
        print("📈 Statistics:")
        print(f"  ⏱️  Total Time: {results.get('total_time_seconds', 0):.1f}s")
        print(f"  📄 Pages Processed: {results.get('pages_processed', 0)}")
        print(f"  🎬 Videos Found: {results.get('videos_found', 0)}")
        print(f"  📥 Videos Enqueued: {results.get('videos_enqueued', 0)}")
        print(f"  ❌ Failed Pages: {results.get('failed_pages', 0)}")
        print(f"  🚫 Permanent Failures: {results.get('permanent_failed_pages', 0)}")

        if "enqueue_success_rate" in results:
            print(f"  📊 Enqueue Success Rate: {results['enqueue_success_rate']:.1f}%")

        if "page_success_rate" in results:
            print(f"  📊 Page Success Rate: {results['page_success_rate']:.1f}%")

        # Final progress stats
        final_stats = results.get("final_progress_stats", {})
        if final_stats:
            print()
            print("🎯 Final Progress:")
            print(f"  📄 Current Page: {final_stats.get('current_page', 'Unknown')}")
            print(f"  📥 Total Downloaded: {final_stats.get('total_downloaded', 0)}")
            print(f"  💾 Total Size: {final_stats.get('total_size_mb', 0):.1f} MB")

        print("=" * 80)

        # Provide next steps
        print("\n📝 Next Steps:")
        if results.get("success"):
            print("  • Check IDM for any remaining downloads")
            print("  • Verify completed videos in downloads folder")
            print("  • Run again to continue from current position")
        else:
            print("  • Check logs for error details")
            print("  • Verify IDM is installed and accessible") 
            print("  • Check network connectivity")
            print("  • Try running with --dry-run to test configuration")

        print("  • Use --help for additional command line options")

        logger.info("Main scraper completed")

        return 0 if results.get("success") else 1

    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
        logging.info("Main scraper interrupted by user")
        return 1

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        logging.error(f"Unexpected error in main: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    """Entry point when run directly."""

    # Set working directory info for debugging
    print(f"Working Directory: {os.getcwd()}")
    print(f"Script Location: {Path(__file__).parent.absolute()}")

    # Check Python version
    if sys.version_info < (3, 7):
        print("❌ Python 3.7 or higher is required")
        sys.exit(1)

    # Run main function
    exit_code = main()

    print("\n👋 Web Parser finished")
    sys.exit(exit_code)
