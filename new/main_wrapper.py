#!/usr/bin/env python3
"""
Main Wrapper - Backward Compatibility Layer

Maintains the existing start/stop API for GUI and other integrations
while forwarding to the new ScrapeOrchestrator.

Author: AI Assistant
Version: 2.0 - Refactored for orchestrator integration
"""

import os
import sys
import threading
import time
from typing import Optional, Dict, Any
import logging

from scrape_orchestrator import ScrapeOrchestrator
from config_manager import ConfigManager
from utils import LoggingConfig

logger = logging.getLogger(__name__)


class ParserWrapper:
    """Wrapper class that maintains backward compatibility."""

    def __init__(self):
        """Initialize the parser wrapper."""
        self.orchestrator: Optional[ScrapeOrchestrator] = None
        self.is_running = False
        self.should_stop = False
        self.worker_thread: Optional[threading.Thread] = None
        self.results: Optional[Dict[str, Any]] = None
        self.error_message: Optional[str] = None

        # Setup basic logging
        try:
            config_manager = ConfigManager()
            config = config_manager.load_config()
            log_config = config.get("logging", {})

            if not logging.getLogger().handlers:  # Only setup if not already configured
                LoggingConfig.setup_logging(
                    log_config.get("log_level", "INFO"),
                    log_config.get("log_file_path") if log_config.get("log_to_file") else None
                )
        except Exception as e:
            # Fallback to basic logging if config fails
            logging.basicConfig(level=logging.INFO)
            logger.warning(f"Could not load logging config: {e}")

    def start(self) -> bool:
        """
        Start the parser process.

        Returns:
            True if started successfully
        """
        if self.is_running:
            logger.warning("Parser is already running")
            return False

        try:
            logger.info("Starting parser via orchestrator")

            # Reset state
            self.should_stop = False
            self.results = None
            self.error_message = None

            # Initialize orchestrator
            try:
                self.orchestrator = ScrapeOrchestrator(
                    config_file="config.json",
                    progress_file="progress.json",
                    dry_run=False  # Live mode for backward compatibility
                )
                logger.info("Orchestrator initialized successfully")
            except Exception as e:
                self.error_message = f"Failed to initialize orchestrator: {e}"
                logger.error(self.error_message)
                return False

            # Start worker thread
            self.worker_thread = threading.Thread(
                target=self._worker_function,
                daemon=True,
                name="ParserWorkerThread"
            )

            self.is_running = True
            self.worker_thread.start()

            logger.info("Parser started successfully in background thread")
            return True

        except Exception as e:
            self.error_message = f"Error starting parser: {e}"
            logger.error(self.error_message)
            self.is_running = False
            return False

    def stop(self) -> bool:
        """
        Stop the parser process.

        Returns:
            True if stopped successfully
        """
        if not self.is_running:
            logger.warning("Parser is not running")
            return True

        try:
            logger.info("Stopping parser...")

            # Signal orchestrator to stop
            self.should_stop = True
            if self.orchestrator:
                self.orchestrator.stop()

            # Wait for worker thread to finish (with timeout)
            if self.worker_thread and self.worker_thread.is_alive():
                logger.info("Waiting for worker thread to finish...")
                self.worker_thread.join(timeout=10.0)

                if self.worker_thread.is_alive():
                    logger.warning("Worker thread did not finish within timeout")
                else:
                    logger.info("Worker thread finished successfully")

            self.is_running = False
            logger.info("Parser stopped successfully")
            return True

        except Exception as e:
            self.error_message = f"Error stopping parser: {e}"
            logger.error(self.error_message)
            return False

    def is_parser_running(self) -> bool:
        """
        Check if parser is currently running.

        Returns:
            True if parser is running
        """
        return self.is_running and (not self.worker_thread or self.worker_thread.is_alive())

    def get_status(self) -> Dict[str, Any]:
        """
        Get current parser status.

        Returns:
            Status information dictionary
        """
        status = {
            "running": self.is_parser_running(),
            "should_stop": self.should_stop,
            "has_results": self.results is not None,
            "error_message": self.error_message,
            "worker_thread_alive": self.worker_thread.is_alive() if self.worker_thread else False
        }

        # Add orchestrator state if available
        if self.orchestrator:
            try:
                orchestrator_state = self.orchestrator.get_current_state()
                status["orchestrator_state"] = orchestrator_state
            except Exception as e:
                status["orchestrator_error"] = str(e)

        return status

    def get_results(self) -> Optional[Dict[str, Any]]:
        """
        Get the results from the last parser run.

        Returns:
            Results dictionary or None if not available
        """
        return self.results

    def _worker_function(self) -> None:
        """
        Worker function that runs in the background thread.
        """
        try:
            logger.info("Parser worker thread started")

            if not self.orchestrator:
                self.error_message = "Orchestrator not initialized"
                logger.error(self.error_message)
                self.is_running = False
                return

            # Run the orchestrator
            logger.info("Starting orchestrator workflow")
            self.results = self.orchestrator.run()

            logger.info("Orchestrator workflow completed")

            # Log results summary
            if self.results:
                success = self.results.get("success", False)
                pages = self.results.get("pages_processed", 0)
                videos = self.results.get("videos_found", 0)

                logger.info(f"Parser completed: success={success}, "
                           f"pages={pages}, videos={videos}")

        except Exception as e:
            self.error_message = f"Error in worker thread: {e}"
            logger.error(self.error_message, exc_info=True)
            self.results = {
                "success": False,
                "error": str(e),
                "interrupted": False
            }

        finally:
            logger.info("Parser worker thread finishing")
            self.is_running = False


# Global instance for backward compatibility
_parser_wrapper = ParserWrapper()


def start() -> bool:
    """
    Start the parser (backward compatibility function).

    Returns:
        True if started successfully
    """
    return _parser_wrapper.start()


def stop() -> bool:
    """
    Stop the parser (backward compatibility function).

    Returns:
        True if stopped successfully
    """
    return _parser_wrapper.stop()


def is_running() -> bool:
    """
    Check if parser is running (backward compatibility function).

    Returns:
        True if parser is running
    """
    return _parser_wrapper.is_parser_running()


def get_status() -> Dict[str, Any]:
    """
    Get parser status (backward compatibility function).

    Returns:
        Status information dictionary
    """
    return _parser_wrapper.get_status()


def get_results() -> Optional[Dict[str, Any]]:
    """
    Get parser results (backward compatibility function).

    Returns:
        Results dictionary or None if not available
    """
    return _parser_wrapper.get_results()


# Legacy compatibility functions
def get_output() -> Optional[str]:
    """
    Legacy function for getting parser output.

    Note: The new orchestrator doesn't provide line-by-line output,
    so this returns status information instead.

    Returns:
        Status message or None
    """
    if not _parser_wrapper.is_parser_running():
        return None

    status = _parser_wrapper.get_status()
    orchestrator_state = status.get("orchestrator_state", {})
    progress_stats = orchestrator_state.get("progress_stats", {})

    if progress_stats:
        current_page = progress_stats.get("current_page", "Unknown")
        total_downloaded = progress_stats.get("total_downloaded", 0)
        total_size_mb = progress_stats.get("total_size_mb", 0)

        return f"Processing page {current_page} - Downloaded: {total_downloaded} videos ({total_size_mb:.1f} MB)"

    return "Parser running..."


def main():
    """Main function for testing the wrapper."""
    print("🧪 Testing Parser Wrapper v2.0")
    print("=" * 50)

    try:
        # Test status
        print("\n1. Testing initial status...")
        status = get_status()
        print(f"Initial status: {status}")

        # Test start
        print("\n2. Testing start()...")
        if start():
            print("✅ Start successful")

            # Monitor for a bit
            print("\n3. Monitoring parser for 30 seconds...")
            for i in range(6):
                time.sleep(5)
                if not is_running():
                    print("Parser finished early")
                    break

                status = get_status()
                print(f"Status update {i+1}: running={status['running']}")

                output = get_output()
                if output:
                    print(f"Output: {output}")

            # Test stop
            print("\n4. Testing stop()...")
            if stop():
                print("✅ Stop successful")
            else:
                print("❌ Stop failed")

            # Get final results
            results = get_results()
            if results:
                print("\n5. Final results:")
                for key, value in results.items():
                    print(f"  {key}: {value}")

        else:
            print("❌ Start failed")

    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
        if is_running():
            print("Stopping parser...")
            stop()
    except Exception as e:
        print(f"\n❌ Error in testing: {e}")
        if is_running():
            stop()


if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    main()
