#!/usr/bin/env python3
"""
Main Wrapper for Parser GUI Integration

This wrapper provides simple start() and stop() functions that can be called
by the GUI to control the main parser process.

Author: AI Assistant
Version: 1.0
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
    """Wrapper class for the main parser"""

    def __init__(self):
        """Initialize the parser wrapper."""
        self.orchestrator: Optional[ScrapeOrchestrator] = None
        self.is_running = False
        self.should_stop = False

    def start(self) -> bool:
        """Start the parser process"""
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

            print("🚀 Starting parser...")

            # Start the process
            self.process = subprocess.Popen(
                [sys.executable, "main_scraper.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )

            self.is_running = True
            self.should_stop = False

            print(f"✅ Parser started with PID: {self.process.pid}")

            # Start output monitoring thread
            self.output_thread = threading.Thread(target=self._monitor_output, daemon=True)
            self.output_thread.start()

            return True

        except Exception as e:
            print(f"❌ Error starting parser: {e}")
            self.is_running = False
            return False

    def stop(self) -> bool:
        """Stop the parser process"""
        if not self.is_running or not self.process:
            print("Parser is not running!")
            return False

        try:
            print("🛑 Stopping parser...")

            self.should_stop = True

            # Try graceful termination first
            if hasattr(self.process, 'terminate'):
                self.process.terminate()

                # Wait for graceful shutdown
                try:
                    self.process.wait(timeout=10)
                    print("✅ Parser stopped gracefully")
                except subprocess.TimeoutExpired:
                    print("⚠️ Graceful shutdown timeout, forcing termination...")
                    self.process.kill()
                    self.process.wait()
                    print("✅ Parser forcefully terminated")
            else:
                # Fallback for older Python versions
                os.kill(self.process.pid, signal.SIGTERM)
                time.sleep(2)
                try:
                    os.kill(self.process.pid, 0)  # Check if still running
                    os.kill(self.process.pid, signal.SIGKILL)  # Force kill
                except OSError:
                    pass  # Process already terminated

            self.is_running = False
            self.process = None

            return True

        except Exception as e:
            print(f"❌ Error stopping parser: {e}")
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
            print(f"Error reading parser output: {e}")
            return None

    def _monitor_output(self):
        """Monitor parser output in background thread"""
        while self.is_running and not self.should_stop:
            try:
                output = self.get_output()
                if output:
                    print(f"[PARSER] {output}")

                # Small delay to prevent busy waiting
                time.sleep(0.1)

            except Exception as e:
                print(f"Error in output monitoring: {e}")
                break

        print("Output monitoring stopped")


# Global parser instance
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


def get_status() -> dict:
    """Get parser status (GUI interface function)"""
    return {
        "running": _parser_wrapper.is_parser_running(),
        "pid": _parser_wrapper.process.pid if _parser_wrapper.process else None
    }


def main():
    """Main function for testing the wrapper"""
    print("🧪 Testing Parser Wrapper")
    print("=" * 50)

    try:
        # Test status
        print("\n1. Testing initial status...")
        status = get_status()
        print(f"Initial status: {status}")

        # Test start
        print("\n2. Testing start()...")
        if start():
            print("Start successful")

            # Let it run for a few seconds
            print("\n2. Letting parser run for 10 seconds...")
            time.sleep(10)

            # Test status
            print("\n3. Testing status...")
            status = get_status()
            print(f"Status: {status}")

            # Test stop
            if is_running():
                print("\n3. Testing stop()...")
                if stop():
                    print("Stop successful")
                else:
                    print("Stop failed")
            else:
                print("❌ Stop failed")
        else:
            print("Start failed")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        if is_running():
            print("Stopping parser...")
            stop()
    except Exception as e:
        print(f"\nError in testing: {e}")
        if is_running():
            stop()


if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    main()
