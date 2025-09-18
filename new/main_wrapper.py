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
import subprocess
import signal
import threading
import time
from typing import Optional

class ParserWrapper:
    """Wrapper class for the main parser"""

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.is_running = False
        self.should_stop = False

    def start(self) -> bool:
        """Start the parser process"""
        if self.is_running:
            print("Parser is already running!")
            return False

        try:
            # Check if main_scraper.py exists
            if not os.path.exists("main_scraper.py"):
                print("Error: main_scraper.py not found!")
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
        """Check if parser is running"""
        if not self.process:
            return False

        # Check if process is still alive
        poll_result = self.process.poll()
        if poll_result is not None:
            # Process has terminated
            self.is_running = False
            return False

        return self.is_running

    def get_output(self) -> Optional[str]:
        """Get output from the parser process"""
        if not self.process or not self.is_running:
            return None

        try:
            # Non-blocking read
            line = self.process.stdout.readline()
            if line:
                return line.strip()

            # Check if process terminated
            if self.process.poll() is not None:
                self.is_running = False

            return None

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
    """Start the parser (GUI interface function)"""
    return _parser_wrapper.start()


def stop() -> bool:
    """Stop the parser (GUI interface function)"""
    return _parser_wrapper.stop()


def is_running() -> bool:
    """Check if parser is running (GUI interface function)"""
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
        # Test start
        print("\n1. Testing start()...")
        if start():
            print("✅ Start successful")

            # Let it run for a few seconds
            print("\n2. Letting parser run for 10 seconds...")
            time.sleep(10)

            # Test status
            print("\n3. Testing status...")
            status = get_status()
            print(f"Status: {status}")

            # Test stop
            print("\n4. Testing stop()...")
            if stop():
                print("✅ Stop successful")
            else:
                print("❌ Stop failed")
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
    main()
