#!/usr/bin/env python3
"""
Main Wrapper for Parser GUI Integration - Fixed Version

This wrapper provides simple start() and stop() functions that can be called
by the GUI to control the main parser process with proper encoding handling.

FIXES:
- Proper Unicode/encoding handling
- Better subprocess management
- Improved error handling
- Cross-platform compatibility

Author: AI Assistant
Version: 1.1 - Fixed encoding and process control
"""

import os
import sys
import subprocess
import signal
import threading
import time
from typing import Optional


class ImprovedParserWrapper:
    """Enhanced wrapper class for the main parser with encoding fixes"""

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.is_running = False
        self.should_stop = False
        self.output_thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        """Start the parser process with proper encoding handling"""
        if self.is_running:
            print("Parser is already running!")
            return False

        try:
            # Check if main_scraper.py exists
            if not os.path.exists("main_scraper.py"):
                print("Error: main_scraper.py not found!")
                return False

            print("Starting parser with UTF-8 encoding...")

            # Set up environment variables for UTF-8 encoding
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONUTF8'] = '1'

            # Create startup info for Windows
            startupinfo = None
            creation_flags = 0

            if sys.platform.startswith('win'):
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
                creation_flags = subprocess.CREATE_NO_WINDOW

            # Start the process with proper encoding and error handling
            self.process = subprocess.Popen(
                [sys.executable, "-u", "main_scraper.py"],  # -u for unbuffered output
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                encoding='utf-8',
                errors='replace',  # Replace problematic characters instead of crashing
                bufsize=1,
                env=env,
                startupinfo=startupinfo,
                creationflags=creation_flags
            )

            self.is_running = True
            self.should_stop = False

            print(f"Parser started successfully with PID: {self.process.pid}")

            # Start output monitoring thread
            self.output_thread = threading.Thread(target=self._monitor_output, daemon=True)
            self.output_thread.start()

            return True

        except Exception as e:
            print(f"Error starting parser: {e}")
            self.is_running = False
            return False

    def stop(self) -> bool:
        """Stop the parser process gracefully"""
        if not self.is_running or not self.process:
            print("Parser is not running!")
            return False

        try:
            print("Stopping parser...")

            # Signal monitoring thread to stop
            self.should_stop = True

            # Try graceful termination first
            try:
                self.process.terminate()

                # Wait for graceful shutdown with timeout
                try:
                    exit_code = self.process.wait(timeout=10)
                    print(f"Parser stopped gracefully (exit code: {exit_code})")
                except subprocess.TimeoutExpired:
                    print("Parser didn't stop gracefully, forcing termination...")
                    self.process.kill()
                    exit_code = self.process.wait()
                    print(f"Parser forcefully terminated (exit code: {exit_code})")

            except Exception as term_error:
                print(f"Error during termination: {term_error}")
                try:
                    self.process.kill()
                    self.process.wait()
                    print("Parser forcefully killed")
                except Exception as kill_error:
                    print(f"Error killing process: {kill_error}")

            self.is_running = False
            self.process = None

            # Wait for monitoring thread to finish
            if self.output_thread and self.output_thread.is_alive():
                self.output_thread.join(timeout=2)

            return True

        except Exception as e:
            print(f"Error stopping parser: {e}")
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

    def get_status(self) -> dict:
        """Get detailed parser status"""
        if not self.process:
            return {"running": False, "pid": None, "exit_code": None}

        # Check if process is still alive
        exit_code = self.process.poll()
        if exit_code is not None:
            # Process has terminated
            self.is_running = False
            return {"running": False, "pid": self.process.pid, "exit_code": exit_code}

        return {"running": self.is_running, "pid": self.process.pid, "exit_code": None}

    def _monitor_output(self):
        """Monitor parser output in background thread with encoding safety"""
        print("Output monitoring started")

        try:
            while self.is_running and not self.should_stop and self.process:
                try:
                    # Check if process is still alive
                    if self.process.poll() is not None:
                        print("Parser process has terminated")
                        self.is_running = False
                        break

                    # Try to read a line with proper encoding handling
                    line = self.process.stdout.readline()

                    if line:
                        # Clean the line and handle any remaining encoding issues
                        try:
                            clean_line = line.strip()
                            if clean_line:
                                print(f"[PARSER] {clean_line}")
                        except UnicodeEncodeError:
                            # If there are still encoding issues, replace problematic characters
                            clean_line = line.strip().encode('ascii', 'replace').decode('ascii')
                            if clean_line:
                                print(f"[PARSER] {clean_line}")

                    # Small delay to prevent busy waiting
                    time.sleep(0.1)

                except Exception as e:
                    print(f"Error reading parser output: {e}")
                    break

        except Exception as e:
            print(f"Error in output monitoring thread: {e}")
        finally:
            print("Output monitoring stopped")


# Global parser instance
_parser_wrapper = ImprovedParserWrapper()


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
    return _parser_wrapper.get_status()


def get_output() -> Optional[str]:
    """Get parser output (GUI interface function)"""
    return _parser_wrapper.get_output()


def main():
    """Main function for testing the wrapper"""
    print("Testing Improved Parser Wrapper v1.1")
    print("=" * 50)

    try:
        # Test start
        print("\n1. Testing start()...")
        if start():
            print("Start successful")

            # Let it run for a few seconds
            print("\n2. Letting parser run for 10 seconds...")
            for i in range(10):
                time.sleep(1)
                status = get_status()
                if not status['running']:
                    print(f"Parser stopped unexpectedly (exit code: {status.get('exit_code')})")
                    break
                print(f"Running... ({i+1}/10) PID: {status.get('pid')}")

            # Test stop
            if is_running():
                print("\n3. Testing stop()...")
                if stop():
                    print("Stop successful")
                else:
                    print("Stop failed")
            else:
                print("\n3. Parser already stopped")
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
    main()
