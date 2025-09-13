# gui_integration.py - Enhanced integration helpers for GUI + Scraper

"""
Optional enhancements for better GUI integration with your main_scraper.py
"""

import signal
import sys
import os
import threading
import time

class ScraperGUIIntegration:
    """Helper class to enhance integration between GUI and scraper"""

    def __init__(self):
        self.force_stop_flag = threading.Event()
        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            print("\n GUI requested graceful shutdown...")
            self.force_stop_flag.set()

        # Handle SIGTERM (sent by GUI when stopping)
        signal.signal(signal.SIGTERM, signal_handler)
        if hasattr(signal, 'SIGBREAK'):  # Windows
            signal.signal(signal.SIGBREAK, signal_handler)

    def check_gui_stop_request(self):
        """Check if GUI requested a stop - call this in your scraper loops"""
        return self.force_stop_flag.is_set()

    def create_gui_stop_check_function(self):
        """Return a function that your scraper can use to check for stops"""
        return self.check_gui_stop_request

# Global instance for easy access
gui_integration = ScraperGUIIntegration()

def gui_stop_check():
    """Simple function your scraper can call to check for GUI stop requests"""
    return gui_integration.check_gui_stop_request()

# Example of how to modify your main_scraper.py to work better with GUI:
"""
# ADD TO YOUR main_scraper.py:

try:
    from gui_integration import gui_stop_check
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False
    def gui_stop_check():
        return False

# Then in your VideoScraper class, modify check_force_stop method:

def check_force_stop(self):
    # Check GUI force stop if available
    if GUI_AVAILABLE and gui_stop_check():
        self.force_stop_requested = True
        self.logger.warning(" GUI STOP REQUEST - Setting force stop flag")

    # Check internal force stop flag
    if hasattr(self, 'force_stop_requested') and self.force_stop_requested:
        self.logger.warning(" FORCE STOP DETECTED - Aborting current operation")
        return True

    return False
"""

print("Enhanced integration helper created!")
print("This is optional - your GUI will work without this file.")
print("But it provides better integration if you want to enhance your scraper.")
