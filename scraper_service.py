# scraper_service.py - Windows Service version with FIXED PATHS

import win32serviceutil
import win32service
import win32event
import servicemanager
import logging
import time
import sys
import os
from pathlib import Path

class VideoScraperService(win32serviceutil.ServiceFramework):
    _svc_name_ = "VideoScraperService"
    _svc_display_name_ = "Video Scraper Service"
    _svc_description_ = "Background video scraper service for VPS"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_alive = True
        
        # CRITICAL FIX: Set correct working directory
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(self.script_dir)
        
        # Add script directory to Python path
        if self.script_dir not in sys.path:
            sys.path.insert(0, self.script_dir)
        
        # Setup logging for service
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging for the service"""
        # Use absolute path for log file
        log_path = os.path.join(self.script_dir, "scraper_service.log")
        
        # Clear any existing handlers
        logging.getLogger().handlers = []
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
            ]
        )
        self.logger = logging.getLogger('ScraperService')
        
        # Log the paths for debugging
        self.logger.info(f"Service script directory: {self.script_dir}")
        self.logger.info(f"Current working directory: {os.getcwd()}")
        self.logger.info(f"Python path: {sys.path[:3]}")  # First 3 entries

    def SvcStop(self):
        """Stop the service"""
        self.logger.info("Service stop requested")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_alive = False

    def SvcDoRun(self):
        """Main service execution"""
        self.logger.info("Video Scraper Service starting...")
        self.logger.info(f"Working directory: {os.getcwd()}")
        
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        
        try:
            self.main()
        except Exception as e:
            self.logger.error(f"Service error: {e}", exc_info=True)
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_ERROR_TYPE,
                servicemanager.PYS_SERVICE_STOPPED,
                (self._svc_name_, str(e))
            )

    def main(self):
        """Main service logic"""
        self.logger.info("Initializing scraper...")
        
        # Ensure we're in the right directory
        os.chdir(self.script_dir)
        
        # Import and run scraper
        try:
            # Try importing the scraper
            self.logger.info("Attempting to import main_scraper...")
            
            try:
                from main_scraper import VideoScraper
                self.logger.info("✅ Successfully imported VideoScraper")
            except ImportError as e:
                self.logger.error(f"❌ Failed to import VideoScraper: {e}")
                self.logger.error(f"Current directory: {os.getcwd()}")
                self.logger.error(f"Files in directory: {os.listdir('.')}")
                raise
            
            while self.is_alive:
                try:
                    # Create downloads directory with absolute path
                    downloads_path = "C:\\scraper_downloads"
                    os.makedirs(downloads_path, exist_ok=True)
                    self.logger.info(f"✅ Downloads directory ready: {downloads_path}")
                    
                    # Create and run scraper
                    self.logger.info("Creating VideoScraper instance...")
                    scraper = VideoScraper()
                    
                    # Add service stop checking to scraper
                    scraper.service_stop_check = lambda: not self.is_alive
                    scraper.force_stop_requested = False
                    
                    def gui_stop_check():
                        return not self.is_alive
                    
                    scraper.gui_force_stop_check = gui_stop_check
                    
                    self.logger.info("🚀 Starting scraper execution...")
                    scraper.run()
                    
                    # If scraper finished normally, wait before restarting
                    if self.is_alive:
                        self.logger.info("Scraper finished normally, waiting 60 seconds before restart...")
                        for i in range(60):
                            if not self.is_alive:
                                break
                            time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"Scraper execution error: {e}", exc_info=True)
                    if self.is_alive:
                        self.logger.info("Restarting scraper in 30 seconds due to error...")
                        for i in range(30):
                            if not self.is_alive:
                                break
                            time.sleep(1)
                        
        except ImportError as e:
            self.logger.error(f"CRITICAL: Failed to import scraper modules: {e}")
            self.logger.error(f"Make sure main_scraper.py is in the same directory as this service file")
            self.logger.error(f"Service directory: {self.script_dir}")
            
        except Exception as e:
            self.logger.error(f"CRITICAL: Unexpected service error: {e}", exc_info=True)
            
        self.logger.info("Video Scraper Service stopped")

if __name__ == '__main__':
    if len(sys.argv) == 1:
        # Running as service
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(VideoScraperService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Command line operation
        win32serviceutil.HandleCommandLine(VideoScraperService)