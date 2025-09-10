@echo off
title Install Video Scraper Service - FIXED VERSION

REM Check if running as administrator
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo.
    echo ================================
    echo    ADMINISTRATOR REQUIRED
    echo ================================
    echo.
    echo This script must be run as Administrator!
    echo Right-click and select "Run as administrator"
    echo.
    pause
    exit /b 1
)

echo.
echo ================================================
echo   INSTALLING VIDEO SCRAPER WINDOWS SERVICE
echo   FIXED VERSION - Path Issue Resolved
echo ================================================
echo.

REM Get current directory
set CURRENT_DIR=%~dp0
echo Current directory: %CURRENT_DIR%

REM Check if main_scraper.py exists
if not exist "%CURRENT_DIR%main_scraper.py" (
    echo.
    echo  ERROR: main_scraper.py not found in current directory!
    echo Please make sure this bat file is in the same folder as main_scraper.py
    echo.
    echo Current directory: %CURRENT_DIR%
    echo Files in directory:
    dir /b *.py
    echo.
    pause
    exit /b 1
)

REM Check if all required files exist
echo Checking required files...
if exist "%CURRENT_DIR%main_scraper.py" echo  main_scraper.py found
if exist "%CURRENT_DIR%scraper_service.py" echo  scraper_service.py found
if exist "%CURRENT_DIR%config_manager.py" echo  config_manager.py found
if exist "%CURRENT_DIR%progress_tracker.py" echo  progress_tracker.py found

echo.
echo Installing required Python packages...
pip install pywin32

REM Remove old service if it exists
echo.
echo Removing old service (if exists)...
python scraper_service.py remove >nul 2>&1

REM Install the service using the FIXED version
echo.
echo Installing Video Scraper Service (FIXED VERSION)...
python "%CURRENT_DIR%scraper_service.py" install

if %errorLevel% neq 0 (
    echo.
    echo  SERVICE INSTALLATION FAILED!
    echo.
    echo Troubleshooting:
    echo 1. Make sure you're running as Administrator
    echo 2. Check if all Python files are in the same directory
    echo 3. Try: pip install pywin32
    echo.
    pause
    exit /b 1
)

REM Start the service
echo.
echo Starting Video Scraper Service...
python "%CURRENT_DIR%scraper_service.py" start

if %errorLevel% equ 0 (
    echo.
    echo ================================================
    echo    SERVICE INSTALLATION SUCCESSFUL!
    echo ================================================
    echo.
    echo The Video Scraper is now running as a Windows Service!
    echo Working directory: %CURRENT_DIR%
    echo.
) else (
    echo.
    echo ️ Service installed but failed to start.
    echo Check the log file for details: scraper_service.log
    echo.
)

echo SERVICE CONTROLS:
echo   Start:   python scraper_service.py start
echo   Stop:    python scraper_service.py stop
echo   Remove:  python scraper_service.py remove
echo.
echo Or use Windows Services Manager:
echo   Press Win+R, type "services.msc", find "Video Scraper Service"
echo.
echo MONITORING:
echo   - Service logs: %CURRENT_DIR%scraper_service.log
echo   - Progress: %CURRENT_DIR%progress.json  
echo   - Downloads: C:\scraper_downloads\
echo.
echo LOG LOCATIONS (check these if there are issues):
echo   - Service log: %CURRENT_DIR%scraper_service.log
echo   - Scraper log: %CURRENT_DIR%scraper.log
echo.

REM Show current service status
echo Current service status:
sc query VideoScraperService

echo.
pause