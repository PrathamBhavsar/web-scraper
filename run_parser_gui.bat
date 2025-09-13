@echo off
title Web Parser GUI Launcher
echo.
echo ================================================
echo          Web Parser GUI Launcher
echo ================================================
echo.
echo Starting Python GUI...
echo.

REM Change to the directory containing the scripts
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and make sure it's in your system PATH
    echo.
    pause
    exit /b 1
)

REM Check if required files exist
if not exist "parser_gui.py" (
    echo ERROR: parser_gui.py not found!
    echo Make sure all files are in the same directory.
    echo.
    pause
    exit /b 1
)

if not exist "main_scraper.py" (
    echo WARNING: main_scraper.py not found!
    echo The GUI will start but the scraper won't work until you add main_scraper.py
    echo.
    echo Press any key to continue anyway...
    pause >nul
)

REM Start the GUI
echo Launching Web Parser GUI...
python parser_gui.py

REM Keep window open if there's an error
if errorlevel 1 (
    echo.
    echo GUI closed with an error. Press any key to close this window...
    pause >nul
)
