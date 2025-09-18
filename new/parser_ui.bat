@echo off
REM Web Parser GUI Launcher - Fixed Version
REM This batch file launches the Python GUI application with proper encoding

title Web Parser Control Panel v1.1

echo ========================================
echo    Web Parser Control Panel v1.1
echo ========================================
echo.
echo Starting GUI application...
echo.

REM Change to the script directory
cd /d "%~dp0"

REM Set encoding environment variables to handle Unicode
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.7+ and try again
    echo.
    pause
    exit /b 1
)

REM Check Python version (should be 3.7+)
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo Found Python version: %PYTHON_VERSION%

REM Check if required files exist
if not exist "parser_ui.py" (
    echo ERROR: parser_ui.py not found!
    echo Please make sure all files are in the same directory
    echo.
    pause
    exit /b 1
)

if not exist "main_scraper.py" (
    echo WARNING: main_scraper.py not found!
    echo The GUI will start but parser functionality will be limited
    echo.
    timeout /t 3 >nul
)

REM Check for config files
if not exist "config.json" (
    echo INFO: config.json not found - will be created with defaults
)

if not exist "progress.json" (
    echo INFO: progress.json not found - will be created when needed
)

echo.
echo Launching GUI with UTF-8 encoding support...
echo.

REM Launch the GUI with proper encoding
python -u parser_ui.py

REM Handle exit
if errorlevel 1 (
    echo.
    echo GUI application exited with error code %errorlevel%
    echo.
    echo Possible solutions:
    echo - Check that Python 3.7+ is installed
    echo - Ensure all required files are present
    echo - Try running: python parser_ui.py directly
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo GUI application closed successfully
echo Thank you for using Web Parser Control Panel!
echo.
pause
