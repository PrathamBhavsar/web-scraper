@echo off
REM Web Parser GUI Launcher
REM This batch file launches the Python GUI application

title Web Parser Control Panel

echo ========================================
echo    Web Parser Control Panel v1.0
echo ========================================
echo.
echo Starting GUI application...
echo.

REM Change to the script directory
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.7+ and try again
    echo.
    pause
    exit /b 1
)

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

REM Launch the GUI
python parser_ui.py

REM Handle exit
if errorlevel 1 (
    echo.
    echo GUI application exited with error code %errorlevel%
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo GUI application closed successfully
echo.
pause
