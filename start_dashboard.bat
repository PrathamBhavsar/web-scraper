@echo off
setlocal enabledelayedexpansion

echo ============================================
echo    Video Scraper Dashboard Starting...
echo ============================================
echo.

REM Set UTF-8 code page to handle Unicode properly
chcp 65001 >nul 2>&1

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again
    pause
    exit /b 1
)

REM Check if virtual environment exists
if not exist ".venv\" (
    echo Creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo ERROR: Failed to create virtual environment
        pause
        exit /b 1
    )
)

REM Activate virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate.bat

REM Upgrade pip first
echo Upgrading pip...
python -m pip install --upgrade pip --quiet

REM Install/upgrade requirements
echo Installing requirements...
pip install -r requirements.txt --quiet

REM Remove any corrupted Streamlit config
echo Cleaning up configuration...
if exist ".streamlit\" (
    rmdir /s /q ".streamlit\"
)
if exist "config.toml" (
    del /q "config.toml"
)

REM Create necessary directories
if not exist "logs\" mkdir logs
if not exist "downloads\" mkdir downloads

REM Check if config exists, create if not
if not exist "config.json" (
    echo Creating default config.json...
    echo {
    echo   "general": {
    echo     "base_url": "https://rule34video.com/latest-updates/",
    echo     "download_path": "downloads/",
    echo     "max_storage_gb": 10,
    echo     "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    echo   },
    echo   "download": {
    echo     "download_method": "direct",
    echo     "max_retries": 3,
    echo     "timeout_seconds": 120,
    echo     "ef2_batch_size": 3
    echo   },
    echo   "scraping": {
    echo     "pages_per_batch": 2,
    echo     "max_pages": 10,
    echo     "wait_time_ms": 3000
    echo   },
    echo   "logging": {
    echo     "log_file_path": "logs/scraper.log",
    echo     "log_level": "INFO"
    echo   }
    echo } > config.json
)

echo.
echo ============================================
echo    Starting Streamlit Dashboard...
echo ============================================
echo.
echo Dashboard will open in your default browser
echo URL: http://localhost:8501
echo To stop: Close this window or press Ctrl+C
echo.

REM Start Streamlit with explicit encoding
set PYTHONIOENCODING=utf-8
streamlit run app.py --server.port 8501 --server.headless false --server.address localhost

echo.
echo Dashboard stopped. Press any key to exit...
pause >nul