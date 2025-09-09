@echo off
title VPS Monitor - GUI Dashboard  
echo.
echo =====================================================
echo   VPS SCRAPER MONITOR - GUI DASHBOARD
echo =====================================================
echo.
echo Starting GUI monitor dashboard...
echo This shows REAL scraper logs in a nice GUI window
echo and works perfectly when you disconnect from VPS!
echo.
echo FEATURES:
echo   - Real-time service and process status
echo   - Download progress and statistics  
echo   - LIVE scraper logs display (not fake logs!)
echo   - File status monitoring
echo   - STOP SCRAPER button that actually works
echo   - Service control integration
echo.
echo =====================================================
echo.

REM Get current directory and run the GUI monitor
set CURRENT_DIR=%~dp0
python "%CURRENT_DIR%gui_monitor.py"

REM If there's an error, show message and pause
if %errorLevel% neq 0 (
    echo.
    echo ❌ Error starting GUI monitor
    echo Make sure Python and required modules are installed
    echo.
    pause
)

REM Keep window open if closed normally
echo.
echo GUI Monitor closed.
pause