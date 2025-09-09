@echo off
title Service Control Panel - FIXED VERSION
echo.
echo ================================================
echo   VIDEO SCRAPER SERVICE CONTROL - FIXED VERSION
echo ================================================
echo.

REM Get current directory
set CURRENT_DIR=%~dp0
echo Working directory: %CURRENT_DIR%

echo Current Service Status:
sc query VideoScraperService 2>nul
if %errorLevel% neq 0 (
    echo ❌ Service not installed
) else (
    echo ✅ Service is installed
)
echo.
echo ================================================
echo   CHOOSE AN ACTION:
echo ================================================
echo.
echo 1. Start Service
echo 2. Stop Service  
echo 3. Restart Service
echo 4. Install Service (needs Admin)
echo 5. Remove Service (needs Admin)
echo 6. View Service Status
echo 7. View Service Log
echo 8. Open Monitor
echo 9. Test Import (Debug)
echo 0. Exit
echo.
set /p choice="Enter your choice (0-9): "

if "%choice%"=="1" goto start_service
if "%choice%"=="2" goto stop_service
if "%choice%"=="3" goto restart_service
if "%choice%"=="4" goto install_service
if "%choice%"=="5" goto remove_service
if "%choice%"=="6" goto status_service
if "%choice%"=="7" goto view_log
if "%choice%"=="8" goto monitor
if "%choice%"=="9" goto test_import
if "%choice%"=="0" goto exit
goto invalid

:start_service
echo.
echo Starting Video Scraper Service...
python "%CURRENT_DIR%scraper_service.py" start
if %errorLevel% equ 0 (
    echo ✅ Service started successfully!
) else (
    echo ❌ Failed to start service
    echo Check service log: %CURRENT_DIR%scraper_service.log
)
echo.
pause
goto menu

:stop_service
echo.
echo Stopping Video Scraper Service...
python "%CURRENT_DIR%scraper_service.py" stop
if %errorLevel% equ 0 (
    echo ✅ Service stopped successfully!
) else (
    echo ❌ Failed to stop service
)
echo.
pause
goto menu

:restart_service
echo.
echo Restarting Video Scraper Service...
python "%CURRENT_DIR%scraper_service.py" stop
timeout /t 3
python "%CURRENT_DIR%scraper_service.py" start
echo.
pause
goto menu

:install_service
echo.
echo Installing Video Scraper Service...
echo Note: This requires Administrator privileges!
echo.
call "%CURRENT_DIR%install_service.bat"
pause
goto menu

:remove_service
echo.
echo Removing Video Scraper Service...
python "%CURRENT_DIR%scraper_service.py" stop
python "%CURRENT_DIR%scraper_service.py" remove
if %errorLevel% equ 0 (
    echo ✅ Service removed successfully!
) else (
    echo ❌ Failed to remove service
)
echo.
pause
goto menu

:status_service
echo.
echo ================================================
echo   DETAILED SERVICE STATUS
echo ================================================
echo.
echo Service Status:
sc query VideoScraperService
echo.
echo Service Configuration:
sc qc VideoScraperService
echo.
echo Python Process Status:
tasklist | findstr python.exe
echo.
echo Files Check:
if exist "%CURRENT_DIR%main_scraper.py" (
    echo ✅ main_scraper.py found
) else (
    echo ❌ main_scraper.py missing
)
if exist "%CURRENT_DIR%scraper_service.py" (
    echo ✅ scraper_service.py found  
) else (
    echo ❌ scraper_service.py missing
)
echo.
pause
goto menu

:view_log
echo.
echo ================================================
echo   SERVICE LOG (Last 20 lines)
echo ================================================
echo.
if exist "%CURRENT_DIR%scraper_service.log" (
    powershell "Get-Content '%CURRENT_DIR%scraper_service.log' | Select-Object -Last 20"
) else (
    echo No service log file found at: %CURRENT_DIR%scraper_service.log
)
echo.
echo Full log path: %CURRENT_DIR%scraper_service.log
echo.
pause
goto menu

:monitor
echo.
echo Opening VPS Monitor...
if exist "%CURRENT_DIR%run_monitor.bat" (
    start "VPS Monitor" "%CURRENT_DIR%run_monitor.bat"
) else (
    echo ❌ run_monitor.bat not found
)
goto menu

:test_import
echo.
echo ================================================
echo   TESTING PYTHON IMPORTS (Debug Mode)
echo ================================================
echo.
echo Current directory: %CURRENT_DIR%
echo.
echo Testing imports...
python -c "import sys; sys.path.insert(0, r'%CURRENT_DIR%'); print('Python path updated'); import main_scraper; print('✅ main_scraper imported successfully'); from main_scraper import VideoScraper; print('✅ VideoScraper imported successfully')"
echo.
echo If you see errors above, that's why the service fails.
echo Make sure all required Python files are in: %CURRENT_DIR%
echo.
pause
goto menu

:invalid
echo Invalid choice! Please enter 0-9.
pause
goto menu

:menu
cls
title Service Control Panel - FIXED VERSION  
goto :eof

:exit
exit