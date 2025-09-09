@echo off
title VPS Monitor - FIXED VERSION
echo.
echo =====================================================
echo   VPS SCRAPER MONITOR - FIXED VERSION  
echo =====================================================
echo.
echo This monitor shows scraper progress without GUI
echo and works perfectly when you disconnect from VPS!
echo.
echo FIXED: Now uses correct file paths
echo =====================================================
echo.

REM Get current directory and run the fixed monitor
set CURRENT_DIR=%~dp0
python "%CURRENT_DIR%vps_monitor.py"

pause