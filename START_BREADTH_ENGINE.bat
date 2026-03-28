@echo off
setlocal enabledelayedexpansion
title Quantum Breadth 360 - Start
color 0B

:: Set root directory (where this BAT file lives)
set "ROOT=%~dp0"
set "BACKEND=%ROOT%backend"

echo.
echo  ============================================
echo    QUANTUM BREADTH 360 v2 - START
echo  ============================================
echo.

:: Step 1: Check if already running
echo  [1/3] Checking for existing server...
netstat -ano 2>nul | findstr ":8001 " >nul 2>&1
if %errorlevel%==0 (
    echo        Server already running on port 8001
    echo        Opening dashboard...
    start "" "http://localhost:8001"
    timeout /t 2 /nobreak >nul
    exit /b
)
echo        Port 8001 is free.

:: Step 2: Setup environment
echo.
echo  [2/3] Setting up environment...
cd /d "%BACKEND%"
if not exist "venv\Scripts\activate.bat" (
    echo        Creating virtual environment...
    python -m venv venv
    call venv\Scripts\activate.bat
    pip install -r requirements.txt --quiet
    echo        Environment created.
) else (
    call venv\Scripts\activate.bat
    echo        Environment ready.
)

:: Step 3: Start server + open browser
echo.
echo  [3/3] Starting Quantum Breadth 360 v2...
start /min "QB360-Backend" cmd /k "cd /d "%BACKEND%" && call venv\Scripts\activate.bat && python main.py"

timeout /t 4 /nobreak >nul
start "" "http://localhost:8001"

echo.
echo  ============================================
echo    Q-BRAM v2 Engine Running!
echo    Dashboard: http://localhost:8001
echo    Press any key to close this window
echo  ============================================
echo.
pause
