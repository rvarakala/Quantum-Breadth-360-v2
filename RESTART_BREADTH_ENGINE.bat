@echo off
setlocal enabledelayedexpansion
title Quantum Breadth 360 - Restart
color 0E

:: Set root directory (where this BAT file lives)
set "ROOT=%~dp0"
set "BACKEND=%ROOT%backend"

echo.
echo  ============================================
echo    QUANTUM BREADTH 360 v2 - RESTART
echo  ============================================
echo.

:: Step 1: Kill existing server on port 8001
echo  [1/4] Stopping server...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8001 "') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo        Done.

:: Step 2: Pull latest code from GitHub
echo.
echo  [2/4] Pulling latest code from GitHub...
cd /d "%ROOT%"
git pull --allow-unrelated-histories --no-edit 2>nul
if errorlevel 1 (
    echo        Pull had conflicts - resetting to remote...
    git fetch origin main
    git reset --hard origin/main
)
echo        Done.

:: Step 3: Check/install dependencies
echo.
echo  [3/4] Checking dependencies...
cd /d "%BACKEND%"
if not exist "venv\Scripts\activate.bat" (
    echo        Creating virtual environment...
    python -m venv venv
)
call venv\Scripts\activate.bat
pip install -r requirements.txt --quiet
echo        Done.

:: Step 4: Start server in background + open browser
echo.
echo  [4/4] Starting Quantum Breadth 360 v2...
start /min "QB360-Backend" cmd /k "cd /d "%BACKEND%" && call venv\Scripts\activate.bat && python main.py"

:: Wait for server to boot
timeout /t 4 /nobreak >nul

:: Open browser
start "" "http://localhost:8001"

echo.
echo  ============================================
echo    Q-BRAM v2 Engine Running!
echo    Dashboard: http://localhost:8001
echo    Press any key to close this window
echo  ============================================
echo.
pause
