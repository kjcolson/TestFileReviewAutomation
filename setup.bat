@echo off
echo ============================================================
echo   PIVOT Test File Review -- Setup
echo ============================================================
echo.

:: Detect Python command (py launcher preferred on Windows)
py --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON=py
) else (
    python --version >nul 2>&1
    if %errorlevel% neq 0 (
        echo ERROR: Python is not installed or not in PATH.
        echo.
        echo Download Python from https://www.python.org/downloads/
        echo During installation, check "Add Python to PATH".
        echo.
        pause
        exit /b 1
    )
    set PYTHON=python
)

echo Installing required packages...
echo.
%PYTHON% -m pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Package installation failed. See error above.
    pause
    exit /b 1
)

:: Create working directories if absent
if not exist "input"  mkdir input
if not exist "output" mkdir output

echo.
echo ============================================================
echo   Setup complete!
echo.
echo   Quick start:
echo     1. Create a folder:  input\{ClientName}\
echo     2. Add source subfolders inside it:
echo          billing_combined\  gl\  payroll\  scheduling\
echo     3. Drop the client's files into the correct subfolders
echo     4. Open a terminal here and run:
echo          py run_all.py "ClientName" v1 --no-prompt
echo.
echo   See Getting_Started.md for a full walkthrough.
echo   See 1_How_To_Run_Phase1.md for detailed Phase 1 options.
echo ============================================================
pause
