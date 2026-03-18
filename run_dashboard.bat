@echo off
echo ============================================================
echo   PIVOT Test File Review -- Dashboard
echo ============================================================
echo.

:: Detect Python command
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

:: Check required packages are installed
%PYTHON% -c "import uvicorn, fastapi" >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Required packages are not installed.
    echo Please run setup.bat first, then try again.
    echo.
    pause
    exit /b 1
)

%PYTHON% scripts\run_dashboard.py
if %errorlevel% neq 0 (
    echo.
    echo Dashboard failed to start. See error above.
    pause
)
