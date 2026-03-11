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

echo.
echo Checking for Node.js (needed to build the dashboard)...
node --version >nul 2>&1
if %errorlevel% equ 0 (
    echo Node.js found. Building dashboard...
    cd dashboard
    call npm install
    call npm run build
    cd ..
    echo Dashboard built successfully.
) else (
    echo NOTE: Node.js not found -- skipping dashboard build.
    echo       Install Node.js from https://nodejs.org/ then run:
    echo         cd dashboard ^&^& npm install ^&^& npm run build
)

:: Create working directories if absent
if not exist "input"  mkdir input
if not exist "output" mkdir output

echo.
echo ============================================================
echo   Setup complete!
echo.
echo   Quick start (Dashboard):
echo     1. Double-click run_dashboard.bat
echo     2. The browser will open at http://localhost:8000
echo     3. Click "Run New Validation" to kick off the pipeline
echo.
echo   Quick start (Command Line):
echo     1. Create a folder:  input\{ClientName}\
echo     2. Add source subfolders inside it:
echo          billing_combined\  gl\  payroll\  scheduling\
echo     3. Drop the client's files into the correct subfolders
echo     4. Open a terminal here and run:
echo          py run_all.py "ClientName" v1 --no-prompt
echo.
echo   See Getting_Started.md for a full walkthrough.
echo   See docs\1_How_To_Run_Phase1.md for detailed Phase 1 options.
echo ============================================================

:: -------------------------------------------------------
:: Hide internal folders and files from Explorer
:: They remain fully functional -- hidden = not shown by default
:: -------------------------------------------------------
for %%D in (api phase1 phase2 phase3 phase4 phase5 shared scripts docs dashboard KnowledgeSources) do (
    if exist "%%D" attrib +h "%%D"
)
for %%F in (.gitignore requirements.txt run_all.py create_handoff_zip.bat ReadMe.md Getting_Started.md TestFileReviewAutomation.code-workspace) do (
    if exist "%%F" attrib +h "%%F"
)
pause
