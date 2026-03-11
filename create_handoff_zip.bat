@echo off
echo ============================================================
echo   PIVOT Test File Review -- Create Handoff ZIP
echo ============================================================
echo.
echo This creates a clean ZIP to share with your team.
echo Client data (input/ and output/) is NOT included.
echo node_modules/ is NOT included (dashboard is pre-built).
echo.

set "PROJECTDIR=%~dp0"
set "DEST=%TEMP%\PIVOT_Handoff_Staging"
set "ZIPNAME=PIVOT_TestFileReview_Handoff.zip"
set "ZIPPATH=%PROJECTDIR%%ZIPNAME%"

:: Clean any prior staging folder
if exist "%DEST%" rmdir /s /q "%DEST%"
mkdir "%DEST%"
if %errorlevel% neq 0 (
    echo ERROR: Could not create staging folder in TEMP.
    pause
    exit /b 1
)

echo Copying project files...
echo.

:: Robocopy the project to staging, excluding unwanted dirs and files
robocopy "%PROJECTDIR%" "%DEST%" /E ^
    /XD node_modules .git .claude __pycache__ ^
    /XD "%PROJECTDIR%input" "%PROJECTDIR%output" ^
    /XF *.pyc tmp_cpt_check.py "%ZIPNAME%" ^
    /NFL /NDL /NJH /NJS >nul

:: Create empty input/ and output/ placeholder folders
mkdir "%DEST%\input"
mkdir "%DEST%\output"

:: Write a placeholder README inside input/ so recipients know what goes there
echo Drop client subfolders here (e.g. input\AcmeMedical\billing_combined\). > "%DEST%\input\README.txt"
echo See Getting_Started.md for folder structure. >> "%DEST%\input\README.txt"

:: Delete prior ZIP if it exists
if exist "%ZIPPATH%" del "%ZIPPATH%"

echo Creating ZIP...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "Compress-Archive -Path '%DEST%\*' -DestinationPath '%ZIPPATH%' -Force"

if %errorlevel% neq 0 (
    echo.
    echo ERROR: ZIP creation failed. Make sure PowerShell is available.
    rmdir /s /q "%DEST%"
    pause
    exit /b 1
)

:: Clean up staging folder
rmdir /s /q "%DEST%"

:: Show file size
for %%F in ("%ZIPPATH%") do set SIZE=%%~zF
set /a SIZEMB=%SIZE% / 1048576

echo.
echo ============================================================
echo   Done!  Created: %ZIPNAME%  (%SIZEMB% MB)
echo ============================================================
echo.
echo Share this ZIP with your team.  Recipients need only:
echo.
echo   1. Unzip to any folder on their PC
echo   2. Install Python 3.10+ from python.org
echo        (check "Add Python to PATH" during install)
echo   3. Double-click  setup.bat
echo.
echo   Then to use:
echo     - Dashboard:     double-click run_dashboard.bat
echo     - Command line:  py run_all.py "ClientName" v1 --no-prompt
echo.
echo   Node.js is NOT required -- the dashboard is pre-built.
echo   See Getting_Started.md for a full walkthrough.
echo ============================================================
echo.
pause
