@echo off
REM Install Python dependencies for backfill scripts
REM Run this from the test directory in your conda_DE environment

echo ========================================
echo Installing Python Dependencies
echo ========================================
echo.

echo Current environment: %CONDA_DEFAULT_ENV%
echo.

REM Check if conda environment is activated
if "%CONDA_DEFAULT_ENV%"=="" (
    echo WARNING: No conda environment detected!
    echo Please activate conda_DE environment first:
    echo   conda activate conda_DE
    echo.
    pause
    exit /b 1
)

echo Installing required packages...
echo.

REM Uninstall and reinstall to fix binary compatibility issues
pip uninstall -y numpy pandas
pip install --no-cache-dir numpy==1.25.2
pip install --no-cache-dir pandas==2.1.0

REM Install core dependencies
pip install psycopg2-binary==2.9.7
pip install yfinance==0.2.54
pip install python-dotenv==1.0.0

echo.
echo ========================================
echo Installation Complete!
echo ========================================
echo.
echo You can now run:
echo   1_run_backfill.bat
echo   2_run_staggered.bat
echo.

pause
