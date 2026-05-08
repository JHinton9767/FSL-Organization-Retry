@echo off
setlocal
cd /d "%~dp0"
set FSL_APP_MODE=
set UV_CACHE_DIR=.uv-cache

echo Starting full FSL Analytics app...
echo.

where uv >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    uv run --with-requirements requirements.txt python run_local_analytics_app.py
    goto done
)

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" run_local_analytics_app.py
    goto done
)

py run_local_analytics_app.py

:done
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo The app did not start cleanly. Confirm dependencies are installed with:
    echo py -m pip install -r requirements.txt
    echo.
    pause
)
