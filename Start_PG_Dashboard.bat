@echo off
setlocal
cd /d "%~dp0"
set "UV_CACHE_DIR=%CD%\.uv-cache"
where uv >nul 2>&1
if errorlevel 1 (
    echo uv was not found. Run this on the computer that already runs the P^&G dashboard.
    pause
    exit /b 1
)
echo Starting the editable P^&G dashboard at http://localhost:8502
echo Keep this window open while using the dashboard. Press Ctrl+C to stop it.
echo Recompilation and publication are separate actions.
uv run --with-requirements requirements.txt python -X utf8 run_sql_compile_dashboard.py --server.port 8502 %*
set "DASHBOARD_RESULT=%ERRORLEVEL%"
if not "%DASHBOARD_RESULT%"=="0" pause
exit /b %DASHBOARD_RESULT%
