@echo off
setlocal
cd /d "%~dp0"
set "UV_CACHE_DIR=%CD%\.uv-cache"
where uv >nul 2>&1
if not errorlevel 1 (
    uv run --with-requirements requirements.txt python -X utf8 run_sql_compile_dashboard.py --shared %*
) else (
    echo uv was not found. Follow docs\shared_dashboard_setup.md on the host computer.
)
pause
