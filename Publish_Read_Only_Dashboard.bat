@echo off
setlocal
cd /d "%~dp0"
set "UV_CACHE_DIR=%CD%\.uv-cache"
where uv >nul 2>&1
if not errorlevel 1 (
    uv run --with-requirements requirements.txt python -X utf8 publish_sql_compile_viewer.py %*
    goto done
) else (
    echo uv was not found. Run this publisher on the computer that already runs sqlCompile.
    exit /b 1
)
:done
set "PUBLISH_RESULT=%ERRORLEVEL%"
pause
exit /b %PUBLISH_RESULT%
