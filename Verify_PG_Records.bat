@echo off
setlocal
cd /d "%~dp0"
set "UV_CACHE_DIR=%CD%\.uv-cache"
where uv >nul 2>&1
if not errorlevel 1 (
    uv run --with-requirements requirements.txt python -X utf8 verify_sql_compile.py --recompile %*
) else (
    echo uv was not found. Run this verifier on the computer that already runs sqlCompile.
)
pause
