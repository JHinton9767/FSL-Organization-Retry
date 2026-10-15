@echo off
setlocal
cd /d "%~dp0"
where uv >nul 2>&1
if not errorlevel 1 (
    set "UV_CACHE_DIR=%CD%\.uv-cache"
    uv run --no-project python -X utf8 export_fsl_student_list.py %*
    goto done
)
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -X utf8 export_fsl_student_list.py %*
    goto done
)
where py >nul 2>&1
if not errorlevel 1 (
    py -3 -X utf8 export_fsl_student_list.py %*
    goto done
)
where python >nul 2>&1
if not errorlevel 1 (
    python -X utf8 export_fsl_student_list.py %*
    goto done
)
echo Python was not found. Run this on the computer that already runs sqlCompile.
exit /b 1
:done
set "EXPORT_RESULT=%ERRORLEVEL%"
echo.
pause
exit /b %EXPORT_RESULT%
