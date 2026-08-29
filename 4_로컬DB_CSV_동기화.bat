@echo off
chcp 65001 > nul
title "[update_stock] 로컬 SQLite DB 및 CSV 백업 동기화"
cd /d "%~dp0"

set "PY_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0.venv\Scripts\python.exe"
) else if exist "%~dp0..\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0..\.venv\Scripts\python.exe"
)

echo ==============================================================================
echo   [update_stock] 로컬 SQLite DB 및 CSV 백업 동기화 실행
echo ==============================================================================
echo.
"%PY_EXE%" -m jobs.local_db.job_sync_local_db
echo.
pause
