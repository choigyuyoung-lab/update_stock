@echo off
chcp 65001 > nul
title "[update_stock] 전체 퀀트 배치 로컬 실행"
cd /d "%~dp0"

set "PY_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0.venv\Scripts\python.exe"
) else if exist "%~dp0..\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0..\.venv\Scripts\python.exe"
)

echo ==============================================================================
echo   [update_stock] 전체 퀀트 배치 로컬 순차 실행 시작
echo ==============================================================================
echo.

echo [1/6] 국내 시세 동기화...
"%PY_EXE%" -m jobs.price.job_sync_price_kr
echo.

echo [2/6] 해외 시세 동기화...
"%PY_EXE%" -m jobs.price.job_sync_price_us
echo.

echo [3/6] 국내 재무 및 5대 퀀트 동기화...
"%PY_EXE%" -m jobs.finance.job_sync_finance_kr
echo.

echo [4/6] 국내 상장 마스터 동기화...
"%PY_EXE%" -m jobs.master.job_sync_master_kr
echo.

echo [5/6] 글로벌 거시 벤치마크 동기화...
"%PY_EXE%" -m jobs.macro.job_sync_benchmark
echo.

echo [6/6] 로컬 SQLite DB 및 CSV 백업 덤프...
"%PY_EXE%" -m jobs.local_db.job_sync_local_db
echo.

echo ==============================================================================
echo   전체 퀀트 배치 로컬 실행 완료!
echo ==============================================================================
pause
