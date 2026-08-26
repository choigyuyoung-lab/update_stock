@echo off
chcp 65001 > nul
title [K-All-Round] 유튜브 시황 자막 수집 및 AI 동기화
cd /d "%~dp0"

echo ==============================================================================
echo   [K-올라운드] 유튜브 시황 자막 수집 및 AI 동기화 (원클릭)
echo ==============================================================================
echo.

set PY_EXE=python
if exist "%~dp0.venv\Scripts\python.exe" set PY_EXE="%~dp0.venv\Scripts\python.exe"
if exist "%~dp0..\.venv\Scripts\python.exe" set PY_EXE="%~dp0..\.venv\Scripts\python.exe"

%PY_EXE% -m jobs.youtube.job_sync_youtube_insights %*

echo.
echo ==============================================================================
echo   수집 및 분석 작업이 완료되었습니다!
echo ==============================================================================
echo.
pause
