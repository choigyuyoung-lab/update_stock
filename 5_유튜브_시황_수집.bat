@echo off
chcp 65001 > nul
title 🎬 [K-All-Round] 유튜브 시황 자막 수집 & AI 분석기
cd /d "%~dp0"

echo ==============================================================================
echo   🎬 [K-올라운드] 유튜브 시황 자막 수집 & AI 동기화 (데스크탑 원클릭)
echo ==============================================================================
echo.

set PYTHON_EXE=""
if exist "%~dp0.venv\Scripts\python.exe" set PYTHON_EXE="%~dp0.venv\Scripts\python.exe"
if %PYTHON_EXE%=="" if exist "%~dp0..\.venv\Scripts\python.exe" set PYTHON_EXE="%~dp0..\.venv\Scripts\python.exe"
if %PYTHON_EXE%=="" set PYTHON_EXE=python

%PYTHON_EXE% -m jobs.youtube.job_sync_youtube_insights %*

echo.
echo ==============================================================================
echo  🎉 수집 및 분석 작업이 완료되었습니다!
echo ==============================================================================
echo.
pause
