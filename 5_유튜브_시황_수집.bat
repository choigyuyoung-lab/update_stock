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

:: 1. 유튜브 자막 수집 및 Gemini AI 분석 실행
%PY_EXE% -m jobs.youtube.job_sync_youtube_insights %*

:: 2. 수집된 대기열(Queue) 및 캐시/데이터를 GitHub에 자동 커밋 & 푸시
echo.
echo ------------------------------------------------------------------------------
echo   🚀 [자동 동기화] 수집된 대기열 및 캐시 데이터를 GitHub에 업로드 중...
echo ------------------------------------------------------------------------------
git add .processed_youtube_videos.json .youtube_pending_queue.json jobs/youtube/.processed_youtube_videos.json jobs/youtube/.youtube_pending_queue.json data/*.csv 2>nul
git commit -m "chore(youtube): auto-sync youtube insights queue & cache [skip ci]" 2>nul
git push origin main 2>nul

echo.
echo ==============================================================================
echo   🎉 유튜브 시황 수집 및 GitHub 원격 업로드가 완벽하게 완료되었습니다!
echo ==============================================================================
echo.
pause
