@echo off
chcp 65001 > nul
title "[K-All-Round] YouTube Insights and AI Sync"
cd /d "%~dp0"

echo ==============================================================================
echo   [K-All-Round] YouTube Insights Sync and AI Analysis
echo ==============================================================================
echo.

set "PY_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0.venv\Scripts\python.exe"
) else if exist "%~dp0..\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0..\.venv\Scripts\python.exe"
)

REM 1. YouTube Subtitle Sync and AI Insights Analysis
"%PY_EXE%" -m jobs.youtube.job_sync_youtube_insights %*

REM 2. Git Auto-Sync Queue and Cache
echo.
echo ------------------------------------------------------------------------------
echo   [Sync] Uploading queue and cache data to GitHub...
echo ------------------------------------------------------------------------------
git add .processed_youtube_videos.json .youtube_pending_queue.json jobs/youtube/.processed_youtube_videos.json jobs/youtube/.youtube_pending_queue.json data/*.csv 2>nul
git commit -m "chore(youtube): auto-sync youtube insights queue and cache [skip ci]" 2>nul
git push origin main 2>nul

echo.
echo ==============================================================================
echo   YouTube Sync and GitHub Upload Completed.
echo ==============================================================================
echo.
pause