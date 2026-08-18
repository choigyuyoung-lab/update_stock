@echo off
chcp 65001 > nul
title [K-올라운드] 작업 종료/퇴근 동기화
cd /d "%~dp0"
python "%~dp0sync_manager.py" finish
echo.
pause
