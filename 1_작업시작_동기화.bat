@echo off
chcp 65001 > nul
title [update_stock] 작업 시작 동기화
cd /d "%~dp0"
python "%~dp0sync_manager.py" start
echo.
pause
