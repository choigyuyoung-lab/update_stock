@echo off
chcp 65001 > nul
title [update_stock] 작업 종료 및 자동 백업 동기화
cd /d "%~dp0"
python "%~dp0sync_manager.py" finish
echo.
pause
