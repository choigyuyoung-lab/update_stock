@echo off
chcp 65001 > nul
title [update_stock] Force Sync All Stocks
cd /d "%~dp0"

set "PY_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0.venv\Scripts\python.exe"
) else if exist "%~dp0..\k_all_round_portfolio\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0..\k_all_round_portfolio\.venv\Scripts\python.exe"
)

"%PY_EXE%" "%~dp0..\k_all_round_portfolio\tools\tool_force_sync_all_stocks.py"
echo.
pause
