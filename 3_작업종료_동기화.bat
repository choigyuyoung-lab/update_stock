@echo off
chcp 65001 > nul
title "K-All-Round Finish Sync"

set "PY_EXE=python"
if exist "%~dp0update_stock\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0update_stock\.venv\Scripts\python.exe"
) else if exist "%~dp0..\update_stock\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0..\update_stock\.venv\Scripts\python.exe"
) else if exist "%~dp0..\..\update_stock\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0..\..\update_stock\.venv\Scripts\python.exe"
)

if exist "%~dp0k_all_round_portfolio\tools\sync_manager.py" (
    "%PY_EXE%" "%~dp0k_all_round_portfolio\tools\sync_manager.py" finish
) else if exist "%~dp0..\k_all_round_portfolio\tools\sync_manager.py" (
    "%PY_EXE%" "%~dp0..\k_all_round_portfolio\tools\sync_manager.py" finish
) else if exist "%~dp0..\..\k_all_round_portfolio\tools\sync_manager.py" (
    "%PY_EXE%" "%~dp0..\..\k_all_round_portfolio\tools\sync_manager.py" finish
) else if exist "%~dp0tools\sync_manager.py" (
    "%PY_EXE%" "%~dp0tools\sync_manager.py" finish
) else (
    echo [ERROR] sync_manager.py not found.
)

echo.
pause
