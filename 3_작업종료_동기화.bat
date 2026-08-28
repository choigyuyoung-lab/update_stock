@echo off
chcp 65001 > nul
title "[K-All-Round] 스마트 작업종료 통합 동기화 (Git Push + 모바일 GDrive 동기화)"
cd /d "%~dp0"

if "%1"=="auto" (
    echo ============================================================================== >> "%~dp0data\sync_finish.log"
    echo   [Auto Sync] %date% %time% >> "%~dp0data\sync_finish.log"
    echo ============================================================================== >> "%~dp0data\sync_finish.log"
    python "%~dp0tools\sync_manager.py" finish >> "%~dp0data\sync_finish.log" 2>&1
    echo. >> "%~dp0data\sync_finish.log"
) else (
    python "%~dp0tools\sync_manager.py" finish
    echo.
    pause
)