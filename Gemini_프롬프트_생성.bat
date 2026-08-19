@echo off
chcp 65001 > nul
title [K-올라운드] Gemini 모바일/웹 세션 프롬프트 생성
cd /d "%~dp0"
python "%~dp0generate_gemini_prompt.py"
echo.
pause
