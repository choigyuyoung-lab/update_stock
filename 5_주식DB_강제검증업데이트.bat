@echo off
chcp 65001 > nul
title "[update_stock] 주식 전체 DB 수동 강제 업데이트 & 검증기"
cd /d "%~dp0"

set "PY_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0.venv\Scripts\python.exe"
) else if exist "%~dp0..\k_all_round_portfolio\.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0..\k_all_round_portfolio\.venv\Scripts\python.exe"
)

echo ==============================================================================
echo   🚀 [update_stock] 주식 전체 DB 수동 강제 업데이트 & 검증기 가동
echo ==============================================================================
echo 👉 휴일 제한 및 스킵을 해제하고 전체 주식 DB/시세/재무/포트폴리오를 전수 갱신합니다.
echo.

:: 1. 강제 실행 환경변수 선언 (휴일/스킵 체크 무조건 패스)
set FORCE_RUN=true
set FORCE_UPDATE=true

:: 2. 데이터 수집 Jobs 실행
echo 📋 [1/5] 국내/미국 상장주식 마스터 데이터베이스 동기화 중...
"%PY_EXE%" -m jobs.master.job_sync_master_kr --force
"%PY_EXE%" -m jobs.master.job_sync_master_us --force

echo.
echo ⚡ [2/5] 국내/미국 주식 실시간 시세 전수 수집 중...
"%PY_EXE%" -m jobs.price.job_sync_price_kr --force
"%PY_EXE%" -m jobs.price.job_sync_price_us --force

echo.
echo 📊 [3/5] 국내/미국 주식 재무제표 & 퀀트 지표 전수 수집 중...
"%PY_EXE%" -m jobs.finance.job_sync_finance_kr --force
"%PY_EXE%" -m jobs.finance.job_sync_finance_us --force

echo.
echo 🌐 [4/5] 벤치마크 지수 및 ETF 구성종목 동기화 중...
"%PY_EXE%" -m jobs.benchmark.job_sync_benchmark --force
"%PY_EXE%" -m jobs.etf.job_sync_etf_holdings --force

:: 3. k_all_round_portfolio 디렉토리로 이동하여 리포트 발행
pushd "%~dp0..\k_all_round_portfolio"
echo.
echo 🏛️ [5/5] K-올라운드 포트폴리오 자산배분 퀀트 리포트 생성 중...
"%PY_EXE%" -m jobs.quant_report.job_generate_portfolio_report --force
popd

echo.
echo ==============================================================================
echo 🎉 [완료] 전체 주식 DB 강제 업데이트 및 검증 작업이 성공적으로 종료되었습니다.
echo ==============================================================================
echo.
pause
