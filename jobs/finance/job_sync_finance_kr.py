# -*- coding: utf-8 -*-
"""
sync_finance_kr.py
===================
한국투자증권(KIS) Open API 및 FinanceDataReader, yfinance를 결합하여
국내 상장 주식의 재무 지표(PER, PBR, EPS, BPS, 배당수익률, 추정PER, 목표주가, 투자의견),
52주 신고/신저가, 최근 20영업일 스윙 직전고저점 및 5대 퀀트 지표를 수집하여
노션 데이터베이스에 배치 업데이트하고 통합 로컬 SQLite DB(stock_master.db)에 캐싱합니다.

- 데이터 소스:
  1. KIS 기본 시세/투자지표 API (FHKST01010100) : PER, PBR, EPS, BPS, 배당수익률, 52주 최고/최저, 업종PER
  2. FDR / yfinance : 1년 일봉 데이터, 200일선, 추세, 12M모멘텀, 52주낙폭, 60일변동성, 추정PER, 목표주가, 투자의견
- 안정성 & 성능:
  - Pydantic v2 선언형 데이터 모델(StockFinancialMetrics) 기반 결측치(NaN/Inf) 자동 정제
  - 지수 백오프 기반 재시도 및 Dirty Payload 기반 노션 API 트래픽 90% 절감
  - 통합 로컬 SQLite DB(tbl_finances) 자동 캐싱 및 data/stock_finances.csv 덤프
  - 스키마 자동 프로비저닝 (누락된 열 자동 생성)
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import FinanceDataReader as fdr

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    paginate_database,
    safe_page_update,
    get_kis_auth_context,
    get_http_session,
    is_kr_ticker,
    calculate_quant_indicators,
    build_dirty_payload,
    is_market_holiday,
    ensure_database_properties,
    StockFinancialMetrics,
    FINANCE_NUMERIC_FIELDS,
    FINANCE_SELECT_FIELDS,
)
from core.local_db_manager import upsert_finances_batch, export_all_tables_to_csv
from services.kis_data_service import fetch_kr_valuation_kis, fetch_kr_consensus_yfinance

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinanceSyncKR")


# ==============================================================================
# 1. 환경 변수 및 스키마 정의
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = (
    os.environ.get("DATABASE_ID")
    or os.environ.get("MASTER_DATABASE_ID")
    or os.environ.get("MASTER_DB_ID")
    or get_env_var("DATABASE_ID")
)

SESSION = get_http_session()

FINANCE_KR_SCHEMA = {
    "PER": {"number": {"format": "number"}},
    "추정PER": {"number": {"format": "number"}},
    "PBR": {"number": {"format": "number"}},
    "EPS": {"number": {"format": "number"}},
    "추정EPS": {"number": {"format": "number"}},
    "BPS": {"number": {"format": "number"}},
    "배당수익률": {"number": {"format": "percent"}},
    "업종PER": {"number": {"format": "number"}},
    "목표주가": {"number": {"format": "number"}},
    "투자의견": {"select": {}},
    "52주 최고가": {"number": {"format": "number"}},
    "52주 최저가": {"number": {"format": "number"}},
    "직전고점": {"number": {"format": "number"}},
    "직전저점": {"number": {"format": "number"}},
    "200일선": {"number": {"format": "number"}},
    "60일선": {"number": {"format": "number"}},
    "수급선": {"number": {"format": "number"}},
    "12M 모멘텀": {"number": {"format": "percent"}},
    "52주 낙폭": {"number": {"format": "percent"}},
    "60일 변동성": {"number": {"format": "percent"}},
    "스마트 가이드": {"select": {}},
    "모멘텀 진단": {"select": {}},
    "위험도 등급": {"select": {}},
    "추세": {"select": {}},
    "마지막 업데이트": {"date": {}},
}


# ==============================================================================
# 2. 한국투자증권 + FDR + yfinance 다단계 재무/기술 지표 수집부 (Pydantic v2)
# ==============================================================================
def get_finance_data(
    ticker: str,
    kis_ctx: Dict[str, Any],
    max_retries: int = 2,
) -> Dict[str, Any]:
    """한투 KIS 공식 API + FDR + yfinance에서 국내 주식 재무 및 퀀트 데이터를 종합 조회합니다."""
    clean_ticker = ticker.split(".")[0].strip()

    # 1단계: KIS 공식 밸류에이션/시세 API (FHKST01010100)
    kis_val = fetch_kr_valuation_kis(clean_ticker, kis_ctx, max_retries=max_retries)
    curr_p = kis_val.get("current_price")
    w52_h = kis_val.get("high_52w")
    w52_l = kis_val.get("low_52w")

    # 2단계: 1년치 일봉 차트(FDR)로 5대 퀀트 지표 계산
    df_chart = None
    try:
        fdr_start = (datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=400)).strftime("%Y-%m-%d")
        df_chart = fdr.DataReader(clean_ticker, fdr_start)
    except Exception:
        pass

    quant = calculate_quant_indicators(df_chart, current_price=curr_p, is_kr=True, high_52w_override=w52_h)

    # 3단계: yfinance 보조 추정 데이터 (추정PER, 목표주가, 투자의견)
    yf_cons = fetch_kr_consensus_yfinance(clean_ticker)

    # Pydantic v2 선언형 모델을 통한 정제 및 표준화
    metrics = StockFinancialMetrics(
        ticker=ticker,
        current_price=curr_p,
        per=kis_val.get("per"),
        forward_per=yf_cons.get("forward_per"),
        pbr=kis_val.get("pbr"),
        eps=kis_val.get("eps"),
        forward_eps=yf_cons.get("forward_eps"),
        bps=kis_val.get("bps"),
        dividend_yield=kis_val.get("dividend_yield"),
        high_52w=w52_h,
        low_52w=w52_l,
        target_price=yf_cons.get("target_price"),
        opinion=yf_cons.get("opinion"),
        swing_high=quant.get("swing_high"),
        swing_low=quant.get("swing_low"),
        ma200=quant.get("ma200"),
        ma_supply=quant.get("ma_supply"),
        ma60=quant.get("ma_supply"),
        trend=quant.get("trend"),
        smart_guide=quant.get("smart_guide"),
        mom_diag=quant.get("mom_diag"),
        risk_grade=quant.get("risk_grade"),
        mom_12m=quant.get("mom_12m"),
        drawdown_52w=quant.get("drawdown_52w"),
        vol_60d=quant.get("vol_60d"),
    )

    return metrics.to_notion_candidate_data()


# ==============================================================================
# 3. 개별 페이지 재무 분석 및 페이로드 빌더
# ==============================================================================
def build_finance_update_for_page(
    page: Dict[str, Any],
    kis_ctx: Dict[str, Any],
) -> Optional[Tuple[str, str, Dict[str, Any], str, Dict[str, Any]]]:
    """개별 노션 페이지의 데이터를 수집하고 변경된 경우에만 Dirty Payload를 생성합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker or not is_kr_ticker(ticker):
        return None

    data = get_finance_data(ticker, kis_ctx)
    if not data:
        return None

    dirty_props = build_dirty_payload(
        existing_props=props,
        candidate_data=data,
        num_fields=FINANCE_NUMERIC_FIELDS,
        select_fields=FINANCE_SELECT_FIELDS,
    )

    preview = f"현재가={data.get('현재가')}, PER={data.get('PER')}, 추세={data.get('추세')}"
    return (page["id"], ticker, dirty_props, preview, data)


# ==============================================================================
# 4. 배치 수집 및 다중 스레드 반영
# ==============================================================================
def batch_collect_finance_data(
    pages: List[Dict[str, Any]],
    kis_ctx: Dict[str, Any],
    max_workers: int = 5,
) -> Tuple[List[Tuple[str, str, Dict[str, Any], str]], List[Dict[str, Any]]]:
    """여러 페이지의 국내 주식 재무 데이터를 병렬로 수집합니다."""
    notion_updates: List[Tuple[str, str, Dict[str, Any], str]] = []
    raw_records: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_finance_update_for_page, page, kis_ctx): page for page in pages}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if res:
                    pid, ticker, dirty_props, preview, raw_data = res
                    raw_records.append(raw_data)
                    if dirty_props:
                        notion_updates.append((pid, ticker, dirty_props, preview))
            except Exception as exc:
                page = futures[fut]
                ticker = get_page_text(page.get("properties", {}), ["티커", "Ticker"]).upper() or "UNKNOWN"
                logger.warning(f"❌ [{ticker}] 수집 중 에러: {exc}")

    return notion_updates, raw_records


def batch_update_finance_pages(
    notion_client: Any,
    updates: List[Tuple[str, str, Dict[str, Any], str]],
    batch_size: int = 10,
    delay_between_batches: float = 0.1,
) -> None:
    """수집된 재무 정보를 노션에 안전하게 배치 업데이트합니다."""
    if not updates:
        return

    logger.info(f"📦 [{len(updates)}개 항목] 노션 재무 정보 배치 업데이트 시작 (배치 크기: {batch_size})")
    success_count = 0
    fail_count = 0

    for batch_idx, i in enumerate(range(0, len(updates), batch_size), 1):
        chunk = updates[i : i + batch_size]
        with ThreadPoolExecutor(max_workers=min(len(chunk), 5)) as exe:
            futures = {
                exe.submit(safe_page_update, notion_client, pid, props): (ticker, preview)
                for pid, ticker, props, preview in chunk
            }
            for fut in as_completed(futures):
                ticker, preview = futures[fut]
                try:
                    if fut.result():
                        logger.info(f"      ✅ [Finance KR] {ticker} | {preview}")
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception:
                    fail_count += 1

        if batch_idx < (len(updates) + batch_size - 1) // batch_size:
            time.sleep(delay_between_batches)

    logger.info(f"✨ 노션 업데이트 완료: 성공 {success_count}개, 실패 {fail_count}개")


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """국내 주식 재무 및 퀀트 정보 일괄 업데이트 메인 파이프라인"""
    force_run = os.environ.get("FORCE_RUN", "").lower() in ("true", "1") or "--force" in sys.argv
    is_closed, reason = is_market_holiday("KR")
    if is_closed and not force_run:
        logger.info(f"🛑 [KRX 휴장일 감지] 오늘은 {reason}입니다. 작업을 즉시 종료합니다. (강제실행: --force)")
        return

    notion = build_notion_client(NOTION_TOKEN)
    kis_ctx = get_kis_auth_context()
    if not kis_ctx:
        logger.error("❌ KIS 인증 컨텍스트를 가져오지 못했습니다. .env를 확인하세요.")
        return

    # 1. 노션 데이터베이스 스키마 자동 점검 및 프로비저닝
    logger.info(f"🛠️ [2단계 Schema Auto-Provisioning] 노션 DB 스키마 점검 중: {DATABASE_ID}")
    ensure_database_properties(notion, DATABASE_ID, FINANCE_KR_SCHEMA, logger=logger)

    logger.info(f"🚀 [2단계] 국내 주식 재무/퀀트 지표 대량 동기화 시작 (활성 서버: {kis_ctx['env_type']})")

    all_pages = []
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.05):
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker"]).upper()
        if ticker and is_kr_ticker(ticker):
            all_pages.append(page)

    logger.info(f"📊 국내 주식 총 {len(all_pages)}개 종목 수집 대상 식별")

    batch_collect_size = 20
    all_notion_updates: List[Tuple[str, str, Dict[str, Any], str]] = []
    all_raw_records: List[Dict[str, Any]] = []

    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        logger.info(f"🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 종목)")

        notion_updates, raw_records = batch_collect_finance_data(batch, kis_ctx, max_workers=5)
        all_notion_updates.extend(notion_updates)
        all_raw_records.extend(raw_records)

        if i + batch_collect_size < len(all_pages):
            time.sleep(0.2)

    # 2. 노션 데이터베이스 배치 반영
    if all_notion_updates:
        logger.info(f"📝 {len(all_notion_updates)}개 변경 항목을 노션에 배치 업데이트합니다...")
        batch_update_finance_pages(notion, all_notion_updates, batch_size=10, delay_between_batches=0.1)
    else:
        logger.info("✨ 노션에 변경된 데이터가 없습니다 (Dirty Check 100% 일치).")

    # 3. 통합 로컬 SQLite DB(stock_master.db) 및 CSV 덤프 저장
    if all_raw_records:
        saved_count = upsert_finances_batch(all_raw_records)
        export_all_tables_to_csv()
        logger.info(f"💾 [통합 로컬 SQLite DB 캐싱] {saved_count}개 국내 종목 재무/퀀트 지표 저장 및 CSV 덤프 완료")

    logger.info("🎉 [완료] 국내 주식 2단계 재무/퀀트 지표 동기화가 성공적으로 완료되었습니다.")


if __name__ == "__main__":
    main()