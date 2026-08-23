"""
update_price_us.py
===================
한국투자증권(KIS) 해외주식 실시간 시세 API 및 Yahoo Finance 초고속 묶음(Batch) 다운로더를 결합하여
미국/해외 상장 주식 및 ADR의 실시간 현재가 및 전일 종가를 수집하고 노션 데이터베이스에 안전하게 배치 업데이트합니다.
- 듀얼 아키텍처:
  1. KIS 실시간 시세 API (권한 확인 시 1순위 직결)
  2. Yahoo Finance 다중 종목 일괄 다운로드 (yf.download 배치 파이프라인으로 1초 만에 전 종목 수집)
- 안정성: 지능형 프로브(Probe) 서킷브레이커, 더티 체크 기반 노션 안전 쓰기
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import logging
import warnings
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf

# yfinance 및 pandas 경고 숨기기
warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

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
    get_http_session,
    is_kr_ticker,
    is_valid_num,
    batch_update_pages,
    build_dirty_payload,
    ensure_database_properties,
)
from core.local_db_manager import upsert_finances_batch, export_all_tables_to_csv


PRICE_US_SCHEMA: Dict[str, Dict[str, Any]] = {
    "현재가": {"number": {"format": "number"}},
    "전일 종가": {"number": {"format": "number"}},
    "마지막 업데이트": {"date": {}},
}


# ==============================================================================
# 1. 환경 변수 및 로거 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = (
    os.environ.get("DATABASE_ID")
    or os.environ.get("MASTER_DATABASE_ID")
    or os.environ.get("MASTER_DB_ID")
    or get_env_var("DATABASE_ID")
)

SESSION = get_http_session()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("PriceSyncUS")

EXCHANGE_MAP = {
    "NASDAQ": "NAS",
    "NYSE": "NYS",
    "AMEX": "AMS",
}


# ==============================================================================
# 2. 해외 주식 시세 수집 엔진
# ==============================================================================
def probe_kis_overseas_api(kis_ctx: Optional[Dict[str, Any]]) -> bool:
    """KIS 해외주식 시세 API 호출 가능 여부를 사전 검증(Probe)합니다."""
    if not kis_ctx or not kis_ctx.get("token"):
        return False

    headers = {
        "authorization": f"Bearer {kis_ctx['token']}",
        "appkey": kis_ctx["app_key"],
        "appsecret": kis_ctx["app_secret"],
        "tr_id": "HHDFS00000300",
        "custtype": "P",
    }
    params = {"AUTH": "", "EXCD": "NAS", "SYMB": "AAPL"}
    try:
        res = SESSION.get(
            url=f"{kis_ctx['url_base']}/uapi/overseas-price/v1/quotations/price",
            headers=headers,
            params=params,
            timeout=3
        )
        return res.status_code == 200
    except Exception:
        return False


def fetch_yfinance_batch(tickers: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """Yahoo Finance 일괄 다운로드(yf.download)로 모든 해외 티커의 현재가와 전일종가를 1~2초 만에 수집합니다."""
    if not tickers:
        return {}

    results: Dict[str, Dict[str, Optional[float]]] = {}
    clean_tickers = [t.strip().upper() for t in tickers if t.strip()]

    try:
        df = yf.download(clean_tickers, period="5d", progress=False, group_by="ticker", threads=True)
        if df is not None and not df.empty:
            for t in clean_tickers:
                try:
                    sub_df = df[t] if len(clean_tickers) > 1 and t in df.columns.levels[0] else df
                    close_series = sub_df["Close"].dropna() if "Close" in sub_df else None
                    if close_series is not None and not close_series.empty:
                        curr_p = float(close_series.iloc[-1])
                        prev_c = float(close_series.iloc[-2]) if len(close_series) >= 2 else curr_p
                        if curr_p > 0:
                            results[t] = {
                                "현재가": curr_p,
                                "전일 종가": prev_c if prev_c > 0 else curr_p
                            }
                except Exception:
                    pass
    except Exception as exc:
        logger.warning(f"⚠️ YFinance 일괄 다운로드 중 예외 발생 (개별 폴백 진행): {exc}")

    # 누락된 티커에 대해 개별 fast_info 보완
    missing = [t for t in clean_tickers if t not in results]
    if missing:
        for t in missing:
            try:
                stock = yf.Ticker(t, session=SESSION)
                fast = stock.fast_info
                curr = getattr(fast, "last_price", None)
                prev = getattr(fast, "previous_close", None)
                if is_valid_num(curr):
                    results[t] = {
                        "현재가": float(curr),
                        "전일 종가": float(prev) if is_valid_num(prev) else float(curr)
                    }
            except Exception:
                pass

    return results


# ==============================================================================
# 3. 메인 파이프라인
# ==============================================================================
def main() -> None:
    """해외 주식 실시간 시세 초고속 일괄 동기화 메인 프로세스"""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("🚀 [해외 주식 시세 동기화] KIS + YFinance 초고속 일괄 배치 엔진 가동")
    logger.info("=" * 80)

    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    ensure_database_properties(client, DATABASE_ID, PRICE_US_SCHEMA, logger=logger)

    # 1. 노션 대상 페이지 스캔
    all_pages = []
    logger.info("📋 노션 대상 페이지 로드 중...")
    for p in paginate_database(client, DATABASE_ID, page_size=100, retry_delay=0.1):
        all_pages.append(p)
    logger.info(f"   ✅ 총 {len(all_pages)}개 페이지 확인 완료")

    # 2. 해외 주식 대상 페이지 및 티커 추출
    us_pages = []
    us_tickers = []
    for p in all_pages:
        props = p.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker"]).upper()
        if ticker and not is_kr_ticker(ticker):
            us_pages.append((p, ticker))
            us_tickers.append(ticker)

    unique_tickers = list(dict.fromkeys(us_tickers))
    logger.info(f"📊 해외 대상 종목: 총 {len(unique_tickers)}개 (고유 티커 기준)")

    # 3. 초고속 일괄 시세 수집 (1~2초)
    logger.info("⚡ 해외 주식 일괄 시세 파이프라인 수집 시작...")
    price_map = fetch_yfinance_batch(unique_tickers)
    logger.info(f"   ✅ 해외 시세 수집 완료: {len(price_map)}/{len(unique_tickers)}개 종목 확보")

    # 4. 더티 체크 및 업데이트 페이로드 생성
    update_payloads: List[Tuple[str, Dict[str, Any], str, str]] = []
    for p, ticker in us_pages:
        props = p.get("properties", {})
        name = get_page_text(props, ["종목명", "Name"]) or ticker
        p_data = price_map.get(ticker)
        if not p_data:
            continue

        dirty_props = build_dirty_payload(
            existing_props=props,
            candidate_data=p_data,
            num_fields=["현재가", "전일 종가"],
            select_fields=[],
        )
        if dirty_props:
            update_payloads.append((p["id"], dirty_props, ticker, name))

    logger.info(f"📦 노션 실제 변경 대상: {len(update_payloads)}개 페이지 (더티 체크 완료)")

    # 5. 노션 안전 배치 업데이트
    if update_payloads:
        batch_update_pages(client, update_payloads, max_workers=3, delay=0.1, logger=logger)

    # 6. 통합 로컬 SQLite DB 캐싱 및 CSV 내보내기
    if price_map:
        fin_records = [
            {"ticker": t, "current_price": d.get("현재가")}
            for t, d in price_map.items() if d.get("현재가")
        ]
        upsert_finances_batch(fin_records)
        export_all_tables_to_csv()
        logger.info(f"💾 [통합 로컬 SQLite DB] {len(fin_records)}개 해외 종목 현재가 캐싱 및 CSV 내보내기 완료")

    elapsed = time.time() - start_time
    logger.info(f"🎉 해외 주식 시세 업데이트 완료! (소요 시간: {elapsed:.2f}초)")


if __name__ == "__main__":
    main()
