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
BENCHMARK_DATABASE_ID = (
    os.environ.get("BENCHMARK_DATABASE_ID")
    or os.environ.get("BENCHMARK_DB_ID")
    or ""
)
MASTER_DATABASE_ID = (
    os.environ.get("MASTER_DATABASE_ID")
    or os.environ.get("MASTER_DB_ID")
    or ""
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
            timeout=3.0
        )
        if res.status_code == 200:
            data = res.json()
            if data.get("rt_cd") == "0":
                return True
    except Exception:
        pass
    return False


def fetch_yfinance_batch(tickers: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """Yahoo Finance 일괄 다운로더를 통해 여러 종목의 시세를 1~2초 만에 일괄 수집합니다."""
    results: Dict[str, Dict[str, Optional[float]]] = {}
    if not tickers:
        return results

    ticker_map = {t.replace(".", "-"): t for t in tickers}
    yf_symbols = list(ticker_map.keys())

    try:
        df = yf.download(
            tickers=" ".join(yf_symbols),
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True
        )

        if df.empty:
            return results

        if len(yf_symbols) == 1:
            sym = yf_symbols[0]
            orig_t = ticker_map[sym]
            c_series = df.get("Close")
            if c_series is not None and not c_series.dropna().empty:
                valid_closes = c_series.dropna()
                cur_p = float(valid_closes.iloc[-1])
                prev_p = float(valid_closes.iloc[-2]) if len(valid_closes) > 1 else cur_p
                results[orig_t] = {"현재가": round(cur_p, 4), "전일 종가": round(prev_p, 4)}
        else:
            for sym, orig_t in ticker_map.items():
                try:
                    if sym in df.columns.levels[0]:
                        sub_df = df[sym]
                        c_series = sub_df.get("Close")
                        if c_series is not None and not c_series.dropna().empty:
                            valid_closes = c_series.dropna()
                            cur_p = float(valid_closes.iloc[-1])
                            prev_p = float(valid_closes.iloc[-2]) if len(valid_closes) > 1 else cur_p
                            results[orig_t] = {"현재가": round(cur_p, 4), "전일 종가": round(prev_p, 4)}
                except Exception:
                    pass
    except Exception as exc:
        logger.warning(f"⚠️ yfinance 일괄 다운로드 실패: {exc}")

    # 누락 종목 개별 조회 폴백
    missing = [t for t in tickers if t not in results]
    if missing:
        logger.info(f"   ℹ️ 누락 {len(missing)}개 종목 개별 폴백 조회 중...")
        for mt in missing:
            try:
                hist = yf.Ticker(mt.replace(".", "-")).history(period="5d")
                if not hist.empty and len(hist) >= 1:
                    cur_p = float(hist["Close"].iloc[-1])
                    prev_p = float(hist["Close"].iloc[-2]) if len(hist) > 1 else cur_p
                    results[mt] = {"현재가": round(cur_p, 4), "전일 종가": round(prev_p, 4)}
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

    # 1. 벤치마크/환율 및 상장주식 Master 색인 로드 (관계형 자동 복구용)
    fx_map: Dict[str, str] = {}
    if BENCHMARK_DATABASE_ID:
        try:
            for p in paginate_database(client, BENCHMARK_DATABASE_ID, page_size=100, retry_delay=0.1):
                t_str = get_page_text(p.get("properties", {}), ["티커", "Ticker", "이름"]).upper()
                if t_str in ("USDKRW", "JPYKRW", "EURKRW"):
                    fx_map[t_str] = p["id"]
        except Exception:
            pass

    master_map: Dict[str, str] = {}
    try:
        from core.local_db_manager import get_db_connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ticker, notion_page_id FROM tbl_stocks WHERE notion_page_id != '';")
            master_map = {r['ticker'].strip().upper(): r['notion_page_id'] for r in cursor.fetchall()}
    except Exception:
        pass

    # 2. 노션 대상 페이지 스캔
    all_pages = []
    logger.info("📋 노션 대상 페이지 로드 중...")
    for p in paginate_database(client, DATABASE_ID, page_size=100, retry_delay=0.1):
        all_pages.append(p)
    logger.info(f"   ✅ 총 {len(all_pages)}개 페이지 확인 완료")

    # 3. 해외 주식 대상 페이지 및 티커 추출
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

    # 4. 초고속 일괄 시세 수집 (1~2초)
    logger.info("⚡ 해외 주식 일괄 시세 파이프라인 수집 시작...")
    price_map = fetch_yfinance_batch(unique_tickers)
    logger.info(f"   ✅ 해외 시세 수집 완료: {len(price_map)}/{len(unique_tickers)}개 종목 확보")

    # 5. 더티 체크 및 업데이트 페이로드 생성 (시세 + 관계형 셀프힐링)
    update_payloads: List[Tuple[str, Dict[str, Any], str, str]] = []
    for p, ticker in us_pages:
        props = p.get("properties", {})
        name = get_page_text(props, ["종목명", "Name"]) or ticker
        p_data = price_map.get(ticker)
        if not p_data:
            continue

        # 관계형 자동 복구(Self-Healing) 대상 구성
        market_val = (props.get("Market", {}).get("select", {}).get("name") or "").upper()
        target_fx_id = fx_map.get("JPYKRW") if (market_val == "TSE" or ticker.endswith(".T")) else fx_map.get("USDKRW")

        rel_candidates = {}
        if target_fx_id and "환율전환" in props:
            rel_candidates["환율전환"] = target_fx_id
        if ticker in master_map and "상장주식DB" in props:
            rel_candidates["상장주식DB"] = master_map[ticker]

        dirty_props = build_dirty_payload(
            existing_props=props,
            candidate_data=p_data,
            num_fields=["현재가", "전일 종가"],
            select_fields=[],
            relation_fields=rel_candidates,
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
