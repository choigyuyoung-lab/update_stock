"""
update_master_db_us.py
=======================
미국/글로벌 상장 주식(S&P 500, NASDAQ, NYSE, 글로벌 ADR, ETF)의 마스터 메타데이터를 노션 상장주식 DB에 동기화합니다.
- 데이터 소스: KIS 공식 해외 마스터(nasmst/nysmst/amsmst/frgn_code) 초고속 인메모리 파서 + FinanceDataReader + yfinance
- 메타데이터: 종목명(공식 한글명/간결한 브랜드명), 마켓(NASDAQ/NYSE/AMEX/ETF), US_섹터, US_업종, 우량주(S&P500/나스닥100/다우30) 태깅
- 지표 연동: 지표 DB의 매칭키워드를 기반으로 시장BM(SPY/QQQ/ONEQ/VTI), G산업BM 동적 릴레이션 연결
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional, Tuple

import pandas as pd
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
    paginate_database,
    extract_short_brand_name,
    is_kr_ticker,
    get_http_session,
    load_benchmark_config,
    batch_update_pages,
    build_master_update_payload,
    ensure_database_properties,
)
from services.kis_master_loader import get_us_master_dataframe
from services.stock_fallback_resolver import resolve_stock_fallback
from core.local_db_manager import (
    load_master_stocks_from_sqlite,
    upsert_stocks_batch,
    export_all_tables_to_csv,
)

MASTER_SCHEMA: Dict[str, Dict[str, Any]] = {
    "업데이트 일자": {"date": {}},
    "종목명": {"rich_text": {}},
    "Market": {"select": {}},
    "국가": {"select": {}},
    "상품유형": {"select": {}},
    "자산군": {"select": {}},
    "섹터/업종": {"rich_text": {}},
    "우량주": {"multi_select": {}},
}

# ==============================================================================
# 1. 환경 변수 및 로거 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
MASTER_DATABASE_ID = (
    os.environ.get("MASTER_DATABASE_ID")
    or os.environ.get("MASTER_DB_ID")
    or os.environ.get("DATABASE_ID")
    or get_env_var("MASTER_DATABASE_ID")
)
BENCHMARK_DATABASE_ID = (
    os.environ.get("BENCHMARK_DATABASE_ID")
    or os.environ.get("BENCHMARK_DB_ID")
    or get_env_var("BENCHMARK_DATABASE_ID")
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("MasterSyncUS")


# ==============================================================================
# 2. 미국 주식 데이터 엔진 (로컬 SQLite 1차 + KIS 공식 마스터 + FDR + yfinance)
# ==============================================================================
class StockAutomationEngineUS:
    """통합 로컬 SQLite DB 1차 캐시, KIS 공식 해외 마스터, FDR 및 yfinance 기반 종목 메타데이터 엔진"""

    def __init__(self):
        logger.info("📡 미국/글로벌 종목 메타데이터 엔진 초기화 중...")
        self.session = get_http_session()

        # 0. 로컬 SQLite DB 마스터 1차 로드 (0.001초)
        self.cached_stocks = load_master_stocks_from_sqlite()
        if self.cached_stocks:
            logger.info(f"   ⚡ [로컬 SQLite 0.001s] 해외 상장주식 마스터 {len(self.cached_stocks)}개 종목 캐시 활성화")

        # 1. KIS 공식 해외 마스터 인메모리 로드
        try:
            self.df_kis_us = get_us_master_dataframe()
            logger.info(f"   ✅ KIS 해외 마스터 {len(self.df_kis_us)}개 종목 캐싱 완료")
        except Exception as exc:
            logger.warning(f"   ⚠️ KIS 해외 마스터 로드 실패: {exc}")
            self.df_kis_us = pd.DataFrame()

        self._sp500_dict = None
        self._df_nasdaq = None
        self._df_nyse = None

    @property
    def sp500_dict(self) -> Dict[str, Any]:
        if self._sp500_dict is None:
            try:
                self._sp500_dict = fdr.StockListing('S&P500').set_index('Symbol').to_dict('index')
            except Exception:
                self._sp500_dict = {}
        return self._sp500_dict

    @property
    def df_nasdaq(self) -> pd.DataFrame:
        if self._df_nasdaq is None:
            try:
                self._df_nasdaq = fdr.StockListing('NASDAQ').set_index('Symbol')
            except Exception:
                self._df_nasdaq = pd.DataFrame()
        return self._df_nasdaq

    @property
    def df_nyse(self) -> pd.DataFrame:
        if self._df_nyse is None:
            try:
                self._df_nyse = fdr.StockListing('NYSE').set_index('Symbol')
            except Exception:
                self._df_nyse = pd.DataFrame()
        return self._df_nyse

# ==============================================================================
# 3. 개별 미국/해외 주식 페이지 처리 함수 (한투 메인 + 독립 폴백 모듈 연동)
# ==============================================================================
def process_page_us(
    page: Dict[str, Any],
    engine: StockAutomationEngineUS,
    client: Any,
    config: Dict[str, Any]
) -> Optional[Tuple[str, Dict[str, Any], str, str]]:
    """개별 미국/해외 주식 페이지의 섹터/산업/벤치마크 매핑 정보를 분석하고 업데이트 페이로드를 생성합니다."""
    pid, props = page["id"], page.get("properties", {})
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop:
        return None

    raw_t = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    if not raw_t or is_kr_ticker(raw_t):
        return None

    # 0. 로컬 SQLite DB (0.001s) 1차 초고속 조회
    name = ""
    m_hint = "NASDAQ"
    c_hint = "미국"
    is_etf_flag = False
    blue_chips = []

    # 1. KIS 공식 글로벌 마스터 매핑 (미국 nas/nys/ams + 일본 TSE + 홍콩 + 중국)
    kis_row = engine.df_kis_us.loc[raw_t] if raw_t in engine.df_kis_us.index else None
    if kis_row is not None:
        kor_name = str(kis_row.get("KoreaName", "")).strip()
        eng_name = str(kis_row.get("EnglishName", "")).strip()
        name = kor_name if kor_name else extract_short_brand_name(eng_name or raw_t)
        m_hint = str(kis_row.get("Exchange", m_hint)).upper()
        sec_type = str(kis_row.get("SecurityType", ""))
        if sec_type == "3":
            is_etf_flag = True
        blue_chips = list(kis_row.get("BlueChips", []))
        if m_hint == "TSE":
            c_hint = "일본"
        elif m_hint == "HKEX":
            c_hint = "홍콩"
        elif m_hint in ("SSE", "SZSE"):
            c_hint = "중국"

    # 2. FDR 보조 우량주 태그 매핑
    if raw_t in engine.sp500_dict and "S&P 500" not in blue_chips:
        blue_chips.append("S&P 500")

    # 3. 독립 폴백 모듈 호출 (노션 사전 DB ➔ yfinance 실시간 API ➔ GICS 표준 온톨로지 변환)
    fallback_res = resolve_stock_fallback(
        ticker=raw_t,
        raw_name=name,
        market_hint=m_hint,
        country_hint=c_hint,
        is_etf=is_etf_flag,
        client=client,
        dictionary_db_id=config.get("dictionary_db_id", "")
    )
    if not fallback_res:
        return None

    if blue_chips:
        cur_chips = list(fallback_res.get("blue_chips", []))
        for tag in blue_chips:
            if tag not in cur_chips:
                cur_chips.append(tag)
        fallback_res["blue_chips"] = cur_chips

    fallback_res["ticker"] = raw_t
    fallback_res["notion_page_id"] = pid

    # 4. 공통 노션 페이로드 빌더 호출 (변경 없으면 None)
    payload = build_master_update_payload(pid, props, raw_t, fallback_res, config)
    return (payload, fallback_res)


# ==============================================================================
# 4. 메인 파이프라인
# ==============================================================================
def main() -> None:
    """미국/글로벌 상장주식 마스터 동기화 메인 프로세스"""
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    ensure_database_properties(client, MASTER_DATABASE_ID, MASTER_SCHEMA, logger=logger)
    config = load_benchmark_config(client, BENCHMARK_DATABASE_ID, logger=logger)
    engine = StockAutomationEngineUS()

    all_pages = []
    logger.info("📋 마스터 DB 스캔 및 대상 페이지 추출 시작...")
    for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100, retry_delay=0.2):
        all_pages.append(page)

    logger.info(f"📊 총 {len(all_pages)}개의 동기화 대상 목록 확보 완료")

    update_payloads = []
    all_master_records = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_page_us, p, engine, client, config) for p in all_pages]
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if res:
                    payload, master_rec = res
                    if payload:
                        update_payloads.append(payload)
                    if master_rec:
                        all_master_records.append(master_rec)
            except Exception as exc:
                logger.warning(f"⚠️ 페이지 처리 오류: {exc}")

    # 1. 노션 데이터베이스 반영 (변경된 페이지만 핀포인트 업데이트)
    if update_payloads:
        logger.info(f"📝 {len(update_payloads)}개 변경 항목을 노션에 배치 업데이트합니다...")
        batch_update_pages(client, update_payloads, max_workers=3, delay=0.1, logger=logger)
    else:
        logger.info("✨ 노션 마스터 DB에 변경된 데이터가 없습니다 (Dirty Check 100% 일치).")

    # 2. 통합 로컬 SQLite DB(stock_master.db) 및 CSV 덤프 저장
    if all_master_records:
        saved_cnt = upsert_stocks_batch(all_master_records)
        export_all_tables_to_csv()
        logger.info(f"💾 [통합 로컬 SQLite DB 캐싱] {saved_cnt}개 해외 상장주식 마스터 저장 및 CSV 덤프 완료")

    logger.info("✨ 미국/글로벌 주식 마스터 DB 통합 업데이트 프로세스 완료")


if __name__ == "__main__":
    main()