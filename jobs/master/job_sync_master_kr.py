"""
update_master_db_kr.py
=======================
한국 상장 주식(KOSPI, KOSDAQ, KONEX, ETF)의 마스터 메타데이터를 노션(Notion) 상장주식 DB에 동기화합니다.
- 데이터 소스: KIS 공식 마스터(kospi/kosdaq/konex) 초고속 인메모리 파서 + FinanceDataReader(백업)
- 메타데이터: 종목명, 마켓(KOSPI/KOSDAQ/KONEX/ETF), KR_섹터, KR_산업, 우량주(K200/K100/K50/K150/KRX300) 태깅, 리스크 태깅
- 지표 연동: 지표 DB의 매칭키워드를 기반으로 시장BM, K산업BM, G산업BM 동적 릴레이션 연결
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import re
import logging
from typing import Any, Optional, Dict, List, Tuple

import pandas as pd
import FinanceDataReader as fdr

from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path 최상단에 선제 등록 (독립 실행 및 모듈 실행 안전 보장)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
    get_kis_auth_context,
    get_http_session,
    is_kr_ticker,
    load_benchmark_config,
    batch_update_pages,
    build_master_update_payload,
    ensure_database_properties,
)
from jobs.master.kis_master_loader import (
    get_kr_master_dataframe,
    get_theme_master_dataframe,
)
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
# 1. 환경 변수 및 로깅 설정
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
logger = logging.getLogger("MasterSyncKR")


# ==============================================================================
# 2. 한국 주식 데이터 엔진 (로컬 SQLite 1차 + KIS 공식 마스터 + FDR 백업)
# ==============================================================================
class StockAutomationEngineKR:
    """통합 로컬 SQLite DB 1차 캐시, KIS 공식 마스터 및 FDR 결합 메타데이터 엔진"""

    def __init__(self, kis_ctx: Optional[Dict[str, Any]] = None):
        logger.info("📡 한국 주식 마스터 엔진 가동 (로컬 SQLite 1차 + KIS 공식 마스터)...")
        self.kis_ctx = kis_ctx
        self.session = get_http_session()

        # 0. 로컬 SQLite DB 마스터 1차 로드 (0.001초)
        self.cached_stocks = load_master_stocks_from_sqlite()
        if self.cached_stocks:
            logger.info(f"   ⚡ [로컬 SQLite 0.001s] 상장주식 마스터 {len(self.cached_stocks)}개 종목 캐시 활성화")

        # 1. KIS 공식 마스터 인메모리 로드
        try:
            self.df_kis_master = get_kr_master_dataframe()
            logger.info(f"   ✅ KIS 마스터 {len(self.df_kis_master)}개 종목 캐싱 완료")
        except Exception as exc:
            logger.warning(f"   ⚠️ KIS 마스터 로드 실패 (FDR로 대체): {exc}")
            self.df_kis_master = pd.DataFrame()

        # 1-2. KIS 공식 테마 마스터 로드
        try:
            self.df_theme = get_theme_master_dataframe()
            self.ticker_to_themes: Dict[str, List[str]] = (
                self.df_theme.groupby("Code")["ThemeName"].apply(list).to_dict()
            )
            logger.info(f"   ✅ KIS 테마 마스터 {len(self.df_theme)}개 매핑 캐싱 완료")
        except Exception as exc:
            logger.warning(f"   ⚠️ KIS 테마 마스터 로드 실패: {exc}")
            self.ticker_to_themes = {}

        self._df_kr_desc = None
        self._kr_etf = None

    @property
    def df_kr_desc(self) -> pd.DataFrame:
        if self._df_kr_desc is None:
            try:
                self._df_kr_desc = fdr.StockListing('KRX-DESC').set_index('Code')
            except Exception:
                try:
                    self._df_kr_desc = fdr.StockListing('KRX').set_index('Code')
                except Exception:
                    self._df_kr_desc = pd.DataFrame()
        return self._df_kr_desc

    @property
    def kr_etf(self) -> Dict[str, Any]:
        if self._kr_etf is None:
            try:
                self._kr_etf = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
            except Exception:
                self._kr_etf = {}
        return self._kr_etf


# ==============================================================================
# 3. 페이지 처리 (기본 정보 + K산업BM / G산업BM / 시장BM 동적 매핑)
# ==============================================================================
def process_page_kr(
    page: Dict[str, Any],
    engine: StockAutomationEngineKR,
    client: Any,
    config: Dict[str, Any]
) -> Optional[Tuple[str, Dict[str, Any], str, str]]:
    """개별 한국 주식 페이지의 섹터/산업/벤치마크 매핑 정보를 분석하고 업데이트 페이로드를 생성합니다."""
    pid, props = page["id"], page.get("properties", {})
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop:
        return None

    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    match = re.search(r'(\d{6}[A-Z]?)', ticker_val.upper())
    clean_t = match.group(1) if match else ticker_val.upper()

    if not clean_t or not is_kr_ticker(clean_t):
        return None

    # KIS 공식 마스터 우선 조회
    kis_item = engine.df_kis_master.loc[clean_t] if clean_t in engine.df_kis_master.index else None
    fdr_item = engine.df_kr_desc.loc[clean_t] if clean_t in engine.df_kr_desc.index else None
    etf_item = engine.kr_etf.get(clean_t)

    if kis_item is None and fdr_item is None and etf_item is None:
        return None

    # 종목명 및 시장 추출
    stock_name = ""
    m_raw = ""
    is_etf = False
    blue_chip_tags = []

    ETF_PREFIXES = ["KODEX", "TIGER", "ACE", "SOL", "PLUS", "KBSTAR", "RISE", "HANARO", "TIMEFOLIO", "KOSEF", "WOORI", "UNICORN"]
    if kis_item is not None:
        stock_name = str(kis_item.get("Name", "")).strip()
        m_raw = str(kis_item.get("Market", "KOSPI")).upper()
        grp_code = str(kis_item.get("GroupCode", ""))
        etp_type = str(kis_item.get("ETPType", ""))
        if grp_code in ["EF", "FE"] or etp_type in ["1", "2", "3", "4", "5"] or any(stock_name.startswith(p) for p in ETF_PREFIXES) or "ETF" in stock_name or etf_item is not None:
            is_etf = True
        blue_chip_tags = list(kis_item.get("BlueChips", []))
    elif fdr_item is not None:
        stock_name = str(fdr_item.get("Name", "")).strip()
        m_raw = str(fdr_item.get("Market", "KOSPI")).upper()
        if any(stock_name.startswith(p) for p in ETF_PREFIXES) or "ETF" in stock_name:
            is_etf = True
    elif etf_item is not None:
        stock_name = str(etf_item.get("Name", "")).strip()
        m_raw = "ETF"
        is_etf = True

    if not stock_name and fdr_item is not None:
        stock_name = str(fdr_item.get("Name", "")).strip()

    # 1. KIS 테마 마스터 로드
    stock_themes = engine.ticker_to_themes.get(clean_t, [])

    # 2. 정제된 표준 메타데이터 산출 (노션 사전 DB & 독립 폴백 모듈)
    fallback_res = resolve_stock_fallback(
        ticker=clean_t,
        raw_name=stock_name,
        market_hint=m_raw,
        is_etf=is_etf,
        stock_themes=stock_themes,
        client=client,
        dictionary_db_id=config.get("dictionary_db_id", "")
    )
    if not fallback_res:
        return None

    if blue_chip_tags:
        cur_chips = list(fallback_res.get("blue_chips", []))
        for tag in blue_chip_tags:
            if tag not in cur_chips:
                cur_chips.append(tag)
        fallback_res["blue_chips"] = cur_chips

    fallback_res["ticker"] = clean_t
    fallback_res["notion_page_id"] = pid

    # 3. 공통 노션 페이로드 빌더 호출 (변경 없으면 None)
    payload = build_master_update_payload(pid, props, clean_t, fallback_res, config)
    return (payload, fallback_res)


# ==============================================================================
# 4. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """한국 주식 마스터 DB 동기화 메인 파이프라인"""
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    ensure_database_properties(client, MASTER_DATABASE_ID, MASTER_SCHEMA, logger=logger)
    kis_ctx = get_kis_auth_context()
    config = load_benchmark_config(client, BENCHMARK_DATABASE_ID, logger=logger)
    engine = StockAutomationEngineKR(kis_ctx)

    all_pages = []
    logger.info("📋 마스터 DB 스캔 및 대상 페이지 추출 시작...")
    for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100, retry_delay=0.2):
        all_pages.append(page)

    logger.info(f"📊 총 {len(all_pages)}개의 동기화 대상 목록 확보 완료")

    update_payloads = []
    all_master_records = []

    for page in all_pages:
        res = process_page_kr(page, engine, client, config)
        if res:
            payload, master_rec = res
            if payload:
                update_payloads.append(payload)
            if master_rec:
                all_master_records.append(master_rec)

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
        logger.info(f"💾 [통합 로컬 SQLite DB 캐싱] {saved_cnt}개 상장주식 마스터 저장 및 CSV 덤프 완료")

    logger.info("✨ 한국 주식 마스터 DB 통합 업데이트 프로세스 완료")


if __name__ == "__main__":
    main()