"""
update_master_db_kr.py
=======================
한국 상장 주식(KOSPI, KOSDAQ, ETF)의 마스터 메타데이터를 노션(Notion) 상장주식 DB에 동기화합니다.
- 데이터 소스: FinanceDataReader (KRX-DESC, ETF/KR) + 한국투자증권(KIS) Open API
- 메타데이터: 종목명, 마켓(KOSPI/KOSDAQ/ETF), KR_섹터, KR_산업, 우량주(K200/K150) 태깅
- 지표 연동: 지표 DB의 매칭키워드를 기반으로 시장BM, K산업BM, G산업BM 동적 릴레이션 연결
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional, Dict, List, Tuple

import pandas as pd
import FinanceDataReader as fdr

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from notion_utils import (
    build_notion_client,
    get_env_var,
    paginate_database,
    safe_page_update,
    make_rich_text,
    kst_isoformat,
    set_page_date_property,
    get_kis_auth_context,
    get_http_session,
    is_kr_ticker,
    get_page_text,
    match_keyword,
    find_best_bm,
    parse_keywords,
    resolve_stock_taxonomy,
    load_benchmark_config,
    batch_update_pages,
)


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
# 2. 한국 주식 데이터 엔진 (FDR + KIS API 연동)
# ==============================================================================
class StockAutomationEngineKR:
    """FinanceDataReader 및 KIS 시세 조회를 결합한 한국 주식 메타데이터 엔진"""

    def __init__(self, kis_ctx: Optional[Dict[str, Any]] = None):
        logger.info("📡 한국 주식 마스터 엔진 가동 (FDR + KIS API)...")
        self.kis_ctx = kis_ctx
        self.session = get_http_session()

        # FDR 오픈 피드를 통한 초고속 메모리 로드
        try:
            self.df_kr_desc = fdr.StockListing('KRX-DESC').set_index('Code')
        except Exception as exc:
            logger.warning(f"⚠️ KRX-DESC 로드 실패 (KRX 기본으로 대체): {exc}")
            self.df_kr_desc = fdr.StockListing('KRX').set_index('Code')

        try:
            self.kr_etf = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        except Exception as exc:
            logger.warning(f"⚠️ ETF/KR 로드 실패: {exc}")
            self.kr_etf = {}

    def get_kis_market_info(self, clean_ticker: str) -> Optional[Dict[str, str]]:
        """한투 API(inquire-price)를 호출하여 공식 시장 및 K200/K150 소속 여부를 조회합니다."""
        if not self.kis_ctx or not self.kis_ctx.get("token"):
            return None

        url = f"{self.kis_ctx['url_base']}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = {
            "authorization": f"Bearer {self.kis_ctx['token']}",
            "appkey": self.kis_ctx["app_key"],
            "appsecret": self.kis_ctx["app_secret"],
            "tr_id": "FHKST01010100",
            "custtype": "P",
        }
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker}

        try:
            res = self.session.get(url, headers=headers, params=params, timeout=5)
            if res.status_code == 200:
                out = res.json().get("output", {})
                return {
                    "rprs_market": out.get("rprs_mrkt_kor_name", ""),
                    "industry_name": out.get("bstp_kor_isnm", ""),
                }
        except Exception:
            pass
        return None


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

    item, is_etf = None, False
    if clean_t in engine.df_kr_desc.index:
        item = engine.df_kr_desc.loc[clean_t]
    elif clean_t in engine.kr_etf:
        item = engine.kr_etf[clean_t]
        is_etf = True

    if item is None:
        return None

    stock_name = str(item.get('Name', '')).strip()
    m_raw = str(item.get('Market', '')).upper()

    # 한투 API를 통해 정밀 시장 구분(KOSPI200, KSQ150 여부) 확인
    kis_info = engine.get_kis_market_info(clean_t)
    rprs_m = (kis_info.get("rprs_market") or "").upper() if kis_info else ""

    if is_etf or rprs_m == "ETF":
        is_etf = True

    # 3D 자산분류 공통 유틸리티 호출
    tax = resolve_stock_taxonomy(ticker=clean_t, name=stock_name, market_hint=m_raw, is_etf=is_etf)
    market_label = tax["market"]

    if is_etf:
        sec_val = "ETF"
        ind_val = "ETF"
    else:
        sec_val = (kis_info.get("industry_name") if kis_info else "") or str(item.get('Sector', '') or item.get('WICS 업종명', '주식'))
        ind_val = str(item.get('Industry', '') or item.get('주요제품', '')) or sec_val

    # 1. 시장BM 및 우량주 태깅
    blue_chip_tags = []
    target_m_t = None

    if is_etf:
        market_bms = [bm for bm in config["benchmarks"] if bm["category"] == "시장"]
        target_m_t = find_best_bm(stock_name.upper(), market_bms)
        if not target_m_t:
            target_m_t = "292190"  # 기본 ETF BM (KRX 300 / 미국S&P500)
    else:
        if "KOSPI200" in rprs_m:
            target_m_t = "069500"  # KODEX 200
            blue_chip_tags.append("KOSPI 200")
        elif "KSQ150" in rprs_m or "KOSDAQ150" in rprs_m:
            target_m_t = "229200"  # KODEX 코스닥150
            blue_chip_tags.append("KOSDAQ 150")
        elif market_label == "KOSPI":
            target_m_t = "226490"  # KODEX 200TR
        else:
            target_m_t = "229200"  # KODEX 코스닥150

    # 2. K산업BM & G산업BM (지표 DB 매칭키워드 기반 동적 매칭)
    text_corpus = f"{stock_name} {sec_val} {ind_val}".upper()
    kr_industry_bms = [bm for bm in config["benchmarks"] if bm["category"] == "산업" and bm["country"] == "KR"]
    target_k_ind_t = find_best_bm(text_corpus, kr_industry_bms)

    us_industry_bms = [bm for bm in config["benchmarks"] if bm["category"] == "산업" and bm["country"] == "US"]
    target_g_ind_t = find_best_bm(text_corpus, us_industry_bms)

    update_props: Dict[str, Any] = {
        "종목명": make_rich_text(stock_name),
        "Market": {"select": {"name": tax["market"]}},
        "국가": {"select": {"name": tax["country"]}},
        "상품유형": {"select": {"name": tax["product_type"]}},
        "자산군": {"select": {"name": tax["asset_class"]}},
        "KR_섹터": make_rich_text(sec_val),
        "KR_산업": make_rich_text(ind_val),
    }
    set_page_date_property(update_props, props)

    if blue_chip_tags:
        update_props["우량주"] = {"multi_select": [{"name": tag} for tag in blue_chip_tags]}

    if target_m_t and target_m_t != clean_t and (m_id := config["ticker_to_id"].get(target_m_t)):
        update_props["시장BM"] = {"relation": [{"id": m_id}]}
    else:
        update_props["시장BM"] = {"relation": []}

    if target_k_ind_t and target_k_ind_t != clean_t and (k_id := config["ticker_to_id"].get(target_k_ind_t)):
        update_props["K산업BM"] = {"relation": [{"id": k_id}]}
    else:
        update_props["K산업BM"] = {"relation": []}

    if target_g_ind_t and target_g_ind_t != clean_t and (g_id := config["ticker_to_id"].get(target_g_ind_t)):
        update_props["G산업BM"] = {"relation": [{"id": g_id}]}
    else:
        update_props["G산업BM"] = {"relation": []}

    return pid, update_props, clean_t, stock_name


# ==============================================================================
# 4. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """한국 주식 마스터 DB 동기화 메인 파이프라인"""
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    kis_ctx = get_kis_auth_context()
    config = load_benchmark_config(client, BENCHMARK_DATABASE_ID, logger=logger)
    engine = StockAutomationEngineKR(kis_ctx)

    all_pages = []
    logger.info("📋 마스터 DB 스캔 및 대상 페이지 추출 시작...")
    for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100, retry_delay=0.2):
        all_pages.append(page)

    logger.info(f"📊 총 {len(all_pages)}개의 동기화 대상 목록 확보 완료")

    update_payloads = []
    for page in all_pages:
        res = process_page_kr(page, engine, client, config)
        if res:
            update_payloads.append(res)

    if update_payloads:
        batch_update_pages(client, update_payloads, max_workers=3, delay=0.1, logger=logger)

    logger.info("✨ 한국 주식 마스터 DB 통합 업데이트 프로세스 완료")


if __name__ == "__main__":
    main()