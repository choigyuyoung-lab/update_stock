"""
update_master_db_us.py
=======================
미국/글로벌 상장 주식(S&P 500, NASDAQ, NYSE, 글로벌 ADR, ETF)의 마스터 메타데이터를 노션 상장주식 DB에 동기화합니다.
- 데이터 소스: FinanceDataReader (S&P 500, NASDAQ, NYSE) + yfinance
- 메타데이터: 종목명(간결한 브랜드명), 마켓(NASDAQ/NYSE/AMEX/ETF), US_섹터, US_업종, 우량주(S&P500/나스닥100) 태깅
- 지표 연동: 지표 DB의 매칭키워드를 기반으로 시장BM(SPY/QQQ/ONEQ/VTI), G산업BM 동적 릴레이션 연결
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple, Set

import pandas as pd
import yfinance as yf
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
    get_page_text,
    paginate_database,
    safe_page_update,
    kst_isoformat,
    set_page_date_property,
    extract_short_brand_name,
    is_kr_ticker,
    make_rich_text,
    get_http_session,
    match_keyword,
    find_best_bm,
    parse_keywords,
    resolve_stock_taxonomy,
    load_benchmark_config,
    batch_update_pages,
)

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
# 2. 미국 주식 데이터 엔진 (인메모리 인덱스 & 실시간 캐시)
# ==============================================================================
class StockAutomationEngineUS:
    """FinanceDataReader 및 yfinance 기반 고속 인메모리 종목 메타데이터 엔진"""

    def __init__(self):
        logger.info("📡 미국/글로벌 종목 메타데이터 엔진 초기화 중...")
        self.session = get_http_session()

        # FDR 오픈 피드를 통한 초고속 메모리 로드
        try:
            self.df_nasdaq = fdr.StockListing('NASDAQ').set_index('Symbol')
            self.nasdaq_symbols = set(self.df_nasdaq.index)
        except Exception as e:
            logger.warning(f"⚠️ NASDAQ 로드 실패: {e}")
            self.df_nasdaq = pd.DataFrame()
            self.nasdaq_symbols = set()

        try:
            self.df_nyse = fdr.StockListing('NYSE').set_index('Symbol')
            self.nyse_symbols = set(self.df_nyse.index)
        except Exception as e:
            logger.warning(f"⚠️ NYSE 로드 실패: {e}")
            self.df_nyse = pd.DataFrame()
            self.nyse_symbols = set()

        try:
            self.sp500_dict = fdr.StockListing('S&P500').set_index('Symbol').to_dict('index')
        except Exception as e:
            logger.warning(f"⚠️ S&P500 로드 실패: {e}")
            self.sp500_dict = {}

        try:
            df_nq100 = fdr.StockListing('NASDAQ')
            self.nasdaq_100 = set(df_nq100.head(100)['Symbol']) if not df_nq100.empty else set()
        except Exception:
            self.nasdaq_100 = set()


# ==============================================================================
# 3. 개별 페이지 분석 및 페이로드 빌더
# ==============================================================================
def process_page_us(
    page: Dict[str, Any],
    engine: StockAutomationEngineUS,
    client: Any,
    config: Dict[str, Any]
) -> Optional[Tuple[str, Dict[str, Any], str, str]]:
    """개별 미국/해외 주식 페이지의 메타데이터와 벤치마크를 정합화하고 업데이트 페이로드를 생성합니다."""
    pid = page["id"]
    props = page.get("properties", {})
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop:
        return None

    raw_t = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    if not raw_t or is_kr_ticker(raw_t):
        return None

    # 기본값
    name = raw_t
    m_hint = "GLOBAL"
    sec = ""
    ind = ""
    target_m_t = None
    target_ind_t = None
    is_etf_flag = False

    # 1. 인메모리 빠른 조회 (S&P500 -> NASDAQ -> NYSE)
    if raw_t in engine.sp500_dict:
        row = engine.sp500_dict[raw_t]
        name = extract_short_brand_name(row.get('Name', raw_t))
        sec = row.get('Sector', '')
        ind = row.get('Industry', '')
        m_hint = "NASDAQ" if raw_t in engine.nasdaq_symbols else "NYSE"
    elif raw_t in engine.df_nasdaq.index:
        row = engine.df_nasdaq.loc[raw_t]
        name = extract_short_brand_name(row.get('Name', raw_t))
        ind = str(row.get('Industry', ''))
        sec = str(row.get('IndustryCode', ''))
        m_hint = "NASDAQ"
    elif raw_t in engine.df_nyse.index:
        row = engine.df_nyse.loc[raw_t]
        name = extract_short_brand_name(row.get('Name', raw_t))
        ind = str(row.get('Industry', ''))
        sec = str(row.get('IndustryCode', ''))
        m_hint = "NYSE"
    else:
        # 2. YFinance 폴백
        try:
            yf_item = yf.Ticker(raw_t, session=engine.session)
            info = yf_item.info
            name = extract_short_brand_name(info.get("longName") or info.get("shortName") or raw_t)
            sec = info.get("sector") or ""
            ind = info.get("industry") or ""
            quote_type = info.get("quoteType", "")
            if quote_type == "ETF":
                is_etf_flag = True

            exch = (info.get("exchange") or "").upper()
            full_exch = (info.get("fullExchangeName") or "").upper()

            if raw_t.endswith(".T") or "TOKYO" in full_exch or "TSE" in exch:
                m_hint = "TSE"
            elif exch in ["NMS", "NGM", "NCM"] or "NAS" in exch or "NASDAQ" in full_exch or raw_t in engine.nasdaq_symbols:
                m_hint = "NASDAQ"
            elif exch in ["NYQ", "NYC"] or "NY" in exch or "NYSE" in full_exch or raw_t in engine.nyse_symbols:
                m_hint = "NYSE"
            else:
                m_hint = "GLOBAL"
        except Exception as exc:
            logger.warning(f"⚠️ [{raw_t}] YFinance 조회 실패: {exc}")
            name = raw_t
            if raw_t.endswith(".T"):
                m_hint = "TSE"

    # 3D 자산분류 공통 함수 호출
    tax = resolve_stock_taxonomy(ticker=raw_t, name=name, market_hint=m_hint, is_etf=is_etf_flag)
    market_label = tax["market"]

    # 3. 시장BM 및 G산업BM 매칭
    if market_label not in ("기타", "TSE", "GLOBAL", "COMEX"):
        if raw_t in engine.nasdaq_100:
            target_m_t = "QQQ"
        elif raw_t in engine.sp500_dict or market_label == "NYSE":
            target_m_t = "SPY"
        elif market_label == "NASDAQ":
            target_m_t = "ONEQ"
        elif market_label == "ETF(US)":
            target_m_t = "SPY"
        else:
            target_m_t = "VTI"

    text_corpus = f"{raw_t} {name} {sec} {ind}".upper()
    us_industry_bms = [bm for bm in config["benchmarks"] if bm["category"] == "산업" and bm["country"] == "US"]
    target_ind_t = find_best_bm(text_corpus, us_industry_bms)

    update_props: Dict[str, Any] = {
        "종목명": make_rich_text(name),
        "Market": {"select": {"name": tax["market"]}},
        "국가": {"select": {"name": tax["country"]}},
        "상품유형": {"select": {"name": tax["product_type"]}},
        "자산군": {"select": {"name": tax["asset_class"]}},
        "US_섹터": make_rich_text(sec),
        "US_업종": make_rich_text(ind),
    }
    set_page_date_property(update_props, props)

    # 4. 우량주 태깅
    blue_chip_tags = []
    if raw_t in engine.sp500_dict:
        blue_chip_tags.append("S&P 500")
    if raw_t in engine.nasdaq_100:
        blue_chip_tags.append("NASDAQ 100")

    if blue_chip_tags:
        update_props["우량주"] = {"multi_select": [{"name": tag} for tag in blue_chip_tags]}

    # 5. 벤치마크 관계형 속성 반영
    update_props["K산업BM"] = {"relation": []}
    if market_label in ("기타", "COMEX"):
        update_props["시장BM"] = {"relation": []}
        update_props["G산업BM"] = {"relation": []}
    else:
        if target_m_t and target_m_t != raw_t and (m_id := config["ticker_to_id"].get(target_m_t)):
            update_props["시장BM"] = {"relation": [{"id": m_id}]}
        else:
            update_props["시장BM"] = {"relation": []}

        if target_ind_t and target_ind_t != raw_t and (ind_id := config["ticker_to_id"].get(target_ind_t)):
            update_props["G산업BM"] = {"relation": [{"id": ind_id}]}
        else:
            update_props["G산업BM"] = {"relation": []}

    return pid, update_props, raw_t, name


# ==============================================================================
# 4. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """미국/글로벌 주식 마스터 DB 동기화 메인 파이프라인"""
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    config = load_benchmark_config(client, BENCHMARK_DATABASE_ID, logger=logger)
    engine = StockAutomationEngineUS()

    all_pages = [page for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100, retry_delay=0.1)]
    logger.info(f"📡 노션 DB 수집 완료: 총 {len(all_pages)}개 페이지 대상 분석 시작...")

    update_payloads = []
    for page in all_pages:
        res = process_page_us(page, engine, client, config)
        if res:
            update_payloads.append(res)

    if update_payloads:
        batch_update_pages(client, update_payloads, max_workers=3, delay=0.1, logger=logger)

    logger.info("✨ 모든 US/Global 종목 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()
