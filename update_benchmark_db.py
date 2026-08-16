"""
update_benchmark_db.py
=======================
노션(Notion) 지표지수(벤치마크) DB를 분석/갱신하고,
상장주식 마스터 DB와의 산업BM/시장BM 매핑 커버리지를 진단(Health Check)합니다.
- 데이터 소스: FinanceDataReader (KRX, ETF/KR, S&P500) + yfinance
- 지표 정합성: 국가(KR/US), 구분(시장/산업/기타), 매칭키워드 자동 보정
- 헬스체크: 상장주식 마스터 DB 전 종목 대상 벤치마크 매칭율 및 미매칭 샘플 리포팅
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import re
import io
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

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
    paginate_database,
    safe_page_update,
    make_rich_text,
    kst_isoformat,
    get_page_text,
    is_kr_ticker,
    get_http_session,
    extract_short_brand_name,
    match_keyword,
    find_best_bm,
    parse_keywords,
)


# ==============================================================================
# 1. 환경 변수 및 로거 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
MASTER_DATABASE_ID = get_env_var("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = get_env_var("BENCHMARK_DATABASE_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("BenchmarkSync")

MARKET_BM_KR = {"069500", "226490", "229200", "292190"}
MARKET_BM_US = {"SPY", "QQQ", "ONEQ", "SCHD", "VTI"}
SPECIAL_BM_US = {"GLD", "USO", "EWY"}


# ==============================================================================
# 2. 지표 자동화 엔진 (FDR + YFinance 인메모리 메타데이터)
# ==============================================================================
class BenchmarkAutomationEngine:
    """FinanceDataReader 및 yfinance 기반 고속 인메모리 종목 메타데이터 엔진"""

    def __init__(self):
        logger.info("📡 지표지수 DB 전담 엔진 초기화 중...")
        self.session = get_http_session()

        try:
            self.df_kr = fdr.StockListing('KRX').set_index('Code')
        except Exception as e:
            logger.warning(f"⚠️ KRX 로드 실패: {e}")
            self.df_kr = pd.DataFrame()

        try:
            self.kr_etf = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        except Exception as e:
            logger.warning(f"⚠️ ETF/KR 로드 실패: {e}")
            self.kr_etf = {}

        try:
            self.df_sp500 = fdr.StockListing('S&P500').set_index('Symbol').to_dict('index')
        except Exception as e:
            logger.warning(f"⚠️ S&P500 로드 실패: {e}")
            self.df_sp500 = {}

    def get_stock_name(self, ticker: str) -> str:
        """티커에 해당하는 공식 종목명을 검색합니다."""
        clean_t = ticker.upper().strip()
        if clean_t in self.kr_etf:
            return str(self.kr_etf[clean_t].get('Name', clean_t))
        if clean_t in self.df_kr.index:
            return str(self.df_kr.loc[clean_t].get('Name', clean_t))
        if clean_t in self.df_sp500:
            return extract_short_brand_name(self.df_sp500[clean_t].get('Name', clean_t))

        try:
            yf_item = yf.Ticker(clean_t, session=self.session)
            info = yf_item.info
            name = info.get("longName") or info.get("shortName") or clean_t
            return extract_short_brand_name(name)
        except Exception:
            return clean_t


# ==============================================================================
# 3. 개별 지표 페이지 분석 및 메타 보정
# ==============================================================================
def process_benchmark_page(
    page: Dict[str, Any],
    engine: BenchmarkAutomationEngine,
    client: Any
) -> Optional[Dict[str, Any]]:
    """개별 지표 페이지의 국가/구분/키워드 메타데이터를 정합화하고 갱신합니다."""
    pid = page["id"]
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker", "이름", "Name"]).upper().strip()
    if not ticker:
        return None

    summary = get_page_text(props, ["요약명"]).strip()
    cat_val = props.get("구분", {}).get("select", {}).get("name", "") if props.get("구분", {}).get("select") else ""
    country_val = props.get("국가", {}).get("select", {}).get("name", "") if props.get("국가", {}).get("select") else ""
    kw_val = get_page_text(props, ["매칭키워드", "키워드"])

    # 1. 국가 및 구분 자동 보정
    target_country = "KR" if is_kr_ticker(ticker) else "US"
    
    if target_country == "KR":
        target_cat = "시장" if ticker in MARKET_BM_KR else "산업"
    else:
        if ticker in MARKET_BM_US:
            target_cat = "시장"
        elif ticker in SPECIAL_BM_US:
            target_cat = "기타"
        else:
            target_cat = "산업"

    update_props: Dict[str, Any] = {}

    if not country_val or country_val != target_country:
        update_props["국가"] = {"select": {"name": target_country}}

    if not cat_val or cat_val != target_cat:
        update_props["구분"] = {"select": {"name": target_cat}}

    # 2. 매칭키워드가 누락된 경우 요약명을 기본값으로 설정
    if not kw_val and summary:
        update_props["매칭키워드"] = make_rich_text(summary)

    if update_props:
        safe_page_update(client, pid, update_props)
        logger.info(f"   🔄 [지표 메타 보정] {ticker} ({summary}) ➔ {list(update_props.keys())} 갱신")

    return {
        "id": pid,
        "ticker": ticker,
        "summary": summary,
        "category": target_cat,
        "country": target_country,
        "keywords": parse_keywords(kw_raw=kw_val, fallback_summary=summary)
    }


# ==============================================================================
# 4. 마스터 DB 정합성 진단 (헬스체크 & 커버리지 리포트)
# ==============================================================================
def run_master_db_health_check(
    client: Any,
    benchmark_list: List[Dict[str, Any]],
    engine: BenchmarkAutomationEngine
) -> None:
    """마스터 DB 전 종목 대상 벤치마크 매칭 정합성 헬스체크 수행"""
    logger.info("\n📊 상장주식 DB와 지표지수 DB 정합성 헬스체크 시작...")

    kr_ind_bms = [b for b in benchmark_list if b["category"] == "산업" and b["country"] == "KR"]
    us_ind_bms = [b for b in benchmark_list if b["category"] == "산업" and b["country"] == "US"]

    total_kr, matched_kr = 0, 0
    total_us, matched_us = 0, 0
    unmatched_kr_list: List[Tuple[str, str, str]] = []
    unmatched_us_list: List[Tuple[str, str, str]] = []

    for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100):
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker"]).upper().strip()
        name = get_page_text(props, ["종목명", "Name"])
        sec = get_page_text(props, ["KR_섹터", "US_섹터"])
        ind = get_page_text(props, ["KR_산업", "US_업종"])
        if not ticker:
            continue

        if not name:
            name = engine.get_stock_name(ticker)

        text_corpus = f"{ticker} {name} {sec} {ind}".upper()

        if is_kr_ticker(ticker):
            total_kr += 1
            k_ind = find_best_bm(text_corpus, kr_ind_bms)
            if k_ind:
                matched_kr += 1
            else:
                unmatched_kr_list.append((ticker, name, sec or "미기입"))
        else:
            total_us += 1
            g_ind = find_best_bm(text_corpus, us_ind_bms)
            if g_ind:
                matched_us += 1
            else:
                unmatched_us_list.append((ticker, name, sec or "미기입"))

    kr_rate = (matched_kr / total_kr * 100) if total_kr > 0 else 0
    us_rate = (matched_us / total_us * 100) if total_us > 0 else 0

    logger.info("=" * 60)
    logger.info(f"📈 [지표 매핑 헬스체크 결과]")
    logger.info(f"  🇰🇷 한국 주식 산업BM : 총 {total_kr}개 중 {matched_kr}개 매칭 완료 ({kr_rate:.1f}%)")
    logger.info(f"  🇺🇸 미국 주식 산업BM : 총 {total_us}개 중 {matched_us}개 매칭 완료 ({us_rate:.1f}%)")
    logger.info("=" * 60)

    if unmatched_kr_list:
        logger.info(f"🔍 [K산업BM 미매칭 종목 샘플 (최대 10개)]:")
        for t, n, s in unmatched_kr_list[:10]:
            logger.info(f"   • [{t:6}] {n:16} (섹터: {s})")

    if unmatched_us_list:
        logger.info(f"🔍 [G산업BM 미매칭 종목 샘플 (최대 10개)]:")
        for t, n, s in unmatched_us_list[:10]:
            logger.info(f"   • [{t:6}] {n:16} (섹터: {s})")


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """지표지수 DB 점검 및 마스터 DB 헬스체크 메인 파이프라인"""
    logger.info("🚀 [지표지수 DB 자동 점검 및 업데이트 프로세스 시작]")
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    engine = BenchmarkAutomationEngine()

    pages = [p for p in paginate_database(client, BENCHMARK_DATABASE_ID, page_size=100)]
    logger.info(f"📋 지표지수 DB에서 총 {len(pages)}개의 지표 항목을 로드했습니다.")

    benchmark_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_benchmark_page, p, engine, client) for p in pages]
        for f in futures:
            res = f.result()
            if res:
                benchmark_list.append(res)

    logger.info(f"✅ 총 {len(benchmark_list)}개 지표 항목의 메타데이터 검증 및 갱신 완료.")

    # 마스터 DB와의 헬스체크 실행
    run_master_db_health_check(client, benchmark_list, engine)

    logger.info("✨ [지표지수 DB 동기화 및 헬스체크 프로세스 완료]")


if __name__ == "__main__":
    main()
