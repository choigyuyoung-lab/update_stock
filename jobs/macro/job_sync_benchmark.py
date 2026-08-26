# -*- coding: utf-8 -*-
"""
sync_benchmark.py
=================
1. [지능형 키워드 자동화] KIS 공인 테마(5,528개) 및 상장주식 마스터/온톨로지 사전 DB에서
   해당 테마와 연관된 대표 종목 및 키워드를 자동 추출하여 지표지수 DB의 '매칭키워드'를 100% 자동 완성
2. [투자주 DB 관계형 자동 연결] 지표지수 DB의 티커를 기반으로 투자주 DB의 해당 종목과 1:1 관계형 자동 연결
   (시세/전일대비/52주 성과는 투자주 DB와의 수식/롤업을 통해 실시간 동기화)
3. [마스터 DB 헬스체크 및 로컬 캐싱] 상장주식 마스터 전 종목 대상 벤치마크 매칭율 진단 및 로컬 SQLite DB 갱신
"""

import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set

from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path 최상단에 선제 등록 (독립 실행 및 모듈 실행 안전 보장)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Windows 콘솔 인코딩 안전화
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    paginate_database,
    safe_page_update,
    make_rich_text,
    get_page_text,
    is_kr_ticker,
    find_best_bm,
    parse_keywords,
    kst_isoformat,
    set_page_date_property,
    ensure_database_properties,
)
from jobs.master.kis_master_loader import (
    get_kr_master_dataframe,
    get_us_master_dataframe,
    get_theme_master_dataframe,
)
from core.local_db_manager import (
    get_db_connection,
    upsert_benchmarks_batch,
    export_all_tables_to_csv,
)

# ==============================================================================
# 1. 환경 변수, 스키마 및 로거 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
MASTER_DATABASE_ID = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID", "DATABASE_ID"], required=True)
BENCHMARK_DATABASE_ID = get_db_id("BENCHMARK_DATABASE_ID", ["BENCHMARK_DB_ID"], required=True)

BENCHMARK_SCHEMA: Dict[str, Dict[str, Any]] = {
    "업데이트 일자": {"date": {}},
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("BenchmarkSync")

MARKET_BM_KR = {"069500", "226490", "229200", "292190"}
MARKET_BM_US = {"SPY", "QQQ", "ONEQ", "SCHD", "VTI"}
SPECIAL_BM_US = {"GLD", "USO", "EWY"}


# ==============================================================================
# 2. KIS 테마 & DB 기반 지능형 키워드 자동화 엔진
# ==============================================================================
class FastBenchmarkAutomationEngine:
    """open-trading-api 및 SQLite DB 기반 KIS 테마/보유종목 지능형 키워드 생성기"""

    def __init__(self, client: Optional[Any] = None):
        logger.info("📡 [Benchmark Engine] KIS 고속 인메모리 마스터 로딩 시작...")

        # 1. 국내 주식 마스터
        self.df_kr = get_kr_master_dataframe()
        self.kr_name_map = dict(zip(self.df_kr.index, self.df_kr["Name"]))

        # 2. 미국 주식 마스터
        self.df_us = get_us_master_dataframe()
        self.us_name_map = dict(zip(self.df_us.index, self.df_us["KoreaName"]))
        self.us_eng_map = dict(zip(self.df_us.index, self.df_us["EnglishName"]))

        # 3. KIS 공인 테마 마스터 (5,528개 매핑)
        self.df_theme = get_theme_master_dataframe()
        self.ticker_to_themes: Dict[str, List[str]] = (
            self.df_theme.groupby("Code")["ThemeName"].apply(list).to_dict()
        )
        self.theme_to_tickers: Dict[str, List[str]] = (
            self.df_theme.groupby("ThemeName")["Code"].apply(list).to_dict()
        )
        logger.info(f"✅ KIS 마스터 연동 완료: KR {len(self.kr_name_map)}개, US {len(self.us_name_map)}개, 테마 {len(self.df_theme)}개")

        # 4. 로컬 SQLite DB(stock_master.db) 온톨로지 사전 & 종목 색인
        self.stock_records: List[Dict[str, Any]] = []
        self.dict_records: List[Dict[str, Any]] = []
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT ticker, name, sector, kis_theme FROM tbl_stocks;")
                self.stock_records = [dict(r) for r in cursor.fetchall()]
                cursor.execute("SELECT keyword, category, target_value FROM tbl_dictionary;")
                self.dict_records = [dict(r) for r in cursor.fetchall()]
            logger.info(f"✅ 로컬 DB 색인 완료: 상장주식 {len(self.stock_records)}건, 온톨로지 사전 {len(self.dict_records)}건")
        except Exception:
            pass

        # 5. 투자주 DB 티커 인덱싱
        self.inv_ticker_to_id: Dict[str, str] = {}
        if client and INVESTMENT_DATABASE_ID:
            try:
                for page in paginate_database(client, INVESTMENT_DATABASE_ID, page_size=100, retry_delay=0.2):
                    p_props = page.get("properties", {})
                    t_str = get_page_text(p_props, ["티커", "Ticker", "종목코드"]).upper().strip()
                    if t_str:
                        self.inv_ticker_to_id[t_str] = page["id"]
                        clean_t = t_str.split(".")[0].strip()
                        if clean_t:
                            self.inv_ticker_to_id[clean_t] = page["id"]
            except Exception as exc:
                logger.warning(f"⚠️ 투자주 DB 인덱싱 실패: {exc}")

    def generate_smart_keywords(self, ticker: str, summary: str, existing_kw: str) -> str:
        """
        KIS 테마 마스터, 상장주식 DB, 온톨로지 사전에서 해당 지표(테마)와 관련된
        대표 종목명과 테마 키워드를 자동으로 추출하여 고품질 매칭키워드로 합성합니다.
        """
        keywords_set: Set[str] = set()

        # 1. 요약명 및 기존 노션 키워드 포함
        if summary:
            for item in summary.split(","):
                clean = item.strip()
                if clean:
                    keywords_set.add(clean)

        if existing_kw:
            for item in existing_kw.split(","):
                clean = item.strip()
                if clean:
                    keywords_set.add(clean)

        # 2. 온톨로지 사전(tbl_dictionary) 매칭 키워드 발굴
        search_terms = [summary, ticker]
        for row in self.dict_records:
            t_val = str(row.get("target_value", ""))
            kw = str(row.get("keyword", ""))
            if any(term and term in t_val for term in search_terms if term):
                if kw:
                    keywords_set.add(kw)

        # 3. KIS 테마 마스터(5,528건)에서 요약명 매칭 테마 및 소속 종목 발굴
        for th_name, codes in self.theme_to_tickers.items():
            if summary and (summary in th_name or th_name in summary):
                keywords_set.add(th_name)
                # 해당 테마의 대표 종목 상위 5개 이름 추가
                for c in codes[:5]:
                    name = self.kr_name_map.get(c)
                    if name:
                        keywords_set.add(name)

        # 4. 상장주식 마스터(tbl_stocks)에서 동일 섹터 종목명 발굴
        matched_stock_names = []
        for s in self.stock_records:
            sec = s.get("sector", "") or ""
            t_name = s.get("name", "") or ""
            kis_th = s.get("kis_theme", "") or ""
            if summary and (summary in sec or summary in kis_th):
                if t_name:
                    matched_stock_names.append(t_name)

        for s_name in matched_stock_names[:10]:
            keywords_set.add(s_name)

        # 5. 깔끔한 콤마 구분 문자열로 결합 (중복 제거)
        sorted_kws = sorted(list(keywords_set), key=lambda x: (x != summary, len(x), x))
        return ", ".join(sorted_kws)


# ==============================================================================
# 3. 개별 지표 페이지 분석 및 메타/관계형 자동 연결 (Enrichment)
# ==============================================================================
def process_benchmark_page(
    page: Dict[str, Any],
    engine: FastBenchmarkAutomationEngine,
    client: Any
) -> Optional[Dict[str, Any]]:
    """개별 지표 페이지의 국가/구분/키워드 메타데이터를 정합화하고 투자주 DB 관계형을 연결합니다."""
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

    # 2. [지능형 키워드 자동화] KIS 테마 및 DB 기반 매칭키워드 자동 생성 & 보강
    smart_kw = engine.generate_smart_keywords(ticker, summary, kw_val)
    if smart_kw and smart_kw != kw_val:
        update_props["매칭키워드"] = make_rich_text(smart_kw)
        kw_val = smart_kw

    # 3. [관계형 자동 연결] 티커 기반 '투자주 DB' 관계형 1:1 연결
    clean_ticker = ticker.split(".")[0].strip()
    inv_id = engine.inv_ticker_to_id.get(ticker) or engine.inv_ticker_to_id.get(clean_ticker)
    rel_prop_name = "투자주 DB" if "투자주 DB" in props else ("투자주DB" if "투자주DB" in props else None)
    if rel_prop_name and inv_id:
        current_inv_rels = props.get(rel_prop_name, {}).get("relation", [])
        current_inv_ids = [r["id"] for r in current_inv_rels]
        if not current_inv_ids or current_inv_ids[0] != inv_id:
            update_props[rel_prop_name] = {"relation": [{"id": inv_id}]}

    # 4. 업데이트 일자(시간) 자동 기록
    set_page_date_property(
        update_props,
        props,
        candidate_names=["업데이트 일자", "마지막 업데이트", "업데이트"],
        iso_date_str=kst_isoformat()
    )

    # 5. 노션 페이지 안전 업데이트
    if update_props:
        safe_page_update(client, pid, update_props, max_retries=2, retry_delay=0.1)

    return {
        "ticker": ticker,
        "summary": summary or ticker,
        "country": target_country,
        "category": target_cat,
        "keywords": kw_val,
        "notion_page_id": pid,
    }


# ==============================================================================
# 4. 상장주식 마스터 DB 대상 벤치마크 매핑 커버리지 헬스체크
# ==============================================================================
def run_master_db_health_check(
    client: Any,
    benchmark_list: List[Dict[str, Any]],
    engine: FastBenchmarkAutomationEngine
) -> None:
    """상장주식 마스터 DB의 모든 종목에 대해 벤치마크 매핑 성공률을 전수 진단합니다."""
    logger.info("\n📊 상장주식 DB와 지표지수 DB 정합성 헬스체크 시작...")

    kr_ind_bms = [
        {**b, "keywords": parse_keywords(b["keywords"]) if isinstance(b["keywords"], str) else b["keywords"]}
        for b in benchmark_list if b["country"] == "KR" and b["category"] == "산업"
    ]
    us_ind_bms = [
        {**b, "keywords": parse_keywords(b["keywords"]) if isinstance(b["keywords"], str) else b["keywords"]}
        for b in benchmark_list if b["country"] == "US" and b["category"] == "산업"
    ]

    all_master_pages = []
    for p in paginate_database(client, MASTER_DATABASE_ID, page_size=100, retry_delay=0.2):
        all_master_pages.append(p)

    total_kr, matched_kr = 0, 0
    total_us, matched_us = 0, 0
    unmatched_kr_list, unmatched_us_list = [], []

    for page in all_master_pages:
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker", "Code"]).strip().upper()
        if not ticker:
            continue

        name = get_page_text(props, ["종목명", "Name", "회사명"])
        sec = get_page_text(props, ["섹터/업종", "섹터", "Sector", "업종"])
        themes = engine.ticker_to_themes.get(ticker, [])
        text_corpus = f"{name} {sec} {' '.join(themes)}"

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
    logger.info("📈 [지표 매핑 헬스체크 결과]")
    logger.info(f"  🇰🇷 한국 주식 산업BM : 총 {total_kr}개 중 {matched_kr}개 매칭 완료 ({kr_rate:.1f}%)")
    logger.info(f"  🇺🇸 미국 주식 산업BM : 총 {total_us}개 중 {matched_us}개 매칭 완료 ({us_rate:.1f}%)")
    logger.info("=" * 60)


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """지표지수 DB 점검, 키워드 자동화 및 마스터 DB 헬스체크 메인 파이프라인"""
    logger.info("🚀 [지표지수 DB 자동 점검, KIS 키워드 자동화 및 관계형 연결 시작]")
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    ensure_database_properties(client, BENCHMARK_DATABASE_ID, BENCHMARK_SCHEMA, logger=logger)
    engine = FastBenchmarkAutomationEngine(client=client)

    pages = [p for p in paginate_database(client, BENCHMARK_DATABASE_ID, page_size=100)]
    logger.info(f"📋 지표지수 DB에서 총 {len(pages)}개의 지표 항목을 로드했습니다.")

    # 1. 개별 지표 페이지 일괄 갱신 (키워드 자동화 + 투자주 DB 관계형 연결)
    benchmark_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_benchmark_page, p, engine, client) for p in pages]
        for f in futures:
            res = f.result()
            if res:
                benchmark_list.append(res)

    logger.info(f"✅ 총 {len(benchmark_list)}개 지표 항목의 메타데이터 및 관계형 연결 완료.")

    # 2. 로컬 SQLite DB (tbl_benchmarks) 및 CSV 자동 갱신
    try:
        upsert_benchmarks_batch(benchmark_list)
        export_all_tables_to_csv()
        logger.info("💾 로컬 SQLite DB (tbl_benchmarks) 및 벤치마크 CSV 파일 갱신 완료.")
    except Exception as e:
        logger.warning(f"⚠️ 로컬 DB 갱신 생략: {e}")

    # 3. 마스터 DB와의 헬스체크 실행
    run_master_db_health_check(client, benchmark_list, engine)

    logger.info("✨ [지표지수 DB 동기화 및 헬스체크 프로세스 완료]")


if __name__ == "__main__":
    main()
