"""
sync_local_db.py
================
100% 공식 API(한투 KIS 테마 마스터, 한국거래소 KSIC, GICS, yfinance) 및 노션 DB(지표지수, 상장주식)의
모든 데이터를 수집/컴파일하여 통합 로컬 SQLite 데이터베이스(data/stock_master.db) 및
CSV 파일(data/stock_dictionary.csv 등)로 원클릭 자동 동기화하는 독립 스케줄링 스크립트입니다.
"""

import os
import sys
import logging
from typing import Any, Dict, List
import FinanceDataReader as fdr

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    paginate_database,
    get_page_text,
    find_best_bm,
    is_kr_ticker,
)
from jobs.master.kis_master_loader import (
    get_kr_master_dataframe,
    get_us_master_dataframe,
    get_theme_master_dataframe,
)
from core.local_db_manager import (
    init_database,
    upsert_dictionary_batch,
    upsert_benchmarks_batch,
    upsert_stocks_batch,
    export_all_tables_to_csv,
    DB_PATH,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("SyncLocalDB")

NOTION_TOKEN = get_env_var("NOTION_TOKEN")
MASTER_DATABASE_ID = os.getenv("MASTER_DATABASE_ID") or os.getenv("DATABASE_ID") or ""
BENCHMARK_DATABASE_ID = os.getenv("BENCHMARK_DATABASE_ID") or ""


# ==============================================================================
# 1. 벤치마크 및 상장주식 노션 DB ➔ SQLite 캐싱
# ==============================================================================
def sync_benchmarks_to_sqlite(client: Any) -> List[Dict[str, Any]]:
    """노션 지표지수 DB를 스캔하여 로컬 SQLite tbl_benchmarks에 동기화합니다."""
    logger.info("1️⃣ 노션 지표지수 DB ➔ SQLite (tbl_benchmarks) 동기화 중...")
    benchmarks = []
    for page in paginate_database(client, BENCHMARK_DATABASE_ID, page_size=100):
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker", "이름", "Name"]).upper().strip()
        if not ticker:
            continue
        summary = get_page_text(props, ["요약명"]).strip()
        cat = props.get("구분", {}).get("select", {}).get("name", "") if props.get("구분", {}).get("select") else ""
        country = props.get("국가", {}).get("select", {}).get("name", "") if props.get("국가", {}).get("select") else ""
        kw_raw = get_page_text(props, ["매칭키워드", "키워드"])
        
        # 키워드 파싱
        kws = [k.strip().upper() for k in kw_raw.replace(",", "\n").splitlines() if k.strip()]
        if summary and summary.upper() not in kws:
            kws.append(summary.upper())
        if ticker not in kws:
            kws.append(ticker)

        bm_item = {
            "ticker": ticker,
            "summary": summary,
            "category": cat,
            "country": country,
            "keywords": kws,
            "notion_page_id": page["id"],
        }
        benchmarks.append(bm_item)

    count = upsert_benchmarks_batch(benchmarks)
    logger.info(f"   ✅ 지표지수 벤치마크 {count}건 SQLite 저장 완료")
    return benchmarks


def sync_stocks_master_to_sqlite(client: Any) -> int:
    """노션 상장주식 마스터 DB의 현재 데이터를 로컬 SQLite tbl_stocks에 동기화합니다."""
    logger.info("2️⃣ 노션 상장주식 마스터 DB ➔ SQLite (tbl_stocks) 동기화 중...")
    stocks = []
    for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100):
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker", "Symbol"]).strip().upper()
        if not ticker:
            continue
        name = get_page_text(props, ["종목명", "Name"]).strip()
        mkt = props.get("Market", {}).get("select", {}).get("name", "") if props.get("Market", {}).get("select") else ""
        cnt = props.get("국가", {}).get("select", {}).get("name", "") if props.get("국가", {}).get("select") else ""
        pt = props.get("상품유형", {}).get("select", {}).get("name", "") if props.get("상품유형", {}).get("select") else ""
        ac = props.get("자산군", {}).get("select", {}).get("name", "") if props.get("자산군", {}).get("select") else ""
        sec = get_page_text(props, ["섹터/업종", "Sector"]).strip()
        
        blue_chips = [
            opt.get("name", "")
            for opt in props.get("우량주", {}).get("multi_select", [])
            if opt.get("name")
        ]

        stocks.append({
            "ticker": ticker,
            "name": name,
            "market": mkt,
            "country": cnt,
            "product_type": pt,
            "asset_class": ac,
            "sector_industry": sec,
            "blue_chips": blue_chips,
            "market_bm": "",
            "ind_bm": "",
            "notion_page_id": page["id"],
        })

    count = upsert_stocks_batch(stocks)
    logger.info(f"   ✅ 상장주식 마스터 {count}건 SQLite 저장 완료")
    return count


# ==============================================================================
# 2. 공식 API 기반 온톨로지 사전 컴파일 및 SQLite 적재
# ==============================================================================
def compile_official_dictionary(benchmarks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """100% 공식 API 소스를 결합하여 온톨로지 사전 레코드 목록을 컴파일합니다."""
    logger.info("3️⃣ 공식 API 기반 온톨로지 사전 컴파일 중...")
    records: List[Dict[str, Any]] = []

    # 1) 한투 공식 테마 마스터 (theme_code.mst)
    df_themes = get_theme_master_dataframe()
    unique_themes = sorted(df_themes["ThemeName"].dropna().unique()) if not df_themes.empty else []
    for theme_name in unique_themes:
        theme_name = str(theme_name).strip()
        if not theme_name or len(theme_name) < 2:
            continue
        cat = "제조업"
        if any(k in theme_name for k in ["반도체", "전력", "전자", "디스플레이", "CXL", "HBM", "PCB"]):
            cat = "전기전자"
        elif any(k in theme_name for k in ["완성차", "자동차", "전기차", "수소차", "자율주행", "방산", "우주", "항공", "조선"]):
            cat = "운수장비"
        elif any(k in theme_name for k in ["2차전지", "배터리", "양극재", "음극재", "전해액", "리튬", "폐배터리"]):
            cat = "2차전지"
        elif any(k in theme_name for k in ["바이오", "제약", "신약", "의료기기", "mRNA", "면역항암제"]):
            cat = "제약바이오"
        elif any(k in theme_name for k in ["IT", "인터넷", "소프트웨어", "클라우드", "AI", "인공지능", "게임", "메타버스", "보안"]):
            cat = "IT서비스"
        elif any(k in theme_name for k in ["금융", "은행", "증권", "지주", "보험", "카드"]):
            cat = "금융"
        elif any(k in theme_name for k in ["화학", "소재", "플라스틱"]):
            cat = "화학소재"
        elif any(k in theme_name for k in ["철강", "비철금속"]):
            cat = "철강금속"
        elif any(k in theme_name for k in ["로봇", "기계", "스마트팩토리", "공작기계", "승강기"]):
            cat = "산업재"
        elif any(k in theme_name for k in ["엔터", "음원", "웹툰", "미디어", "드라마", "카지노"]):
            cat = "레저엔터"
        elif any(k in theme_name for k in ["식품", "음식료", "주류", "담배", "제과"]):
            cat = "필수소비재"
        elif any(k in theme_name for k in ["원자력", "SMR", "신재생", "태양광", "풍력", "정유", "가스"]):
            cat = "에너지인프라"

        g_bms = [b for b in benchmarks if b["category"] == "산업" and b["country"] == "KR"]
        target_bm = find_best_bm(theme_name, g_bms) or "091160"

        records.append({
            "keyword": theme_name,
            "dict_type": "1.한국테마",
            "category": cat,
            "subcategory": theme_name,
            "standard_sector": f"{cat} / {theme_name}",
            "product_type": "개별기업주식",
            "asset_class": "국내주식밸류",
            "market": "KOSPI",
            "country": "한국",
            "currency": "KRW",
            "ind_bm": target_bm,
            "market_bm": "069500",
            "priority": 30,
            "note": f"한투 공식 테마 ({theme_name})",
        })

    # 2) KRX-DESC 표준산업분류
    try:
        df_krx = fdr.StockListing('KRX-DESC')
        ksic_industries = set(df_krx['Industry'].dropna().unique())
        for ind in sorted(ksic_industries):
            ind = str(ind).strip()
            if not ind or len(ind) < 2:
                continue
            cat = "제조업"
            if any(k in ind for k in ["반도체", "전자", "통신", "디스플레이", "전기", "전선", "PCB"]):
                cat = "전기전자"
            elif any(k in ind for k in ["자동차", "조선", "항공", "운송", "차량", "철도", "부품"]):
                cat = "운수장비"
            elif any(k in ind for k in ["소프트웨어", "정보", "인터넷", "SI", "포털", "데이터", "게임"]):
                cat = "IT서비스"
            elif any(k in ind for k in ["의약", "바이오", "의료", "제약", "치료", "백신"]):
                cat = "제약바이오"
            elif any(k in ind for k in ["금융", "은행", "증권", "지주", "보험", "투자"]):
                cat = "금융"
            elif any(k in ind for k in ["화학", "플라스틱", "고무", "석유", "수지"]):
                cat = "화학소재"
            elif any(k in ind for k in ["철강", "금속", "알루미늄", "동"]):
                cat = "철강금속"
            elif any(k in ind for k in ["기계", "공작", "로봇", "장비", "플랜트"]):
                cat = "산업재"
            elif any(k in ind for k in ["식품", "음료", "담배", "제과", "유제품"]):
                cat = "필수소비재"
            elif any(k in ind for k in ["의복", "패션", "화장품", "가구", "관광", "엔터", "호텔"]):
                cat = "자유소비재"

            kr_bms = [b for b in benchmarks if b["category"] == "산업" and b["country"] == "KR"]
            target_bm = find_best_bm(ind, kr_bms) or "091160"

            records.append({
                "keyword": ind,
                "dict_type": "3.KSIC산업",
                "category": cat,
                "subcategory": ind,
                "standard_sector": f"{cat} / {ind}",
                "product_type": "개별기업주식",
                "asset_class": "국내주식밸류",
                "market": "KOSPI",
                "country": "한국",
                "currency": "KRW",
                "ind_bm": target_bm,
                "market_bm": "069500",
                "priority": 50,
                "note": f"KRX KSIC 공식 산업분류 ({ind})",
            })
    except Exception as e:
        logger.warning(f"⚠️ KRX-DESC 파싱 중 예외: {e}")

    # 3) seed_dictionary.json 기반 글로벌 GICS, ETF 및 해외 종목 룰셋 로드
    seed_json_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "seed_dictionary.json")
    if os.path.exists(seed_json_path):
        try:
            import json
            with open(seed_json_path, "r", encoding="utf-8") as f:
                seed_data = json.load(f)

            # 3-1) GICS 글로벌 표준 섹터/산업
            for item in seed_data.get("gics_rules", []):
                records.append({
                    "keyword": item["keyword"],
                    "dict_type": "4.GICS산업",
                    "category": item["category"],
                    "subcategory": item["subcategory"],
                    "standard_sector": item["standard_sector"],
                    "product_type": "개별기업주식",
                    "asset_class": "글로벌성장주",
                    "market": "NASDAQ",
                    "country": "미국",
                    "currency": "USD",
                    "ind_bm": item.get("ind_bm", "SOXX"),
                    "market_bm": item.get("market_bm", "QQQ"),
                    "priority": 50,
                    "note": f"GICS 공식 글로벌 분류 ({item['keyword']})",
                })

            # 3-2) 글로벌 ETF 표준 3D 분류 및 테마
            for item in seed_data.get("etf_rules", []):
                ticker = item["ticker"]
                theme_nm = item["theme_name"]
                cnt = "한국" if is_kr_ticker(ticker) else "미국"
                mkt = "ETF(KR)" if cnt == "한국" else "ETF(US)"
                curr = "KRW" if cnt == "한국" else "USD"
                records.append({
                    "keyword": ticker,
                    "dict_type": "5.글로벌ETF",
                    "category": "ETF",
                    "subcategory": theme_nm,
                    "standard_sector": f"ETF / {theme_nm}",
                    "product_type": item.get("product_type", "지수추종패시"),
                    "asset_class": item.get("asset_class", "글로벌성장주"),
                    "market": mkt,
                    "country": cnt,
                    "currency": curr,
                    "ind_bm": item.get("ind_bm", ticker),
                    "market_bm": item.get("market_bm", "SPY"),
                    "priority": 80,
                    "note": f"글로벌 대표 ETF ({theme_nm})",
                })

            # 3-3) 글로벌 ADR 및 해외 특수 종목
            for item in seed_data.get("global_special_stocks", []):
                sec = item["sector"]
                parts = sec.split(" / ")
                cat = parts[0]
                sub = parts[1] if len(parts) > 1 else parts[0]
                cnt = item["country"]
                records.append({
                    "keyword": item["ticker"],
                    "dict_type": "6.해외주식지정",
                    "category": cat,
                    "subcategory": sub,
                    "standard_sector": sec,
                    "official_name": item["name"],
                    "product_type": "개별기업주식",
                    "asset_class": "글로벌성장주",
                    "market": item.get("market", "GLOBAL"),
                    "country": cnt,
                    "currency": "USD" if cnt == "미국" else ("JPY" if cnt == "일본" else "USD"),
                    "yahoo_ticker": item.get("yahoo_ticker", item["ticker"]),
                    "ind_bm": item.get("ind_bm", "SOXX"),
                    "market_bm": item.get("market_bm", "QQQ"),
                    "priority": 100,
                    "note": f"글로벌 대표 ADR/해외주식 ({item['name']})",
                })
        except Exception as e:
            logger.warning(f"⚠️ seed_dictionary.json 로드 중 예외: {e}")

    upsert_dictionary_batch(records)
    logger.info(f"   ✅ 공식 온톨로지 사전 {len(records)}건 SQLite (tbl_dictionary) 저장 완료")
    return records


# ==============================================================================
# 3. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """통합 로컬 SQLite DB 자동 컴파일 및 동기화 파이프라인"""
    logger.info("=" * 70)
    logger.info("🚀 [시작] 통합 로컬 SQLite DB (stock_master.db) 자동 컴파일 & 동기화")
    logger.info("=" * 70)

    client = build_notion_client(NOTION_TOKEN)
    init_database()

    # 1. 지표지수 벤치마크 동기화
    benchmarks = sync_benchmarks_to_sqlite(client)

    # 2. 상장주식 마스터 동기화
    sync_stocks_master_to_sqlite(client)

    # 3. 공식 온톨로지 사전 컴파일 및 적재
    compile_official_dictionary(benchmarks)

    # 4. CSV 자동 내보내기 (사용자 확인용)
    export_all_tables_to_csv()

    logger.info("=" * 70)
    logger.info(f"🎉 [완료] 통합 로컬 SQLite DB 구축 성공! (DB 경로: {DB_PATH})")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
