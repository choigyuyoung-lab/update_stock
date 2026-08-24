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

    # 3) GICS 글로벌 표준 섹터/산업 (FDR S&P500 & yfinance GICS)
    GICS_OFFICIAL_TABLE = [
        ("Information Technology", "IT", "전자기술소프트웨어", "IT", "XLK", "QQQ"),
        ("Semiconductors", "IT", "AI반도체", "IT / AI반도체", "SOXX", "QQQ"),
        ("Semiconductor Equipment & Materials", "IT", "반도체소부장", "IT / 반도체소부장", "SOXX", "QQQ"),
        ("Semiconductors & Semiconductor Equipment", "IT", "반도체소부장", "IT / 반도체소부장", "SOXX", "QQQ"),
        ("Software—Infrastructure", "IT", "클라우드소프트웨어", "IT / 클라우드소프트웨어", "IGV", "QQQ"),
        ("Software—Application", "IT", "응용소프트웨어", "IT / 응용소프트웨어", "IGV", "QQQ"),
        ("Software - Infrastructure", "IT", "클라우드소프트웨어", "IT / 클라우드소프트웨어", "IGV", "QQQ"),
        ("Software - Application", "IT", "응용소프트웨어", "IT / 응용소프트웨어", "IGV", "QQQ"),
        ("Technology Hardware, Storage & Peripherals", "IT", "빅테크하드웨어", "IT / 빅테크하드웨어", "XLK", "QQQ"),
        ("Consumer Electronics", "IT", "빅테크디바이스", "IT / 빅테크디바이스", "XLK", "QQQ"),
        ("Electronic Components", "IT", "전자부품·소재", "IT / 전자부품·소재", "XLK", "QQQ"),
        ("Internet Content & Information", "통신미디어", "인터넷플랫폼", "통신미디어 / 인터넷플랫폼", "XLC", "QQQ"),
        ("Biotechnology", "제약바이오", "바이오텍", "제약바이오 / 바이오텍", "XBI", "SPY"),
        ("Drug Manufacturers—General", "제약바이오", "글로벌제약", "제약바이오 / 글로벌제약", "XLV", "SPY"),
        ("Pharmaceuticals", "제약바이오", "글로벌제약", "제약바이오 / 글로벌제약", "XLV", "SPY"),
        ("Health Care", "제약바이오", "글로벌헬스케어", "제약바이오 / 글로벌헬스케어", "XLV", "SPY"),
        ("Automobile Manufacturers", "자유소비재", "전기차완성차", "자유소비재 / 전기차완성차", "XLY", "QQQ"),
        ("Auto Manufacturers", "자유소비재", "전기차완성차", "자유소비재 / 전기차완성차", "XLY", "QQQ"),
        ("Aerospace & Defense", "산업재", "우주항공방산", "산업재 / 우주항공방산", "XAR", "SPY"),
        ("Specialty Industrial Machinery", "산업재", "로봇·자동화", "산업재 / 로봇·자동화", "BOTZ", "SPY"),
        ("Electrical Equipment & Parts", "산업재", "전력기기·자동화", "산업재 / 전력기기·자동화", "XLI", "SPY"),
        ("Banks—Diversified", "금융", "글로벌은행", "금융 / 글로벌은행", "XLF", "SPY"),
        ("Oil & Gas Integrated", "에너지", "정유에너지", "에너지 / 정유에너지", "XLE", "SPY"),
    ]
    for eng_k, cat, sub, sec, ind_bm, m_bm in GICS_OFFICIAL_TABLE:
        records.append({
            "keyword": eng_k,
            "dict_type": "4.GICS산업",
            "category": cat,
            "subcategory": sub,
            "standard_sector": sec,
            "product_type": "개별기업주식",
            "asset_class": "글로벌성장주",
            "market": "NASDAQ",
            "country": "미국",
            "currency": "USD",
            "ind_bm": ind_bm,
            "market_bm": m_bm,
            "priority": 50,
            "note": f"GICS 공식 글로벌 분류 ({eng_k})",
        })

    # 4) 글로벌 ETF 표준 3D 분류 및 테마
    ETF_THEMES = [
        ("SPY", "S&P500", "지수추종패시", "글로벌성장주", "SPY", "SPY"),
        ("QQQ", "나스닥100", "지수추종패시", "글로벌성장주", "QQQ", "QQQ"),
        ("DIA", "다우존스30", "지수추종패시", "글로벌성장주", "DIA", "DIA"),
        ("SOXX", "미국반도체", "섹터테마알파", "글로벌성장주", "SOXX", "QQQ"),
        ("XLK", "미국테크", "섹터테마알파", "글로벌성장주", "XLK", "QQQ"),
        ("XLF", "미국금융", "섹터테마알파", "글로벌성장주", "XLF", "SPY"),
        ("XLE", "미국에너지", "섹터테마알파", "글로벌성장주", "XLE", "SPY"),
        ("XLV", "미국헬스케어", "섹터테마알파", "글로벌성장주", "XLV", "SPY"),
        ("XLI", "미국산업재", "섹터테마알파", "글로벌성장주", "XLI", "SPY"),
        ("SCHD", "미국배당다우존스", "배당인컴상품", "한미배당성장", "SCHD", "SPY"),
        ("TLT", "미국장기국채20년", "채권금리상품", "미국장기국채", "TLT", "SPY"),
        ("BOTZ", "글로벌로봇AI", "섹터테마알파", "글로벌성장주", "BOTZ", "QQQ"),
        ("LIT", "글로벌2차전지", "섹터테마알파", "글로벌성장주", "LIT", "SPY"),
        ("069500", "KOSPI 200", "지수추종패시", "국내주식밸류", "069500", "069500"),
        ("229200", "KOSDAQ 150", "지수추종패시", "국내주식밸류", "229200", "229200"),
        ("091160", "KODEX 반도체", "섹터테마알파", "국내주식밸류", "091160", "069500"),
        ("091170", "KODEX 은행", "섹터테마알파", "국내주식밸류", "091170", "069500"),
        ("091180", "KODEX 자동차", "섹터테마알파", "국내주식밸류", "091180", "069500"),
    ]
    for ticker, theme_nm, pt, ac, ind_bm, m_bm in ETF_THEMES:
        cnt = "한국" if is_kr_ticker(ticker) else "미국"
        mkt = "ETF(KR)" if cnt == "한국" else "ETF(US)"
        curr = "KRW" if cnt == "한국" else "USD"
        records.append({
            "keyword": ticker,
            "dict_type": "5.글로벌ETF",
            "category": "ETF",
            "subcategory": theme_nm,
            "standard_sector": f"ETF / {theme_nm}",
            "product_type": pt,
            "asset_class": ac,
            "market": mkt,
            "country": cnt,
            "currency": curr,
            "ind_bm": ind_bm,
            "market_bm": m_bm,
            "priority": 80,
            "note": f"글로벌 대표 ETF ({theme_nm})",
        })

    # 5) 글로벌 ADR 및 해외 특수 종목
    GLOBAL_ADR_SPECIALS = [
        ("TSM", "TSMC(ADR)", "대만", "GLOBAL", "IT / AI반도체", "SOXX", "QQQ", "2330.TW"),
        ("ASML", "ASML 홀딩(ADR)", "유럽", "GLOBAL", "IT / 반도체소부장", "SOXX", "QQQ", "ASML.AS"),
        ("ARM", "Arm 홀딩스(ADR)", "영국", "NASDAQ", "IT / AI반도체", "SOXX", "QQQ", "ARM.L"),
        ("NXPI", "NXP 세미컨덕터스", "유럽", "GLOBAL", "IT / AI반도체", "SOXX", "QQQ", "NXPI"),
        ("STM", "ST 마이크로일렉트로닉스(ADR)", "유럽", "GLOBAL", "IT / AI반도체", "SOXX", "QQQ", "STM.PA"),
        ("SAP", "SAP SE(ADR)", "유럽", "GLOBAL", "IT / 클라우드소프트웨어", "IGV", "SPY", "SAP.DE"),
        ("BABA", "알리바바 그룹(ADR)", "중국", "NYSE", "통신미디어 / 이커머스플랫폼", "XLC", "SPY", "9988.HK"),
        ("PDD", "핀둬둬(PDD Holdings)", "중국", "NASDAQ", "통신미디어 / 이커머스플랫폼", "XLC", "QQQ", "PDD"),
        ("SE", "Sea Limited(ADR)", "싱가포르", "NYSE", "통신미디어 / 이커머스플랫폼", "XLC", "SPY", "SE"),
        ("MBGYY", "메르세데스-벤츠 그룹 ADR", "유럽", "GLOBAL", "자유소비재 / 전기차완성차", "XLY", "SPY", "MBG.DE"),
        ("SIEGY", "지멘스 AG ADR", "유럽", "GLOBAL", "산업재 / 로봇·자동화", "BOTZ", "SPY", "SIE.DE"),
        ("SBGSY", "슈나이더 일렉트릭 ADR", "유럽", "GLOBAL", "산업재 / 로봇·자동화", "BOTZ", "SPY", "SU.PA"),
        ("ABBNY", "ABB Ltd ADR", "유럽", "GLOBAL", "산업재 / 전력기기·자동화", "XLI", "SPY", "ABBN.SW"),
        ("2454", "대만 미디어텍(MediaTek)", "글로벌", "GLOBAL", "전기전자 / AI반도체", "SOXX", "QQQ", "2454.TW"),
        ("6525.T", "고쿠사이일렉트릭", "일본", "TSE", "IT / 반도체소부장", "SOXX", "QQQ", "6525.T"),
        ("6758.T", "소니", "일본", "TSE", "IT / 빅테크디바이스", "XLK", "QQQ", "6758.T"),
        ("6954.T", "화낙", "일본", "TSE", "산업재 / 로봇·자동화", "BOTZ", "QQQ", "6954.T"),
        ("6871.T", "일본마이크로닉스", "일본", "TSE", "IT / 반도체소부장", "SOXX", "QQQ", "6871.T"),
        ("6656.T", "인스펙", "일본", "TSE", "IT / 반도체소부장", "SOXX", "QQQ", "6656.T"),
        ("6981.T", "무라타 제작", "일본", "TSE", "IT / 전자부품·소재", "XLK", "QQQ", "6981.T"),
        ("7203.T", "토요타 자동차", "일본", "TSE", "자유소비재 / 전기차완성차", "XLY", "SPY", "7203.T"),
    ]
    for ticker, nm, cnt, mkt, sec, ind_bm, m_bm, yf_t in GLOBAL_ADR_SPECIALS:
        parts = sec.split(" / ")
        cat = parts[0]
        sub = parts[1] if len(parts) > 1 else parts[0]
        records.append({
            "keyword": ticker,
            "dict_type": "6.해외주식지정",
            "category": cat,
            "subcategory": sub,
            "standard_sector": sec,
            "official_name": nm,
            "product_type": "개별기업주식",
            "asset_class": "글로벌성장주",
            "market": mkt,
            "country": cnt,
            "currency": "USD" if cnt == "미국" else ("JPY" if cnt == "일본" else "USD"),
            "yahoo_ticker": yf_t,
            "ind_bm": ind_bm,
            "market_bm": m_bm,
            "priority": 100,
            "note": f"글로벌 대표 ADR/해외주식 ({nm})",
        })

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
