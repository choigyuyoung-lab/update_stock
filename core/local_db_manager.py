"""
local_db_manager.py
===================
통합 로컬 SQLite 데이터베이스(data/stock_master.db)의 생성, 스키마 관리,
고속 인덱싱(0.001s), CRUD 작업 및 CSV 자동 덤프를 전담하는 표준 데이터베이스 매니저입니다.
"""

import os
import json
import sqlite3
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

logger = logging.getLogger("LocalDBManager")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "stock_master.db")
DICTIONARY_CSV_PATH = os.path.join(DATA_DIR, "stock_dictionary.csv")
STOCKS_CSV_PATH = os.path.join(DATA_DIR, "stock_master.csv")
BENCHMARKS_CSV_PATH = os.path.join(DATA_DIR, "stock_benchmarks.csv")


def ensure_data_dir() -> None:
    """data/ 폴더가 없으면 자동 생성합니다."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)


def get_db_connection() -> sqlite3.Connection:
    """WAL 모드 및 Row 팩토리가 적용된 SQLite 커넥션을 반환합니다."""
    ensure_data_dir()
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    # WAL 모드로 동시 읽기/쓰기 성능 극대화
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def init_database() -> None:
    """통합 로컬 데이터베이스의 테이블 및 인덱스를 생성/검증합니다."""
    ensure_data_dir()
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # 1. tbl_dictionary (온톨로지 & 섹터/업종 사전)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tbl_dictionary (
                keyword TEXT PRIMARY KEY,
                dict_type TEXT,
                category TEXT,
                subcategory TEXT,
                standard_sector TEXT,
                official_name TEXT,
                product_type TEXT,
                asset_class TEXT,
                market TEXT,
                country TEXT,
                currency TEXT,
                blue_chips TEXT,
                yahoo_ticker TEXT,
                market_bm TEXT,
                ind_bm TEXT,
                priority INTEGER DEFAULT 50,
                note TEXT,
                updated_at TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dict_type ON tbl_dictionary(dict_type);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dict_priority ON tbl_dictionary(priority DESC);")

        # 2. tbl_benchmarks (지표지수 벤치마크 마스터)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tbl_benchmarks (
                ticker TEXT PRIMARY KEY,
                summary TEXT,
                category TEXT,
                country TEXT,
                keywords TEXT,
                notion_page_id TEXT,
                updated_at TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bm_category ON tbl_benchmarks(category);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bm_country ON tbl_benchmarks(country);")

        # 3. tbl_stocks (상장주식 마스터 캐시)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tbl_stocks (
                ticker TEXT PRIMARY KEY,
                name TEXT,
                market TEXT,
                country TEXT,
                product_type TEXT,
                asset_class TEXT,
                sector_industry TEXT,
                blue_chips TEXT,
                market_bm TEXT,
                ind_bm TEXT,
                notion_page_id TEXT,
                updated_at TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_market ON tbl_stocks(market);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_country ON tbl_stocks(country);")

        # 4. tbl_etf_holdings (ETF 구성종목 매핑)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tbl_etf_holdings (
                etf_ticker TEXT,
                holding_ticker TEXT,
                holding_name TEXT,
                weight REAL,
                updated_at TEXT,
                PRIMARY KEY (etf_ticker, holding_ticker)
            );
        """)

        # 5. tbl_finances (퀀트 밸류에이션 및 기술 지표 캐시)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tbl_finances (
                ticker TEXT PRIMARY KEY,
                name TEXT,
                current_price REAL,
                per REAL,
                forward_per REAL,
                pbr REAL,
                eps REAL,
                forward_eps REAL,
                bps REAL,
                dividend_yield REAL,
                industry_per REAL,
                target_price REAL,
                opinion TEXT,
                high_52w REAL,
                low_52w REAL,
                recent_high REAL,
                recent_low REAL,
                ma_200 REAL,
                trend TEXT,
                momentum_12m REAL,
                drop_52w REAL,
                volatility_60d REAL,
                smart_guide TEXT,
                momentum_diag TEXT,
                risk_grade TEXT,
                updated_at TEXT
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fin_per ON tbl_finances(per);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fin_pbr ON tbl_finances(pbr);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fin_trend ON tbl_finances(trend);")
        conn.commit()

        # CSV 파일이 존재하고 DB 테이블이 비어있다면 자동 복원 (GitHub Actions 및 신규 PC 100% 자가 복구)
        auto_restore_from_csv_if_needed(conn)

    logger.info(f"💾 통합 로컬 SQLite DB 스키마 초기화 완료: {DB_PATH}")


def auto_restore_from_csv_if_needed(conn: sqlite3.Connection) -> None:
    """
    GitHub Actions 러너 환경이나 새 컴퓨터처럼 .db 파일이 없는 상태에서 실행될 때,
    Git으로 추적 중인 data/*.csv 파일들로부터 SQLite DB를 0.01초 만에 자동 복원합니다.
    """
    import pandas as pd
    cursor = conn.cursor()

    # 1. tbl_dictionary 복원
    cursor.execute("SELECT count(*) FROM tbl_dictionary;")
    if cursor.fetchone()[0] == 0 and os.path.exists(DICTIONARY_CSV_PATH):
        try:
            df = pd.read_csv(DICTIONARY_CSV_PATH)
            df.to_sql("tbl_dictionary", conn, if_exists="append", index=False)
            logger.info(f"✨ [DB 자동 복원] stock_dictionary.csv -> tbl_dictionary ({len(df)}건)")
        except Exception as e:
            logger.warning(f"⚠️ stock_dictionary.csv 복원 실패: {e}")

    # 2. tbl_stocks 복원
    cursor.execute("SELECT count(*) FROM tbl_stocks;")
    if cursor.fetchone()[0] == 0 and os.path.exists(STOCKS_CSV_PATH):
        try:
            df = pd.read_csv(STOCKS_CSV_PATH)
            df.to_sql("tbl_stocks", conn, if_exists="append", index=False)
            logger.info(f"✨ [DB 자동 복원] stock_master.csv -> tbl_stocks ({len(df)}건)")
        except Exception as e:
            logger.warning(f"⚠️ stock_master.csv 복원 실패: {e}")

    # 3. tbl_benchmarks 복원
    cursor.execute("SELECT count(*) FROM tbl_benchmarks;")
    if cursor.fetchone()[0] == 0 and os.path.exists(BENCHMARKS_CSV_PATH):
        try:
            df = pd.read_csv(BENCHMARKS_CSV_PATH)
            df.to_sql("tbl_benchmarks", conn, if_exists="append", index=False)
            logger.info(f"✨ [DB 자동 복원] stock_benchmarks.csv -> tbl_benchmarks ({len(df)}건)")
        except Exception as e:
            logger.warning(f"⚠️ stock_benchmarks.csv 복원 실패: {e}")

    # 4. tbl_finances 복원
    cursor.execute("SELECT count(*) FROM tbl_finances;")
    if cursor.fetchone()[0] == 0 and os.path.exists(FINANCES_CSV_PATH):
        try:
            df = pd.read_csv(FINANCES_CSV_PATH)
            df.to_sql("tbl_finances", conn, if_exists="append", index=False)
            logger.info(f"✨ [DB 자동 복원] stock_finances.csv -> tbl_finances ({len(df)}건)")
        except Exception as e:
            logger.warning(f"⚠️ stock_finances.csv 복원 실패: {e}")

    # 5. tbl_etf_holdings 복원
    cursor.execute("SELECT count(*) FROM tbl_etf_holdings;")
    if cursor.fetchone()[0] == 0 and os.path.exists(ETF_HOLDINGS_CSV_PATH):
        try:
            df = pd.read_csv(ETF_HOLDINGS_CSV_PATH)
            df.to_sql("tbl_etf_holdings", conn, if_exists="append", index=False)
            logger.info(f"✨ [DB 자동 복원] stock_etf_holdings.csv -> tbl_etf_holdings ({len(df)}건)")
        except Exception as e:
            logger.warning(f"⚠️ stock_etf_holdings.csv 복원 실패: {e}")

    conn.commit()


# ==============================================================================
# 1. tbl_dictionary 관련 함수
# ==============================================================================
def upsert_dictionary_batch(records: List[Dict[str, Any]]) -> int:
    """사전 레코드 목록을 일괄 삽입/갱신합니다."""
    init_database()
    now_str = datetime.now().isoformat()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for r in records:
            k = str(r.get("keyword", "")).strip()
            if not k:
                continue
            chips_json = json.dumps(r.get("blue_chips", []), ensure_ascii=False) if isinstance(r.get("blue_chips"), list) else "[]"
            cursor.execute("""
                INSERT INTO tbl_dictionary (
                    keyword, dict_type, category, subcategory, standard_sector,
                    official_name, product_type, asset_class, market, country,
                    currency, blue_chips, yahoo_ticker, market_bm, ind_bm,
                    priority, note, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(keyword) DO UPDATE SET
                    dict_type=excluded.dict_type,
                    category=excluded.category,
                    subcategory=excluded.subcategory,
                    standard_sector=excluded.standard_sector,
                    official_name=excluded.official_name,
                    product_type=excluded.product_type,
                    asset_class=excluded.asset_class,
                    market=excluded.market,
                    country=excluded.country,
                    currency=excluded.currency,
                    blue_chips=excluded.blue_chips,
                    yahoo_ticker=excluded.yahoo_ticker,
                    market_bm=excluded.market_bm,
                    ind_bm=excluded.ind_bm,
                    priority=excluded.priority,
                    note=excluded.note,
                    updated_at=excluded.updated_at
            """, (
                k,
                r.get("dict_type") or r.get("type", ""),
                r.get("category") or r.get("cat", ""),
                r.get("subcategory") or r.get("sub", ""),
                r.get("standard_sector") or r.get("sector", "") or r.get("sector_industry", ""),
                r.get("official_name") or r.get("name", ""),
                r.get("product_type", ""),
                r.get("asset_class", ""),
                r.get("market", ""),
                r.get("country", ""),
                r.get("currency", ""),
                chips_json,
                r.get("yahoo_ticker", ""),
                r.get("market_bm", ""),
                r.get("ind_bm", ""),
                r.get("priority", 50),
                r.get("note", ""),
                now_str,
            ))
        conn.commit()
    return len(records)


def load_dictionary_index_from_sqlite() -> Dict[str, Any]:
    """
    SQLite DB(tbl_dictionary)에서 전체 사전을 0.001초만에 읽어와
    다차원 인덱스(by_ticker, by_ksic, by_gics, by_theme, by_etf, all_sorted)를 반환합니다.
    """
    index_structure: Dict[str, Any] = {
        "by_ticker": {},
        "by_ksic": {},
        "by_gics": {},
        "by_theme": {},
        "by_etf": {},
        "all_sorted": [],
    }

    init_database()

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tbl_dictionary ORDER BY priority DESC;")
        rows = cursor.fetchall()
        for row in rows:
            chips = []
            if row["blue_chips"]:
                try:
                    chips = json.loads(row["blue_chips"])
                except Exception:
                    chips = []

            rec = {
                "keyword": row["keyword"],
                "type": row["dict_type"],
                "cat": row["category"],
                "sub": row["subcategory"],
                "sector_industry": row["standard_sector"],
                "name": row["official_name"],
                "product_type": row["product_type"],
                "asset_class": row["asset_class"],
                "market": row["market"],
                "country": row["country"],
                "currency": row["currency"],
                "yahoo_ticker": row["yahoo_ticker"],
                "market_bm": row["market_bm"],
                "ind_bm": row["ind_bm"],
                "blue_chips": chips,
                "priority": row["priority"],
            }
            index_structure["all_sorted"].append(rec)

            k = row["keyword"]
            t_type = row["dict_type"] or ""

            # 1) 티커 인덱스
            if "종목지정" in t_type or "6." in t_type or (not k.isdigit() and len(k) <= 6 and " " not in k):
                index_structure["by_ticker"][k.upper()] = rec

            # 2) KSIC 인덱스
            if "KSIC" in t_type or "3." in t_type:
                index_structure["by_ksic"][k] = rec

            # 3) GICS 인덱스
            if "GICS" in t_type or "4." in t_type:
                index_structure["by_gics"][k.upper()] = rec
                index_structure["by_gics"][k] = rec

            # 4) 테마 인덱스
            if "테마" in t_type or "1." in t_type:
                index_structure["by_theme"][k] = rec

            # 5) ETF 인덱스
            if "ETF" in t_type or "5." in t_type:
                index_structure["by_etf"][k.upper()] = rec
                index_structure["by_etf"][k] = rec

    return index_structure


# ==============================================================================
# 2. tbl_benchmarks 관련 함수
# ==============================================================================
def upsert_benchmarks_batch(benchmarks: List[Dict[str, Any]]) -> int:
    """지표지수 벤치마크 목록을 일괄 삽입/갱신합니다."""
    init_database()
    now_str = datetime.now().isoformat()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for b in benchmarks:
            ticker = str(b.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            kw_list = b.get("keywords", [])
            kw_json = json.dumps(kw_list, ensure_ascii=False) if isinstance(kw_list, list) else "[]"
            cursor.execute("""
                INSERT INTO tbl_benchmarks (
                    ticker, summary, category, country, keywords, notion_page_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    summary=excluded.summary,
                    category=excluded.category,
                    country=excluded.country,
                    keywords=excluded.keywords,
                    notion_page_id=excluded.notion_page_id,
                    updated_at=excluded.updated_at
            """, (
                ticker,
                b.get("summary", ""),
                b.get("category", ""),
                b.get("country", ""),
                kw_json,
                b.get("notion_page_id", "") or b.get("id", ""),
                now_str,
            ))
        conn.commit()
    return len(benchmarks)


def load_benchmark_config_from_sqlite() -> Optional[Dict[str, Any]]:
    """SQLite DB에서 지표지수 벤치마크 설정을 0.001초만에 로드합니다."""
    if not os.path.exists(DB_PATH):
        return None

    config: Dict[str, Any] = {"ticker_to_id": {}, "benchmarks": []}
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tbl_benchmarks;")
        rows = cursor.fetchall()
        if not rows:
            return None
        for row in rows:
            t = row["ticker"]
            kws = []
            if row["keywords"]:
                try:
                    kws = json.loads(row["keywords"])
                except Exception:
                    kws = []
            bm_item = {
                "ticker": t,
                "summary": row["summary"],
                "category": row["category"],
                "country": row["country"],
                "keywords": kws,
                "notion_page_id": row["notion_page_id"],
            }
            config["benchmarks"].append(bm_item)
            if row["notion_page_id"]:
                config["ticker_to_id"][t] = row["notion_page_id"]

    return config


# ==============================================================================
# 3. tbl_stocks 관련 함수
# ==============================================================================
def upsert_stocks_batch(stocks: List[Dict[str, Any]]) -> int:
    """상장주식 마스터 캐시를 일괄 삽입/갱신합니다."""
    init_database()
    now_str = datetime.now().isoformat()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for s in stocks:
            ticker = str(s.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            chips = s.get("blue_chips", [])
            chips_json = json.dumps(chips, ensure_ascii=False) if isinstance(chips, list) else "[]"
            cursor.execute("""
                INSERT INTO tbl_stocks (
                    ticker, name, market, country, product_type, asset_class,
                    sector_industry, blue_chips, market_bm, ind_bm, notion_page_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    name=excluded.name,
                    market=excluded.market,
                    country=excluded.country,
                    product_type=excluded.product_type,
                    asset_class=excluded.asset_class,
                    sector_industry=excluded.sector_industry,
                    blue_chips=excluded.blue_chips,
                    market_bm=excluded.market_bm,
                    ind_bm=excluded.ind_bm,
                    notion_page_id=excluded.notion_page_id,
                    updated_at=excluded.updated_at
            """, (
                ticker,
                s.get("name", ""),
                s.get("market", ""),
                s.get("country", ""),
                s.get("product_type", ""),
                s.get("asset_class", ""),
                s.get("sector_industry", ""),
                chips_json,
                s.get("market_bm", ""),
                s.get("ind_bm", ""),
                s.get("notion_page_id", "") or s.get("page_id", ""),
                now_str,
            ))
        conn.commit()
    return len(stocks)


def load_master_stocks_from_sqlite() -> Dict[str, Dict[str, Any]]:
    """SQLite DB에서 전체 상장주식 마스터 캐시를 딕셔너리로 즉시 로드합니다 (0.001s)."""
    ensure_data_dir()
    init_database()
    res: Dict[str, Dict[str, Any]] = {}
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tbl_stocks;")
        for row in cursor.fetchall():
            d = dict(row)
            res[d["ticker"].upper()] = d
    return res


# ==============================================================================
# 4. tbl_finances 관련 함수
# ==============================================================================
FINANCES_CSV_PATH = os.path.join(DATA_DIR, "stock_finances.csv")


def upsert_finances_batch(records: List[Dict[str, Any]]) -> int:
    """여러 종목의 재무/퀀트 지표를 SQLite DB(tbl_finances)에 일괄 저장합니다."""
    init_database()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for r in (records or []):
            ticker = str(r.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            cursor.execute("""
                INSERT INTO tbl_finances (
                    ticker, name, current_price, per, forward_per, pbr, eps, forward_eps, bps,
                    dividend_yield, industry_per, target_price, opinion, high_52w, low_52w,
                    recent_high, recent_low, ma_200, trend, momentum_12m, drop_52w, volatility_60d,
                    smart_guide, momentum_diag, risk_grade, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    name=COALESCE(excluded.name, tbl_finances.name),
                    current_price=COALESCE(excluded.current_price, tbl_finances.current_price),
                    per=COALESCE(excluded.per, tbl_finances.per),
                    forward_per=COALESCE(excluded.forward_per, tbl_finances.forward_per),
                    pbr=COALESCE(excluded.pbr, tbl_finances.pbr),
                    eps=COALESCE(excluded.eps, tbl_finances.eps),
                    forward_eps=COALESCE(excluded.forward_eps, tbl_finances.forward_eps),
                    bps=COALESCE(excluded.bps, tbl_finances.bps),
                    dividend_yield=COALESCE(excluded.dividend_yield, tbl_finances.dividend_yield),
                    industry_per=COALESCE(excluded.industry_per, tbl_finances.industry_per),
                    target_price=COALESCE(excluded.target_price, tbl_finances.target_price),
                    opinion=COALESCE(excluded.opinion, tbl_finances.opinion),
                    high_52w=COALESCE(excluded.high_52w, tbl_finances.high_52w),
                    low_52w=COALESCE(excluded.low_52w, tbl_finances.low_52w),
                    recent_high=COALESCE(excluded.recent_high, tbl_finances.recent_high),
                    recent_low=COALESCE(excluded.recent_low, tbl_finances.recent_low),
                    ma_200=COALESCE(excluded.ma_200, tbl_finances.ma_200),
                    trend=COALESCE(excluded.trend, tbl_finances.trend),
                    momentum_12m=COALESCE(excluded.momentum_12m, tbl_finances.momentum_12m),
                    drop_52w=COALESCE(excluded.drop_52w, tbl_finances.drop_52w),
                    volatility_60d=COALESCE(excluded.volatility_60d, tbl_finances.volatility_60d),
                    smart_guide=COALESCE(excluded.smart_guide, tbl_finances.smart_guide),
                    momentum_diag=COALESCE(excluded.momentum_diag, tbl_finances.momentum_diag),
                    risk_grade=COALESCE(excluded.risk_grade, tbl_finances.risk_grade),
                    updated_at=excluded.updated_at
            """, (
                ticker,
                r.get("name") or r.get("종목명"),
                r.get("current_price") or r.get("현재가"),
                r.get("per") or r.get("PER"),
                r.get("forward_per") or r.get("추정PER"),
                r.get("pbr") or r.get("PBR"),
                r.get("eps") or r.get("EPS"),
                r.get("forward_eps") or r.get("추정EPS"),
                r.get("bps") or r.get("BPS"),
                r.get("dividend_yield") or r.get("배당수익률"),
                r.get("industry_per") or r.get("업종PER"),
                r.get("target_price") or r.get("목표주가"),
                r.get("opinion") or r.get("투자의견"),
                r.get("high_52w") or r.get("52주 최고가"),
                r.get("low_52w") or r.get("52주 최저가"),
                r.get("recent_high") or r.get("직전고점"),
                r.get("recent_low") or r.get("직전저점"),
                r.get("ma_200") or r.get("200일선"),
                r.get("trend") or r.get("추세"),
                r.get("momentum_12m") or r.get("12M 모멘텀"),
                r.get("drop_52w") or r.get("52주 낙폭") or r.get("낙폭율"),
                r.get("volatility_60d") or r.get("60일 변동성"),
                r.get("smart_guide") or r.get("스마트 가이드"),
                r.get("momentum_diag") or r.get("모멘텀 진단"),
                r.get("risk_grade") or r.get("위험도 등급"),
                now_str,
            ))

        # tbl_stocks에 등록된 종목명이 있다면 name 누락분 자동 보강
        cursor.execute("""
            UPDATE tbl_finances
            SET name = (SELECT tbl_stocks.name FROM tbl_stocks WHERE tbl_stocks.ticker = tbl_finances.ticker)
            WHERE (name IS NULL OR name = '') AND EXISTS (SELECT 1 FROM tbl_stocks WHERE tbl_stocks.ticker = tbl_finances.ticker AND tbl_stocks.name != '');
        """)
        conn.commit()
    return len(records)


def load_finances_from_sqlite() -> Dict[str, Dict[str, Any]]:
    """SQLite DB에서 전체 종목의 최신 재무/퀀트 지표를 딕셔너리로 즉시 로드합니다 (0.001s)."""
    ensure_data_dir()
    if not os.path.exists(DB_PATH):
        return {}
    res: Dict[str, Dict[str, Any]] = {}
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tbl_finances;")
        for row in cursor.fetchall():
            d = dict(row)
            res[d["ticker"]] = d
    return res


# ==============================================================================
# 5. tbl_etf_holdings 관련 함수
# ==============================================================================
ETF_HOLDINGS_CSV_PATH = os.path.join(DATA_DIR, "stock_etf_holdings.csv")


def upsert_etf_holdings_batch(records: List[Dict[str, Any]]) -> int:
    """ETF 구성종목 및 편입 비중 데이터를 SQLite DB(tbl_etf_holdings)에 일괄 저장합니다."""
    init_database()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for r in (records or []):
            etf_t = str(r.get("etf_ticker", "")).strip().upper()
            h_t = str(r.get("holding_ticker", "")).strip().upper()
            if not etf_t or not h_t:
                continue
            cursor.execute("""
                INSERT INTO tbl_etf_holdings (
                    etf_ticker, holding_ticker, holding_name, weight, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(etf_ticker, holding_ticker) DO UPDATE SET
                    holding_name=COALESCE(excluded.holding_name, tbl_etf_holdings.holding_name),
                    weight=COALESCE(excluded.weight, tbl_etf_holdings.weight),
                    updated_at=excluded.updated_at
            """, (
                etf_t,
                h_t,
                r.get("holding_name", "") or r.get("name", ""),
                r.get("weight") if r.get("weight") is not None else 0.0,
                now_str,
            ))
        conn.commit()
    return len(records or [])


def load_etf_holdings_from_sqlite(etf_ticker: Optional[str] = None) -> List[Dict[str, Any]]:
    """SQLite DB에서 ETF 구성종목 목록을 즉시 조회합니다 (0.001s)."""
    ensure_data_dir()
    if not os.path.exists(DB_PATH):
        return []
    res: List[Dict[str, Any]] = []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if etf_ticker:
            cursor.execute("SELECT * FROM tbl_etf_holdings WHERE etf_ticker = ? ORDER BY weight DESC;", (etf_ticker.upper(),))
        else:
            cursor.execute("SELECT * FROM tbl_etf_holdings ORDER BY etf_ticker, weight DESC;")
        for row in cursor.fetchall():
            res.append(dict(row))
    return res


# ==============================================================================
# 6. CSV 자동 내보내기 함수
# ==============================================================================
def export_all_tables_to_csv() -> None:
    """사람이 엑셀로 쉽게 확인할 수 있도록 SQLite DB의 모든 테이블을 CSV로 내보냅니다."""
    import pandas as pd
    ensure_data_dir()
    with get_db_connection() as conn:
        # 1. tbl_dictionary -> stock_dictionary.csv
        df_dict = pd.read_sql_query("SELECT * FROM tbl_dictionary ORDER BY priority DESC;", conn)
        df_dict.to_csv(DICTIONARY_CSV_PATH, index=False, encoding="utf-8-sig")

        # 2. tbl_stocks -> stock_master.csv
        df_stocks = pd.read_sql_query("SELECT * FROM tbl_stocks ORDER BY market, ticker;", conn)
        df_stocks.to_csv(STOCKS_CSV_PATH, index=False, encoding="utf-8-sig")

        # 3. tbl_benchmarks -> stock_benchmarks.csv
        df_bm = pd.read_sql_query("SELECT * FROM tbl_benchmarks ORDER BY category, country, ticker;", conn)
        df_bm.to_csv(BENCHMARKS_CSV_PATH, index=False, encoding="utf-8-sig")

        # 4. tbl_finances -> stock_finances.csv
        try:
            df_fin = pd.read_sql_query("SELECT * FROM tbl_finances ORDER BY ticker;", conn)
            df_fin.to_csv(FINANCES_CSV_PATH, index=False, encoding="utf-8-sig")
        except Exception:
            pass

        # 5. tbl_etf_holdings -> stock_etf_holdings.csv
        try:
            df_etf = pd.read_sql_query("SELECT * FROM tbl_etf_holdings ORDER BY etf_ticker, weight DESC;", conn)
            df_etf.to_csv(ETF_HOLDINGS_CSV_PATH, index=False, encoding="utf-8-sig")
        except Exception:
            pass

    logger.info(f"📊 [CSV 내보내기 완료] -> {DICTIONARY_CSV_PATH}, {STOCKS_CSV_PATH}, {BENCHMARKS_CSV_PATH}, {FINANCES_CSV_PATH}, {ETF_HOLDINGS_CSV_PATH}")
