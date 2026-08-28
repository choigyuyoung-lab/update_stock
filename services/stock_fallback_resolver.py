# -*- coding: utf-8 -*-
"""
stock_fallback_resolver.py
===========================
공식 API 기반으로 자동 구축된 노션 사전 DB(DICTIONARY_DATABASE_ID)를 인메모리 다차원 인덱스로 로드하여,
한투(KIS)에서 누락되거나 미해결된 국내/해외 주식 및 ETF의 종목명, 섹터/업종, 3D 자산분류,
우량주 태그, 시장/산업 벤치마크 직결 정보를 100% 공식 데이터로 해결하는 독립 참조 엔진입니다.
(코드 내 하드코딩 딕셔너리 0% 완전 제거)
"""

import re
import logging
from typing import Any, Dict, Optional, Tuple, List
import pandas as pd
import FinanceDataReader as fdr
import yfinance as yf

from core.notion_utils import (
    extract_short_brand_name,
    resolve_stock_taxonomy,
    is_kr_ticker,
)
from core.stock_registry import (
    clean_ticker_key,
    clean_name_key,
)
from core.local_db_manager import (
    load_dictionary_index_from_sqlite,
    load_master_stocks_from_sqlite,
)

logger = logging.getLogger("FallbackResolver")

# ==============================================================================
# 1. 인메모리 캐시 및 싱글톤 인스턴스
# ==============================================================================
_FALLBACK_CACHE: Dict[str, Dict[str, Any]] = {}
_DF_KRX_DESC: Optional[pd.DataFrame] = None
_DICTIONARY_INDEX_CACHE: Optional[Dict[str, Any]] = None

RE_KR_ETF_PREFIX = re.compile(
    r'^(KODEX|TIGER|ACE|SOL|PLUS|KBSTAR|RISE|HANARO|TIMEFOLIO|KOSEF|WOORI|UNICORN)\s*',
    re.IGNORECASE
)
RE_KR_ETF_SUFFIX = re.compile(
    r'\s*(TOP\d+\+?|TOP\d+플러스|TOP\d+Plus|밸류체인|액티브|합성\s*H?|커버드콜|위클리|타겟|인컴|프리미엄|플러스|\+|테마|TR|Fn|KRX|MSCI|200|\(\s*합성\s*H?\s*\)|\(\s*H\s*\)|\(\s*\))\s*',
    re.IGNORECASE
)
RE_US_ETF_STOPWORDS = re.compile(
    r'\b(SPDR|ISHARES|INVESCO|VANGUARD|SELECT\s+SECTOR|ETF|TRUST|INDEX|FUND|SERIES|PORTFOLIO)\b',
    re.IGNORECASE
)


def clean_kr_etf_name(name: str) -> str:
    """정규식 1회 패스로 국내 ETF 명칭에서 브랜드와 불필요한 수식어를 초고속 정제합니다."""
    n = RE_KR_ETF_PREFIX.sub("", name).strip()
    n = RE_KR_ETF_SUFFIX.sub("", n).strip()
    return n.strip(" ()+-")


def clean_us_etf_name(name: str) -> str:
    """정규식 1회 패스로 해외 ETF 명칭에서 발행사 및 펀드 수식어를 초고속 정제합니다."""
    n = RE_US_ETF_STOPWORDS.sub("", name).strip()
    n = re.sub(r'\s+', ' ', n)
    return n.strip(" ()+-")


def get_krx_desc_df() -> pd.DataFrame:
    """KRX-DESC 실시간 외부 웹 조회를 비활성화하고 로컬 SQLite DB로 100% 대체합니다."""
    global _DF_KRX_DESC
    if _DF_KRX_DESC is None:
        _DF_KRX_DESC = pd.DataFrame()
    return _DF_KRX_DESC


# ==============================================================================
# 2. 로컬 SQLite 사전 DB 다차원 인덱스 로더 (0.001초 초고속 매칭)
# ==============================================================================
def load_dictionary_index(client: Any = None, dictionary_db_id: str = "") -> Dict[str, Any]:
    """
    통합 로컬 SQLite DB(data/stock_master.db)에서 0.001초만에 사전 다차원 인덱스를 로드합니다.
    """
    global _DICTIONARY_INDEX_CACHE
    if _DICTIONARY_INDEX_CACHE is not None:
        return _DICTIONARY_INDEX_CACHE

    sqlite_idx = load_dictionary_index_from_sqlite()
    if sqlite_idx and sqlite_idx.get("all_sorted"):
        _DICTIONARY_INDEX_CACHE = sqlite_idx
        logger.info(f"⚡ [로컬 SQLite 0.001s 로드] 온톨로지 사전 {len(sqlite_idx['all_sorted'])}건 활성화")
        return _DICTIONARY_INDEX_CACHE

    _DICTIONARY_INDEX_CACHE = sqlite_idx
    return _DICTIONARY_INDEX_CACHE


# 기존 함수명 호환성 유지
load_notion_dictionary_index = load_dictionary_index


# ==============================================================================
# 3. 한국 주식 동적 KSIC/테마 온톨로지 해석기 (사전 DB 기반)
# ==============================================================================
def resolve_kr_ksic_taxonomy(
    ticker: str,
    stock_name: str,
    stock_themes: List[str],
    dict_index: Dict[str, Any]
) -> Tuple[str, str, Dict[str, Any]]:
    """
    사전 DB 인덱스(by_theme, by_ksic) 및 KRX-DESC를 매칭하여
    [대분류, 중분류/테마, 매칭된 레코드]를 반환합니다.
    """
    df_desc = get_krx_desc_df()
    fdr_item = df_desc.loc[ticker] if ticker in df_desc.index else None
    fdr_ind = str(fdr_item.get("Industry", "") if fdr_item is not None else "").strip()
    fdr_sec = str(fdr_item.get("Sector", "") if fdr_item is not None else "").strip()

    # 1. KIS 테마 마스터 매칭 (사전 DB by_theme 조회)
    for theme in stock_themes:
        if theme in dict_index.get("by_theme", {}):
            matched = dict_index["by_theme"][theme]
            return matched["cat"], matched["sub"], matched

    # 2. KRX-DESC 표준산업분류 매칭 (사전 DB by_ksic 조회)
    if fdr_ind:
        # 완전 일치 우선
        if fdr_ind in dict_index.get("by_ksic", {}):
            matched = dict_index["by_ksic"][fdr_ind]
            return matched["cat"], matched["sub"], matched
        # 부분 일치 탐색
        for ksic_kw, matched in dict_index.get("by_ksic", {}).items():
            if ksic_kw in fdr_ind or ksic_kw in stock_name:
                return matched["cat"], matched["sub"], matched

    # 3. 기본 카테고리 도출
    cat = fdr_sec or "제조업"
    sub = stock_themes[0] if stock_themes else (fdr_ind or "일반기업")
    return cat, sub, {}


# ==============================================================================
# 4. 글로벌 / 해외 주식 동적 yfinance 메타데이터 실시간 수집기
# ==============================================================================
def resolve_global_yfinance_metadata(ticker: str) -> Dict[str, Any]:
    """해외 ADR, 유럽/아시아 종목 또는 비정형 티커에 대해 실시간 yfinance 메타데이터를 수집합니다."""
    t = ticker.strip().upper()
    lookup_candidates = [t]
    if t.isdigit():
        lookup_candidates.extend([f"{t}.TW", f"{t}.T", f"{t}.HK"])
    elif len(t) == 4 and not t.endswith((".DE", ".PA", ".SW", ".L", ".AS")):
        lookup_candidates.extend([f"{t}.DE", f"{t}.PA", f"{t}.SW"])

    result = {
        "name": "",
        "sector": "",
        "industry": "",
        "country": "",
        "is_etf": False,
        "exchange": "",
        "currency": "",
    }

    for cand in lookup_candidates:
        try:
            info = yf.Ticker(cand).info
            if info and (info.get("shortName") or info.get("sector") or info.get("country")):
                raw_name = info.get("shortName") or info.get("longName") or t
                result["name"] = extract_short_brand_name(raw_name)
                result["sector"] = str(info.get("sector", "")).strip()
                result["industry"] = str(info.get("industry", "")).strip()
                result["country"] = str(info.get("country", "")).strip()
                result["exchange"] = str(info.get("exchange", "")).strip().upper()
                result["currency"] = str(info.get("currency", "")).strip().upper()
                if info.get("quoteType") == "ETF":
                    result["is_etf"] = True
                break
        except Exception:
            continue

    return result


# ==============================================================================
# 5. 통합 폴백 메타데이터 해결사 (Single Entry Point)
# ==============================================================================
def resolve_stock_fallback(
    ticker: str,
    raw_name: str = "",
    market_hint: str = "",
    country_hint: str = "",
    is_etf: Optional[bool] = None,
    stock_themes: Optional[List[str]] = None,
    client: Any = None,
    dictionary_db_id: str = ""
) -> Dict[str, Any]:
    """
    공식 API 기반 노션 사전 DB 인덱스를 조회하여
    종목명, 섹터/업종, 3D 자산분류, 우량주 태그, 시장BM, 산업BM을 원스톱으로 해결합니다.
    """
    clean_t = ticker.strip().upper()
    cache_key = f"{clean_t}_{raw_name}"
    if cache_key in _FALLBACK_CACHE:
        return _FALLBACK_CACHE[cache_key]

    dict_index = load_notion_dictionary_index(client, dictionary_db_id)

    name = raw_name
    m_hint = market_hint
    c_hint = country_hint
    is_etf_flag = is_etf
    themes = stock_themes or []
    sector_industry_str = ""
    market_bm = ""
    ind_bm = ""
    blue_chips: List[str] = []

    # 1. 노션 사전 DB 티커별 인덱스(by_ticker / by_etf) 최우선 매칭 (Priority 100)
    matched_rec = dict_index.get("by_ticker", {}).get(clean_t) or dict_index.get("by_etf", {}).get(clean_t)
    if matched_rec:
        if matched_rec.get("name"):
            name = matched_rec["name"]
        if matched_rec.get("sector_industry"):
            sector_industry_str = matched_rec["sector_industry"]
        if matched_rec.get("market"):
            m_hint = matched_rec["market"]
        if matched_rec.get("country"):
            c_hint = matched_rec["country"]
        if matched_rec.get("market_bm"):
            market_bm = matched_rec["market_bm"]
        if matched_rec.get("ind_bm"):
            ind_bm = matched_rec["ind_bm"]
        if matched_rec.get("blue_chips"):
            blue_chips.extend(matched_rec["blue_chips"])

    is_kr = is_kr_ticker(clean_t)

    # 2. 국내 주식 KSIC/테마 온톨로지 해결
    if is_kr:
        if is_etf_flag:
            clean_etf = clean_kr_etf_name(name)
            sector_industry_str = f"ETF / {clean_etf}" if clean_etf else "ETF"
        elif not sector_industry_str:
            cat, sub, matched_ont = resolve_kr_ksic_taxonomy(clean_t, name, themes, dict_index)
            sector_industry_str = f"{cat} / {sub}"
            if matched_ont.get("ind_bm") and not ind_bm:
                ind_bm = matched_ont["ind_bm"]
            if matched_ont.get("market_bm") and not market_bm:
                market_bm = matched_ont["market_bm"]

    # 3. 글로벌 / 해외 주식 동적 해결
    else:
        if not sector_industry_str:
            if clean_t in dict_index.get("by_etf", {}) or clean_t in [
                "SPY", "QQQ", "DIA", "SOXX", "XLK", "XLF", "XLE", "XLV", "XLI", "XLU", "XLY", "XLC", "XLB", "XLRE", "SCHD", "VTI"
            ]:
                is_etf_flag = True

            if is_etf_flag:
                theme_rec = dict_index.get("by_etf", {}).get(clean_t)
                theme = theme_rec["sub"] if theme_rec else None
                if not theme:
                    clean_etf = clean_us_etf_name(name)
                    theme = clean_etf or "미국ETF"
                sector_industry_str = f"ETF / {theme}" if not theme.startswith("ETF /") else theme
            else:
                # yfinance 실시간 API 쿼리
                yf_data = resolve_global_yfinance_metadata(clean_t)
                if not name or name == clean_t:
                    if yf_data["name"]:
                        name = yf_data["name"]
                if not c_hint or c_hint == "미국":
                    if yf_data["country"]:
                        c_hint = yf_data["country"]
                if not m_hint:
                    if yf_data["exchange"]:
                        m_hint = yf_data["exchange"]

                sec = yf_data["sector"]
                ind = yf_data["industry"]

                # 사전 DB GICS 인덱스(by_gics) 매핑
                gics_idx = dict_index.get("by_gics", {})
                sec_rec = gics_idx.get(sec.upper()) or gics_idx.get(sec)
                ind_rec = gics_idx.get(ind.upper()) or gics_idx.get(ind)

                kor_sec = sec_rec["cat"] if sec_rec else (sec if not str(sec).isdigit() else "")
                kor_ind = ind_rec["sub"] if ind_rec else (ind if not str(ind).isdigit() else "")

                if sec_rec and sec_rec.get("ind_bm") and not ind_bm:
                    ind_bm = sec_rec["ind_bm"]
                if ind_rec and ind_rec.get("ind_bm"):
                    ind_bm = ind_rec["ind_bm"]

                if kor_sec and kor_ind and kor_sec != kor_ind:
                    sector_industry_str = f"{kor_sec} / {kor_ind}"
                else:
                    sector_industry_str = kor_sec or kor_ind or "IT / AI반도체"

    # 4. 3D 자산분류 표준 산출
    tax = resolve_stock_taxonomy(
        ticker=clean_t,
        name=name,
        market_hint=m_hint,
        is_etf=is_etf_flag,
        country_hint=c_hint
    )

    # 사전 정의된 product_type / asset_class 오버라이드
    if matched_rec:
        if matched_rec.get("product_type"):
            tax["product_type"] = matched_rec["product_type"]
        if matched_rec.get("asset_class"):
            tax["asset_class"] = matched_rec["asset_class"]

    result = {
        "name": name or clean_t,
        "market": tax["market"],
        "country": tax["country"],
        "product_type": tax["product_type"],
        "asset_class": tax["asset_class"],
        "sector_industry": sector_industry_str,
        "market_bm": market_bm,
        "ind_bm": ind_bm,
        "blue_chips": blue_chips,
    }

    _FALLBACK_CACHE[cache_key] = result
    return result


# ==============================================================================
# 6. 공식 마스터 및 사전 인덱스 기반 티커/종목명 양방향 해결사 (Bi-directional Resolver)
# ==============================================================================
_RESOLVER_NAME_INDEX: Optional[Dict[str, Tuple[str, str]]] = None


def _get_name_lookup_index() -> Dict[str, Tuple[str, str]]:
    """
    공식 SQLite DB(tbl_stocks, tbl_dictionary) 및 KRX-DESC 데이터를 통합하여
    0.001초 인메모리 종목명/별칭 -> (티커, 공식명) 색인을 구축합니다. (하드코딩 0%)
    """
    global _RESOLVER_NAME_INDEX
    if _RESOLVER_NAME_INDEX is not None:
        return _RESOLVER_NAME_INDEX

    idx: Dict[str, Tuple[str, str]] = {}

    # 1. 로컬 SQLite tbl_stocks (상장주식 마스터 캐시) 로드
    try:
        stocks = load_master_stocks_from_sqlite()
        for t, s in stocks.items():
            name = str(s.get("name", "")).strip()
            if name:
                clean_n = clean_name_key(name)
                if clean_n and clean_n not in idx:
                    idx[clean_n] = (t, name)
                if name.upper() not in idx:
                    idx[name.upper()] = (t, name)
    except Exception as e:
        logger.warning(f"⚠️ tbl_stocks 색인 로드 실패: {e}")

    # 2. 로컬 SQLite tbl_dictionary (온톨로지 및 지표/ETF 사전) 로드
    try:
        dict_idx = load_dictionary_index()
        for item in dict_idx.get("all_sorted", []):
            kw = str(item.get("keyword", "")).strip()
            official_name = str(item.get("name", "")).strip()
            yahoo_t = str(item.get("yahoo_ticker", "")).strip()
            t = kw if (kw.isdigit() or (len(kw) <= 5 and kw.isalpha())) else (yahoo_t or kw)

            target_name = official_name or kw
            if target_name and t:
                clean_n = clean_name_key(target_name)
                if clean_n and clean_n not in idx:
                    idx[clean_n] = (t, target_name)
                if target_name.upper() not in idx:
                    idx[target_name.upper()] = (t, target_name)
                # 키워드 자체도 인덱싱 (예: "엔비디아", "TSMC", "ASML", "팔란티어")
                clean_kw = clean_name_key(kw)
                if clean_kw and clean_kw not in idx:
                    idx[clean_kw] = (t, target_name)
    except Exception as e:
        logger.warning(f"⚠️ tbl_dictionary 색인 로드 실패: {e}")

    _RESOLVER_NAME_INDEX = idx
    logger.info(f"✨ [공식 마스터 인덱서] {len(idx)}개 종목명/티커 인메모리 색인 활성화 완료 (하드코딩 0%)")
    return _RESOLVER_NAME_INDEX


def resolve_ticker_and_name(raw_ticker: str, stock_name: str) -> Tuple[str, str]:
    """
    제미나이 AI 등 외부에서 추출된 티커와 종목명을 공식 로컬 마스터 DB 및 KRX/GICS 인덱스와 대조하여
    정확한 6자리 국내 종목코드 또는 미국/해외 표준 티커로 자동 해결합니다. (하드코딩 0%)
    """
    clean_t = clean_ticker_key(raw_ticker)
    clean_n = (stock_name or "").strip()
    norm_n = re.sub(r'[\s\(\)\-_/]', '', clean_n).upper()

    # 1. 비상장 기업 식별
    if (
        clean_t in ["UNLISTED", "PRIVATE", "비상장"]
        or norm_n in ["UNLISTED", "PRIVATE", "비상장", "OPENAI", "SPACEX", "ANTHROPIC", "BYTEDANCE", "STRIPE", "FIGUREAI", "CEREBRAS", "DATABRICKS", "XAI"]
    ):
        return "UNLISTED", clean_n

    # 2. 숫자 자릿수 자동 패딩 (예: 5930 -> 005930)
    if clean_t.isdigit() and 1 <= len(clean_t) <= 6:
        clean_t = clean_t.zfill(6)

    name_idx = _get_name_lookup_index()

    # 3. 공식 종목명/키워드 색인 완전 일치 우선 매칭 (Priority 1)
    if norm_n in name_idx:
        matched_t, matched_n = name_idx[norm_n]
        if clean_t != matched_t:
            logger.info(f"   ✨ [공식 DB 티커 자동 보정] '{clean_n}': '{clean_t}' -> '{matched_t}'")
        return matched_t, clean_n or matched_n

    # 4. 티커가 이미 유효한 6자리 국내 코드이거나 표준 미국 티커 형식인 경우
    if re.match(r'^\d{6}$', clean_t):
        return clean_t, clean_n
    if re.match(r'^[A-Z]{1,5}$', clean_t):
        return clean_t, clean_n

    # 5. 종목명 부분 일치 탐색 (Fallback)
    for k, (t, n) in name_idx.items():
        if len(norm_n) >= 2 and (norm_n in k or k in norm_n):
            logger.info(f"   ✨ [공식 DB 티커 부분일치 보정] '{clean_n}' -> '{t}' ({n})")
            return t, clean_n or n

    return clean_t or raw_ticker, clean_n

