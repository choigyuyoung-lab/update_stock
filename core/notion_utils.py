"""
notion_utils.py
================
노션(Notion) 데이터베이스 연동, 한국투자증권(KIS) API 인증,
주식 티커/종목명 정제 및 벤치마크 매칭을 위한 통합 유틸리티 모듈입니다.
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import json
import re
import math
import time
from functools import lru_cache
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

from notion_client import Client
from notion_client.errors import HTTPResponseError

# .env 환경변수 로드
load_dotenv()

# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass


# ==============================================================================
# 1. 공통 상수 및 네트워크 세션 관리
# ==============================================================================
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_PAGE_SIZE = 100
KIS_PROD_URL = "https://openapi.koreainvestment.com:9443"


def get_http_session(user_agent: Optional[str] = None) -> requests.Session:
    """Connection: close 및 지수 백오프 Retry가 적용된 고신뢰성 HTTP 세션을 반환합니다."""
    session = requests.Session()
    headers = {
        "Connection": "close",
        "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    session.headers.update(headers)

    retries = Retry(
        total=3,
        backoff_factor=0.2,
        status_forcelist=list(RETRY_STATUS_CODES),
        raise_on_status=False
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


# ==============================================================================
# 2. 환경 변수, DB ID 및 시간 처리 유틸리티
# ==============================================================================
def get_env_var(name: str, required: bool = True, default: Optional[str] = None) -> str:
    """환경 변수를 안전하게 가져옵니다. 필수 변수 누락 시 오류를 발생시킵니다."""
    value: Optional[str] = os.environ.get(name, default)
    if required and not value:
        raise EnvironmentError(f"환경 변수 {name}이(가) 설정되지 않았습니다.")
    return cast(str, value)


def get_kst_now() -> datetime:
    """한국 표준시(KST, Asia/Seoul) 기준 현재 datetime 객체를 반환합니다."""
    return datetime.now(ZoneInfo("Asia/Seoul"))


def get_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """한국 표준시(KST) 기준 현재 일시를 지정된 포맷 문자열로 반환합니다."""
    return get_kst_now().strftime(fmt)


def kst_isoformat() -> str:
    """전 세계 어느 가상 서버에서 실행되든 한국 표준시(KST, Asia/Seoul)를 기준으로 ISO 일시를 반환합니다."""
    return get_kst_now().isoformat()


# ==============================================================================
# 2-1. 한국(KRX) & 미국(NYSE) 정밀 휴장일/공휴일 판별 엔진 (exchange_calendars 기반)
# ==============================================================================
@lru_cache(maxsize=4)
def _get_market_calendar(market: str):
    """
    한국(XKRX) 및 미국(XNYS) 거래소 캘린더 인스턴스를 캐싱하여 반환합니다.
    """
    import exchange_calendars as xcals
    iso_code = "XKRX" if market.upper() == "KR" else "XNYS"
    return xcals.get_calendar(iso_code)


def get_kr_market_holidays(year: int) -> Dict[date, str]:
    """한국 증권시장(KRX) 연간 공식 휴장일 맵을 반환합니다."""
    try:
        cal = _get_market_calendar("KR")
        start_dt = f"{year}-01-01"
        end_dt = f"{year}-12-31"
        holidays_dt = cal.regular_holidays.holidays(start=start_dt, end=end_dt)
        return {ts.date(): "KRX 공식 공휴일/휴장일" for ts in holidays_dt}
    except Exception:
        return {}


def get_us_market_holidays(year: int) -> Dict[date, str]:
    """미국 증권시장(NYSE/NASDAQ) 연간 공식 휴장일 맵을 반환합니다."""
    try:
        cal = _get_market_calendar("US")
        start_dt = f"{year}-01-01"
        end_dt = f"{year}-12-31"
        holidays_dt = cal.regular_holidays.holidays(start=start_dt, end=end_dt)
        return {ts.date(): "미국 증시 공식 휴장일" for ts in holidays_dt}
    except Exception:
        return {}


def is_market_holiday(market: str = "KR", dt: Optional[datetime] = None) -> Tuple[bool, str]:
    """
    지정된 시장('KR' 또는 'US')의 특정 일시(기본값: 현지 시각 기준 오늘)가 휴장일인지 정밀 판별합니다.
    - KR: Asia/Seoul 현지 기준
    - US: America/New_York 현지 기준 (한국 시간 토요일 아침 미국 금요일 종가 수집 누락 방지)
    Returns: (is_closed, reason)
    """
    m = market.upper()
    tz = ZoneInfo("Asia/Seoul" if m == "KR" else "America/New_York")

    if dt is not None:
        local_dt = dt if dt.tzinfo else dt.replace(tzinfo=tz)
        local_dt = local_dt.astimezone(tz)
    else:
        local_dt = datetime.now(tz)

    local_date_str = local_dt.strftime("%Y-%m-%d")
    weekday = local_dt.weekday()

    # 1. 주말 판별
    if weekday == 5:
        return True, f"토요일 ({'주말 휴장' if m == 'KR' else '미국 주말 휴장'})"
    if weekday == 6:
        return True, f"일요일 ({'주말 휴장' if m == 'KR' else '미국 주말 휴장'})"

    # 2. 거래소 정규 세션(개장일) 확인 (exchange_calendars)
    try:
        cal = _get_market_calendar(m)
        if cal.is_session(local_date_str):
            return False, "정규 영업일" if m == "KR" else "미국 정규 영업일"
        return True, f"{'KRX' if m == 'KR' else '미국'} 증시 공식 공휴일/휴장일"
    except Exception as exc:
        # 비상시 폴백
        return False, f"정규 영업일 (캘린더 조회 예외: {exc})"


def get_db_id(
    primary_name: str,
    fallback_names: Optional[List[str]] = None,
    default: str = "",
    required: bool = False
) -> str:
    """
    지정된 주요 환경변수 및 폴백 목록에서 첫 번째로 유효한 노션 데이터베이스 ID(32자리)를 안전하게 로드합니다.
    """
    candidates = [primary_name] + (fallback_names or [])
    for name in candidates:
        val = os.environ.get(name)
        if val and val.strip():
            return val.strip()

    if required:
        raise EnvironmentError(f"필수 노션 데이터베이스 환경변수 '{primary_name}'이(가) 설정되지 않았습니다.")

    return default


def get_all_portfolio_db_ids() -> Dict[str, str]:
    """
    포트폴리오 분석 및 리포트 파이프라인에 필요한 7대 노션 DB ID를 안전하게 일괄 로드하여 딕셔너리로 반환합니다.
    """
    return {
        "investment_db_id": get_db_id("DATABASE_ID", ["INVESTMENT_DB_ID", "INVESTMENT_DATABASE_ID"], required=False),
        "account_status_db_id": get_db_id("ACCOUNT_STATUS_DB_ID", ["ACCOUNT_STATUS_DATABASE_ID"]),
        "stock_holdings_db_id": get_db_id("STOCK_HOLDINGS_DB_ID", ["STOCK_HOLDINGS_DATABASE_ID"]),
        "account_holdings_db_id": get_db_id("ACCOUNT_HOLDINGS_DB_ID", ["ACCOUNT_HOLDINGS_DATABASE_ID"]),
        "trade_log_db_id": get_db_id("TRADE_LOG_DB_ID", ["TRADE_LOG_DATABASE_ID"]),
        "cash_flow_db_id": get_db_id("CASH_FLOW_DB_ID", ["CASH_FLOW_DATABASE_ID"]),
        "notion_report_db_id": get_db_id("NOTION_REPORT_DB_ID", ["REPORT_DATABASE_ID", "REPORT_DB_ID"]),
        "youtube_db_id": get_db_id("YOUTUBE_DATABASE_ID", ["YOUTUBE_DB_ID"], required=False),
    }


# ==============================================================================
# 3. 데이터 검증 및 변환 유틸리티
# ==============================================================================
def is_kr_ticker(ticker: str) -> bool:
    """국내 종목코드(보통주, 신형 우선주, .KS/.KQ 접미사 포함)를 정밀 판별합니다."""
    if not ticker:
        return False
    
    t = ticker.strip().upper()
    
    # 1. 해외 거래소 접미어가 명시된 경우 -> 해외(False)
    if t.endswith((".T", ".TA", ".TW", ".HK", ".L", ".DE", ".AS", ".PA", ".SW", ".CO")):
        return False

    # 2. 국내 거래소 접미어가 붙은 경우 (.KS, .KQ) -> 국내(True)
    if t.endswith((".KS", ".KQ")):
        return True

    # 3. 접미어 분리 후 순수 티커 검사 (6자리 & 숫자로 시작하는 KRX 코드)
    clean_t = t.split(".")[0].strip()
    if len(clean_t) == 6 and clean_t[0].isdigit() and clean_t.isalnum():
        return True

    return False


def is_valid_num(value: Any) -> bool:
    """숫자 값이 유효한지 검증합니다 (NaN, Inf, None, 빈문자열, 특수문자 차단)."""
    if value is None:
        return False
    try:
        if isinstance(value, str):
            clean = value.replace(",", "").strip()
            if not clean or clean.lower() in ["null", "none", "nan", "-", ""]:
                return False
            val = float(clean)
        else:
            val = float(value)
        return not (math.isnan(val) or math.isinf(val))
    except (TypeError, ValueError):
        return False


def safe_float(value: Any) -> Optional[float]:
    """문자열/숫자를 안전하게 float로 변환합니다. 유효하지 않거나 0이면 None을 반환합니다."""
    if not is_valid_num(value):
        return None
    try:
        val = float(str(value).replace(",", "").strip())
        return val if val != 0 else None
    except (TypeError, ValueError):
        return None


def make_rich_text(val: Any) -> Dict[str, Any]:
    """노션 rich_text 속성 구조 객체를 생성합니다."""
    text_val = str(val).strip() if val is not None else ""
    return {"rich_text": [{"text": {"content": text_val}}]} if text_val else {"rich_text": []}


# ==============================================================================
# 4. 종목명 정제 및 글로벌 티커 검색
# ==============================================================================
def extract_short_brand_name(name: str) -> str:
    """노션 열 너비 및 가독성을 위해 법인형태/접미사를 제거한 핵심 브랜드명을 추출합니다."""
    if not name:
        return ""
    n = name.strip()

    # 1. 글로벌/일본 대형주 주요 별칭 매핑
    brand_map = {
        r'(?i)\bTAIWAN SEMICONDUCTOR\b': 'TSMC',
        r'(?i)\bALPHABET\b': 'Alphabet',
        r'(?i)\bAMAZON\b': 'Amazon',
        r'(?i)\bMETA PLATFORMS\b': 'Meta Platforms',
        r'(?i)\bASML\b': 'ASML',
        r'(?i)\bORACLE\b': 'Oracle',
        r'(?i)\bDELL\b': 'Dell',
        r'(?i)\bVERTIV\b': 'Vertiv',
        r'(?i)\bCROWDSTRIKE\b': 'CrowdStrike',
        r'(?i)\bMICROSOFT\b': 'Microsoft',
        r'(?i)\bAPPLE\b': 'Apple',
        r'(?i)\bNVIDIA\b': 'NVIDIA',
        r'(?i)\bBROADCOM\b': 'Broadcom',
        r'(?i)\bQUALCOMM\b': 'Qualcomm',
        r'(?i)\bNOVO NORDISK\b': 'Novo Nordisk',
        r'(?i)\bASTRAZENECA\b': 'AstraZeneca',
        r'(?i)\bARM HOLDINGS\b': 'ARM',
        r'(?i)\bTOYOTA(\s+MOTOR)?\b': 'Toyota',
        r'(?i)\bSONY(\s+GROUP)?\b': 'Sony',
        r'(?i)\bSAP(\s+SE)?\b': 'SAP',
        r'(?i)\bMURATA(\s+MFG|\s+MANUFACTURING)?\b': 'Murata',
        r'(?i)\bKEYENCE(\s+CORP)?\b': 'Keyence',
        r'(?i)\bTOKYO\s+ELECTRON\b': 'Tokyo Electron',
        r'(?i)\bSHIN[\s\-]ETSU(\s+CHEMICAL)?\b': 'Shin-Etsu',
        r'(?i)\bHOYA(\s+CORP)?\b': 'Hoya',
        r'(?i)\bLASERTEC\b': 'Lasertec',
        r'(?i)\bDISCO(\s+CORP)?\b': 'Disco',
        r'(?i)\bADVANTEST\b': 'Advantest',
        r'(?i)\bHITACHI\b': 'Hitachi',
    }
    for pat, brand in brand_map.items():
        if re.search(pat, n):
            return brand

    # 2. 특수기호 및 법인 형태 수식어 제거
    clean = re.sub(r'[\(\)\[\],\.\-\/\:\'\"]', ' ', n)
    remove_patterns = [
        r'(?i)\bCL(ASS)?\s*[A-Z0-9]?\b', r'(?i)\bORD(INARY)?\b', r'(?i)\bREG(ISTERED)?\b',
        r'(?i)\bSHS\b', r'(?i)\bSHARES\b', r'(?i)\bSP\s*ADR\b', r'(?i)\bADR\b', r'(?i)\bADS\b',
        r'(?i)\bNV\b', r'(?i)\bDE\b', r'(?i)\bCORP(ORATION)?\b', r'(?i)\bINC(ORPORATED)?\b',
        r'(?i)\bLTD\b', r'(?i)\bLIMITED\b', r'(?i)\bCO\b', r'(?i)\bCOS\b', r'(?i)\bLLC\b',
        r'(?i)\bPLC\b', r'(?i)\bHOLDINGS?\b', r'(?i)\bGROUP\b', r'(?i)\bHOLDI\b', r'(?i)\bUSA\b',
        r'(?i)\bCOM\b', r'(?i)\bNY\b', r'(?i)\bS\s*A\b', r'(?i)\bAG\b', r'(?i)\bSE\b',
        r'(?i)\bK\s*K\b', r'(?i)\bSPONSORED\b', r'(?i)\bSOLUTIONS\b',
        r'(?i)\bMFG\b', r'(?i)\bMANUFACTURING\b', r'(?i)\bIND\b', r'(?i)\bINDUSTRIES\b'
    ]
    for p in remove_patterns:
        clean = re.sub(p, ' ', clean)

    tokens = [t for t in clean.split() if len(t) >= 2]
    # 최대 3단어까지만 허용하여 간결성 유지
    res = " ".join(tokens[:3])
    return res.title() if (res.isupper() and len(res) > 4) else res


def search_foreign_ticker(name: str) -> Optional[Tuple[str, str]]:
    """야후 파이낸스 Search API를 통해 미국 메이저 거래소(NYSE, NASDAQ), ADR 및 글로벌 거래소 상장 티커를 추출합니다."""
    if not name:
        return None

    short_brand = extract_short_brand_name(name)
    
    # 다중 검색 후보군 생성
    search_queries: List[str] = []
    if short_brand.upper() == "TSMC":
        search_queries.append("Taiwan Semiconductor")
    elif short_brand.upper() in ["MURATA", "MURATA MFG"]:
        search_queries.extend(["Murata Manufacturing", "Murata", "MRAAY"])
    elif short_brand:
        search_queries.append(short_brand)
    
    # 약어 확장 및 원본 정제 후보 추가
    mfg_expanded = re.sub(r'(?i)\bMFG\b', 'MANUFACTURING', name).strip()
    clean_raw = re.sub(r'[\(\)\[\],\.\-\/\:\'\"]', ' ', name).strip()
    for cand in [mfg_expanded, clean_raw, name]:
        if cand and cand not in search_queries:
            search_queries.append(cand)

    session = get_http_session()
    url = "https://query2.finance.yahoo.com/v1/finance/search"

    try:
        for q in search_queries:
            r = session.get(url, params={"q": q, "quotesCount": 6, "newsCount": 0}, timeout=5)
            if r.status_code != 200:
                continue

            quotes = r.json().get("quotes", [])
            if not quotes:
                continue

            us_major_pick, jp_pick, us_otc_pick, other_pick = None, None, None, None
            for item in quotes:
                sym = str(item.get("symbol") or "").strip().upper()
                typ = item.get("quoteType", "")
                exch = str(item.get("exchange") or item.get("exchDisp") or "").upper()
                sname = str(item.get("shortname") or item.get("longname") or "").strip()

                if typ not in ["EQUITY", "ETF"] or sym.endswith("-USD"):
                    continue

                is_otc = any(m in exch for m in ["PNK", "OTC", "OOTC", "PINK", "GREY"]) or (len(sym) == 5 and sym.endswith("F") and "." not in sym)

                # 1. 미국 정규 메이저 거래소 상장주 (NYSE, NASDAQ, BATS)
                if ("." not in sym) and not is_otc and any(m in exch for m in ["NY", "NASD", "NMS", "BATS", "NGM", "NCM"]):
                    if not us_major_pick:
                        us_major_pick = (sym, short_brand or sname)
                # 2. 일본 도쿄 증시 (.T / JPX / TSE)
                elif sym.endswith(".T") or "TOKYO" in exch or "JPX" in exch or "TSE" in exch:
                    if not jp_pick:
                        t_sym = sym if sym.endswith(".T") else f"{sym}.T"
                        jp_pick = (t_sym, short_brand or sname)
                # 3. 미국 장외시장 / ADR / OTC (PNK, OTC)
                elif is_otc and not sym.endswith(".T"):
                    if not us_otc_pick:
                        us_otc_pick = (sym, short_brand or sname)
                # 4. 기타 해외 증시 (유럽, 홍콩, 대만 등)
                elif not other_pick:
                    other_pick = (sym, short_brand or sname)

            best = us_major_pick or jp_pick or us_otc_pick or other_pick
            if best:
                return best

        return None
    except Exception:
        return None


# ==============================================================================
# 5. 노션 API 클라이언트 및 데이터베이스 연동
# ==============================================================================
def build_notion_client(auth_token: str, use_httpx: bool = False, timeout: float = 60.0) -> Client:
    """공식 notion_client Client 인스턴스를 생성합니다."""
    if use_httpx:
        import httpx
        httpx_client: Any = httpx.Client(timeout=timeout)
        return Client(auth=auth_token, client=httpx_client)
    return Client(auth=auth_token)


def _format_notion_error(error: Exception) -> str:
    """노션 API 오류 발생 시 상태 코드 및 메시지를 포맷팅합니다."""
    if isinstance(error, HTTPResponseError):
        status = getattr(error, "status", None)
        message = getattr(error, "message", None) or str(error)
        body = getattr(error, "body", None)
        return f"status={status}, message={message}, body={body}"
    return str(error)


def safe_databases_query(
    client: Any,
    database_id: str,
    filter: Optional[Dict[str, Any]] = None,
    sorts: Optional[List[Dict[str, Any]]] = None,
    start_cursor: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> Dict[str, Any]:
    """재시도(Retry) 로직이 포함된 안전한 노션 데이터베이스 쿼리 함수"""
    attempt = 1
    while True:
        try:
            params: Dict[str, Any] = {"database_id": database_id, "page_size": page_size}
            if filter:
                params["filter"] = filter
            if sorts:
                params["sorts"] = sorts
            if start_cursor:
                params["start_cursor"] = start_cursor
            if hasattr(client, "databases") and hasattr(client.databases, "query"):
                return cast(Dict[str, Any], client.databases.query(**params))
            elif hasattr(client, "data_sources") and hasattr(client.data_sources, "query"):
                db_info = client.databases.retrieve(database_id=database_id)
                data_sources = db_info.get("data_sources", [])
                ds_id = data_sources[0]["id"] if data_sources else database_id
                ds_params: Dict[str, Any] = {"data_source_id": ds_id, "page_size": page_size}
                if filter:
                    ds_params["filter"] = filter
                if sorts:
                    ds_params["sorts"] = sorts
                if start_cursor:
                    ds_params["start_cursor"] = start_cursor
                return cast(Dict[str, Any], client.data_sources.query(**ds_params))
            else:
                return cast(Dict[str, Any], client.databases.query(**params))
        except HTTPResponseError as error:
            status = getattr(error, "status", None)
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                print(f"   ⚠️ Notion query retry {attempt}/{max_retries} - status={status}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            raise
        except Exception as error:
            if attempt < max_retries:
                print(f"   ⚠️ Notion query retry {attempt}/{max_retries}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            raise


def paginate_database(
    client: Any,
    database_id: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    retry_delay: float = 0.05,
) -> Iterable[Dict[str, Any]]:
    """노션 데이터베이스 전체 페이지를 고속으로 페이지네이션하며 하나씩 yield하는 Generator"""
    start_cursor = None
    while True:
        response = safe_databases_query(client, database_id, start_cursor=start_cursor, page_size=page_size)
        for page in response.get("results", []):
            yield page
        if not response.get("has_more"):
            break
        start_cursor = response.get("next_cursor")
        if retry_delay > 0:
            time.sleep(retry_delay)


def safe_page_update(
    client: Any,
    page_id: str,
    properties: Dict[str, Any],
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> bool:
    """재시도 로직이 포함된 안전한 노션 페이지 속성 갱신 함수"""
    if not properties:
        return False

    attempt = 1
    while True:
        try:
            _ = cast(Any, client.pages.update(page_id=page_id, properties=properties))
            return True
        except HTTPResponseError as error:
            status = getattr(error, "status", None)
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                print(f"   ⚠️ Notion update retry {attempt}/{max_retries} - status={status}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            print(f"   ❌ Notion update failed: {_format_notion_error(error)}")
            return False
        except Exception as error:
            if attempt < max_retries:
                print(f"   ⚠️ Notion update retry {attempt}/{max_retries}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            print(f"   ❌ Notion update failed: {error}")
            return False


def safe_page_create(
    client: Any,
    database_id: str,
    properties: Dict[str, Any],
    children: Optional[List[Dict[str, Any]]] = None,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> Optional[Dict[str, Any]]:
    """재시도 로직 및 자식 블록 지원이 포함된 안전한 노션 신규 페이지 생성 함수"""
    return safe_create_page(
        client=client,
        database_id=database_id,
        properties=properties,
        children=children,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )


def get_page_text(props: Dict[str, Any], names: List[str]) -> str:
    """노션 페이지의 title, rich_text, select, multi_select, status, number 등에서 텍스트를 추출합니다."""
    for name in names:
        prop = props.get(name, {})
        for key in ("title", "rich_text"):
            content = prop.get(key)
            if content and isinstance(content, list) and len(content) > 0:
                text = content[0].get("plain_text", "")
                if text:
                    return text.strip()
        if prop.get("select"):
            return str(prop["select"].get("name", "")).strip()
        if prop.get("multi_select"):
            return ", ".join(item.get("name", "") for item in prop["multi_select"] if item.get("name"))
        if prop.get("status"):
            return str(prop["status"].get("name", "")).strip()
        if prop.get("number") is not None:
            return str(prop["number"])
    return ""


def set_page_date_property(
    props_to_update: Dict[str, Any],
    existing_page_props: Dict[str, Any],
    candidate_names: Optional[List[str]] = None,
    iso_date_str: Optional[str] = None,
) -> str:
    """
    기존 노션 페이지의 속성 목록을 확인하여 실제 존재하는 날짜 컬럼명으로 업데이트 프로퍼티를 설정합니다.
    기본 탐색 순서: ["마지막 업데이트", "업데이트 일자", "업데이트", "최종수정일", "수정일", "일자"]
    매칭된 속성명을 반환합니다.
    """
    if candidate_names is None:
        candidate_names = ["마지막 업데이트", "업데이트 일자", "업데이트", "최종수정일", "수정일", "일자"]
    
    date_val = iso_date_str or kst_isoformat()
    
    # 1. 기존 속성 중 후보와 일치하는 것이 있는지 탐색
    for name in candidate_names:
        if name in existing_page_props:
            props_to_update[name] = {"date": {"start": date_val}}
            return name
            
    # 2. 일치하는 컬럼이 없으면 후보 중 첫 번째 이름으로 기본 설정
    default_name = candidate_names[0]
    props_to_update[default_name] = {"date": {"start": date_val}}
    return default_name


# ==============================================================================
# 5-1. Smart Dirty Checking (속성 변경 감지 엔진 및 I/O 절감)
# ==============================================================================
def extract_prop_raw_value(prop: Dict[str, Any]) -> Any:
    """노션 속성 객체에서 순수 원시 데이터 값을 추출합니다."""
    if not prop or not isinstance(prop, dict):
        return None
    
    p_type = prop.get("type", "")
    if p_type == "number":
        return prop.get("number")
    elif p_type == "select":
        sel = prop.get("select")
        return sel.get("name") if sel else None
    elif p_type == "status":
        st = prop.get("status")
        return st.get("name") if st else None
    elif p_type in ("rich_text", "title"):
        texts = prop.get(p_type, [])
        if texts and isinstance(texts, list):
            return "".join(t.get("plain_text", "") for t in texts).strip()
        return ""
    elif p_type == "date":
        d = prop.get("date")
        return d.get("start") if d else None
    return None


def is_value_different(old_val: Any, new_val: Any, tolerance: float = 1e-4) -> bool:
    """기존 값과 신규 값의 실질적 차이 여부를 판별합니다 (부동소수점 오차 감안)."""
    if old_val is None and new_val is None:
        return False
    if old_val is None or new_val is None:
        return True
    
    # 숫자형 비교
    if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
        return abs(float(old_val) - float(new_val)) > tolerance
        
    return str(old_val).strip() != str(new_val).strip()


def get_diagnostic_color(text: str) -> str:
    """기호에 따른 노션 텍스트 컬러 반환 (▲: red, ━: green, ▼: blue, 기타: default)"""
    if not text:
        return "default"
    if "▲" in text:
        return "red"
    elif "━" in text:
        return "green"
    elif "▼" in text:
        return "blue"
    return "default"


class StockFinancialMetrics(BaseModel):
    """
    국내 및 해외 주식 재무비율 및 5대 퀀트 지표를 위한 단일 표준 데이터 모델 (Pydantic v2).
    - NaN, Inf, 빈 문자열, 비정상 결측치를 자동으로 None으로 정제(Sanitize)합니다.
    - 배당수익률(0~100% 또는 0.0~1.0)을 노션/SQLite 표준 소수점 비율로 자동 정규화합니다.
    """
    ticker: str
    current_price: Optional[float] = Field(None, description="현재가")
    per: Optional[float] = Field(None, description="PER")
    forward_per: Optional[float] = Field(None, description="추정PER")
    pbr: Optional[float] = Field(None, description="PBR")
    eps: Optional[float] = Field(None, description="EPS")
    forward_eps: Optional[float] = Field(None, description="추정EPS")
    bps: Optional[float] = Field(None, description="BPS")
    dividend_yield: Optional[float] = Field(None, description="배당수익률")
    industry_per: Optional[float] = Field(None, description="업종PER")
    high_52w: Optional[float] = Field(None, description="52주 최고가")
    low_52w: Optional[float] = Field(None, description="52주 최저가")
    target_price: Optional[float] = Field(None, description="목표주가")
    opinion: Optional[str] = Field(None, description="투자의견")

    # 5대 퀀트 및 스윙 지표
    swing_high: Optional[float] = Field(None, description="직전고점")
    swing_low: Optional[float] = Field(None, description="직전저점")
    ma200: Optional[float] = Field(None, description="200일선")
    ma_supply: Optional[float] = Field(None, description="수급선(50/60일선)")
    ma50: Optional[float] = Field(None, description="50일선")
    ma60: Optional[float] = Field(None, description="60일선")
    trend: Optional[str] = Field(None, description="추세")
    smart_guide: Optional[str] = Field(None, description="스마트 가이드")
    mom_diag: Optional[str] = Field(None, description="모멘텀 진단")
    risk_grade: Optional[str] = Field(None, description="위험도 등급")
    mom_12m: Optional[float] = Field(None, description="12M 모멘텀")
    drawdown_52w: Optional[float] = Field(None, description="52주 낙폭")
    vol_60d: Optional[float] = Field(None, description="60일 변동성")

    @field_validator("*", mode="before")
    @classmethod
    def sanitize_values(cls, v: Any) -> Any:
        if v in (None, "", "-", "N/A", "null", "None"):
            return None
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    @field_validator("dividend_yield", mode="after")
    @classmethod
    def normalize_dividend_yield(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v > 0.5:
            return round(v / 100.0, 4)
        return v

    def to_notion_candidate_data(self) -> Dict[str, Any]:
        """노션 DB 컬럼명과 1:1 매핑되는 딕셔너리로 변환합니다."""
        return {
            "ticker": self.ticker,
            "현재가": self.current_price,
            "PER": self.per,
            "추정PER": self.forward_per,
            "PBR": self.pbr,
            "EPS": self.eps,
            "추정EPS": self.forward_eps,
            "BPS": self.bps,
            "배당수익률": self.dividend_yield,
            "업종PER": self.industry_per,
            "목표주가": self.target_price,
            "투자의견": self.opinion,
            "52주 최고가": self.high_52w,
            "52주 최저가": self.low_52w,
            "직전고점": self.swing_high,
            "직전저점": self.swing_low,
            "200일선": self.ma200,
            "50일선": self.ma50 or self.ma_supply,
            "60일선": self.ma60 or self.ma_supply,
            "수급선": self.ma_supply or self.ma50 or self.ma60,
            "추세": self.trend,
            "스마트 가이드": self.smart_guide,
            "모멘텀 진단": self.mom_diag,
            "위험도 등급": self.risk_grade,
            "12M 모멘텀": self.mom_12m,
            "52주 낙폭": self.drawdown_52w,
            "낙폭율": self.drawdown_52w,
            "60일 변동성": self.vol_60d,
        }


FINANCE_NUMERIC_FIELDS = [
    "현재가", "PER", "추정PER", "PBR", "EPS", "추정EPS", "BPS", "배당수익률", "업종PER",
    "목표주가", "52주 최고가", "52주 최저가",
    "직전고점", "직전저점", "200일선", "50일선", "60일선", "수급선", "12M 모멘텀", "52주 낙폭", "낙폭율", "60일 변동성"
]
FINANCE_SELECT_FIELDS = ["추세", "스마트 가이드", "모멘텀 진단", "위험도 등급", "투자의견"]
PRICE_NUMERIC_FIELDS = ["현재가", "전일 종가"]


def build_dirty_payload(
    existing_props: Dict[str, Any],
    candidate_data: Dict[str, Any],
    num_fields: Optional[List[str]] = None,
    select_fields: Optional[List[str]] = None,
    relation_fields: Optional[Dict[str, Optional[str]]] = None,
    date_candidate_names: Optional[List[str]] = None,
    diagnostic_color_fn: Optional[Any] = None,
    iso_date_str: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    [Smart Dirty Checking 엔진]
    기존 노션 속성과 신규 수집 데이터를 대조하여, '실질적으로 변경된 속성'만 추출합니다.
    변경된 항목이 전혀 없는 경우 None을 반환하여 불필요한 Notion API I/O를 100% Skip합니다.
    """
    if num_fields is None:
        num_fields = []
    if select_fields is None:
        select_fields = []
    if diagnostic_color_fn is None:
        diagnostic_color_fn = get_diagnostic_color

    dirty_props: Dict[str, Any] = {}
    has_meaningful_change = False

    # 1. 숫자형 지표 검사 및 변경 감지 (스키마 방어: field in existing_props)
    for field in num_fields:
        if field not in existing_props:
            continue
        new_val = candidate_data.get(field)
        if new_val is None:
            continue
            
        old_val = extract_prop_raw_value(existing_props[field])
        if is_value_different(old_val, new_val):
            dirty_props[field] = {"number": new_val}
            has_meaningful_change = True

    # 2. 선택형/진단형(Select / Status / RichText) 지표 검사 및 변경 감지
    for field in select_fields:
        if field not in existing_props:
            continue
        new_val = candidate_data.get(field)
        if not new_val:
            continue

        old_val = extract_prop_raw_value(existing_props[field])
        if is_value_different(old_val, new_val):
            p_type = existing_props[field].get("type", "select")
            if p_type == "status":
                dirty_props[field] = {"status": {"name": str(new_val)}}
            elif p_type == "rich_text":
                color = diagnostic_color_fn(str(new_val)) if diagnostic_color_fn else "default"
                dirty_props[field] = {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": str(new_val)},
                            "annotations": {"color": color, "bold": True}
                        }
                    ]
                }
            else:
                dirty_props[field] = {"select": {"name": str(new_val)}}
            has_meaningful_change = True

    # 3. 관계형(Relation) 지표 검사 및 누락/변경 감지
    if relation_fields:
        for field, target_id in relation_fields.items():
            if field not in existing_props or not target_id:
                continue
            cur_rels = existing_props[field].get("relation", [])
            cur_ids = [r.get("id") for r in cur_rels if isinstance(r, dict) and r.get("id")]
            if target_id not in cur_ids:
                dirty_props[field] = {"relation": [{"id": target_id}]}
                has_meaningful_change = True

    # 4. 실질 데이터 변경이 없을 경우 API 호출 차단 (Skip)
    if not has_meaningful_change:
        return None

    # 5. 실질 데이터 변경이 확인된 경우에만 날짜 속성 주입
    set_page_date_property(
        dirty_props,
        existing_props,
        candidate_names=date_candidate_names,
        iso_date_str=iso_date_str
    )

    return dirty_props


# ==============================================================================
# 6. 한국투자증권(KIS) API 인증 관리 (지능형 디스크 캐싱: 하루 1회 재사용 보장)
# ==============================================================================
def _get_token_cache_paths() -> List[str]:
    """KIS 토큰 캐시 파일이 위치할 수 있는 모든 유효 경로 목록을 반환합니다."""
    candidates = []
    env_path = os.environ.get("KIS_TOKEN_CACHE_FILE")
    if env_path:
        candidates.append(env_path)
    core_dir: Path = Path(__file__).resolve().parent
    project_root: Path = core_dir.parent
    workspace_root: Path = project_root.parent

    candidates.append(str(Path.cwd() / ".kis_token_cache.json"))
    candidates.append(str(project_root / ".kis_token_cache.json"))
    candidates.append(str(core_dir / ".kis_token_cache.json"))
    candidates.append(str(workspace_root / ".kis_token_cache.json"))
    candidates.append(str(workspace_root / "k_all_round_portfolio" / ".kis_token_cache.json"))

    unique_paths = []
    for p in candidates:
        norm = os.path.normpath(p)
        if norm not in unique_paths:
            unique_paths.append(norm)
    return unique_paths


def _load_token_cache() -> Dict[str, Any]:
    """프로젝트 루트 및 core 디렉토리에 캐시된 KIS 토큰 파일을 탐색하여 읽어옵니다."""
    for path in _get_token_cache_paths():
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict) and data:
                        return data
            except Exception:
                continue
    return {}


def _save_token_cache(cache_data: Dict[str, Any]) -> None:
    """KIS 토큰 정보를 프로젝트 루트 및 core 디렉토리 캐시 파일에 모두 동기화하여 저장합니다."""
    for path in _get_token_cache_paths():
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass


def _request_kis_token(
    url_base: str,
    app_key: str,
    app_secret: str,
    max_retries: int = 2,
    base_delay: float = 1.5,
    env_name: str = "모의투자"
) -> Optional[str]:
    """한투 API 액세스 토큰을 발급받거나 유효한 캐시 토큰을 반환합니다."""
    if not app_key or not app_secret:
        return None

    # 1. 캐시 검증: 만료 10분 전까지는 기존 토큰 즉시 재사용
    cache_key = f"{url_base}_{app_key[:6]}"
    cache_data = _load_token_cache()
    cached_entry = cache_data.get(cache_key)

    if cached_entry and isinstance(cached_entry, dict):
        cached_token = cached_entry.get("token")
        expires_at = cached_entry.get("expires_at", 0)
        remaining_sec = expires_at - time.time()
        if cached_token and remaining_sec > 600:
            remaining_min = int(remaining_sec) // 60
            print(f"   ⚡ [{env_name}] 유효한 캐시 토큰 재사용 (만료까지 {remaining_min}분 남음)")
            return str(cached_token)

    # 2. 신규 토큰 발급 요청
    url = f"{url_base}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}

    for attempt in range(1, max_retries + 1):
        try:
            res = requests.post(url, json=body, timeout=8)
            if res.status_code in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ [{env_name}] KIS 토큰 재시도 {attempt}/{max_retries} - status={res.status_code}, {delay}초 대기")
                time.sleep(delay)
                continue

            res.raise_for_status()
            res_json = res.json()
            token = res_json.get("access_token")
            expires_in = res_json.get("expires_in", 86400)
            
            if token:
                # 캐시 저장 (안전 버퍼 포함)
                cache_data[cache_key] = {
                    "token": token,
                    "expires_at": time.time() + float(expires_in),
                    "created_at": kst_isoformat(),
                    "env_name": env_name
                }
                _save_token_cache(cache_data)
                return str(token)
            else:
                print(f"   ⚠️ [{env_name}] KIS 토큰 응답에서 access_token을 찾을 수 없음")
        except Exception as exc:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ [{env_name}] KIS 토큰 통신 실패 (시도 {attempt}/{max_retries}): {exc}, {delay}초 대기")
                time.sleep(delay)
            else:
                print(f"   ❌ [{env_name}] KIS 토큰 발급 최종 실패: {exc}")
    return None


def get_kis_auth_context(max_retries: int = 2, base_delay: float = 1.5) -> Optional[Dict[str, Any]]:
    """
    한국투자증권(KIS) 실전투자(PROD) API 인증 컨텍스트를 반환합니다.
    """
    app_key = (
        os.environ.get("KIS_APP_KEY")
        or os.environ.get("KIS_PROD_APP_KEY")
        or os.environ.get("KIS_REAL_APP_KEY")
        or ""
    ).strip()
    app_secret = (
        os.environ.get("KIS_APP_SECRET")
        or os.environ.get("KIS_PROD_APP_SECRET")
        or os.environ.get("KIS_REAL_APP_SECRET")
        or ""
    ).strip()

    if not app_key or not app_secret:
        print("⚠️ [KIS API] KIS_APP_KEY 또는 KIS_APP_SECRET이 설정되지 않았습니다.")
        return None

    token = _request_kis_token(
        url_base=KIS_PROD_URL,
        app_key=app_key,
        app_secret=app_secret,
        max_retries=max_retries,
        base_delay=base_delay,
        env_name="실전투자(PROD)"
    )
    if token:
        print("✅ [KIS API] 실전투자(PROD) 서버 인증 완료")
        return {
            "token": token,
            "url_base": KIS_PROD_URL,
            "app_key": app_key,
            "app_secret": app_secret,
            "env_type": "PROD"
        }

    print("❌ [KIS API] 실전투자(PROD) 서버 토큰 발급에 실패하였습니다.")
    return None


# ==============================================================================
# 7. 벤치마크 및 키워드 매칭 엔진
# ==============================================================================
def parse_keywords(kw_raw: str, fallback_summary: str = "") -> List[str]:
    """쉼표, 세미콜론, 줄바꿈 등으로 구분된 키워드 문자열을 대문자 정규화 리스트로 파싱합니다."""
    keywords = [k.strip().upper() for k in re.split(r'[,;|\n]+', kw_raw or "") if k.strip()]
    if not keywords and fallback_summary:
        keywords = [fallback_summary.strip().upper()]
    return keywords


def match_keyword(kw: str, text: str) -> bool:
    """단어 길이 및 영문 단어 경계(\\b)를 고려한 고정밀 키워드 매칭 함수"""
    if not kw or len(kw) < 2:
        return False
    if kw.isascii() and len(kw) <= 3:
        return bool(re.search(r'\b' + re.escape(kw) + r'\b', text))
    return kw in text


def find_best_bm(text: str, candidates: List[Dict[str, Any]]) -> Optional[str]:
    """가장 길고 구체적인 키워드가 매칭되는 벤치마크 티커를 선별합니다."""
    best_bm = None
    best_len = 0
    for bm in candidates:
        for kw in bm.get("keywords", []):
            if match_keyword(kw, text):
                if len(kw) > best_len:
                    best_len = len(kw)
                    best_bm = bm["ticker"]
    return best_bm


# ==============================================================================
# 8. 노션 Formula 2.0 속성 추출 및 마크다운 블록/페이지 생성 유틸리티
# ==============================================================================
def get_prop_value(props: Dict[str, Any], names: List[str]) -> Any:
    """
    노션 페이지 속성에서 Formula 2.0, number, title, rich_text, select, multi_select 등
    다양한 타입의 실제 값을 안전하게 추출합니다.
    """
    for name in names:
        if name not in props:
            continue
        prop = props[name]
        ptype = prop.get("type")
        
        # 1. Formula 2.0 파싱
        if ptype == "formula":
            formula_obj = prop.get("formula", {})
            f_type = formula_obj.get("type")
            if f_type in ("number", "string", "boolean"):
                return formula_obj.get(f_type)
            elif f_type == "date":
                date_val = formula_obj.get("date")
                return date_val.get("start") if isinstance(date_val, dict) else date_val
            return None
        
        # 2. 기본 숫자 및 날짜
        elif ptype == "number":
            return prop.get("number")
        elif ptype == "date":
            d = prop.get("date")
            return d.get("start") if isinstance(d, dict) else None
        
        # 3. 텍스트 및 타이틀
        elif ptype in ("title", "rich_text"):
            arr = prop.get(ptype, [])
            if arr and isinstance(arr, list):
                return "".join([item.get("plain_text", "") for item in arr if isinstance(item, dict)]).strip()
            return ""
        
        # 4. Select 및 Multi-select
        elif ptype == "select":
            sel = prop.get("select")
            return sel.get("name") if isinstance(sel, dict) else None
        elif ptype == "multi_select":
            m_sels = prop.get("multi_select", [])
            return [m.get("name") for m in m_sels if isinstance(m, dict) and m.get("name")]
        
        # 5. Checkbox
        elif ptype == "checkbox":
            return prop.get("checkbox", False)
            
        # 6. Relation
        elif ptype == "relation":
            return [r.get("id") for r in prop.get("relation", []) if isinstance(r, dict) and r.get("id")]

        # 6-1. Created Time / Last Edited Time
        elif ptype in ("created_time", "last_edited_time"):
            return prop.get(ptype)

        # 7. Rollup (상장주식DB 전체 등 상위 DB 롤업 속성 완벽 지원)
        elif ptype == "rollup":
            rollup_obj = prop.get("rollup", {})
            r_type = rollup_obj.get("type")
            if r_type == "number":
                return rollup_obj.get("number")
            elif r_type == "date":
                d_val = rollup_obj.get("date")
                return d_val.get("start") if isinstance(d_val, dict) else d_val
            elif r_type == "array":
                arr = rollup_obj.get("array", [])
                extracted = []
                for item in arr:
                    if not isinstance(item, dict):
                        continue
                    itype = item.get("type")
                    if itype in ("title", "rich_text"):
                        txt = "".join([t.get("plain_text", "") for t in item.get(itype, []) if isinstance(t, dict)]).strip()
                        if txt:
                            extracted.append(txt)
                    elif itype == "select":
                        s_name = item.get("select", {}).get("name")
                        if s_name:
                            extracted.append(s_name)
                    elif itype == "multi_select":
                        extracted.extend([m.get("name") for m in item.get("multi_select", []) if isinstance(m, dict) and m.get("name")])
                    elif itype == "number":
                        num_val = item.get("number")
                        if num_val is not None:
                            extracted.append(num_val)
                extracted = [x for x in extracted if x is not None and str(x).strip() != ""]
                if len(extracted) == 1:
                    return extracted[0]
                return extracted if extracted else None
            
    return None


def split_text_chunks(text: str, max_length: int = 1800) -> List[str]:
    """노션의 단일 rich_text 블록 제한(2,000자)을 방어하기 위해 텍스트를 안전한 크기로 분할합니다."""
    if not text:
        return [""]
    if len(text) <= max_length:
        return [text]
    
    chunks = []
    current = 0
    while current < len(text):
        chunks.append(text[current : current + max_length])
        current += max_length
    return chunks


def _build_rich_text_array(text: str, max_length: int = 1800) -> List[Dict[str, Any]]:
    """
    마크다운 인라인 서식(**볼드**, *이탤릭*, `코드`, [링크](url))을 완벽히 파싱하여
    Notion Rich Text 객체 배열(annotations)로 변환합니다.
    HTML 줄바꿈 태그(<br>, <br/>)를 개행(\\n)으로 자동 치환합니다.
    """
    if not text:
        return []

    # HTML 태그 및 개행 태그 정제
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = text.replace('&nbsp;', ' ')
    text = re.sub(r'</?(?:span|div|p|b|i|strong|em|font)[^>]*>', '', text, flags=re.IGNORECASE)

    pattern = re.compile(
        r'(\*\*(?:[^\*]+)\*\*|'        # **bold**
        r'`(?:[^`]+)`|'                # `code`
        r'\[(?:[^\]]+)\]\((?:[^)]+)\)|'# [link](url)
        r'\*(?:[^\*]+)\*)'             # *italic*
    )

    tokens = pattern.split(text)
    rich_text_elements: List[Dict[str, Any]] = []

    for t in tokens:
        if not t:
            continue

        # 1. Bold (**text**)
        if t.startswith("**") and t.endswith("**") and len(t) >= 4:
            content = t[2:-2][:max_length]
            if content:
                rich_text_elements.append({
                    "type": "text",
                    "text": {"content": content},
                    "annotations": {"bold": True}
                })
        # 2. Inline Code (`code`)
        elif t.startswith("`") and t.endswith("`") and len(t) >= 2:
            content = t[1:-1][:max_length]
            if content:
                rich_text_elements.append({
                    "type": "text",
                    "text": {"content": content},
                    "annotations": {"code": True}
                })
        # 3. Link ([text](url))
        elif t.startswith("[") and "](" in t and t.endswith(")"):
            m = re.match(r"^\[([^\]]+)\]\(([^)]+)\)$", t)
            if m:
                link_text, link_url = m.group(1)[:max_length], m.group(2)
                rich_text_elements.append({
                    "type": "text",
                    "text": {"content": link_text, "link": {"url": link_url}}
                })
            else:
                rich_text_elements.append({"type": "text", "text": {"content": t[:max_length]}})
        # 4. Italic (*text*)
        elif t.startswith("*") and t.endswith("*") and len(t) >= 2:
            content = t[1:-1][:max_length]
            if content:
                rich_text_elements.append({
                    "type": "text",
                    "text": {"content": content},
                    "annotations": {"italic": True}
                })
        # 5. Plain Text
        else:
            for chunk in split_text_chunks(t, max_length=max_length):
                if chunk:
                    rich_text_elements.append({
                        "type": "text",
                        "text": {"content": chunk}
                    })

    return rich_text_elements or [{"type": "text", "text": {"content": ""}}]



def markdown_to_notion_blocks(markdown_text: str) -> List[Dict[str, Any]]:
    """
    마크다운 텍스트를 Notion API가 수용 가능한 블록 객체 리스트로 변환합니다.
    제목(H1~H3), 계층형 들여쓰기 불릿/번호 리스트(Nested Children), 인용구(Quote),
    구분선(Divider), 네이티브 표(Table), 코드블록, 일반 문단을 지원합니다.
    """
    blocks: List[Dict[str, Any]] = []
    lines = markdown_text.splitlines()
    i = 0
    in_code_block = False
    code_lang = "plain text"
    code_lines: List[str] = []

    # 계층형 목록 구조(들여쓰기) 추적 스택: [(indent_level, children_list)]
    list_stack: List[Tuple[int, List[Dict[str, Any]]]] = [(-1, blocks)]

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # 1. 코드 블록 처리
        if stripped.startswith("```"):
            list_stack = [(-1, blocks)]
            if not in_code_block:
                in_code_block = True
                lang_tag = stripped[3:].strip().lower()
                code_lang = lang_tag if lang_tag else "plain text"
                code_lines = []
            else:
                in_code_block = False
                code_content = "\n".join(code_lines)
                for chunk in split_text_chunks(code_content, max_length=1800):
                    blocks.append({
                        "object": "block",
                        "type": "code",
                        "code": {
                            "rich_text": [{"type": "text", "text": {"content": chunk}}],
                            "language": code_lang if code_lang in [
                                "python", "javascript", "json", "markdown", "html", "css", "sql", "shell", "bash", "plain text"
                            ] else "plain text"
                        }
                    })
                code_lines = []
            i += 1
            continue

        if in_code_block:
            code_lines.append(line)
            i += 1
            continue

        # 2. 빈 줄 건너뛰기
        if not stripped:
            i += 1
            continue

        # 3. 구분선 (Divider)
        if stripped in ("---", "***", "___", "- - -", "* * *"):
            list_stack = [(-1, blocks)]
            blocks.append({"object": "block", "type": "divider", "divider": {}})
            i += 1
            continue

        # 4. 마크다운 표 감지 (| ... |) -> Notion 네이티브 Table 블록으로 완벽 변환
        if stripped.startswith("|") and stripped.endswith("|"):
            list_stack = [(-1, blocks)]
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith("|") and lines[i].strip().endswith("|"):
                table_lines.append(lines[i].strip())
                i += 1

            # 표 파싱 및 네이티브 블록 생성
            rows_data: List[List[str]] = []
            for t_line in table_lines:
                s_t = t_line.strip()
                if not s_t.startswith("|") or not s_t.endswith("|"):
                    continue
                # 구분선 (|---|---|) 건너뛰기
                cleaned_cells = [c.strip() for c in s_t.split("|")[1:-1]]
                if all(re.match(r"^:?-+:?$", c) for c in cleaned_cells if c):
                    continue
                rows_data.append(cleaned_cells)

            if rows_data:
                max_width = max(len(r) for r in rows_data)
                if max_width >= 1:
                    table_rows = []
                    for r in rows_data:
                        padded = r + [""] * (max_width - len(r))
                        cells_rich = []
                        for cell_text in padded:
                            cell_rt = _build_rich_text_array(cell_text)
                            cells_rich.append(cell_rt if cell_rt else [{"type": "text", "text": {"content": ""}}])
                        table_rows.append({
                            "type": "table_row",
                            "table_row": {"cells": cells_rich}
                        })

                    blocks.append({
                        "object": "block",
                        "type": "table",
                        "table": {
                            "table_width": max_width,
                            "has_column_header": True,
                            "has_row_header": False,
                            "children": table_rows
                        }
                    })
                    continue

            # 파싱 실패 시 fallback (code/markdown 블록)
            table_content = "\n".join(table_lines)
            for chunk in split_text_chunks(table_content, max_length=1800):
                blocks.append({
                    "object": "block",
                    "type": "code",
                    "code": {
                        "rich_text": [{"type": "text", "text": {"content": chunk}}],
                        "language": "markdown"
                    }
                })
            continue

        # 5. 제목 (H1 ~ H3)
        if stripped.startswith("# "):
            list_stack = [(-1, blocks)]
            blocks.append({
                "object": "block",
                "type": "heading_1",
                "heading_1": {"rich_text": _build_rich_text_array(stripped[2:].strip())}
            })
            i += 1
            continue
        elif stripped.startswith("## "):
            list_stack = [(-1, blocks)]
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": _build_rich_text_array(stripped[3:].strip())}
            })
            i += 1
            continue
        elif stripped.startswith("### "):
            list_stack = [(-1, blocks)]
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": _build_rich_text_array(stripped[4:].strip())}
            })
            i += 1
            continue
        elif stripped.startswith("#### "):
            list_stack = [(-1, blocks)]
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": _build_rich_text_array(stripped[5:].strip())}
            })
            i += 1
            continue

        # 들여쓰기 크기 계산 (공백)
        indent = len(line) - len(line.lstrip(" "))

        # 6. 불릿 리스트 (- or *)
        if stripped.startswith("- ") or stripped.startswith("* "):
            content = stripped[2:].strip()
            block = {
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": _build_rich_text_array(content),
                    "children": []
                }
            }
            while len(list_stack) > 1 and list_stack[-1][0] >= indent:
                list_stack.pop()
            list_stack[-1][1].append(block)
            list_stack.append((indent, block["bulleted_list_item"]["children"]))
            i += 1
            continue

        # 7. 번호 리스트 (1. , 2. ...)
        elif re.match(r"^\d+\.\s+", stripped):
            match = re.match(r"^\d+\.\s+", stripped)
            content = stripped[match.end():].strip() if match else stripped
            block = {
                "object": "block",
                "type": "numbered_list_item",
                "numbered_list_item": {
                    "rich_text": _build_rich_text_array(content),
                    "children": []
                }
            }
            while len(list_stack) > 1 and list_stack[-1][0] >= indent:
                list_stack.pop()
            list_stack[-1][1].append(block)
            list_stack.append((indent, block["numbered_list_item"]["children"]))
            i += 1
            continue

        # 8. 인용구 (Quote > )
        elif stripped.startswith("> "):
            list_stack = [(-1, blocks)]
            content = stripped[2:].strip()
            blocks.append({
                "object": "block",
                "type": "quote",
                "quote": {"rich_text": _build_rich_text_array(content)}
            })
            i += 1
            continue

        # 9. 일반 문단 (Paragraph)
        else:
            list_stack = [(-1, blocks)]
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": _build_rich_text_array(stripped)}
            })
            i += 1

    # 후처리: 빈 children 배열 제거 및 재귀 정리
    def _clean_empty_children(block_list: List[Dict[str, Any]]) -> None:
        for b in block_list:
            btype = b.get("type")
            if btype in ("bulleted_list_item", "numbered_list_item"):
                ch = b.get(btype, {}).get("children", [])
                if not ch:
                    b[btype].pop("children", None)
                else:
                    _clean_empty_children(ch)

    _clean_empty_children(blocks)
    return blocks



def safe_append_blocks(
    client: Any,
    block_id: str,
    children: List[Dict[str, Any]],
    batch_size: int = 80,
    max_retries: int = 3,
    retry_delay: float = 2.0
) -> bool:
    """노션 페이지/블록에 자식 블록들을 80~100개 단위로 안전하게 나누어 추가합니다."""
    if not children:
        return True
    
    total = len(children)
    for i in range(0, total, batch_size):
        chunk = children[i : i + batch_size]
        attempt = 1
        success = False
        while attempt <= max_retries:
            try:
                client.blocks.children.append(block_id=block_id, children=chunk)
                success = True
                break
            except HTTPResponseError as error:
                status = getattr(error, "status", None)
                if status in RETRY_STATUS_CODES and attempt < max_retries:
                    print(f"   ⚠️ Blocks append retry {attempt}/{max_retries} - status={status}: {error}")
                    time.sleep(retry_delay * attempt)
                    attempt += 1
                    continue
                print(f"   ❌ Blocks append failed: {_format_notion_error(error)}")
                break
            except Exception as error:
                if attempt < max_retries:
                    print(f"   ⚠️ Blocks append retry {attempt}/{max_retries}: {error}")
                    time.sleep(retry_delay * attempt)
                    attempt += 1
                    continue
                print(f"   ❌ Blocks append failed: {error}")
                break
        if not success:
            return False
        time.sleep(0.3)
    return True


def safe_create_page(
    client: Any,
    database_id: str,
    properties: Dict[str, Any],
    children: Optional[List[Dict[str, Any]]] = None,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> Optional[Dict[str, Any]]:
    """
    재시도 로직과 100개 초과 블록 청크 분할을 지원하는 안전한 노션 페이지 생성 함수.
    생성된 페이지 객체를 반환합니다.
    """
    children_to_send = children or []
    initial_children = children_to_send[:80]
    remaining_children = children_to_send[80:]

    attempt = 1
    page: Optional[Dict[str, Any]] = None
    
    while attempt <= max_retries:
        try:
            payload: Dict[str, Any] = {
                "parent": {"database_id": database_id},
                "properties": properties,
            }
            if initial_children:
                payload["children"] = initial_children
                
            page = cast(Dict[str, Any], client.pages.create(**payload))
            break
        except HTTPResponseError as error:
            status = getattr(error, "status", None)
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                print(f"   ⚠️ Notion page create retry {attempt}/{max_retries} - status={status}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            print(f"   ❌ Notion page create failed: {_format_notion_error(error)}")
            return None
        except Exception as error:
            if attempt < max_retries:
                print(f"   ⚠️ Notion page create retry {attempt}/{max_retries}: {error}")
                time.sleep(retry_delay * attempt)
                attempt += 1
                continue
            print(f"   ❌ Notion page create failed: {error}")
            return None

    if page and remaining_children:
        page_id = page.get("id")
        if page_id:
            ok = safe_append_blocks(client, page_id, remaining_children)
            if not ok:
                print("   ⚠️ 나머지 블록 추가 중 일부 오류가 발생했습니다.")

    return page


# ==============================================================================
# 9. 공통 비즈니스 로직 및 퀀트/마스터 도메인 유틸리티 (SSOT Hub)
# ==============================================================================

def resolve_stock_taxonomy(
    ticker: str,
    name: str = "",
    market_hint: str = "",
    is_etf: Optional[bool] = None,
    country_hint: str = ""
) -> Dict[str, str]:
    """
    티커, 종목명, 마켓/국가 힌트를 종합 분석하여 4대 표준 분류 메타데이터를 원스톱으로 산출합니다.
    Returns:
        {
            "market": "KOSPI" | "KOSDAQ" | "ETF(KR)" | "NASDAQ" | "NYSE" | "AMEX" | "ETF(US)" | "TSE" | "GLOBAL" | "COMEX",
            "country": "한국" | "미국" | "일본" | "유럽" | "글로벌",
            "product_type": "지수추종패시" | "개별기업주식" | "섹터테마알파" | "배당인컴상품" | "채권금리상품" | "실물현금자산",
            "asset_class": "글로벌성장주" | "한미배당성장" | "국내주식밸류" | "미국장기국채" | "국내단기채권" | "골드실물자산" | "원자재와달러"
        }
    """
    t = (ticker or "").strip().upper()
    n = (name or "").strip()
    m_hint = (market_hint or "").strip().upper()
    c_hint = (country_hint or "").strip()

    # 1. 선물 / 원자재
    if m_hint == "COMEX" or t in ("GC", "CL", "NG", "HG", "SI"):
        is_gold = t in ("GC", "SI") or any(k in n for k in ["금", "골드", "Gold"])
        return {
            "market": "COMEX",
            "country": "글로벌",
            "product_type": "실물현금자산",
            "asset_class": "골드실물자산" if is_gold else "원자재와달러"
        }

    # 2. 일본 주식 (거래소 TSE, 접미사 .T 또는 국가힌트 Japan/일본)
    if t.endswith(".T") or m_hint == "TSE" or c_hint in ("일본", "Japan"):
        return {
            "market": "TSE",
            "country": "일본",
            "product_type": "개별기업주식",
            "asset_class": "글로벌성장주"
        }

    # 3. 유럽 주식 및 ADR (국가힌트 유럽 국가들 또는 유럽 거래소 접미사)
    european_countries = {
        "유럽", "독일", "프랑스", "스위스", "영국", "네덜란드", "스웨덴", "이탈리아", "스페인", "아일랜드",
        "Germany", "France", "Switzerland", "United Kingdom", "Netherlands", "Sweden", "Italy", "Spain", "Ireland"
    }
    if c_hint in european_countries or t.endswith((".DE", ".PA", ".SW", ".AS", ".L")):
        return {
            "market": "GLOBAL",
            "country": "유럽",
            "product_type": "개별기업주식",
            "asset_class": "글로벌성장주"
        }

    # 4. 아시아 / 기타 글로벌 주식 (대만, 홍콩, 중국, 베트남 등)
    global_countries = {
        "대만", "홍콩", "중국", "베트남", "싱가포르", "인도", "브라질",
        "Taiwan", "Hong Kong", "China", "Vietnam", "Singapore", "India", "Brazil", "글로벌"
    }
    if c_hint in global_countries or t.endswith((".TW", ".HK", ".SS", ".SZ")) or m_hint in ("HKEX", "SSE", "SZSE", "HOSE", "HNX", "TWSE", "GLOBAL"):
        return {
            "market": "GLOBAL",
            "country": "글로벌",
            "product_type": "개별기업주식",
            "asset_class": "글로벌성장주"
        }

    # 4. 한국 상장 종목 (KRX)
    is_kr = is_kr_ticker(t) or m_hint in ("KOSPI", "KOSDAQ", "ETF(KR)", "KRX")
    if is_kr:
        # ETF 여부 판별
        kr_etf_prefixes = ["KODEX", "TIGER", "ACE", "SOL", "PLUS", "KBSTAR", "TIMEFOLIO", "KOACT", "1Q", "ARIRANG", "HANARO", "RISE", "WOORI"]
        detected_etf = is_etf if is_etf is not None else (m_hint == "ETF(KR)" or any(n.upper().startswith(p) for p in kr_etf_prefixes) or "ETF" in n.upper())

        if not detected_etf:
            m_label = "KOSDAQ" if "KOSDAQ" in m_hint or "KSQ" in m_hint else "KOSPI"
            return {
                "market": m_label,
                "country": "한국",
                "product_type": "개별기업주식",
                "asset_class": "국내주식밸류"
            }

        # 4-1. 한국 ETF: 실물/금/원자재
        if any(k in n for k in ["KRX금", "금현물", "골드선물", "금선물"]):
            return {"market": "ETF(KR)", "country": "한국", "product_type": "실물현금자산", "asset_class": "골드실물자산"}
        if any(k in n for k in ["원자재", "구리", "원유", "WTI", "달러", "USD"]):
            return {"market": "ETF(KR)", "country": "글로벌", "product_type": "실물현금자산", "asset_class": "원자재와달러"}

        # 4-2. 한국 ETF: 채권/금리
        if any(k in n for k in ["CD금리", "KOFR", "단기채", "153130", "단기자금", "머니마켓"]):
            return {"market": "ETF(KR)", "country": "한국", "product_type": "채권금리상품", "asset_class": "국내단기채권"}
        if any(k in n for k in ["미국채", "30년국채", "장기국채", "TLT", "미국30년", "국채10년"]):
            return {"market": "ETF(KR)", "country": "미국", "product_type": "채권금리상품", "asset_class": "미국장기국채"}
        if any(k in n for k in ["국고채", "채권"]):
            return {"market": "ETF(KR)", "country": "한국", "product_type": "채권금리상품", "asset_class": "국내단기채권"}

        # 4-3. 한국 ETF: 배당 인컴
        if any(k in n for k in ["미국배당다우존스", "SCHD", "배당다우존스", "고배당"]):
            c = "미국" if any(k in n for k in ["미국", "글로벌"]) else "한국"
            return {"market": "ETF(KR)", "country": c, "product_type": "배당인컴상품", "asset_class": "한미배당성장"}

        # 4-4. 한국 ETF: 지수추종 패시브
        if any(k in n for k in ["S&P500", "S&P 500", "나스닥100", "NASDAQ100"]):
            return {"market": "ETF(KR)", "country": "미국", "product_type": "지수추종패시", "asset_class": "글로벌성장주"}
        if any(k in n for k in ["KODEX 200", "TIGER 200", "KBSTAR 200", "코스피200", "코스닥150", "KODEX 코스닥150"]):
            return {"market": "ETF(KR)", "country": "한국", "product_type": "지수추종패시", "asset_class": "국내주식밸류"}

        # 4-5. 한국 ETF: 해외 테마 / 빅테크 밸류체인
        if any(k in n for k in ["미국", "글로벌", "빅테크", "우주항공", "AI광통신", "HBM", "엔비디아", "구글", "마이크로소프트", "밸류체인"]):
            c = "미국" if not any(k in n for k in ["글로벌", "우주"]) else "글로벌"
            return {"market": "ETF(KR)", "country": c, "product_type": "섹터테마알파", "asset_class": "글로벌성장주"}

        # 4-6. 한국 ETF: 국내 테마
        return {"market": "ETF(KR)", "country": "한국", "product_type": "섹터테마알파", "asset_class": "국내주식밸류"}

    # 5. 미국 상장 ETF (ETF(US))
    if m_hint == "ETF(US)" or (is_etf is True and not is_kr):
        if any(k in t for k in ["GLD", "IAU", "SGOL", "BAR"]):
            return {"market": "ETF(US)", "country": "미국", "product_type": "실물현금자산", "asset_class": "골드실물자산"}
        if any(k in t for k in ["DBC", "GSG", "USO", "BNO", "CPER"]):
            return {"market": "ETF(US)", "country": "미국", "product_type": "실물현금자산", "asset_class": "원자재와달러"}
        if any(k in t for k in ["TLT", "TMF", "SPTLL", "TLH", "EDV", "ZROZ"]):
            return {"market": "ETF(US)", "country": "미국", "product_type": "채권금리상품", "asset_class": "미국장기국채"}
        if any(k in t for k in ["SCHD", "DGRO", "VIG", "NOBL", "DVY"]):
            return {"market": "ETF(US)", "country": "미국", "product_type": "배당인컴상품", "asset_class": "한미배당성장"}
        if any(k in t for k in ["SPY", "VOO", "IVV", "SPLG", "QQQ", "QQQM", "DIA", "VTI"]):
            return {"market": "ETF(US)", "country": "미국", "product_type": "지수추종패시", "asset_class": "글로벌성장주"}
        return {"market": "ETF(US)", "country": "미국", "product_type": "섹터테마알파", "asset_class": "글로벌성장주"}

    # 6. 미국 정규 상장 개별주 (NASDAQ, NYSE, AMEX)
    m_us = m_hint if m_hint in ("NASDAQ", "NYSE", "AMEX") else "NASDAQ"
    return {
        "market": m_us,
        "country": "미국",
        "product_type": "개별기업주식",
        "asset_class": "글로벌성장주"
    }


def load_benchmark_config(client: Any, benchmark_db_id: str, logger: Optional[Any] = None) -> Dict[str, Any]:
    """지표지수 DB를 로컬 SQLite(0.001s) 또는 노션 API를 통해 동적으로 구성하여 반환합니다."""
    # 1. 로컬 SQLite DB 우선 로드 (0.001s)
    try:
        from local_db_manager import load_benchmark_config_from_sqlite
        sqlite_cfg = load_benchmark_config_from_sqlite()
        if sqlite_cfg and sqlite_cfg.get("benchmarks"):
            if logger:
                logger.info(f"⚡ [로컬 SQLite 0.001s 로드] 지표 벤치마크 {len(sqlite_cfg['benchmarks'])}개 즉시 활성화")
            return sqlite_cfg
    except Exception:
        pass

    if logger:
        logger.info("🔍 지표지수 DB 동적 분석 및 매칭키워드 로드 시작...")
    config: Dict[str, Any] = {"ticker_to_id": {}, "benchmarks": []}
    try:
        for page in paginate_database(client, benchmark_db_id, page_size=100, retry_delay=0.2):
            props = page.get("properties", {})
            ticker = get_page_text(props, ["티커", "Ticker", "이름", "Name"]).upper()
            if not ticker:
                continue
            summary = get_page_text(props, ["요약명"]).strip()
            cat = props.get("구분", {}).get("select", {}).get("name", "") if props.get("구분", {}).get("select") else ""
            country = props.get("국가", {}).get("select", {}).get("name", "") if props.get("국가", {}).get("select") else ""
            kw_raw = get_page_text(props, ["매칭키워드", "키워드"])
            keywords = parse_keywords(kw_raw, fallback_summary=summary)

            config["ticker_to_id"][ticker] = page["id"]
            config["benchmarks"].append({
                "ticker": ticker,
                "summary": summary,
                "category": cat,
                "country": country,
                "keywords": keywords,
                "id": page["id"]
            })

        if logger:
            logger.info(f"✅ 지표 로드 완료 (총 {len(config['benchmarks'])}개 지표 및 키워드 활성화)")
    except Exception as e:
        if logger:
            logger.error(f"❌ 지표 DB 로드 실패: {e}")
    return config


def build_master_update_payload(
    page_id: str,
    page_props: Dict[str, Any],
    ticker: str,
    meta: Dict[str, Any],
    bm_config: Dict[str, Any]
) -> Tuple[str, Dict[str, Any], str, str]:
    """
    사전 DB 및 API 해결사에서 정규화된 종목 메타데이터(meta)를 바탕으로
    노션 마스터 DB의 모든 표준 속성(종목명, Market, 국가, 상품유형, 자산군, 섹터/업종, 우량주, 시장BM, 산업BM, 업데이트일자)
    을 안전하고 완벽하게 구성하여 (page_id, update_props, ticker, name) 튜플을 반환합니다.
    """
    name = meta.get("name") or ticker
    sector_str = meta.get("sector_industry") or ""
    market = meta.get("market") or "KOSPI"
    country = meta.get("country") or "한국"
    product_type = meta.get("product_type") or "개별기업주식"
    asset_class = meta.get("asset_class") or "국내주식밸류"
    blue_chips = list(meta.get("blue_chips") or [])

    # 벤치마크 결정 (사전 지정 BM 우선 ➔ 없을 시 알고리즘 매칭)
    market_bm_ticker = meta.get("market_bm")
    ind_bm_ticker = meta.get("ind_bm")

    # BM 명칭이 한글/영문 텍스트인 경우 티커로 정규화
    bm_name_to_ticker = {
        "KODEX 200": "069500",
        "KODEX 코스닥150": "229200",
        "KODEX 200TR": "226490",
        "KRX 300": "292190",
        "S&P 500": "SPY",
        "S&P500": "SPY",
        "나스닥 100": "QQQ",
        "나스닥100": "QQQ",
        "다우 30": "DIA",
        "다우30": "DIA",
    }
    if market_bm_ticker in bm_name_to_ticker:
        market_bm_ticker = bm_name_to_ticker[market_bm_ticker]
    if ind_bm_ticker in bm_name_to_ticker:
        ind_bm_ticker = bm_name_to_ticker[ind_bm_ticker]

    # 시장BM 자동 결정
    if not market_bm_ticker:
        is_etf_flag = product_type in ("지수추종패시", "섹터테마알파", "배당인컴상품", "채권금리상품", "실물현금자산")
        if is_etf_flag:
            m_bms = [b for b in bm_config.get("benchmarks", []) if b["category"] == "시장"]
            market_bm_ticker = find_best_bm(name.upper(), m_bms) or ("069500" if is_kr_ticker(ticker) else "SPY")
        else:
            if "코스피 200" in blue_chips:
                market_bm_ticker = "069500"
            elif "코스닥 150" in blue_chips:
                market_bm_ticker = "229200"
            elif "나스닥 100" in blue_chips:
                market_bm_ticker = "QQQ"
            elif "S&P 500" in blue_chips:
                market_bm_ticker = "SPY"
            elif "다우 30" in blue_chips:
                market_bm_ticker = "DIA"
            elif market == "KOSDAQ":
                market_bm_ticker = "229200"
            elif market == "NASDAQ":
                market_bm_ticker = "QQQ"
            elif market in ("KOSPI", "ETF(KR)"):
                market_bm_ticker = "069500"
            else:
                market_bm_ticker = "SPY"

    # 산업BM 자동 결정
    if not ind_bm_ticker:
        bm_country = "KR" if is_kr_ticker(ticker) else "US"
        ind_bms = [b for b in bm_config.get("benchmarks", []) if b["category"] == "산업" and b["country"] == bm_country]
        text_corpus = f"{ticker} {name} {sector_str}".upper()
        ind_bm_ticker = find_best_bm(text_corpus, ind_bms)

    ticker_to_id = bm_config.get("ticker_to_id", {})
    ind_bm_prop = "K산업BM" if is_kr_ticker(ticker) else "G산업BM"
    target_ind_id = ticker_to_id.get(ind_bm_ticker) if (ind_bm_ticker and ind_bm_ticker != ticker) else None
    target_mbm_id = ticker_to_id.get(market_bm_ticker) if (market_bm_ticker and market_bm_ticker != ticker) else None

    # -------------------------------------------------------------
    # Smart Dirty Checking: 기존 노션 속성과 완전히 동일하면 불필요한 I/O 차단
    # -------------------------------------------------------------
    is_dirty = False
    if get_page_text(page_props, ["종목명"]).strip() != name.strip():
        is_dirty = True
    elif get_page_text(page_props, ["섹터/업종"]).strip() != sector_str.strip():
        is_dirty = True
    elif (extract_prop_raw_value(page_props.get("Market")) or "").strip() != market.strip():
        is_dirty = True
    elif (extract_prop_raw_value(page_props.get("국가")) or "").strip() != country.strip():
        is_dirty = True
    elif (extract_prop_raw_value(page_props.get("상품유형")) or "").strip() != product_type.strip():
        is_dirty = True
    elif (extract_prop_raw_value(page_props.get("자산군")) or "").strip() != asset_class.strip():
        is_dirty = True
    else:
        existing_chips = set([x.get("name", "") for x in page_props.get("우량주", {}).get("multi_select", []) if x.get("name")])
        if existing_chips != set(blue_chips):
            is_dirty = True

    if not is_dirty:
        existing_mbm = [x.get("id") for x in page_props.get("시장BM", {}).get("relation", []) if x.get("id")]
        target_mbm = [target_mbm_id] if target_mbm_id else []
        if existing_mbm != target_mbm:
            is_dirty = True

    if not is_dirty:
        existing_indbm = [x.get("id") for x in page_props.get(ind_bm_prop, {}).get("relation", []) if x.get("id")]
        target_indbm = [target_ind_id] if target_ind_id else []
        if existing_indbm != target_indbm:
            is_dirty = True

    if not is_dirty:
        return None

    update_props: Dict[str, Any] = {
        "종목명": make_rich_text(name),
        "Market": {"select": {"name": market}},
        "국가": {"select": {"name": country}},
        "상품유형": {"select": {"name": product_type}},
        "자산군": {"select": {"name": asset_class}},
        "섹터/업종": make_rich_text(sector_str),
    }
    set_page_date_property(update_props, page_props)

    if blue_chips:
        update_props["우량주"] = {"multi_select": [{"name": tag} for tag in blue_chips]}
    else:
        update_props["우량주"] = {"multi_select": []}

    if target_mbm_id:
        update_props["시장BM"] = {"relation": [{"id": target_mbm_id}]}
    else:
        update_props["시장BM"] = {"relation": []}

    if target_ind_id:
        update_props[ind_bm_prop] = {"relation": [{"id": target_ind_id}]}
    else:
        update_props[ind_bm_prop] = {"relation": []}

    return page_id, update_props, ticker, name


def batch_update_pages(
    client: Any,
    update_payloads: List[Tuple[str, Dict[str, Any], str, str]],
    max_workers: int = 3,
    delay: float = 0.05,
    logger: Optional[Any] = None
) -> Tuple[int, int]:
    """
    [(page_id, properties, ticker, name), ...] 형태의 페이로드를 받아 멀티스레드로 노션 DB에 안전하게 일괄 전송합니다.
    Returns:
        (success_count, fail_count)
    """
    total_cnt = len(update_payloads)
    if total_cnt == 0:
        return 0, 0

    if logger:
        logger.info(f"📝 총 {total_cnt}개 종목 노션 DB 반영 시작 (병렬 워커: {max_workers})...")

    success_cnt = 0
    fail_cnt = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(safe_page_update, client, pid, props): (ticker, name)
            for pid, props, ticker, name in update_payloads
        }

        for idx, future in enumerate(as_completed(futures), 1):
            ticker, name = futures[future]
            try:
                ok = future.result()
                if ok:
                    success_cnt += 1
                    if logger and (idx % 25 == 0 or idx == total_cnt):
                        logger.info(f"   ✅ [{idx}/{total_cnt}] [Batch Sync] {ticker} ({name}) 성공")
                else:
                    fail_cnt += 1
                    if logger:
                        logger.warning(f"   ❌ [{idx}/{total_cnt}] [Batch Sync] {ticker} ({name}) 실패")
            except Exception as exc:
                fail_cnt += 1
                if logger:
                    logger.error(f"   ❌ [{ticker}] 트랜잭션 에러: {exc}")

            if delay > 0:
                time.sleep(delay)

    if logger:
        logger.info(f"✨ 노션 배치 업데이트 완료: 성공 {success_cnt}건 / 실패 {fail_cnt}건 (총 {total_cnt}건)")

    return success_cnt, fail_cnt


def calc_margin_of_safety(current_price: float, target_price: float) -> str:
    """목표주가 대비 현재가 괴리율 기반 안전마진 라벨을 산출합니다."""
    if not target_price or target_price <= 0 or not current_price or current_price <= 0:
        return ""
    upside = current_price / target_price
    pct_txt = f"{upside * 100:.1f}%"
    if upside <= 0.6:
        return f"🚀 {pct_txt}"
    elif upside <= 0.8:
        return f"✅ {pct_txt}"
    elif upside < 1.0:
        return f"⚠️ {pct_txt}"
    else:
        return f"🚨 {pct_txt}"


def calc_52w_position(current_price: float, high_52w: float, low_52w: float) -> Optional[float]:
    """52주 최고가/최저가 대비 현재가 위치(0.0~1.0)를 산출합니다."""
    if not current_price or current_price <= 0:
        return None
    if not high_52w or not low_52w or high_52w <= low_52w:
        return None
    pos = (current_price - low_52w) / (high_52w - low_52w)
    return round(pos, 4)


def calculate_quant_indicators(
    df_chart: Any,
    current_price: Optional[float] = None,
    is_kr: bool = False,
    high_52w_override: Optional[float] = None
) -> Dict[str, Any]:
    """
    GEMINI.md 5대 퀀트 엔진 수식에 따라 200일선, 수급선, 추세, 12M 모멘텀,
    모멘텀 진단, 52주 낙폭, 60일 변동성, 위험도 등급, 스마트 가이드, 스윙 고저점을 산출합니다.
    """
    res: Dict[str, Any] = {
        "ma200": None,
        "ma_supply": None,
        "trend": None,
        "mom_12m": None,
        "mom_diag": None,
        "drawdown_52w": None,
        "vol_60d": None,
        "risk_grade": None,
        "smart_guide": None,
        "swing_high": None,
        "swing_low": None,
    }

    if df_chart is None or getattr(df_chart, "empty", True):
        return res

    try:
        # 종가 시리즈 추출
        c = df_chart["Close"].dropna() if "Close" in df_chart.columns else df_chart.iloc[:, 0].dropna()
        if c.empty:
            return res

        curr_p = float(current_price) if current_price and current_price > 0 else float(c.iloc[-1])
        supply_window = 60 if is_kr else 50
        ma_sup = float(c.rolling(supply_window).mean().iloc[-1]) if len(c) >= supply_window else float(c.mean())
        ma_200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else float(c.mean())

        res["ma200"] = round(ma_200, 2 if not is_kr else 0)
        res["ma_supply"] = round(ma_sup, 2 if not is_kr else 0)

        # 1. 추세 판정 (수급선 + 200일선)
        if is_kr:
            if curr_p >= ma_sup and curr_p >= ma_200:
                res["trend"] = "▲ 수급유입"
            elif curr_p >= ma_200:
                res["trend"] = "━ 박스권세"
            else:
                res["trend"] = "▼ 하락추세"
        else:
            if curr_p >= ma_sup and curr_p >= ma_200:
                res["trend"] = "▲ 기관주도"
            elif curr_p >= ma_200:
                res["trend"] = "━ 눌림조정"
            else:
                res["trend"] = "▼ 하락추세"

        # 2. 12M 모멘텀 및 5단계 진단
        start_p = float(c.iloc[0])
        mom_12m = ((curr_p - start_p) / start_p) if start_p > 0 else 0.0
        res["mom_12m"] = round(mom_12m, 4)

        if mom_12m >= 0.50:
            res["mom_diag"] = "▲ 주도대장"
        elif mom_12m >= 0.20:
            res["mom_diag"] = "▲ 실적지속"
        elif mom_12m >= 0.05:
            res["mom_diag"] = "▲ 시장동행"
        elif mom_12m >= -0.10:
            res["mom_diag"] = "━ 방향탐색"
        else:
            res["mom_diag"] = "▼ 자금이탈"

        # 3. 52주 최고가 대비 낙폭 (drawdown_52w)
        peak_52w = float(high_52w_override) if high_52w_override and high_52w_override > 0 else (
            float(df_chart["High"].tail(252).max()) if "High" in df_chart.columns else float(c.tail(252).max())
        )
        drawdown_52w = None
        if peak_52w > 0:
            drawdown_52w = (curr_p - peak_52w) / peak_52w
            res["drawdown_52w"] = round(drawdown_52w, 4)

        # 4. 60일 연환산 변동성 및 위험도 등급
        returns_60 = c.pct_change().tail(60).dropna()
        if len(returns_60) >= 5:
            vol_60d = float(returns_60.std() * math.sqrt(252))
            res["vol_60d"] = round(vol_60d, 4)

            if vol_60d < 0.20:
                res["risk_grade"] = "▲ 비중확대"
            elif vol_60d < 0.35:
                res["risk_grade"] = "━ 정상비중"
            elif vol_60d < 0.60:
                res["risk_grade"] = "▼ 비중조절"
            else:
                res["risk_grade"] = "▼ 소액접근"

        # 5. 스마트 가이드 (표준 6종)
        if curr_p >= ma_200:
            if drawdown_52w is not None and drawdown_52w <= -0.20:
                res["smart_guide"] = "▲ 분할매수"
            elif curr_p >= ma_sup and mom_12m >= 0.50:
                res["smart_guide"] = "▲ 추세탑승"
            elif curr_p < ma_sup:
                res["smart_guide"] = "━ 눌림지지"
            else:
                res["smart_guide"] = "▲ 상승유지"
        else:
            if drawdown_52w is not None and drawdown_52w <= -0.35:
                res["smart_guide"] = "▼ 바닥확인"
            else:
                res["smart_guide"] = "▼ 하락관망"

        # 6. 최근 20영업일 스윙 고점/저점
        if "High" in df_chart.columns and "Low" in df_chart.columns:
            recent_20 = df_chart.tail(20)
            res["swing_high"] = round(float(recent_20["High"].max()), 2 if not is_kr else 0)
            res["swing_low"] = round(float(recent_20["Low"].min()), 2 if not is_kr else 0)
        else:
            recent_20_c = c.tail(20)
            res["swing_high"] = round(float(recent_20_c.max()), 2 if not is_kr else 0)
            res["swing_low"] = round(float(recent_20_c.min()), 2 if not is_kr else 0)

    except Exception:
        pass

    return res


def ensure_database_properties(
    client: Any,
    database_id: str,
    properties_schema: Dict[str, Dict[str, Any]],
    logger: Optional[Any] = None
) -> None:
    """
    지정된 노션 데이터베이스에 필요한 속성(Property/열)이 존재하는지 확인하고,
    누락된 열이 있다면 Notion API(PATCH /v1/databases/{id})를 호출하여 자동으로 생성합니다.
    """
    clean_id = database_id.replace("-", "").strip()
    try:
        db = client.databases.retrieve(database_id=clean_id)
        existing_props = db.get("properties", {})
        
        # 최신 노션 API(Data Sources 구조)에서 DB retrieve의 properties가 비어있는 경우 샘플 페이지로 기존 속성 교차 확인
        if not existing_props:
            try:
                sample_query = safe_databases_query(client, clean_id, page_size=1)
                results = sample_query.get("results", [])
                if results and isinstance(results[0], dict):
                    existing_props = results[0].get("properties", {})
            except Exception:
                pass

        missing_props = {}
        for prop_name, prop_def in properties_schema.items():
            if prop_name not in existing_props:
                missing_props[prop_name] = prop_def

        if missing_props:
            if logger:
                logger.info(f"✨ [Schema Auto-Provisioning] 노션 DB에 누락된 {len(missing_props)}개 열 자동 생성 중: {list(missing_props.keys())}")
            else:
                print(f"✨ [Schema Auto-Provisioning] 노션 DB에 누락된 {len(missing_props)}개 열 자동 생성 중: {list(missing_props.keys())}")
            client.databases.update(database_id=clean_id, properties=missing_props)
            if logger:
                logger.info(f"   ✅ 열 자동 생성 완료: {list(missing_props.keys())}")
            else:
                print(f"   ✅ 열 자동 생성 완료: {list(missing_props.keys())}")
    except Exception as exc:
        msg = f"⚠️ 노션 DB 스키마 확인/생성 중 예외 발생: {exc}"
        if logger:
            logger.warning(msg)
        else:
            print(msg)


# ==============================================================================
# 로컬 SQLite DB (stock_master.db) 0.001초 초고속 데이터 로더
# ==============================================================================
def get_local_master_db_path() -> Optional[str]:
    """통합 로컬 SQLite DB (stock_master.db) 파일 경로를 탐색하여 반환합니다."""
    core_dir: Path = Path(__file__).resolve().parent
    project_root: Path = core_dir.parent
    workspace_root: Path = project_root.parent

    candidates: List[Path] = [
        project_root / "data" / "stock_master.db",
        workspace_root / "update_stock" / "data" / "stock_master.db",
        core_dir / "data" / "stock_master.db",
        Path("d:/Github IDE/update_stock/data/stock_master.db"),
    ]
    for p in candidates:
        if p.exists() and p.is_file():
            return str(p.resolve())
    return None


def load_local_finances_db() -> Dict[str, Dict[str, Any]]:
    """로컬 SQLite DB에서 전 종목의 최신 재무/퀀트 지표를 0.001초만에 로드합니다."""
    from core.local_db_manager import load_finances_from_sqlite
    return load_finances_from_sqlite()


def load_local_stocks_db() -> Dict[str, Dict[str, Any]]:
    """로컬 SQLite DB에서 상장주식 마스터 정보를 0.001초만에 로드합니다."""
    from core.local_db_manager import load_master_stocks_from_sqlite
    return load_master_stocks_from_sqlite()


def load_local_etf_holdings_db(etf_ticker: Optional[str] = None) -> List[Dict[str, Any]]:
    """로컬 SQLite DB에서 ETF 구성종목 정보를 0.001초만에 로드합니다."""
    from core.local_db_manager import load_etf_holdings_from_sqlite
    return load_etf_holdings_from_sqlite(etf_ticker)




