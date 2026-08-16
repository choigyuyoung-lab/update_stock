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
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv

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

KIS_VTS_URL = "https://openapivts.koreainvestment.com:29443"
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
# 2. 환경 변수 및 시간 처리 유틸리티
# ==============================================================================
def get_env_var(name: str, required: bool = True, default: Optional[str] = None) -> str:
    """환경 변수를 안전하게 가져옵니다. 필수 변수 누락 시 오류를 발생시킵니다."""
    value: Optional[str] = os.environ.get(name, default)
    if required and not value:
        raise EnvironmentError(f"환경 변수 {name}이(가) 설정되지 않았습니다.")
    return cast(str, value)


def kst_isoformat() -> str:
    """전 세계 어느 가상 서버에서 실행되든 한국 표준시(KST, Asia/Seoul)를 기준으로 ISO 일시를 반환합니다."""
    return datetime.now(ZoneInfo("Asia/Seoul")).isoformat()


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

            us_pick, jp_pick, other_pick = None, None, None
            for item in quotes:
                sym = str(item.get("symbol") or "").strip().upper()
                typ = item.get("quoteType", "")
                exch = str(item.get("exchange") or item.get("exchDisp") or "").upper()
                sname = str(item.get("shortname") or item.get("longname") or "").strip()

                if typ not in ["EQUITY", "ETF"] or sym.endswith("-USD"):
                    continue

                # 1. 미국 메이저 거래소 상장주 및 ADR / OTC (점 없는 티커 최우선)
                if ("." not in sym) and any(m in exch for m in ["NY", "NASD", "NMS", "BATS", "NGM", "NCM", "PNK", "OTC"]):
                    if not us_pick:
                        us_pick = (sym, short_brand or sname)
                # 2. 일본 도쿄 증시 (.T)
                elif sym.endswith(".T") or "TOKYO" in exch or "JPX" in exch:
                    if not jp_pick:
                        jp_pick = (sym if sym.endswith(".T") else f"{sym}.T", short_brand or sname)
                # 3. 기타 해외 증시
                elif not other_pick:
                    other_pick = (sym, short_brand or sname)

            best = us_pick or jp_pick or other_pick
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
    start_cursor: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> Dict[str, Any]:
    """재시도(Retry) 로직이 포함된 안전한 노션 데이터베이스 쿼리 함수"""
    attempt = 1
    while True:
        try:
            params = {"database_id": database_id, "page_size": page_size}
            if start_cursor:
                params["start_cursor"] = start_cursor
            if hasattr(client, "databases") and hasattr(client.databases, "query"):
                return cast(Dict[str, Any], client.databases.query(**params))
            elif hasattr(client, "data_sources") and hasattr(client.data_sources, "query"):
                db_info = client.databases.retrieve(database_id=database_id)
                data_sources = db_info.get("data_sources", [])
                ds_id = data_sources[0]["id"] if data_sources else database_id
                ds_params = {"data_source_id": ds_id, "page_size": page_size}
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
    retry_delay: float = 1.0,
) -> Iterable[Dict[str, Any]]:
    """노션 데이터베이스 전체 페이지를 페이지네이션하며 하나씩 yield하는 Generator"""
    start_cursor = None
    while True:
        response = safe_databases_query(client, database_id, start_cursor=start_cursor, page_size=page_size)
        for page in response.get("results", []):
            yield page
        if not response.get("has_more"):
            break
        start_cursor = response.get("next_cursor")
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


def get_page_text(props: Dict[str, Any], names: List[str]) -> str:
    """노션 페이지의 title 또는 rich_text 속성에서 첫 번째로 발견된 문자열 텍스트를 추출합니다."""
    for name in names:
        prop = props.get(name, {})
        for key in ("title", "rich_text"):
            content = prop.get(key)
            if content and isinstance(content, list) and len(content) > 0:
                text = content[0].get("plain_text", "")
                if text:
                    return text.strip()
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
# 6. 한국투자증권(KIS) API 인증 관리 (지능형 디스크 캐싱)
# ==============================================================================
TOKEN_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".kis_token_cache.json")


def _load_token_cache() -> Dict[str, Any]:
    """로컬에 캐시된 KIS 토큰 파일을 읽어옵니다."""
    if os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_token_cache(cache_data: Dict[str, Any]) -> None:
    """KIS 토큰 정보를 로컬 캐시 파일에 안전하게 저장합니다."""
    try:
        with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
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
    한국투자증권 인증 컨텍스트를 반환합니다.
    기본적으로 실전투자 서버(PROD)를 1순위로 연결하며, 실전 인증 실패 시 모의투자 서버(VTS)로 자동 전환(Fallback)합니다.
    """
    # 1. 키 설정 수집 (실전투자 키 우선 매핑)
    prod_app_key = (
        os.environ.get("KIS_PROD_APP_KEY")
        or os.environ.get("KIS_REAL_APP_KEY")
        or os.environ.get("KIS_APP_KEY")
        or ""
    )
    prod_app_secret = (
        os.environ.get("KIS_PROD_APP_SECRET")
        or os.environ.get("KIS_REAL_APP_SECRET")
        or os.environ.get("KIS_APP_SECRET")
        or ""
    )

    vts_app_key = os.environ.get("KIS_VTS_APP_KEY") or ""
    vts_app_secret = os.environ.get("KIS_VTS_APP_SECRET") or ""

    # 2. 실전투자(PROD) 우선 시도
    if prod_app_key and prod_app_secret:
        token = _request_kis_token(
            url_base=KIS_PROD_URL,
            app_key=prod_app_key,
            app_secret=prod_app_secret,
            max_retries=max_retries,
            base_delay=base_delay,
            env_name="실전투자(PROD)"
        )
        if token:
            print("✅ [KIS API] 실전투자(PROD) 서버 인증 완료")
            return {
                "token": token,
                "url_base": KIS_PROD_URL,
                "app_key": prod_app_key,
                "app_secret": prod_app_secret,
                "env_type": "PROD"
            }
        print("⚠️ [KIS API] 실전투자 서버 응답 실패. 모의투자(VTS) 서버로 전환(Fallback) 시도...")

    # 3. 모의투자(VTS) Fallback 시도
    if vts_app_key and vts_app_secret:
        token = _request_kis_token(
            url_base=KIS_VTS_URL,
            app_key=vts_app_key,
            app_secret=vts_app_secret,
            max_retries=max_retries,
            base_delay=base_delay,
            env_name="모의투자(VTS)"
        )
        if token:
            print("✅ [KIS API] 모의투자(VTS) 서버 인증 완료")
            return {
                "token": token,
                "url_base": KIS_VTS_URL,
                "app_key": vts_app_key,
                "app_secret": vts_app_secret,
                "env_type": "VTS"
            }

    print("❌ [KIS API] 실전투자 및 모의투자 서버 모두 토큰 발급에 실패하였습니다.")
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
