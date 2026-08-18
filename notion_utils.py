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

