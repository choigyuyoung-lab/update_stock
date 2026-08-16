import os
import sys
import json
import re
import math
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv
load_dotenv()
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # 🌟 파이썬 3.9+ 해외 서버 시간 왜곡 차단 표준 라이브러리
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

# Windows 콘솔 utf-8 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from notion_client import Client
from notion_client.errors import HTTPResponseError

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_PAGE_SIZE = 100

KIS_VTS_URL = "https://openapivts.koreainvestment.com:29443"
KIS_PROD_URL = "https://openapi.koreainvestment.com:9443"


def get_http_session(user_agent: Optional[str] = None) -> requests.Session:
    """Connection: close 및 지수 백오프 Retry가 탑재된 고신뢰성 HTTP 세션을 생성합니다."""
    session = requests.Session()
    headers = {"Connection": "close"}
    if user_agent:
        headers["User-Agent"] = user_agent
    else:
        headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    session.headers.update(headers)

    retries = Retry(
        total=3,
        backoff_factor=0.2,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def is_kr_ticker(ticker: str) -> bool:
    """
    국내 종목코드(보통주, 알파벳 포함 신형 우선주/전환주, .KS/.KQ)를 정밀 판별합니다.
    """
    if not ticker:
        return False
    
    t = ticker.strip().upper()
    
    # 1. 해외 거래소 접미어가 명시된 경우 -> 무조건 해외(False)
    if t.endswith((".T", ".TA", ".TW", ".HK", ".L", ".DE", ".AS", ".PA", ".SW", ".CO")):
        return False

    # 2. 국내 거래소 접미어가 붙은 경우 (.KS, .KQ) -> 무조건 국내(True)
    if t.endswith((".KS", ".KQ")):
        return True

    # 3. 접미어 분리 후 순수 티커 검사
    clean_t = t.split(".")[0].strip()

    # 4. KRX 6자리 표준 코드 판별 (6자리 & 숫자로 시작 & 영문+숫자)
    if len(clean_t) == 6 and clean_t[0].isdigit() and clean_t.isalnum():
        return True

    return False


def is_valid_num(value: Any) -> bool:
    """숫자 값이 유효한지 검증합니다 (NaN, Inf, None, 빈문자열 차단)."""
    if value is None:
        return False
    try:
        if isinstance(value, str):
            clean = value.replace(",", "").strip()
            if not clean or clean.lower() in ["null", "none", "nan", "-"]:
                return False
            val = float(clean)
        else:
            val = float(value)
        return not (math.isnan(val) or math.isinf(val))
    except (TypeError, ValueError):
        return False


def safe_float(value: Any) -> Optional[float]:
    """문자열/숫자를 안전하게 float로 변환합니다. 0이거나 유효하지 않으면 None 반환."""
    if not is_valid_num(value):
        return None
    try:
        val = float(str(value).replace(",", "").strip())
        return val if val != 0 else None
    except (TypeError, ValueError):
        return None


def make_rich_text(val: Any) -> Dict[str, Any]:
    """노션 rich_text 속성 객체를 생성합니다."""
    text_val = str(val).strip() if val is not None else ""
    return {"rich_text": [{"text": {"content": text_val}}]} if text_val else {"rich_text": []}


def search_foreign_ticker(name: str) -> Optional[Tuple[str, str]]:
    """
    야후 파이낸스 Search API를 통해 종목명으로 미국 메이저 거래소(NYSE, NASDAQ) 상장 티커 및 ADR 티커를 동적으로 추출합니다.
    """
    if not name:
        return None

    short_brand = extract_short_brand_name(name)
    query = "Taiwan Semiconductor" if short_brand.upper() == "TSMC" else (short_brand or name)

    try:
        url = "https://query2.finance.yahoo.com/v1/finance/search"
        session = get_http_session()
        r = session.get(url, params={"q": query, "quotesCount": 6, "newsCount": 0}, timeout=5)
        if r.status_code != 200:
            return None

        quotes = r.json().get("quotes", [])
        us_pick, jp_pick, other_pick = None, None, None

        for q in quotes:
            sym = q.get("symbol", "").strip().upper()
            typ = q.get("quoteType", "")
            exch = (q.get("exchange") or q.get("exchDisp") or "").upper()
            sname = (q.get("shortname") or q.get("longname") or "").strip()

            if typ not in ["EQUITY", "ETF"] or sym.endswith("-USD"):
                continue

            # 1. 미국 메이저 거래소 상장주 및 ADR (점 없는 티커 최우선)
            if ("." not in sym) and any(m in exch for m in ["NY", "NASD", "NMS", "BATS", "NGM", "NCM"]):
                if not us_pick:
                    us_pick = (sym, short_brand or sname)
            # 2. 일본 도쿄 증시 (.T)
            elif sym.endswith(".T") or "TOKYO" in exch or "JPX" in exch:
                if not jp_pick:
                    jp_pick = (sym if sym.endswith(".T") else f"{sym}.T", short_brand or sname)
            # 3. 기타 해외 증시
            elif not other_pick:
                other_pick = (sym, short_brand or sname)

        return us_pick or jp_pick or other_pick
    except Exception:
        return None


def extract_short_brand_name(name: str) -> str:
    """노션 열 너비가 길어지지 않도록 법인형태/접미사를 제거한 핵심 브랜드명만 추출"""
    if not name:
        return ""
    n = name.strip()

    # 1. 글로벌 대형주 주요 별칭 매핑
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
    }
    for pat, brand in brand_map.items():
        if re.search(pat, n):
            return brand

    # 2. 특수기호 및 법인/주식 형태 수식어 제거
    clean = re.sub(r'[\(\)\[\],\.\-\/\:\'\"]', ' ', n)
    remove_patterns = [
        r'(?i)\bCL(ASS)?\s*[A-Z0-9]?\b', r'(?i)\bORD(INARY)?\b', r'(?i)\bREG(ISTERED)?\b',
        r'(?i)\bSHS\b', r'(?i)\bSHARES\b', r'(?i)\bSP\s*ADR\b', r'(?i)\bADR\b', r'(?i)\bADS\b',
        r'(?i)\bNV\b', r'(?i)\bDE\b', r'(?i)\bCORP(ORATION)?\b', r'(?i)\bINC(ORPORATED)?\b',
        r'(?i)\bLTD\b', r'(?i)\bLIMITED\b', r'(?i)\bCO\b', r'(?i)\bCOS\b', r'(?i)\bLLC\b',
        r'(?i)\bPLC\b', r'(?i)\bHOLDINGS?\b', r'(?i)\bGROUP\b', r'(?i)\bHOLDI\b', r'(?i)\bUSA\b',
        r'(?i)\bCOM\b', r'(?i)\bNY\b', r'(?i)\bS\s*A\b', r'(?i)\bAG\b', r'(?i)\bSE\b',
        r'(?i)\bK\s*K\b', r'(?i)\bSPONSORED\b', r'(?i)\bSOLUTIONS\b'
    ]
    for p in remove_patterns:
        clean = re.sub(p, ' ', clean)

    tokens = [t for t in clean.split() if len(t) >= 2]
    # 최대 3단어까지만 허용하여 간결성 유지
    res = " ".join(tokens[:3])
    return res.title() if (res.isupper() and len(res) > 4) else res


def get_env_var(name: str, required: bool = True, default: Optional[str] = None) -> str:
    value: Optional[str] = os.environ.get(name, default)
    if required and not value:
        raise EnvironmentError(f"환경 변수 {name}이(가) 설정되지 않았습니다.")
    return cast(str, value)


def build_notion_client(auth_token: str, use_httpx: bool = False, timeout: float = 60.0) -> Client:
    if use_httpx:
        import httpx
        httpx_client: Any = httpx.Client(timeout=timeout)
        return Client(auth=auth_token, client=httpx_client)
    return Client(auth=auth_token)


def _format_notion_error(error: Exception) -> str:
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
    attempt = 1
    while True:
        try:
            params = {"database_id": database_id, "page_size": page_size}
            if start_cursor:
                params["start_cursor"] = start_cursor
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
    for name in names:
        prop = props.get(name, {})
        for key in ("title", "rich_text"):
            content = prop.get(key)
            if content and isinstance(content, list) and len(content) > 0:
                text = content[0].get("plain_text", "")
                if text:
                    return text.strip()
    return ""


def kst_isoformat() -> str:
    """🌟 전 세계 어느 가상 서버에서 실행되든 실제 대한민국 서울 표준시(KST)를 절대값으로 계산해 반환합니다."""
    return datetime.now(ZoneInfo("Asia/Seoul")).isoformat()


def _request_kis_token(url_base: str, app_key: str, app_secret: str, max_retries: int = 2, base_delay: float = 1.5, env_name: str = "모의투자") -> Optional[str]:
    """한투 API 액세스 토큰을 발급받는 내부 헬퍼 함수"""
    if not app_key or not app_secret:
        return None

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
            token = res.json().get("access_token")
            if token:
                return token
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
    1차로 모의투자 서버(VTS) 연결을 시도하고, 실패 시 실전투자 서버(PROD)로 자동 전환(Fallback)합니다.
    """
    # 1. 키 설정 수집
    vts_app_key = os.environ.get("KIS_VTS_APP_KEY") or os.environ.get("KIS_APP_KEY") or ""
    vts_app_secret = os.environ.get("KIS_VTS_APP_SECRET") or os.environ.get("KIS_APP_SECRET") or ""

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

    # 2. 모의투자(VTS) 시도
    if vts_app_key and vts_app_secret:
        print("🔄 [KIS API] 모의투자(VTS) 서버 토큰 발급 시도 중...")
        token = _request_kis_token(
            url_base=KIS_VTS_URL,
            app_key=vts_app_key,
            app_secret=vts_app_secret,
            max_retries=max_retries,
            base_delay=base_delay,
            env_name="모의투자(VTS)"
        )
        if token:
            print("✅ [KIS API] 모의투자(VTS) 서버 인증 성공")
            return {
                "token": token,
                "url_base": KIS_VTS_URL,
                "app_key": vts_app_key,
                "app_secret": vts_app_secret,
                "env_type": "VTS"
            }
        print("⚠️ [KIS API] 모의투자 서버 응답 실패. 실전투자(PROD) 서버로 전환(Fallback) 시도...")

    # 3. 실전투자(PROD) Fallback 시도
    if prod_app_key and prod_app_secret:
        print("🔄 [KIS API] 실전투자(PROD) 서버 토큰 발급 시도 중...")
        token = _request_kis_token(
            url_base=KIS_PROD_URL,
            app_key=prod_app_key,
            app_secret=prod_app_secret,
            max_retries=max_retries,
            base_delay=base_delay,
            env_name="실전투자(PROD)"
        )
        if token:
            print("✅ [KIS API] 실전투자(PROD) 서버 인증 성공")
            return {
                "token": token,
                "url_base": KIS_PROD_URL,
                "app_key": prod_app_key,
                "app_secret": prod_app_secret,
                "env_type": "PROD"
            }

    print("❌ [KIS API] 모의투자 및 실전투자 서버 모두 토큰 발급에 실패하였습니다.")
    return None


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


def parse_keywords(kw_raw: str, fallback_summary: str = "") -> List[str]:
    """쉼표, 세미콜론, 줄바꿈 등으로 구분된 키워드 문자열을 정규화된 리스트로 파싱합니다."""
    keywords = [k.strip().upper() for k in re.split(r'[,;|\n]+', kw_raw or "") if k.strip()]
    if not keywords and fallback_summary:
        keywords = [fallback_summary.strip().upper()]
    return keywords