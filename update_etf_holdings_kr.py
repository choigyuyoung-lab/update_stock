import os
import sys
import time
import json
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Any, List, Dict, Optional, Set, Tuple

# Windows 콘솔 한글 및 이모지 출력 인코딩 설정
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

import pandas as pd
import FinanceDataReader as fdr

from notion_utils import (
    build_notion_client,
    get_env_var,
    paginate_database,
    get_page_text,
    kst_isoformat,
    RETRY_STATUS_CODES,
)

# ==========================================
# 1. 환경 변수 및 실전투자 전용 설정
# ==========================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
# INVESTMENT_DB_ID: 투자주 DB (종목 및 ETF 릴레이션 타겟 DB)
INVESTMENT_DB_ID = os.environ.get("DATABASE_ID") or get_env_var("DATABASE_ID")
# MASTER_DB_ID: 상장주식DB 전체 (ETF 및 전종목 마스터 DB)
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID") or os.environ.get("MASTER_DB_ID") or INVESTMENT_DB_ID
# ETF_DB_ID: ETF 구성종목이 기록되는 하위 DB
ETF_DB_ID = get_env_var("ETF_DB_ID")

# 실전투자(PROD) API 전용 설정
KIS_DOMAIN = "https://openapi.koreainvestment.com:9443"
KIS_APP_KEY = (os.environ.get("KIS_APP_KEY") or os.environ.get("KIS_PROD_APP_KEY") or "").strip()
KIS_APP_SECRET = (os.environ.get("KIS_APP_SECRET") or os.environ.get("KIS_PROD_APP_SECRET") or "").strip()

SESSION = requests.Session()
SESSION.headers.update({
    "Content-Type": "application/json",
    "Connection": "close"
})
retries = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))


# ==========================================
# 2. 동적 티커 매핑 엔진 (FDR + 글로벌 표준 심볼)
# ==========================================
GLOBAL_SYMBOL_MAP = {
    # 일본 주요 반도체 / 로봇 / 장비주 (도쿄거래소 티커)
    "MURATA MFG CO": "6981.T",
    "MURATA": "6981.T",
    "ADVANTEST CORP": "6857.T",
    "ADVANTEST": "6857.T",
    "IBIDEN": "4062.T",
    "MICRONICS JAPAN CO LTD": "6871.T",
    "MICRONICS JAPAN": "6871.T",
    "KIOXIA HOLDINGS CORP": "285A.T",
    "KIOXIA": "285A.T",
    "TOKYO ELECTRON": "8035.T",
    "TOKYO ELECTRON LTD": "8035.T",
    "KOKUSAI ELECTRIC CORP": "6525.T",
    "KOKUSAI ELECTRIC": "6525.T",
    "LASERTEC CORP": "6920.T",
    "LASERTEC": "6920.T",
    "INSPEC INC": "6656.T",
    "INSPEC": "6656.T",
    "DISCO CORP": "6146.T",
    "SCREEN HOLDINGS CO LTD": "7735.T",
    "SHIN-ETSU CHEMICAL CO LTD": "4063.T",
    "SUMCO CORP": "3436.T",
    "HOYA CORP": "7741.T",
    "KEYENCE CORP": "6861.T",
    "FANUC CORP": "6954.T",
    "YASKAWA ELECTRIC CORP": "6506.T",
    "SMC CORP": "6273.T",
    "DAIFUKU CO LTD": "6383.T",
    "SONY GROUP CORP": "6758.T",
    "TOYOTA MOTOR CORP": "7203.T",
    "MITSUBISHI HEAVY INDUSTRIES": "7011.T",

    # 미국 및 글로벌 주요 반도체 / AI / 빅테크
    "MICRON TECH": "MU",
    "MICRON TECHNOLOGY INC": "MU",
    "MICRON TECHNOLOGY": "MU",
    "LAM RESEARCH CORP": "LRCX",
    "LAM RESEARCH": "LRCX",
    "APPLIED MATERIALS INC": "AMAT",
    "APPLIED MATERIALS": "AMAT",
    "AXCELIS TECHNOLOGIES INC": "ACLS",
    "AXCELIS TECHNOLOGIES": "ACLS",
    "WESTERN DIGITAL CORP": "WDC",
    "WESTERN DIGITAL": "WDC",
    "SANDISK CORP/DE": "WDC",
    "SANDISK": "WDC",
    "SEAGATE TECHNOLOGY HOLDINGS PLC": "STX",
    "SEAGATE TECHNOLOGY": "STX",
    "TERADYNE INC": "TER",
    "TERADYNE": "TER",
    "MKS INSTRUMENTS INC": "MKSI",
    "MKS INC": "MKSI",
    "FORMFACTOR INC": "FORM",
    "FORMFACTOR": "FORM",
    "KLA CORP": "KLAC",
    "KLA-TENCOR CORP": "KLAC",
    "ASML HOLDING NV": "ASML",
    "ASML": "ASML",
    "TAIWAN SEMICONDUCTOR MANUFACTURING": "TSM",
    "TSMC": "TSM",
    "NVIDIA CORP": "NVDA",
    "NVIDIA": "NVDA",
    "APPLE INC": "AAPL",
    "APPLE": "AAPL",
    "MICROSOFT CORP": "MSFT",
    "MICROSOFT": "MSFT",
    "ALPHABET INC-CL A": "GOOGL",
    "ALPHABET INC-CL C": "GOOG",
    "ALPHABET INC": "GOOGL",
    "AMAZON.COM INC": "AMZN",
    "AMAZON": "AMZN",
    "META PLATFORMS INC": "META",
    "META PLATFORMS INC-CL A": "META",
    "META": "META",
    "TESLA INC": "TSLA",
    "TESLA MOTORS": "TSLA",
    "TESLA": "TSLA",
    "BROADCOM INC": "AVGO",
    "BROADCOM LTD": "AVGO",
    "QUALCOMM INC": "QCOM",
    "ADVANCED MICRO DEVICES INC": "AMD",
    "AMD": "AMD",
    "INTEL CORP": "INTC",
    "TEXAS INSTRUMENTS INC": "TXN",
    "ANALOG DEVICES INC": "ADI",
    "MARVELL TECHNOLOGY INC": "MRVL",
    "NXP SEMICONDUCTORS NV": "NXPI",
    "ON SEMICONDUCTOR CORP": "ON",
    "MONOLITHIC POWER SYSTEMS INC": "MPWR",
    "MICROCHIP TECHNOLOGY INC": "MCHP",
    "ENTEGRIS INC": "ENTG",
    "PALANTIR TECHNOLOGIES INC": "PLTR",
    "ARM HOLDINGS PLC": "ARM",
    "SUPER MICRO COMPUTER INC": "SMCI",
    "COHERENT CORP": "COHR",
    "CIENA CORP": "CIEN",
    "APPLIED OPTOELECTRONICS INC": "AAOI",
    "CORNING INC": "GLW",
    "LUMENTUM HOLDINGS INC": "LITE",
    "TOWER SEMICONDUCTOR LTD": "TSEM",
    "MACOM TECHNOLOGY SOLUTIONS HOLDINGS INC": "MTSI",
    "MACOM TECHNOLOGY SOLUTIONS": "MTSI",
    "FABRINET": "FN",
    "NOKIA OYJ": "NOK",
    "SPACE EXPLORATION TECHNOLOGIES CORP": "SPACEX",
    "REDWIRE CORP": "RDW",
    "SATELLOGIC INC": "SATL",
    "INTUITIVE MACHINES INC": "LUNR",
    "PLANET LABS PBC": "PL",
    "ROCKET LAB USA INC": "RKLB",
    "FIREFLY AEROSPACE INC": "FLY",
    "AST SPACEMOBILE INC": "ASTS",
    "VIASAT INC": "VSAT",
    "BLACKSKY TECHNOLOGY INC": "BKSY",
    "WALMART INC": "WMT",
    "NETFLIX INC": "NFLX",
    "CISCO SYSTEMS INC": "CSCO",
    "COMCAST CORP-CLASS A": "CMCSA",
    "WARNER BROS DISCOVERY INC": "WBD",
    "AT&T INC": "T",
    "BANK OF AMERICA CORP": "BAC",
    "PFIZER INC": "PFE"
}


class TickerEngine:
    """국내/해외 주식 및 ETF 종목명 ↔ 정밀 티커 동적 변환 엔진"""
    def __init__(self):
        print("📡 종목 마스터 데이터 엔진 초기화 중...", flush=True)
        self.kr_name_to_code: Dict[str, str] = {}
        self._load_krx_listings()

    def _load_krx_listings(self) -> None:
        """한국거래소(KRX) 전체 상장 종목 및 ETF 목록 로드"""
        try:
            df_krx = fdr.StockListing('KRX')
            for _, row in df_krx.iterrows():
                name = str(row.get('Name', '')).strip()
                code = str(row.get('Code', '')).strip()
                if name and code:
                    self.kr_name_to_code[name] = code
                    self.kr_name_to_code[name.replace(" ", "")] = code
            print(f"   ✅ KRX 국내 상장 종목 사전 {len(self.kr_name_to_code)}건 구축 완료", flush=True)
        except Exception as e:
            print(f"   ⚠️ KRX 데이터 로드 실패 (기본 매핑 사용): {e}", flush=True)

    def resolve(self, raw_ticker: str, name: str) -> str:
        """
        국내/해외 종목명과 티커를 분석하여 표준 정규 티커를 반환합니다.
        1. 이미 6자리 국내 표준 종목코드(숫자/알파벳 혼용)인 경우 그대로 반환
        2. 국내 종목명 사전에 일치하는 경우 6자리 코드 반환
        3. 글로벌 / 일본 / 미국 표준 티커 사전에 일치하는 경우 해당 티커 반환
        4. 영문 티커 심볼 형태인 경우 대문자 티커 반환
        5. 최종 fallback: 종목명
        """
        clean_ticker = (raw_ticker or "").strip().split(".")[0].upper()
        clean_name = (name or "").strip()
        upper_name = clean_name.upper()

        # 1. 6자리 국내 표준 종목코드 판별 (예: 005930, 058610, 0177X0, 0174B0)
        if len(clean_ticker) == 6 and clean_ticker.isalnum() and not clean_ticker.isalpha():
            return clean_ticker

        # 2. 국내 거래소(KRX) 종목명 매핑
        if clean_name in self.kr_name_to_code:
            return self.kr_name_to_code[clean_name]
        name_no_space = clean_name.replace(" ", "")
        if name_no_space in self.kr_name_to_code:
            return self.kr_name_to_code[name_no_space]

        # 3. 글로벌 / 일본 / 미국 정밀 심볼 매핑
        if upper_name in GLOBAL_SYMBOL_MAP:
            return GLOBAL_SYMBOL_MAP[upper_name]

        for k, v in GLOBAL_SYMBOL_MAP.items():
            if k == upper_name or k in upper_name:
                return v

        # 4. 알파벳 티커 fallback (1~5자리 영문 심볼)
        if clean_ticker and clean_ticker.isalpha() and len(clean_ticker) <= 5 and clean_ticker != "UNKNOWN":
            return clean_ticker

        return clean_name or clean_ticker or "UNKNOWN"


TICKER_ENGINE = TickerEngine()


def sf(value: Any) -> Optional[float]:
    """안전하게 float으로 변환하며 유효하지 않으면 None을 반환합니다."""
    if value is None:
        return None
    try:
        val = float(value)
        if val != val or val < 0:  # NaN or 음수
            return None
        return round(val, 2)
    except (TypeError, ValueError):
        return None


# ==========================================
# 3. 수집 엔진 (한투 실전 API + WiseReport 하이브리드 수집)
# ==========================================
def get_kis_token(max_retries: int = 3, base_delay: float = 2.0) -> Optional[str]:
    """한투 실전투자 접속 토큰 발급"""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        print("❌ KIS 실전투자 API Key 또는 Secret이 설정되지 않았습니다.", flush=True)
        return None

    url = f"{KIS_DOMAIN}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET
    }

    for attempt in range(1, max_retries + 1):
        try:
            res = SESSION.post(url, json=body, timeout=10)
            if res.status_code in RETRY_STATUS_CODES and attempt < max_retries:
                time.sleep(base_delay * (2 ** (attempt - 1)))
                continue
            res.raise_for_status()
            token = res.json().get("access_token")
            if token:
                return token
        except Exception as exc:
            if attempt < max_retries:
                time.sleep(base_delay * (2 ** (attempt - 1)))
                continue
            print(f"❌ KIS 실전투자 토큰 발급 실패: {exc}", flush=True)
            return None
    return None


def get_etf_composition_kis(token: str, clean_ticker: str) -> List[Dict[str, Any]]:
    """한투 Open API(FHKST121600C0): 국내 ETF 구성종목 및 공식 비중 수집"""
    url = f"{KIS_DOMAIN}/uapi/etfetn/v1/quotations/inquire-component-stock-price"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST121600C0",
        "custtype": "P"
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": clean_ticker,
        "FID_COND_SCR_DIV_CODE": "11216"
    }

    holdings: List[Dict[str, Any]] = []
    try:
        res = SESSION.get(url, headers=headers, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        if data.get("rt_cd") != "0":
            return holdings

        items = data.get("output2") or []
        for item in items:
            raw_ticker = str(item.get("stck_shrn_iscd") or item.get("stck_iscd") or "").strip()
            name = (item.get("hts_kor_isnm") or "").strip()
            if not raw_ticker and not name:
                continue

            ticker = TICKER_ENGINE.resolve(raw_ticker, name)
            # 한투 공식 비중: etf_cnfg_issu_rlim (구성비중 %)
            weight = sf(item.get("etf_cnfg_issu_rlim"))

            holdings.append({
                "ticker": ticker,
                "name": name or ticker,
                "weight": weight
            })
        return holdings
    except Exception:
        return holdings


def get_etf_composition_wisereport(clean_ticker: str) -> List[Dict[str, Any]]:
    """WiseReport / Naver 금융 ETF 분석 엔진에서 미국 및 글로벌 ETF 구성종목 수집"""
    url = f"https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd={clean_ticker}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    holdings: List[Dict[str, Any]] = []
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return holdings

        match = re.search(r'var\s+CU_data\s*=\s*(\{.*?\});', r.text, re.DOTALL)
        if not match:
            return holdings

        data = json.loads(match.group(1))
        grid_data = data.get("grid_data", [])

        for item in grid_data:
            name = (item.get("STK_NM_KOR") or item.get("ITEM_NM") or "").strip()
            if not name or name in ["설정현금액", "원화현금", "USD현금", "외화예치금"]:
                continue

            raw_ticker = str(item.get("STK_CD") or item.get("CMP_CD") or "").strip()
            ticker = TICKER_ENGINE.resolve(raw_ticker, name)
            # WiseReport 공식 비중: ETF_WEIGHT (수량 AGMT_STK_CNT는 비중 계산에 절대 쓰지 않음!)
            weight = sf(item.get("ETF_WEIGHT"))

            holdings.append({
                "ticker": ticker,
                "name": name,
                "weight": weight
            })

        return holdings
    except Exception:
        return holdings


def get_hybrid_etf_composition(token: str, etf_ticker: str, etf_name: str) -> List[Dict[str, Any]]:
    """
    한투 실전 API + WiseReport 하이브리드 수집 & 정밀 티커 기반 중복 제거 병합
    """
    clean_ticker = etf_ticker.split(".")[0]

    # 1. 한투 실전 API 수집 (국내 주식 및 정확한 비중)
    kis_items = get_etf_composition_kis(token, clean_ticker)

    # 2. WiseReport 수집 (해외/글로벌 종목 및 백업)
    backup_items = get_etf_composition_wisereport(clean_ticker)

    # 3. 정제된 티커 기준 스마트 병합 (Deduplication)
    combined: Dict[str, Dict[str, Any]] = {}

    for item in kis_items + backup_items:
        key = item["ticker"] if item["ticker"] and item["ticker"] != "UNKNOWN" else item["name"]

        if key not in combined:
            combined[key] = {
                "ticker": item["ticker"],
                "name": item["name"],
                "weight": item["weight"]
            }
        else:
            existing = combined[key]
            # 더 정밀한 티커(종목코드 또는 영문심볼)가 있으면 갱신
            if (len(existing["ticker"]) > 6 or existing["ticker"] == existing["name"]) and (item["ticker"] and item["ticker"] != item["name"]):
                existing["ticker"] = item["ticker"]
            # 공식 비중 수치가 있는 항목 우선 적용
            if existing["weight"] is None and item["weight"] is not None:
                existing["weight"] = item["weight"]
            # 이름이 더 명확한 것 유지
            if len(item["name"]) > len(existing["name"]):
                existing["name"] = item["name"]

    merged_list = list(combined.values())
    if not merged_list:
        return []

    # 비중 기준 내림차순 정렬 (None인 항목은 뒤로)
    merged_list.sort(key=lambda x: (x["weight"] is not None, x["weight"] if x["weight"] is not None else -1), reverse=True)
    return merged_list


# ==========================================
# 4. 노션 DB 연동 및 인메모리 종목 캐시 엔진
# ==========================================
class NotionInvestmentDBCache:
    """투자주 DB의 종목 ID를 인메모리에 사전 로드하여 노션 쿼리 폭주 및 오매핑 방지"""
    def __init__(self, client: Any):
        self.client = client
        self.ticker_to_id: Dict[str, str] = {}
        self.name_to_id: Dict[str, str] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        print(f"📦 투자주 DB({INVESTMENT_DB_ID})의 전체 종목 목록을 메모리에 로드합니다...", flush=True)
        count = 0
        for page in paginate_database(self.client, INVESTMENT_DB_ID, page_size=100, retry_delay=0.1):
            pid = page["id"]
            props = page.get("properties", {})
            
            # 티커 (title)
            ticker_list = props.get("티커", {}).get("title", [])
            ticker = ticker_list[0].get("plain_text", "").strip() if ticker_list else ""
            
            # 종목명 (formula 또는 rich_text)
            name = ""
            name_prop = props.get("종목명", {})
            if name_prop.get("type") == "formula":
                name = str(name_prop.get("formula", {}).get("string") or "").strip()
            elif name_prop.get("type") == "rich_text":
                r_list = name_prop.get("rich_text", [])
                name = r_list[0].get("plain_text", "").strip() if r_list else ""

            if ticker:
                clean_t = ticker.split(".")[0].strip().upper()
                self.ticker_to_id[clean_t] = pid
                self.ticker_to_id[ticker.strip().upper()] = pid
            if name:
                self.name_to_id[name.strip()] = pid
                self.name_to_id[name.replace(" ", "")] = pid
            count += 1
        print(f"   ✅ 총 {count}개 투자주 종목 캐싱 완료 (티커 {len(self.ticker_to_id)}개, 종목명 {len(self.name_to_id)}개)", flush=True)

    def get_or_create(self, ticker: str, name: str) -> Optional[str]:
        """정확한 티커 및 종목명으로 매핑하고, 없으면 투자주 DB에 신규 추가"""
        clean_t = ticker.split(".")[0].strip().upper()
        clean_n = name.strip()

        # 1. 티커 기준 완전 일치 조회
        if clean_t in self.ticker_to_id:
            return self.ticker_to_id[clean_t]

        # 2. 종목명 기준 완전 일치 조회
        if clean_n in self.name_to_id:
            return self.name_to_id[clean_n]
        if clean_n.replace(" ", "") in self.name_to_id:
            return self.name_to_id[clean_n.replace(" ", "")]

        # 3. 미등록 종목 신규 생성 (동적 추가)
        create_title = clean_t if (clean_t and clean_t != "UNKNOWN") else clean_n
        try:
            new_payload = {
                "parent": {"database_id": INVESTMENT_DB_ID},
                "properties": {
                    "티커": {"title": [{"text": {"content": create_title}}]}
                }
            }
            created = self.client.pages.create(**new_payload)
            new_id = created["id"]
            self.ticker_to_id[clean_t] = new_id
            if clean_n:
                self.name_to_id[clean_n] = new_id
            print(f"   ✨ 투자주 DB 신규 종목 자동 등록: {clean_n} ({create_title})", flush=True)
            return new_id
        except Exception as exc:
            print(f"   ❌ 투자주 DB 종목 생성 실패 ({clean_t}): {exc}", flush=True)
            return None


def get_target_etfs(client: Any) -> List[Dict[str, str]]:
    """
    ETF DB(ETF_DB_ID)에 현재 등록/연결되어 있는 부모 ETF(투자DB) 페이지만 역스캔하여
    실제 사용자가 모니터링 중인 대상 ETF 목록만 추출합니다.
    """
    print(f"📋 ETF DB({ETF_DB_ID})에 연결된 모니터링 대상 ETF 목록을 스캔합니다...", flush=True)
    target_etfs: List[Dict[str, str]] = []
    parent_etf_ids: set = set()

    try:
        # 1. ETF DB에서 'ETF(투자DB)' 릴레이션에 지정된 모든 고유 부모 ETF ID 수집
        for page in paginate_database(client, ETF_DB_ID, page_size=100):
            props = page.get("properties", {})
            rel_list = props.get("ETF(투자DB)", {}).get("relation", [])
            for rel in rel_list:
                if rel.get("id"):
                    parent_etf_ids.add(rel["id"])

        print(f"   🔍 ETF DB에서 발견된 부모 ETF 페이지 수: {len(parent_etf_ids)}개", flush=True)

        # 2. 각 부모 ETF 페이지 정보를 조회하여 티커 및 이름 추출
        for page_id in parent_etf_ids:
            try:
                parent_page = client.pages.retrieve(page_id=page_id)
                parent_props = parent_page.get("properties", {})
                ticker = get_page_text(parent_props, ["티커", "Ticker"])
                name = get_page_text(parent_props, ["종목명", "이름", "Title"])

                if ticker:
                    clean_ticker = ticker.split(".")[0].strip().upper()
                    target_etfs.append({
                        "etf_page_id": page_id,
                        "ticker": clean_ticker,
                        "name": name or clean_ticker
                    })
                    print(f"   🎯 대상 ETF 확인: {name or clean_ticker} ({clean_ticker})", flush=True)
            except Exception as exc:
                print(f"   ⚠️ 부모 ETF 페이지({page_id}) 조회 실패: {exc}", flush=True)
    except Exception as exc:
        print(f"⚠️ ETF DB 부모 릴레이션 스캔 중 오류 발생: {exc}", flush=True)

    print(f"   ✅ 총 {len(target_etfs)}개의 관리 대상 ETF 갱신 준비 완료.\n", flush=True)
    return target_etfs


def ensure_date_property_exists(client: Any) -> str:
    """ETF DB에 '업데이트' (날짜 속성) 열이 있는지 확인 및 생성"""
    date_prop_name = "업데이트"
    try:
        db = client.databases.retrieve(database_id=ETF_DB_ID)
        props = db.get("properties", {})
        for candidate in ["업데이트", "업데이트 일자", "마지막 업데이트"]:
            if candidate in props and props[candidate]["type"] == "date":
                return candidate
        client.databases.update(
            database_id=ETF_DB_ID,
            properties={date_prop_name: {"date": {}}}
        )
        return date_prop_name
    except Exception:
        return date_prop_name


def archive_existing_etf_holdings(client: Any, etf_page_id: str) -> None:
    """기존 ETF DB에서 해당 ETF 페이지의 과거 데이터를 전량 완전 삭제(Archive)"""
    pages_to_archive: List[str] = []
    start_cursor = None

    while True:
        try:
            params = {
                "database_id": ETF_DB_ID,
                "filter": {
                    "property": "ETF(투자DB)",
                    "relation": {"contains": etf_page_id}
                },
                "page_size": 100
            }
            if start_cursor:
                params["start_cursor"] = start_cursor

            res = client.databases.query(**params)
            results = res.get("results", [])
            for page in results:
                pages_to_archive.append(page["id"])

            if not res.get("has_more"):
                break
            start_cursor = res.get("next_cursor")
            time.sleep(0.03)
        except Exception:
            break

    for page_id in pages_to_archive:
        try:
            client.pages.update(page_id=page_id, archived=True)
            time.sleep(0.02)
        except Exception:
            pass


# ==========================================
# 5. 메인 실행 파이프라인
# ==========================================
def main() -> None:
    print("🚀 [ETF 구성종목 자동 수집 및 동기화 파이프라인] 가동 시작", flush=True)
    print(f"📡 한투 실전투자 전용 서버: {KIS_DOMAIN}", flush=True)

    notion_client = build_notion_client(NOTION_TOKEN)

    kis_token = get_kis_token()
    if not kis_token:
        print("❌ KIS 실전투자 토큰을 발급받지 못해 작업을 중단합니다.", flush=True)
        return

    date_prop_name = ensure_date_property_exists(notion_client)
    db_cache = NotionInvestmentDBCache(notion_client)
    target_etfs = get_target_etfs(notion_client)

    if not target_etfs:
        print("⚠️ 갱신 대상 ETF를 찾지 못했습니다.", flush=True)
        return

    # ----------------------------------------------------
    # PHASE 1: 전체 ETF 구성종목 수집 및 정제
    # ----------------------------------------------------
    print(f"\n🔄 [Phase 1/2] 총 {len(target_etfs)}개 ETF의 구성종목 배치 수집 및 티커 정제를 시작합니다...", flush=True)
    batch_records: List[Dict[str, Any]] = []

    for idx, target in enumerate(target_etfs, 1):
        etf_page_id = target["etf_page_id"]
        etf_ticker = target["ticker"]
        etf_name = target.get("name", etf_ticker)

        print(f"   [{idx}/{len(target_etfs)}] 수집 중: {etf_name}({etf_ticker})...", flush=True)
        holdings = get_hybrid_etf_composition(kis_token, etf_ticker, etf_name)

        if not holdings:
            print(f"   ⚠️ {etf_name}({etf_ticker}) 구성종목 수집 결과 없음 (건너뜀)", flush=True)
            continue

        prepared_items = []
        for item in holdings:
            stock_id = db_cache.get_or_create(item["ticker"], item["name"])
            if stock_id:
                prepared_items.append({
                    "stock_id": stock_id,
                    "name": item["name"],
                    "ticker": item["ticker"],
                    "weight": item["weight"]
                })

        batch_records.append({
            "etf_page_id": etf_page_id,
            "etf_ticker": etf_ticker,
            "etf_name": etf_name,
            "items": prepared_items
        })
        print(f"   └ {etf_name}({etf_ticker}): {len(prepared_items)}건 수집 및 티커 매핑 완료", flush=True)

    # ----------------------------------------------------
    # PHASE 2: 노션 ETF DB에 일괄 기록
    # ----------------------------------------------------
    print(f"\n📝 [Phase 2/2] 노션 DB에 정제된 {len(batch_records)}개 ETF 데이터를 일괄 기록합니다...", flush=True)
    now_kst = kst_isoformat()

    for idx, record in enumerate(batch_records, 1):
        etf_page_id = record["etf_page_id"]
        etf_name = record["etf_name"]
        items = record["items"]

        print(f"\n▶ [{idx}/{len(batch_records)}] 노션 갱신 진행: {etf_name} (기존 데이터 삭제 후 {len(items)}건 기록)", flush=True)

        # 1. 과거 데이터 일괄 아카이브
        archive_existing_etf_holdings(notion_client, etf_page_id)

        # 2. 신규 정제 데이터 삽입
        success_count = 0
        for item in items:
            props: Dict[str, Any] = {
                "이름": {"title": [{"text": {"content": item["name"] or item["ticker"]}}]},
                "티커": {"rich_text": [{"text": {"content": item["ticker"]}}]},
                "종목(투자DB)": {"relation": [{"id": item["stock_id"]}]},
                "ETF(투자DB)": {"relation": [{"id": etf_page_id}]},
                date_prop_name: {"date": {"start": now_kst}}
            }
            # 비중이 존재하는 경우에만 number 속성에 기록 (None이면 공백 유지)
            if item["weight"] is not None:
                props["비중"] = {"number": item["weight"]}

            payload = {
                "parent": {"database_id": ETF_DB_ID},
                "properties": props
            }

            try:
                notion_client.pages.create(**payload)
                success_count += 1
                time.sleep(0.06)
            except Exception as exc:
                print(f"❌ [{etf_name}] {item['name']} 노션 기록 실패: {exc}", flush=True)

        print(f"✅ [{etf_name}] 노션 DB 갱신 완료! ({success_count}/{len(items)}건 입력)", flush=True)

    print("\n✨ 모든 ETF 구성종목 수집 및 노션 DB 동기화가 성공적으로 완료되었습니다.", flush=True)


if __name__ == "__main__":
    main()