import os
import sys
import time
import json
import re
import difflib
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Any, List, Dict, Optional, Tuple

# Windows 콘솔 인코딩 설정
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from notion_utils import (
    build_notion_client,
    get_env_var,
    paginate_database,
    get_page_text,
    kst_isoformat,
)

# ==========================================
# 1. 환경 변수 및 실전투자 설정
# ==========================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
INVESTMENT_DB_ID = (
    os.environ.get("DATABASE_ID")
    or os.environ.get("INVESTMENT_DB_ID")
    or os.environ.get("INVESTMENT_DATABASE_ID")
    or get_env_var("DATABASE_ID")
)
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID") or os.environ.get("MASTER_DB_ID")
ETF_DB_ID = (
    os.environ.get("ETF_DB_ID")
    or os.environ.get("ETF_DATABASE_ID")
    or get_env_var("ETF_DB_ID")
)

KIS_DOMAIN = "https://openapi.koreainvestment.com:9443"
KIS_APP_KEY = (os.environ.get("KIS_APP_KEY") or os.environ.get("KIS_PROD_APP_KEY") or "").strip()
KIS_APP_SECRET = (os.environ.get("KIS_APP_SECRET") or os.environ.get("KIS_PROD_APP_SECRET") or "").strip()

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json", "Connection": "close"})
retries = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504], raise_on_status=False)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))


def parse_quantity(val: Any) -> Optional[float]:
    """수량 문자열/숫자를 안전하게 float로 파싱"""
    if val is None:
        return None
    try:
        s = str(val).replace(",", "").strip()
        if not s or s == "null" or s == "-":
            return None
        num = float(s)
        return num if num > 0 else None
    except (ValueError, TypeError):
        return None


def normalize_company_name(name: str) -> str:
    """해외/일본/미국 기업명 축약어 확장 및 법인 형태 제거 정규화"""
    if not name:
        return ""
    name_clean = name.upper()
    name_clean = re.sub(r'[\(\)\[\],\.\-]', ' ', name_clean)

    # 1. 글로벌/일본 기업 주요 축약어 표준 확장
    abbrev_map = {
        r'\bMFG\b': 'MANUFACTURING',
        r'\bIND\b': 'INDUSTRIES',
        r'\bINDS\b': 'INDUSTRIES',
        r'\bINTL\b': 'INTERNATIONAL',
        r'\bTECH\b': 'TECHNOLOGY',
        r'\bTECHNOLOGIES\b': 'TECHNOLOGY',
        r'\bSYS\b': 'SYSTEMS',
        r'\bELEC\b': 'ELECTRIC',
        r'\bELECTR\b': 'ELECTRIC',
        r'\bSEMICON\b': 'SEMICONDUCTOR',
        r'\bCHEM\b': 'CHEMICAL',
        r'\bCOMM\b': 'COMMUNICATIONS',
        r'\bLAB\b': 'LABORATORIES',
        r'\bLABS\b': 'LABORATORIES',
    }
    for pat, rep in abbrev_map.items():
        name_clean = re.sub(pat, rep, name_clean)

    # 2. 법인 형태 및 공통 접미사 제거
    suffixes = [
        r'\bCORP\b', r'\bCORPORATION\b', r'\bINC\b', r'\bLTD\b', r'\bLIMITED\b',
        r'\bCO\b', r'\bHOLDINGS?\b', r'\bGROUP\b', r'\bPLC\b', r'\bADR\b',
        r'\bCLASS [AB]\b', r'\bS A\b', r'\bAG\b', r'\bSE\b', r'\bNV\b', r'\bK K\b'
    ]
    for suf in suffixes:
        name_clean = re.sub(suf, '', name_clean)

    return " ".join(name_clean.split())


# ==========================================
# 2. 한국투자증권 실전 API & WiseReport 수집부
# ==========================================
def get_kis_token() -> Optional[str]:
    """KIS 실전투자 접속 토큰 발급"""
    url = f"{KIS_DOMAIN}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET
    }
    try:
        res = requests.post(url, json=body, timeout=10)
        res.raise_for_status()
        token = res.json().get("access_token")
        if token:
            print("🔑 KIS 실전투자 토큰 발급 성공", flush=True)
            return token
        return None
    except Exception as exc:
        print(f"❌ KIS 토큰 발급 실패: {exc}", flush=True)
        return None


def get_etf_composition_kis(token: str, clean_ticker: str) -> List[Dict[str, Any]]:
    """한투 실전 API: 한국 ETF 구성종목 코드 및 수량(etf_cu_unit_scrt_cnt) 수집"""
    url = f"{KIS_DOMAIN}/uapi/etfetn/v1/quotations/inquire-component-stock-price"
    headers = {
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
    holdings = []
    try:
        res = SESSION.get(url, headers=headers, params=params, timeout=10)
        if res.status_code != 200:
            return holdings
        data = res.json()
        if data.get("rt_cd") != "0":
            return holdings

        for item in data.get("output2") or []:
            raw_ticker = str(item.get("stck_shrn_iscd") or "").strip()
            name = (item.get("hts_kor_isnm") or "").strip()
            qty = parse_quantity(item.get("etf_cu_unit_scrt_cnt"))
            if not raw_ticker and not name:
                continue

            holdings.append({
                "raw_ticker": raw_ticker,
                "name": name or raw_ticker,
                "quantity": qty
            })
        return holdings
    except Exception:
        return holdings


def get_etf_composition_wisereport(clean_ticker: str) -> List[Dict[str, Any]]:
    """WiseReport / Naver 금융: 해외/글로벌/일본 ETF 구성종목 및 계약수량(AGMT_STK_CNT) 수집"""
    url = f"https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd={clean_ticker}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    holdings = []
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return holdings

        match = re.search(r'var\s+CU_data\s*=\s*(\{.*?\});', r.text, re.DOTALL)
        if not match:
            return holdings

        data = json.loads(match.group(1))
        for item in data.get("grid_data", []):
            name = (item.get("STK_NM_KOR") or item.get("ITEM_NM") or "").strip()
            if not name or name in ["설정현금액", "원화현금", "USD현금", "외화예치금"]:
                continue

            raw_ticker = str(item.get("STK_CD") or item.get("CMP_CD") or "").strip()
            if raw_ticker.lower() in ["none", "null"]:
                raw_ticker = ""
            qty = parse_quantity(item.get("AGMT_STK_CNT"))

            holdings.append({
                "raw_ticker": raw_ticker,
                "name": name,
                "quantity": qty
            })
        return holdings
    except Exception:
        return holdings


# ==========================================
# 3. 노션 DB 인메모리 매칭 & 퍼지 유사도 엔진
# ==========================================
class StockMatchEngine:
    """
    [종목 매칭 엔진]
    1. 한국주식 (한투 API 추출):
       - API에서 직접 추출된 6자리 공식 종목코드(티커)를 100% 우선 적용
       - 투자주 DB 등록 여부만 확인하여 있으면 릴레이션 연결, 없으면 공백 유지 (마스터 DB 퍼지 매칭 불필요)

    2. 해외/일본/미국 주식 (티커 부재 또는 외국 종목):
       - 1순위: 투자주 DB에 이미 등록된 해외 종목(티커 또는 종목명)과 완전 일치 확인
       - 2순위: 해외주식에 한해 상장주식DB 전체(마스터 DB)의 해외 종목 풀과 정규화 퍼지(Fuzzy >= 0.80) 매칭하여 글로벌 티커 추출
       - 3순위: 미매칭 시 릴레이션 및 티커를 안전하게 공백 처리
    """
    def __init__(self, client: Any):
        self.client = client
        self.inv_ticker_to_page: Dict[str, Dict[str, str]] = {}
        self.inv_name_to_page: Dict[str, Dict[str, str]] = {}
        self.master_foreign_candidates: List[Dict[str, str]] = []
        self._load_cache()

    def _load_cache(self) -> None:
        # 1. 투자주 DB 로드 (국내/해외 투자 대상 종목 캐시)
        print(f"📦 [1/2] 투자주 DB({INVESTMENT_DB_ID}) 목록을 메모리에 로드합니다...", flush=True)
        count_inv = 0
        for page in paginate_database(self.client, INVESTMENT_DB_ID, page_size=100):
            pid = page["id"]
            props = page.get("properties", {})
            
            t_prop = props.get("티커", {}).get("title", [])
            ticker = t_prop[0]["plain_text"].strip().upper() if t_prop else ""
            
            name = ""
            for k in ["종목명", "이름", "Name"]:
                if k in props:
                    val = props[k]
                    if val.get("type") == "formula":
                        name = str(val.get("formula", {}).get("string") or "").strip()
                    elif val.get("type") == "rich_text" and val.get("rich_text"):
                        name = val["rich_text"][0]["plain_text"].strip()
                    elif val.get("type") == "title" and val.get("title"):
                        name = val["title"][0]["plain_text"].strip()

            item_info = {"id": pid, "ticker": ticker, "name": name}
            if ticker:
                clean_t = ticker.split(".")[0].strip().upper()
                self.inv_ticker_to_page[clean_t] = item_info
                self.inv_ticker_to_page[ticker] = item_info
            if name:
                self.inv_name_to_page[name] = item_info
                self.inv_name_to_page[name.replace(" ", "")] = item_info
            count_inv += 1
        print(f"   ✅ 투자주 DB 종목 {count_inv}개 로드 완료", flush=True)

        # 2. 상장주식DB 전체 (마스터 DB)에서 해외 종목 풀만 선별 로드
        if MASTER_DB_ID and MASTER_DB_ID != INVESTMENT_DB_ID:
            print(f"📦 [2/2] 상장주식DB 전체({MASTER_DB_ID})에서 해외/일본 종목 마스터 풀을 로드합니다...", flush=True)
            count_master_foreign = 0
            for page in paginate_database(self.client, MASTER_DB_ID, page_size=100):
                props = page.get("properties", {})
                t_prop = props.get("티커", {}).get("title", [])
                ticker = t_prop[0]["plain_text"].strip().upper() if t_prop else ""
                
                n_prop = props.get("종목명", {}).get("rich_text", [])
                name = n_prop[0]["plain_text"].strip() if n_prop else ""
                
                m_val = props.get("Market", {}).get("select")
                market = m_val["name"] if m_val else ""

                # 한국 종목코드(6자리 숫자 등)가 아니거나 해외 마켓 종목만 해외 후보군에 추가
                is_kr_stock = (market in ["KOSPI", "KOSDAQ", "KONEX", "ETF(KR)"] and ticker.isdigit() and len(ticker) == 6)

                if not is_kr_stock and ticker and name:
                    rel_inv = props.get("투자주 DB", {}).get("relation", [])
                    inv_id = rel_inv[0]["id"] if rel_inv else None
                    norm_n = normalize_company_name(name)
                    self.master_foreign_candidates.append({
                        "ticker": ticker,
                        "name": name,
                        "norm_name": norm_n,
                        "inv_id": inv_id
                    })
                    count_master_foreign += 1
            print(f"   ✅ 해외/일본 종목 마스터 풀 {count_master_foreign}개 로드 완료", flush=True)

    def _create_investment_page(self, ticker: str, name: str) -> Optional[str]:
        """투자주 DB에 존재하지 않는 경우 신규 페이지를 생성하고 인메모리 캐시에 즉시 등록"""
        if not ticker:
            return None
        try:
            props: Dict[str, Any] = {
                "티커": {"title": [{"text": {"content": ticker}}]},
            }
            if name:
                props["종목명"] = {"rich_text": [{"text": {"content": name}}]}

            new_page = self.client.pages.create(
                parent={"database_id": INVESTMENT_DB_ID},
                properties=props
            )
            new_id = new_page["id"]
            
            # 생성 즉시 인메모리 캐시에 등록하여 이후 루프에서의 중복 생성 방지
            item_info = {"id": new_id, "ticker": ticker, "name": name}
            clean_t = ticker.split(".")[0].strip().upper()
            self.inv_ticker_to_page[clean_t] = item_info
            self.inv_ticker_to_page[ticker] = item_info
            if name:
                self.inv_name_to_page[name] = item_info
                self.inv_name_to_page[name.replace(" ", "")] = item_info
                
            print(f"      ✨ [투자주 DB 자동등록] {name}({ticker}) 신규 등록 완료", flush=True)
            time.sleep(0.05)
            return new_id
        except Exception as exc:
            print(f"      ⚠️ [투자주 DB 등록 실패] {name}({ticker}): {exc}", flush=True)
            return None

    def match(self, raw_ticker: str, name: str) -> Tuple[Optional[str], str]:
        """
        종목 매칭:
        - 한국주식: API 추출 6자리 코드 기준, 투자주 DB에 없으면 신규 생성 후 릴레이션 연결
        - 해외주식: 마스터 DB 해외 종목 풀과 정규화 퍼지(유사도 80% 이상) 매칭 후 투자주 DB 미등록 시 자동 생성
        """
        t = raw_ticker.strip().upper()
        n = name.strip()

        # 한국 공식 종목코드 판별 (6자리 숫자 또는 0으로 시작하는 6자리 단축코드)
        is_kr_code = bool(re.match(r'^\d{6}$', t) or (len(t) == 6 and t.isalnum() and t[0] == '0'))

        # ========================================================
        # CASE A: 한국주식 (한투 API를 통해 6자리 코드가 명확히 추출된 경우)
        # ========================================================
        if is_kr_code:
            # 1. 투자주 DB에 이미 등록되어 있으면 기존 페이지 연결
            if t in self.inv_ticker_to_page:
                return self.inv_ticker_to_page[t]["id"], t
            if n in self.inv_name_to_page:
                return self.inv_name_to_page[n]["id"], t
            # 2. 투자주 DB에 없으면 신규 페이지 자동 생성 후 릴레이션 연결
            new_inv_id = self._create_investment_page(t, n)
            return new_inv_id, t

        # ========================================================
        # CASE B: 해외/일본/미국 주식 (티커가 없거나 한국 코드가 아닌 외국 종목)
        # ========================================================
        # 1. 투자주 DB에 이미 등록된 해외 종목인지 확인 (완전 일치)
        if t and t in self.inv_ticker_to_page:
            match_info = self.inv_ticker_to_page[t]
            return match_info["id"], match_info["ticker"]

        if n and n in self.inv_name_to_page:
            match_info = self.inv_name_to_page[n]
            return match_info["id"], match_info["ticker"]
        if n and n.replace(" ", "") in self.inv_name_to_page:
            match_info = self.inv_name_to_page[n.replace(" ", "")]
            return match_info["id"], match_info["ticker"]

        # 2. 해외주식에 한해 상장주식DB 전체 (마스터 DB) 해외 종목 풀과 퍼지(Fuzzy >= 0.80) 매칭
        if n and self.master_foreign_candidates:
            norm_target = normalize_company_name(n)
            if norm_target:
                best_ratio = 0.0
                best_cand = None
                for cand in self.master_foreign_candidates:
                    norm_c = cand["norm_name"]
                    # 2-1. 정규화 후 완전 일치 또는 4글자 이상 상호 포함 (100% 신뢰)
                    if norm_target == norm_c or (len(norm_target) >= 4 and (norm_target in norm_c or norm_c in norm_target)):
                        best_ratio = 1.0
                        best_cand = cand
                        break
                    
                    # 2-2. SequenceMatcher 유사도 계산 (임계값 0.80 이상)
                    ratio = difflib.SequenceMatcher(None, norm_target, norm_c).ratio()
                    if ratio > best_ratio and ratio >= 0.80:
                        best_ratio = ratio
                        best_cand = cand

                if best_cand:
                    inv_id = best_cand["inv_id"]
                    if not inv_id and best_cand["ticker"] in self.inv_ticker_to_page:
                        inv_id = self.inv_ticker_to_page[best_cand["ticker"]]["id"]
                    
                    # 마스터 DB와 매칭되었으나 투자주 DB에는 없는 경우 -> 자동 등록
                    if not inv_id and best_cand["ticker"]:
                        inv_id = self._create_investment_page(best_cand["ticker"], best_cand["name"])
                    
                    return inv_id, best_cand["ticker"]

        # 3. 미매칭 외국 종목: 원본에 유효한 해외 티커(예: AAPL)가 있으면 사용하고, 없으면 공백
        fallback_ticker = t if (re.match(r'^[A-Z0-9.\-_]{1,10}$', t) and not t.isdigit()) else ""
        return None, fallback_ticker


# ==========================================
# 4. 대상 ETF 식별 및 노션 동기화
# ==========================================
def get_target_etfs(client: Any) -> List[Dict[str, str]]:
    """ETF DB에서 'ETF(투자DB)' 릴레이션에 등록된 부모 ETF 페이지만 역스캔"""
    print(f"📋 ETF DB({ETF_DB_ID})에 등록된 모니터링 대상 ETF 목록을 스캔합니다...", flush=True)
    target_etfs: List[Dict[str, str]] = []
    parent_etf_ids: set = set()

    for page in paginate_database(client, ETF_DB_ID, page_size=100):
        props = page.get("properties", {})
        rel_list = props.get("ETF(투자DB)", {}).get("relation", [])
        for rel in rel_list:
            if rel.get("id"):
                parent_etf_ids.add(rel["id"])

    print(f"   🔍 발견된 모니터링 대상 부모 ETF 수: {len(parent_etf_ids)}개", flush=True)
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
                print(f"   🎯 대상 ETF: {name or clean_ticker} ({clean_ticker})", flush=True)
        except Exception as exc:
            print(f"   ⚠️ 부모 ETF({page_id}) 조회 실패: {exc}", flush=True)

    print(f"   ✅ 총 {len(target_etfs)}개 대상 ETF 확정 완료.\n", flush=True)
    return target_etfs


def archive_existing_etf_holdings(client: Any, etf_page_id: str) -> None:
    """해당 ETF의 기존 과거 데이터를 전량 아카이브"""
    pages_to_archive = []
    start_cursor = None
    while True:
        try:
            params = {
                "database_id": ETF_DB_ID,
                "filter": {"property": "ETF(투자DB)", "relation": {"contains": etf_page_id}},
                "page_size": 100
            }
            if start_cursor:
                params["start_cursor"] = start_cursor
            res = client.databases.query(**params)
            for page in res.get("results", []):
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
# 5. 메인 파이프라인
# ==========================================
def main() -> None:
    print("🚀 [ETF 구성종목 자동 수집 및 동기화 파이프라인] 가동 시작", flush=True)
    notion_client = build_notion_client(NOTION_TOKEN)

    kis_token = get_kis_token()
    if not kis_token:
        print("❌ KIS 토큰 발급 실패로 중단합니다.", flush=True)
        return

    db_cache = StockMatchEngine(notion_client)
    target_etfs = get_target_etfs(notion_client)

    if not target_etfs:
        print("⚠️ 갱신 대상 ETF가 ETF DB에 등록되어 있지 않습니다.", flush=True)
        return

    now_kst = kst_isoformat()

    for idx, target in enumerate(target_etfs, 1):
        etf_page_id = target["etf_page_id"]
        etf_ticker = target["ticker"]
        etf_name = target["name"]

        print(f"\n[{idx}/{len(target_etfs)}] 🔄 수집 진행: {etf_name}({etf_ticker})...", flush=True)
        
        # 1. KIS 실전 API + WiseReport 하이브리드 수집
        kis_items = get_etf_composition_kis(kis_token, etf_ticker)
        wise_items = get_etf_composition_wisereport(etf_ticker)
        
        raw_holdings = kis_items if kis_items else wise_items
        # KIS에 없는 해외/일본 종목 병합
        if kis_items and wise_items:
            existing_names = {it["name"].replace(" ", "") for it in kis_items}
            for w in wise_items:
                if w["name"].replace(" ", "") not in existing_names:
                    raw_holdings.append(w)

        if not raw_holdings:
            print(f"   ⚠️ {etf_name} 구성종목 수집 결과 없음 (건너뜀)", flush=True)
            continue

        # 2. 투자주 DB & 마스터 DB 퍼지 매칭 (한국주식은 API 티커 우선 / 해외/일본 주식은 마스터 DB 매칭)
        items_to_insert = []
        for h in raw_holdings:
            stock_id, matched_ticker = db_cache.match(h["raw_ticker"], h["name"])
            items_to_insert.append({
                "name": h["name"],
                "ticker": matched_ticker,
                "stock_id": stock_id,
                "quantity": h["quantity"]
            })

        # 3. 노션 ETF DB 갱신 (과거 삭제 후 신규 등록)
        print(f"   🧹 기존 과거 데이터 정리 중...", flush=True)
        archive_existing_etf_holdings(notion_client, etf_page_id)

        print(f"   📝 최신 {len(items_to_insert)}개 구성종목 입력 중 (수량 열 반영)...", flush=True)
        success_count = 0
        for item in items_to_insert:
            props: Dict[str, Any] = {
                "이름": {"title": [{"text": {"content": item["name"]}}]},
                "ETF(투자DB)": {"relation": [{"id": etf_page_id}]},
                "업데이트": {"date": {"start": now_kst}}
            }
            # 티커가 있으면 기입, 없으면 공백
            if item["ticker"]:
                props["티커"] = {"rich_text": [{"text": {"content": item["ticker"]}}]}
            # 투자주 DB에 매칭된 경우에만 종목(투자DB) 릴레이션 연결
            if item["stock_id"]:
                props["종목(투자DB)"] = {"relation": [{"id": item["stock_id"]}]}
            # 수량이 있으면 수량 열에 숫자 기입
            if item["quantity"] is not None:
                props["수량"] = {"number": item["quantity"]}

            try:
                notion_client.pages.create(parent={"database_id": ETF_DB_ID}, properties=props)
                success_count += 1
                time.sleep(0.05)
            except Exception as exc:
                print(f"   ❌ {item['name']} 등록 실패: {exc}", flush=True)

        print(f"   ✅ [{etf_name}] 완료 ({success_count}/{len(items_to_insert)}건 입력)", flush=True)

    print("\n✨ 모든 관리 대상 ETF 갱신 작업이 성공적으로 완료되었습니다.", flush=True)


if __name__ == "__main__":
    main()