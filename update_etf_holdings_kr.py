import os
import sys
import time
import json
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Any, List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

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
# 1. 환경 변수 및 공통 세션 설정
# ==========================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
INVESTMENT_DB_ID = (
    os.environ.get("DATABASE_ID")
    or os.environ.get("INVESTMENT_DB_ID")
    or os.environ.get("INVESTMENT_DATABASE_ID")
    or get_env_var("DATABASE_ID")
)
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID") or os.environ.get("MASTER_DB_ID")
ETF_DB_ID = os.environ.get("ETF_DB_ID") or os.environ.get("ETF_DATABASE_ID") or get_env_var("ETF_DB_ID")

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
        if not s or s in ["null", "-", "none"]:
            return None
        num = float(s)
        return num if num > 0 else None
    except (ValueError, TypeError):
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


def normalize_company_name(name: str) -> str:
    """금융 API 검색용 정규화 쿼리 생성"""
    if not name:
        return ""
    n = name.upper()
    n = re.sub(r'[\(\)\[\],\.\-\/\:\'\"]', ' ', n)
    # 약어 확장
    abbrev_map = {
        r'\bMFG\b': 'MANUFACTURING', r'\bIND\b': 'INDUSTRIES', r'\bTECH\b': 'TECHNOLOGY',
        r'\bSYS\b': 'SYSTEMS', r'\bELEC\b': 'ELECTRIC', r'\bSEMICON\b': 'SEMICONDUCTOR'
    }
    for pat, rep in abbrev_map.items():
        n = re.sub(pat, rep, n)

    remove_words = [
        r'\bCL(ASS)?\s*[A-Z0-9]?\b', r'\bORD(INARY)?\b', r'\bREG(ISTERED)?\b',
        r'\bSP\s*ADR\b', r'\bADR\b', r'\bADS\b', r'\bNV\b', r'\bDE\b',
        r'\bCORP(ORATION)?\b', r'\bINC(ORPORATED)?\b', r'\bLTD\b', r'\bCO\b',
        r'\bHOLDINGS?\b', r'\bGROUP\b', r'\bUSA\b', r'\bCOM\b', r'\bNY\b',
    ]
    for p in remove_words:
        n = re.sub(p, ' ', n)
    return " ".join([t for t in n.split() if len(t) >= 2])


# ==========================================
# 2. 한국투자증권 실전 API & WiseReport 수집부
# ==========================================
def get_kis_token() -> Optional[str]:
    """KIS 실전투자 접속 토큰 발급"""
    url = f"{KIS_DOMAIN}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    try:
        res = requests.post(url, json=body, timeout=10)
        res.raise_for_status()
        token = res.json().get("access_token")
        if token:
            print("🔑 KIS 실전투자 토큰 발급 성공", flush=True)
            return token
    except Exception as exc:
        print(f"❌ KIS 토큰 발급 실패: {exc}", flush=True)
    return None


def get_etf_composition_kis(token: str, clean_ticker: str) -> List[Dict[str, Any]]:
    """한투 실전 API: 한국 ETF 구성종목 코드 및 CU 수량 수집"""
    url = f"{KIS_DOMAIN}/uapi/etfetn/v1/quotations/inquire-component-stock-price"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST121600C0",
        "custtype": "P"
    }
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": clean_ticker, "FID_COND_SCR_DIV_CODE": "11216"}
    holdings = []
    try:
        res = SESSION.get(url, headers=headers, params=params, timeout=10)
        if res.status_code == 200:
            data = res.json()
            for item in data.get("output2") or []:
                raw_ticker = str(item.get("stck_shrn_iscd") or "").strip()
                name = (item.get("hts_kor_isnm") or "").strip()
                qty = parse_quantity(item.get("etf_cu_unit_scrt_cnt"))
                if raw_ticker or name:
                    holdings.append({"raw_ticker": raw_ticker, "name": name or raw_ticker, "quantity": qty})
    except Exception:
        pass
    return holdings


def get_etf_composition_wisereport(clean_ticker: str) -> List[Dict[str, Any]]:
    """WiseReport: 해외/글로벌/일본 ETF 구성종목 및 계약수량 수집"""
    url = f"https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd={clean_ticker}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    holdings = []
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            match = re.search(r'var\s+CU_data\s*=\s*(\{.*?\});', r.text, re.DOTALL)
            if match:
                data = json.loads(match.group(1))
                for item in data.get("grid_data", []):
                    name = (item.get("STK_NM_KOR") or item.get("ITEM_NM") or "").strip()
                    if not name or name in ["설정현금액", "원화현금", "USD현금", "외화예치금"]:
                        continue
                    raw_ticker = str(item.get("STK_CD") or item.get("CMP_CD") or "").strip()
                    if raw_ticker.lower() in ["none", "null"]:
                        raw_ticker = ""
                    qty = parse_quantity(item.get("AGMT_STK_CNT"))
                    holdings.append({"raw_ticker": raw_ticker, "name": name, "quantity": qty})
    except Exception:
        pass
    return holdings


# ==========================================
# 3. 고성능 종목 매칭 & 투자주 DB 자동등록 엔진
# ==========================================
class StockMatchEngine:
    def __init__(self, client: Any):
        self.client = client
        self.inv_ticker_to_page: Dict[str, Dict[str, str]] = {}
        self.inv_name_to_page: Dict[str, Dict[str, str]] = {}
        self.online_search_cache: Dict[str, Optional[Tuple[str, str]]] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        print(f"📦 투자주 DB({INVESTMENT_DB_ID}) 목록을 메모리에 로드합니다...", flush=True)
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
                    elif val.get("type") in ["rich_text", "title"] and val.get(val["type"]):
                        name = val[val["type"]][0]["plain_text"].strip()

            item_info = {"id": pid, "ticker": ticker, "name": name}
            if ticker:
                self.inv_ticker_to_page[ticker.split(".")[0].strip().upper()] = item_info
                self.inv_ticker_to_page[ticker] = item_info
            if name:
                self.inv_name_to_page[name] = item_info
                self.inv_name_to_page[name.replace(" ", "")] = item_info
            count_inv += 1
        print(f"   ✅ 투자주 DB 종목 {count_inv}개 캐싱 완료", flush=True)

    def _create_investment_page(self, ticker: str, name: str) -> Optional[str]:
        """투자주 DB에 신규 페이지 생성 후 인메모리 캐시 즉시 갱신"""
        if not ticker:
            return None
        try:
            props = {
                "티커": {"title": [{"text": {"content": ticker}}]},
                "종목명": {"rich_text": [{"text": {"content": name}}]} if name else {}
            }
            props = {k: v for k, v in props.items() if v}
            new_page = self.client.pages.create(parent={"database_id": INVESTMENT_DB_ID}, properties=props)
            new_id = new_page["id"]

            item_info = {"id": new_id, "ticker": ticker, "name": name}
            self.inv_ticker_to_page[ticker.split(".")[0].strip().upper()] = item_info
            self.inv_ticker_to_page[ticker] = item_info
            if name:
                self.inv_name_to_page[name] = item_info
                self.inv_name_to_page[name.replace(" ", "")] = item_info

            print(f"      ✨ [투자주 DB 자동등록] {name}({ticker}) 완료", flush=True)
            time.sleep(0.02)
            return new_id
        except Exception as exc:
            print(f"      ⚠️ [투자주 DB 등록 실패] {name}({ticker}): {exc}", flush=True)
            return None

    def _search_foreign_ticker(self, name: str) -> Optional[Tuple[str, str]]:
        """Yahoo Finance 검색: 미국 메이저 거래소 & ADR 최우선 탐색"""
        norm_name = normalize_company_name(name)
        search_query = norm_name or name
        if not search_query or len(search_query) < 2:
            return None

        if search_query in self.online_search_cache:
            return self.online_search_cache[search_query]

        def _pick_best(quotes: List[Dict[str, Any]], query_str: str) -> Optional[Tuple[str, str]]:
            clean_q = normalize_company_name(query_str)
            us_major, jp_pick, other_pick = None, None, None

            for q in quotes:
                sym = q.get("symbol", "").strip().upper()
                typ = q.get("quoteType", "")
                exch = (q.get("exchange") or q.get("exchDisp") or "").upper()
                sname = (q.get("shortname") or q.get("longname") or "").strip()
                fname = f"{sname} {q.get('longname', '')}".upper()

                if typ not in ["EQUITY", "ETF"] or sym.endswith("-USD"):
                    continue

                # 검색어와 일치성 검증 (오매칭 배제)
                is_match = (
                    sname.upper().startswith(clean_q)
                    or fname.startswith(clean_q)
                    or bool(re.search(r'\b' + re.escape(clean_q) + r'\b', fname))
                    or (clean_q == sym or clean_q.replace(" ", "") == sym)
                )
                if not is_match and len(quotes) > 1:
                    continue

                # 1. 미국 메이저 거래소 (NASDAQ, NYSE) 및 미국 ADR (점 없는 티커) -> 최우선
                if (exch in ["NMS", "NYQ", "NASDAQ", "NYSE", "NGM", "NCM", "BTS", "BATS"] or "NASDAQ" in exch or "NYSE" in exch) and "." not in sym:
                    if not us_major:
                        us_major = (sym, sname or query_str)
                # 2. 일본 도쿄 증시 (.T)
                elif sym.endswith(".T") or exch in ["JPX", "TYO"] or "TOKYO" in exch:
                    if not sym.endswith(".T") and re.match(r'^\d{4}$', sym):
                        sym = f"{sym}.T"
                    if not jp_pick:
                        jp_pick = (sym, sname or query_str)
                # 3. 기타 거래소
                elif not other_pick:
                    other_pick = (sym, sname or query_str)

            return us_major or jp_pick or other_pick

        try:
            url = "https://query2.finance.yahoo.com/v1/finance/search"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            
            # 1차: 정규화 기업명으로 검색
            res = requests.get(url, params={"q": search_query, "quotesCount": 8, "newsCount": 0}, headers=headers, timeout=5)
            best = _pick_best(res.json().get("quotes", []), search_query) if res.status_code == 200 else None

            # 2차: 미발견 시 원본명 보조 검색
            if not best and name != search_query:
                res2 = requests.get(url, params={"q": name, "quotesCount": 8, "newsCount": 0}, headers=headers, timeout=5)
                if res2.status_code == 200:
                    best = _pick_best(res2.json().get("quotes", []), name)

            if best:
                self.online_search_cache[search_query] = best
                print(f"      🔍 [글로벌 검색] '{name}' ➔ 공식 티커: {best[0]} ({best[1]})", flush=True)
                return best

            self.online_search_cache[search_query] = None
            return None
        except Exception:
            self.online_search_cache[search_query] = None
            return None

    def match(self, raw_ticker: str, name: str) -> Tuple[Optional[str], str, str]:
        """종목 매칭 -> (투자주ID, 확정티커, 짧고간결한브랜드명) 반환"""
        t = raw_ticker.strip().upper()
        n = name.strip()
        is_kr_code = bool(re.match(r'^\d{6}$', t) or (len(t) == 6 and t.isalnum() and t[0] == '0'))

        # ==========================================
        # CASE A: 한국 주식 (6자리 공식 코드)
        # ==========================================
        if is_kr_code:
            if t in self.inv_ticker_to_page:
                return self.inv_ticker_to_page[t]["id"], t, self.inv_ticker_to_page[t]["name"] or n
            if n in self.inv_name_to_page:
                return self.inv_name_to_page[n]["id"], t, self.inv_name_to_page[n]["name"] or n
            
            new_id = self._create_investment_page(t, n)
            return new_id, t, n

        # ==========================================
        # CASE B: 해외 / 일본 주식
        # ==========================================
        # 1. 투자주 DB 완전 일치 확인
        if t and t in self.inv_ticker_to_page:
            info = self.inv_ticker_to_page[t]
            return info["id"], info["ticker"], extract_short_brand_name(info["name"] or n)
        if n and n in self.inv_name_to_page:
            info = self.inv_name_to_page[n]
            return info["id"], info["ticker"], extract_short_brand_name(info["name"] or n)

        # 2. Yahoo Finance 최우선 검색
        matched_ticker = ""
        matched_name = ""
        if n:
            search_res = self._search_foreign_ticker(n)
            if search_res:
                matched_ticker, matched_name = search_res

        # 3. 티커 확인 시: 투자주 DB 매칭 및 신규 생성
        if matched_ticker:
            short_brand = extract_short_brand_name(matched_name or n)
            clean_t = matched_ticker.split(".")[0].strip().upper()
            inv_id = None

            if clean_t in self.inv_ticker_to_page:
                inv_id = self.inv_ticker_to_page[clean_t]["id"]
            elif matched_ticker in self.inv_ticker_to_page:
                inv_id = self.inv_ticker_to_page[matched_ticker]["id"]
            elif matched_name and matched_name in self.inv_name_to_page:
                inv_id = self.inv_name_to_page[matched_name]["id"]
            else:
                inv_id = self._create_investment_page(matched_ticker, short_brand)

            return inv_id, matched_ticker, short_brand

        # 4. 미매칭 fallback
        fallback_t = t if (re.match(r'^[A-Z0-9.\-_]{1,10}$', t) and not t.isdigit()) else ""
        return None, fallback_t, extract_short_brand_name(n)


# ==========================================
# 4. 대상 ETF 식별 및 노션 동기화
# ==========================================
def get_target_etfs(client: Any) -> List[Dict[str, str]]:
    """ETF DB에서 'ETF(투자DB)' 릴레이션에 등록된 부모 ETF 페이지만 역스캔"""
    print(f"📋 ETF DB({ETF_DB_ID})에서 대상 부모 ETF를 스캔합니다...", flush=True)
    target_etfs: List[Dict[str, str]] = []
    parent_ids: set = set()

    for page in paginate_database(client, ETF_DB_ID, page_size=100):
        for rel in page.get("properties", {}).get("ETF(투자DB)", {}).get("relation", []):
            if rel.get("id"):
                parent_ids.add(rel["id"])

    print(f"   🔍 대상 부모 ETF 수: {len(parent_ids)}개", flush=True)
    for pid in parent_ids:
        try:
            page = client.pages.retrieve(page_id=pid)
            props = page.get("properties", {})
            ticker = get_page_text(props, ["티커", "Ticker"])
            name = get_page_text(props, ["종목명", "이름", "Title"])
            if ticker:
                clean_t = ticker.split(".")[0].strip().upper()
                target_etfs.append({"etf_page_id": pid, "ticker": clean_t, "name": name or clean_t})
                print(f"   🎯 대상 ETF: {name or clean_t} ({clean_t})", flush=True)
        except Exception:
            pass

    print(f"   ✅ 총 {len(target_etfs)}개 대상 ETF 확정 완료.\n", flush=True)
    return target_etfs


def archive_existing_etf_holdings(client: Any, etf_page_id: str) -> None:
    """과거 구성종목 전량 병렬 아카이브 (속도 5배 향상)"""
    page_ids = []
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
            page_ids.extend([p["id"] for p in res.get("results", [])])
            if not res.get("has_more"):
                break
            start_cursor = res.get("next_cursor")
        except Exception:
            break

    if page_ids:
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(lambda pid: client.pages.update(page_id=pid, archived=True), page_ids)


# ==========================================
# 5. 메인 파이프라인
# ==========================================
def main() -> None:
    print("🚀 [ETF 구성종목 자동 수집 및 동기화 파이프라인] 가동 시작", flush=True)
    notion = build_notion_client(NOTION_TOKEN)

    kis_token = get_kis_token()
    if not kis_token:
        print("❌ KIS 토큰 발급 실패로 중단합니다.", flush=True)
        return

    db_cache = StockMatchEngine(notion)
    target_etfs = get_target_etfs(notion)
    if not target_etfs:
        print("⚠️ 갱신 대상 ETF가 없습니다.", flush=True)
        return

    now_kst = kst_isoformat()

    for idx, target in enumerate(target_etfs, 1):
        etf_page_id = target["etf_page_id"]
        etf_ticker = target["ticker"]
        etf_name = target["name"]

        print(f"\n[{idx}/{len(target_etfs)}] 🔄 수집 진행: {etf_name}({etf_ticker})...", flush=True)
        kis_items = get_etf_composition_kis(kis_token, etf_ticker)
        wise_items = get_etf_composition_wisereport(etf_ticker)
        
        raw_holdings = kis_items if kis_items else wise_items
        if kis_items and wise_items:
            existing = {it["name"].replace(" ", "") for it in kis_items}
            for w in wise_items:
                if w["name"].replace(" ", "") not in existing:
                    raw_holdings.append(w)

        if not raw_holdings:
            print(f"   ⚠️ {etf_name} 구성종목 없음 (건너뜀)", flush=True)
            continue

        # 종목 매칭 및 간결한 브랜드명 추출
        items_to_insert = []
        for h in raw_holdings:
            stock_id, matched_ticker, short_brand = db_cache.match(h["raw_ticker"], h["name"])
            items_to_insert.append({
                "name": short_brand,
                "ticker": matched_ticker,
                "stock_id": stock_id,
                "quantity": h["quantity"]
            })

        # 과거 데이터 아카이브 및 최신 데이터 등록
        print(f"   🧹 기존 과거 데이터 정리 중...", flush=True)
        archive_existing_etf_holdings(notion, etf_page_id)

        print(f"   📝 최신 {len(items_to_insert)}개 구성종목 입력 중...", flush=True)
        success = 0
        for item in items_to_insert:
            props = {
                "이름": {"title": [{"text": {"content": item["name"]}}]},
                "ETF(투자DB)": {"relation": [{"id": etf_page_id}]},
                "업데이트": {"date": {"start": now_kst}}
            }
            if item["ticker"]:
                props["티커"] = {"rich_text": [{"text": {"content": item["ticker"]}}]}
            if item["stock_id"]:
                props["종목(투자DB)"] = {"relation": [{"id": item["stock_id"]}]}
            if item["quantity"] is not None:
                props["수량"] = {"number": item["quantity"]}

            try:
                notion.pages.create(parent={"database_id": ETF_DB_ID}, properties=props)
                success += 1
                time.sleep(0.02)
            except Exception as exc:
                print(f"   ❌ {item['name']} 등록 실패: {exc}", flush=True)

        print(f"   ✅ [{etf_name}] 완료 ({success}/{len(items_to_insert)}건 입력)", flush=True)

    print("\n✨ 모든 관리 대상 ETF 갱신 작업이 성공적으로 완료되었습니다.", flush=True)


if __name__ == "__main__":
    main()