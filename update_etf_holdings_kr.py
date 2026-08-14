import os
import sys
import time
import json
import re
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
INVESTMENT_DB_ID = os.environ.get("DATABASE_ID") or get_env_var("DATABASE_ID")
ETF_DB_ID = get_env_var("ETF_DB_ID")

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
        res = SESSION.post(url, json=body, timeout=10)
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
    """한투 실전 API: ETF 구성종목 및 수량(etf_cu_unit_scrt_cnt) 수집"""
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
    """WiseReport / Naver 금융: 해외/글로벌 ETF 구성종목 및 계약수량(AGMT_STK_CNT) 수집"""
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
# 3. 투자주 DB 경량 인메모리 매칭 캐시
# ==========================================
class InvestmentDBCache:
    """투자주 DB 종목을 사전 로드하여 1:1 매칭 (미등록 종목은 신규 생성하지 않고 None 반환)"""
    def __init__(self, client: Any):
        self.client = client
        self.ticker_to_page: Dict[str, Dict[str, str]] = {}
        self.name_to_page: Dict[str, Dict[str, str]] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        print(f"📦 투자주 DB({INVESTMENT_DB_ID}) 목록을 메모리에 로드합니다...", flush=True)
        count = 0
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
                self.ticker_to_page[clean_t] = item_info
                self.ticker_to_page[ticker] = item_info
            if name:
                self.name_to_page[name] = item_info
                self.name_to_page[name.replace(" ", "")] = item_info
            count += 1
        print(f"   ✅ 총 {count}개 투자주 DB 종목 로드 완료", flush=True)

    def match(self, raw_ticker: str, name: str) -> Tuple[Optional[str], str]:
        """
        투자주 DB에 존재하면 (page_id, 등록된_티커) 반환.
        없으면 (None, 원본티커 또는 공백) 반환 (신규 생성 X).
        """
        t = raw_ticker.strip().upper()
        n = name.strip()

        # 1. 티커 매칭
        if t and t in self.ticker_to_page:
            match_info = self.ticker_to_page[t]
            return match_info["id"], match_info["ticker"]

        # 2. 종목명 매칭
        if n and n in self.name_to_page:
            match_info = self.name_to_page[n]
            return match_info["id"], match_info["ticker"]
        if n and n.replace(" ", "") in self.name_to_page:
            match_info = self.name_to_page[n.replace(" ", "")]
            return match_info["id"], match_info["ticker"]

        # 3. 미매칭 시: 투자주 릴레이션은 None, 티커는 유효한 6자리/영문 코드가 있으면 넣고 없으면 공백
        fallback_ticker = t if (re.match(r'^[A-Z0-9.\-_]{1,10}$', t) and not t.isdigit() or len(t) == 6) else ""
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


def get_etf_quantity_prop_name(client: Any) -> str:
    """ETF DB에서 수량 열 이름 확인 ('수량' 우선, 없으면 '비중')"""
    try:
        db = client.databases.retrieve(database_id=ETF_DB_ID)
        props = db.get("properties", {})
        if "수량" in props:
            return "수량"
        if "비중" in props:
            return "비중"
    except Exception:
        pass
    return "수량"


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

    qty_prop_name = get_etf_quantity_prop_name(notion_client)
    db_cache = InvestmentDBCache(notion_client)
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
        # KIS에 없는 해외종목 병합
        if kis_items and wise_items:
            existing_names = {it["name"].replace(" ", "") for it in kis_items}
            for w in wise_items:
                if w["name"].replace(" ", "") not in existing_names:
                    raw_holdings.append(w)

        if not raw_holdings:
            print(f"   ⚠️ {etf_name} 구성종목 수집 결과 없음 (건너뜀)", flush=True)
            continue

        # 2. 투자주 DB 매칭 (있으면 연결, 없으면 공백)
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

        print(f"   📝 최신 {len(items_to_insert)}개 구성종목 입력 중 (열: {qty_prop_name})...", flush=True)
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
                props[qty_prop_name] = {"number": item["quantity"]}

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