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
    get_kis_auth_context,
    extract_short_brand_name,
    search_foreign_ticker,
    get_http_session,
    is_kr_ticker,
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

SESSION = get_http_session()



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
# 2. 한국투자증권 실전/모의 API & WiseReport 수집부
# ==========================================
def get_etf_composition_kis(kis_ctx: Optional[Dict[str, Any]], clean_ticker: str) -> List[Dict[str, Any]]:
    """한투 API (모의/실전 자동 Fallback): 한국 ETF 구성종목 코드 및 CU 수량 수집"""
    if not kis_ctx or not isinstance(kis_ctx, dict) or not kis_ctx.get("token"):
        return []
    url = f"{kis_ctx['url_base']}/uapi/etfetn/v1/quotations/inquire-component-stock-price"
    headers = {
        "authorization": f"Bearer {kis_ctx['token']}",
        "appkey": kis_ctx["app_key"],
        "appsecret": kis_ctx["app_secret"],
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
        if not name or len(name) < 2:
            return None

        if name in self.online_search_cache:
            return self.online_search_cache[name]

        best = search_foreign_ticker(name)
        if best:
            self.online_search_cache[name] = best
            print(f"      🔍 [글로벌 검색] '{name}' ➔ 공식 티커: {best[0]} ({best[1]})", flush=True)
            return best

        self.online_search_cache[name] = None
        return None

    def match(self, raw_ticker: str, name: str) -> Tuple[Optional[str], str, str]:
        """종목 매칭 -> (투자주ID, 확정티커, 짧고간결한브랜드명) 반환"""
        t = raw_ticker.strip().upper()
        n = name.strip()
        is_kr_code = is_kr_ticker(t)

        # ==========================================
        # CASE A: 한국 주식 (KRX 6자리 코드 및 .KS/.KQ)
        # ==========================================
        if is_kr_code:
            if t in self.inv_ticker_to_page:
                return self.inv_ticker_to_page[t]["id"], t, self.inv_ticker_to_page[t]["name"] or n
            if n in self.inv_name_to_page:
                return self.inv_name_to_page[n]["id"], t, self.inv_name_to_page[n]["name"] or n
            
            new_id = self._create_investment_page(t, n)
            return new_id, t, n

        # ==========================================
        # CASE B: 해외 / 일본 주식 (Yahoo 검색 및 ADR 최우선)
        # ==========================================
        # 1. 투자주 DB 완전 일치 확인
        if t and t in self.inv_ticker_to_page:
            info = self.inv_ticker_to_page[t]
            return info["id"], info["ticker"], extract_short_brand_name(info["name"] or n)
        if n and n in self.inv_name_to_page:
            info = self.inv_name_to_page[n]
            return info["id"], info["ticker"], extract_short_brand_name(info["name"] or n)

        # 2. Yahoo Finance 최우선 검색 (ADR 및 메이저 증시 자동 탐색)
        matched_ticker = ""
        matched_name = ""
        if n:
            search_res = self._search_foreign_ticker(n)
            if search_res:
                matched_ticker, matched_name = search_res

        # 3. 티커 확인 시: 투자주 DB 매칭 및 신규 생성
        if matched_ticker:
            clean_brand = extract_short_brand_name(matched_name or n)
            clean_t = matched_ticker.split(".")[0].strip().upper()
            inv_id = None

            if clean_t in self.inv_ticker_to_page:
                inv_id = self.inv_ticker_to_page[clean_t]["id"]
            elif matched_ticker in self.inv_ticker_to_page:
                inv_id = self.inv_ticker_to_page[matched_ticker]["id"]
            elif matched_name and matched_name in self.inv_name_to_page:
                inv_id = self.inv_name_to_page[matched_name]["id"]
            else:
                inv_id = self._create_investment_page(matched_ticker, clean_brand)

            return inv_id, matched_ticker, clean_brand

        # 4. 미매칭 fallback (영문/숫자 티커 유지)
        short_brand = extract_short_brand_name(n)
        fallback_t = t if (re.match(r'^[A-Z0-9.\-_]{1,10}$', t) and not t.isdigit()) else ""
        return None, fallback_t, short_brand


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

    kis_ctx = get_kis_auth_context()
    if not kis_ctx:
        print("⚠️ KIS 토큰 발급 실패: WiseReport 수집 전용 모드로 진행합니다.", flush=True)

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
        kis_items = get_etf_composition_kis(kis_ctx, etf_ticker) if kis_ctx else []
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