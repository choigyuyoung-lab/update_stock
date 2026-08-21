"""
update_etf_holdings_kr.py
==========================
국내 상장 ETF의 구성종목(Holdings/PDF) 및 편입 수량을 수집하여
노션(Notion)의 ETF 구성종목 DB에 지능형 증분 동기화(Upsert)를 수행합니다.
- 데이터 소스: 한국투자증권(KIS) Open API + WiseReport
- 필터링: 선물/옵션/스왑/현금 등 비주식 파생자산 원천 제외
- 동기화: 수량 변경 종목 수정(Update), 신규 종목 생성(Create), 편출 종목 아카이브(Archive)
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import json
import re
from typing import Any, List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
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
    set_page_date_property,
    get_kis_auth_context,
    extract_short_brand_name,
    search_foreign_ticker,
    get_http_session,
    is_kr_ticker,
)


# ==============================================================================
# 1. 환경 변수 및 공통 세션 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
INVESTMENT_DB_ID = (
    os.environ.get("DATABASE_ID")
    or os.environ.get("INVESTMENT_DB_ID")
    or os.environ.get("INVESTMENT_DATABASE_ID")
    or get_env_var("DATABASE_ID")
)
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID") or os.environ.get("MASTER_DB_ID")
ETF_DB_ID = os.environ.get("ETF_DB_ID") or os.environ.get("ETF_DATABASE_ID") or get_env_var("ETF_DB_ID")
BENCHMARK_DB_ID = os.environ.get("BENCHMARK_DATABASE_ID") or os.environ.get("BENCHMARK_DB_ID")

SESSION = get_http_session()


# ==============================================================================
# 2. 데이터 정제 및 파생자산 필터링
# ==============================================================================
def parse_quantity(val: Any) -> Optional[float]:
    """수량 문자열/숫자를 안전하게 float로 파싱합니다."""
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


def is_derivative_or_cash(name: str, raw_ticker: str = "") -> bool:
    """선물, 옵션, 스왑, 현금, 금리상품 등 비주식 파생자산을 필터링합니다."""
    if not name:
        return True
    n = name.strip()
    n_clean = n.replace(" ", "").upper()
    t = (raw_ticker or "").strip().upper()

    # 1. 한글 및 명확한 현금/예치금/금리형 자산
    cash_keywords = [
        "설정현금액", "원화현금", "USD현금", "외화예치금", "예탁금", "미수금", "예치금",
        "KOFR", "CD금리", "SOFR", "콜론", "RP형", "원화RP", "외화RP"
    ]
    for kw in cash_keywords:
        if kw.upper() in n_clean:
            return True

    # 단독 현금/RP (단어 경계 체크: CORP 등의 영문 기업명 부분 일치 원천 방지)
    if re.search(r'\b(CASH|RP|MMF)\b', n, re.IGNORECASE) or n_clean in ["CASH", "RP", "현금"]:
        return True

    # 2. 파생상품 (선물, 옵션, 스왑 등)
    if re.search(r'(선물|위클리|콜옵션|풋옵션|옵션|스왑|\bSWAP\b|\bFUTURES\b|\bFUT\b|\bOPTION\b)', n, re.IGNORECASE):
        return True

    # 3. 정규식 패턴 (연월 만기 선물/옵션: 2026 09 SK하이닉스개별선물, 코스피위클리M 2608W3 등)
    patterns = [
        r'^\d{4}\s*\d{2}\s*.*선물',
        r'코스피\s*위클리',
        r'KOSPI\s*위클리',
        r'코스닥\s*선물',
        r'KOSDAQ\s*선물',
        r'미국\s*달러\s*선물',
    ]
    for pat in patterns:
        if re.search(pat, n, re.IGNORECASE):
            return True

    return False


# ==============================================================================
# 3. 한국투자증권 실전/모의 API & WiseReport 수집부
# ==============================================================================
def get_etf_composition_kis(kis_ctx: Optional[Dict[str, Any]], clean_ticker: str) -> List[Dict[str, Any]]:
    """한투 API (모의/실전 자동 Fallback): 한국 ETF 구성종목 코드 및 CU 수량 수집 (선물/현금 제외)"""
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
                if is_derivative_or_cash(name, raw_ticker):
                    continue
                qty = parse_quantity(item.get("etf_cu_unit_scrt_cnt"))
                if (raw_ticker or name) and qty is not None and qty > 0:
                    holdings.append({"raw_ticker": raw_ticker, "name": name or raw_ticker, "quantity": qty})
    except Exception:
        pass
    return holdings


def get_etf_composition_wisereport(clean_ticker: str) -> List[Dict[str, Any]]:
    """WiseReport: 해외/글로벌/일본 ETF 구성종목 및 계약수량 수집 (선물/현금 제외)"""
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
                    raw_ticker = str(item.get("STK_CD") or item.get("CMP_CD") or "").strip()
                    if is_derivative_or_cash(name, raw_ticker):
                        continue
                    if raw_ticker.lower() in ["none", "null"]:
                        raw_ticker = ""
                    qty = parse_quantity(item.get("AGMT_STK_CNT"))
                    if qty is not None and qty > 0:
                        holdings.append({"raw_ticker": raw_ticker, "name": name, "quantity": qty})
    except Exception:
        pass
    return holdings


# ==============================================================================
# 4. 고성능 종목 매칭 & 투자주 DB 자동등록 엔진
# ==============================================================================
class StockMatchEngine:
    def __init__(self, client: Any):
        self.client = client
        self.inv_ticker_to_page: Dict[str, Dict[str, str]] = {}
        self.inv_name_to_page: Dict[str, Dict[str, str]] = {}
        self.inv_id_to_page: Dict[str, Dict[str, str]] = {}
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
            self.inv_id_to_page[pid] = item_info
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
            self.inv_id_to_page[new_id] = item_info
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


# ==============================================================================
# 5. 대상 ETF 식별 및 증분 Upsert 동기화
# ==============================================================================
def get_target_etfs(client: Any, db_cache: StockMatchEngine) -> List[Dict[str, str]]:
    """ETF 구성종목 DB에 등록된 부모 ETF만 정확히 식별 (지표 DB 전체 스캔 배제)"""
    print(f"📋 ETF DB({ETF_DB_ID})에서 등록된 부모 ETF를 스캔합니다...", flush=True)
    target_etfs: List[Dict[str, str]] = []
    parent_ids: set = set()

    # 1. ETF DB에서 사용자가 입력/연결한 부모 ETF ID 역스캔
    for page in paginate_database(client, ETF_DB_ID, page_size=100):
        for rel in page.get("properties", {}).get("ETF(투자DB)", {}).get("relation", []):
            if rel.get("id"):
                parent_ids.add(rel["id"])

    print(f"   🔍 등록된 부모 ETF 수: {len(parent_ids)}개", flush=True)
    for pid in parent_ids:
        # 인메모리 캐시에서 0ms 즉시 조회
        if pid in db_cache.inv_id_to_page:
            info = db_cache.inv_id_to_page[pid]
            ticker = info.get("ticker", "")
            name = info.get("name", "")
            if ticker:
                clean_t = ticker.split(".")[0].strip().upper()
                target_etfs.append({"etf_page_id": pid, "ticker": clean_t, "name": name or clean_t})
                print(f"   🎯 대상 ETF: {name or clean_t} ({clean_t})", flush=True)
                continue

        # 캐시에 없는 경우에만 단건 API 조회
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

    # 중복 제거
    unique_targets = []
    seen = set()
    for t in target_etfs:
        if t["etf_page_id"] not in seen:
            seen.add(t["etf_page_id"])
            unique_targets.append(t)

    print(f"   ✅ 총 {len(unique_targets)}개 대상 ETF 확정 완료.\n", flush=True)
    return unique_targets


def sync_etf_holdings_upsert(
    client: Any,
    etf_page_id: str,
    items_to_insert: List[Dict[str, Any]],
    now_kst: Optional[str] = None
) -> Tuple[int, int, int]:
    """
    개별 ETF에 대해 지능형 증분 동기화(Upsert) 및 편출입 상태 관리(Soft Delete)를 수행합니다.
    1. 신규 편입: 생성 (상태: 편입(보유), 편입일: 오늘, 수량: 최신 수량)
    2. 유지/재편입: 수정 (상태: 편입(보유), 수량 갱신, 과거 편출일 초기화)
    3. 편출(제외): 수정 (상태: 편출, 수량: 0, 편출일: 오늘) ➔ 아카이브 대신 이력 보존
    """
    now_kst = now_kst or kst_isoformat()
    today_date_str = now_kst[:10]  # YYYY-MM-DD

    # 1. 해당 부모 ETF에 연결된 기존 레코드 전량 조회
    existing_pages = []
    start_cursor = None
    while True:
        try:
            params: Dict[str, Any] = {
                "database_id": ETF_DB_ID,
                "filter": {"property": "ETF(투자DB)", "relation": {"contains": etf_page_id}},
                "page_size": 100
            }
            if start_cursor:
                params["start_cursor"] = start_cursor
            if hasattr(client, "databases") and hasattr(client.databases, "query"):
                res = client.databases.query(**params)
            elif hasattr(client, "data_sources") and hasattr(client.data_sources, "query"):
                db_info = client.databases.retrieve(database_id=ETF_DB_ID)
                data_sources = db_info.get("data_sources", [])
                ds_id = data_sources[0]["id"] if data_sources else ETF_DB_ID
                ds_params = {
                    "data_source_id": ds_id,
                    "filter": params.get("filter"),
                    "page_size": params.get("page_size", 100)
                }
                if start_cursor:
                    ds_params["start_cursor"] = start_cursor
                res = client.data_sources.query(**ds_params)
            else:
                res = client.databases.query(**params)
            existing_pages.extend(res.get("results", []))
            if not res.get("has_more"):
                break
            start_cursor = res.get("next_cursor")
        except Exception as e:
            print(f"      ⚠️ 기존 데이터 조회 중 오류: {e}", flush=True)
            break

    # 기존 데이터 인덱싱: ticker -> page_info, name -> page_info
    existing_by_ticker: Dict[str, Dict[str, Any]] = {}
    existing_by_name: Dict[str, Dict[str, Any]] = {}
    all_existing_ids: set = set()
    id_to_existing_info: Dict[str, Dict[str, Any]] = {}

    for page in existing_pages:
        pid = page["id"]
        props = page.get("properties", {})
        
        name_list = props.get("이름", {}).get("title", [])
        page_name = name_list[0]["plain_text"].strip() if name_list else ""
        
        ticker_list = props.get("티커", {}).get("rich_text", [])
        page_ticker = ticker_list[0]["plain_text"].strip().upper() if ticker_list else ""
        
        page_qty = props.get("수량", {}).get("number")
        
        stock_rels = props.get("종목(투자DB)", {}).get("relation", [])
        page_stock_id = stock_rels[0]["id"] if stock_rels else None

        page_status = props.get("상태", {}).get("select", {})
        page_status_name = page_status.get("name") if page_status else ""

        page_out_date = props.get("편출일", {}).get("date")

        info = {
            "id": pid,
            "name": page_name,
            "ticker": page_ticker,
            "quantity": page_qty,
            "stock_id": page_stock_id,
            "status": page_status_name,
            "out_date": page_out_date,
            "properties": props
        }
        all_existing_ids.add(pid)
        id_to_existing_info[pid] = info
        if page_ticker:
            existing_by_ticker[page_ticker] = info
            existing_by_ticker[page_ticker.split(".")[0].strip().upper()] = info
        if page_name:
            existing_by_name[page_name] = info
            existing_by_name[page_name.replace(" ", "")] = info

    matched_page_ids: set = set()
    created_cnt, updated_cnt, excluded_cnt = 0, 0, 0

    # 2. 최신 수집 데이터 순회 및 수정/생성
    for item in items_to_insert:
        item_ticker = (item.get("ticker") or "").strip().upper()
        clean_t = item_ticker.split(".")[0].strip().upper() if item_ticker else ""
        item_name = (item.get("name") or "").strip()
        item_qty = item.get("quantity")
        item_stock_id = item.get("stock_id")

        # 기존 레코드 매칭 시도
        matched_info = None
        if clean_t and clean_t in existing_by_ticker:
            matched_info = existing_by_ticker[clean_t]
        elif item_ticker and item_ticker in existing_by_ticker:
            matched_info = existing_by_ticker[item_ticker]
        elif item_name and item_name in existing_by_name:
            matched_info = existing_by_name[item_name]
        elif item_name and item_name.replace(" ", "") in existing_by_name:
            matched_info = existing_by_name[item_name.replace(" ", "")]

        if matched_info and matched_info["id"] not in matched_page_ids:
            # CASE A: 기존 레코드 존재 ➔ 유지 또는 재편입 업데이트
            pid = matched_info["id"]
            matched_page_ids.add(pid)
            page_props = matched_info.get("properties", {})

            update_props: Dict[str, Any] = {}
            need_update = False

            # 수량 변동 확인
            if item_qty is not None and matched_info["quantity"] != item_qty:
                update_props["수량"] = {"number": item_qty}
                need_update = True

            # 티커 보강
            if item_ticker and matched_info["ticker"] != item_ticker:
                update_props["티커"] = {"rich_text": [{"text": {"content": item_ticker}}]}
                need_update = True

            # 종목 연결 보강
            if item_stock_id and matched_info["stock_id"] != item_stock_id:
                update_props["종목(투자DB)"] = {"relation": [{"id": item_stock_id}]}
                need_update = True

            # 상태 동기화 (기존이 '편출'이거나 비어있으면 '편입(보유)'로 복귀)
            if "상태" in page_props:
                if matched_info.get("status") != "편입(보유)":
                    update_props["상태"] = {"select": {"name": "편입(보유)"}}
                    need_update = True
                    # 재편입 시 과거 편출일 초기화
                    if "편출일" in page_props and matched_info.get("out_date"):
                        update_props["편출일"] = None
                        need_update = True

            # 편입일 미설정 시 보강
            if "편입일" in page_props and not page_props.get("편입일", {}).get("date"):
                update_props["편입일"] = {"date": {"start": today_date_str}}
                need_update = True

            if need_update:
                set_page_date_property(update_props, page_props, candidate_names=["업데이트", "마지막 업데이트", "업데이트 일자"], iso_date_str=now_kst)
                try:
                    client.pages.update(page_id=pid, properties=update_props)
                    updated_cnt += 1
                    time.sleep(0.01)
                except Exception as e:
                    print(f"      ❌ {item_name} 수정 실패: {e}", flush=True)

        else:
            # CASE B: 신규 편입 종목 ➔ 생성(Create)
            new_props: Dict[str, Any] = {
                "이름": {"title": [{"text": {"content": item_name}}]},
                "ETF(투자DB)": {"relation": [{"id": etf_page_id}]},
                "상태": {"select": {"name": "편입(보유)"}},
                "편입일": {"date": {"start": today_date_str}},
            }
            set_page_date_property(new_props, {}, candidate_names=["업데이트", "마지막 업데이트", "업데이트 일자"], iso_date_str=now_kst)
            if item_ticker:
                new_props["티커"] = {"rich_text": [{"text": {"content": item_ticker}}]}
            if item_stock_id:
                new_props["종목(투자DB)"] = {"relation": [{"id": item_stock_id}]}
            if item_qty is not None:
                new_props["수량"] = {"number": item_qty}

            try:
                new_p = client.pages.create(parent={"database_id": ETF_DB_ID}, properties=new_props)
                created_cnt += 1
                matched_page_ids.add(new_p["id"])
                time.sleep(0.01)
            except Exception as e:
                print(f"      ❌ {item_name} 생성 실패: {e}", flush=True)

    # 3. 편출된 종목 처리 (상태: 편출, 수량: 0, 편출일: 오늘 기록 ➔ Soft Delete)
    to_exclude_ids = list(all_existing_ids - matched_page_ids)
    if to_exclude_ids:
        def _exclude_page(pid: str) -> bool:
            info = id_to_existing_info.get(pid, {})
            # 이미 편출 상태이고 수량이 0인 경우 불필요한 추가 API 호출 생략
            if info.get("status") == "편출" and info.get("quantity") == 0:
                return False

            page_props = info.get("properties", {})
            exclude_props: Dict[str, Any] = {
                "상태": {"select": {"name": "편출"}},
                "수량": {"number": 0},
            }
            if "편출일" in page_props:
                exclude_props["편출일"] = {"date": {"start": today_date_str}}

            set_page_date_property(exclude_props, page_props, candidate_names=["업데이트", "마지막 업데이트", "업데이트 일자"], iso_date_str=now_kst)

            try:
                client.pages.update(page_id=pid, properties=exclude_props)
                return True
            except Exception as e:
                print(f"      ⚠️ 편출 상태 업데이트 실패 ({info.get('name', pid)}): {e}", flush=True)
                return False

        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(_exclude_page, to_exclude_ids))
            excluded_cnt = sum(1 for r in results if r)

    return created_cnt, updated_cnt, excluded_cnt


# ==============================================================================
# 6. 메인 파이프라인
# ==============================================================================
def main() -> None:
    print("🚀 [ETF 구성종목 자동 수집 및 증분 Upsert 파이프라인] 가동 시작", flush=True)
    notion = build_notion_client(NOTION_TOKEN)

    kis_ctx = get_kis_auth_context()
    if not kis_ctx:
        print("⚠️ KIS 토큰 발급 실패: WiseReport 수집 전용 모드로 진행합니다.", flush=True)

    db_cache = StockMatchEngine(notion)
    target_etfs = get_target_etfs(notion, db_cache)
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
            print(f"   ⚠️ {etf_name} 유효 구성종목 없음 (건너뜀)", flush=True)
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

        # 지능형 증분 동기화 (Upsert: 편입(보유) 생성/수정 + 편출 상태/수량0 관리)
        print(f"   ⚡ 최신 {len(items_to_insert)}개 구성종목 증분 동기화(Upsert) 진행 중...", flush=True)
        created_cnt, updated_cnt, excluded_cnt = sync_etf_holdings_upsert(
            notion, etf_page_id, items_to_insert, now_kst
        )

        print(f"   ✅ [{etf_name}] 완료 (생성(신규편입): {created_cnt}건 | 수정(유지): {updated_cnt}건 | 편출: {excluded_cnt}건)", flush=True)

    print("\n✨ 모든 관리 대상 ETF 갱신 작업이 성공적으로 완료되었습니다.", flush=True)


if __name__ == "__main__":
    main()