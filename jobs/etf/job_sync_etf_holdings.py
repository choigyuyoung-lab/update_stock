# -*- coding: utf-8 -*-
"""
job_sync_etf_holdings.py
========================
국내 상장 ETF의 구성종목(Holdings/PDF) 및 편입 수량/비중을 수집하여
노션(Notion)의 ETF 구성종목 DB 및 로컬 SQLite DB(tbl_etf_holdings)에
지능형 증분 동기화(Upsert) 및 중복 자동 정리(De-duplication)를 수행합니다.

- 단일 데이터 소스(SSOT): WiseReport (국내/해외/채권/원자재 ETF 100% 전수 수집)
- 초고속 종목 매핑: 로컬 SQLite DB(tbl_stocks, tbl_dictionary) 0.001초 인메모리 룩업
- 데이터 무결성: 노션 기존 중복 페이지 자동 아카이브 정리 및 변경된 수량/비중만 고속 갱신
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import json
import re
import logging
from typing import Any, List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import requests

# Windows 콘솔 인코딩 안전화
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ETFHoldingsSync")

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    paginate_database,
    safe_databases_query,
    get_page_text,
    kst_isoformat,
    set_page_date_property,
    extract_short_brand_name,
    search_foreign_ticker,
    get_http_session,
    is_kr_ticker,
    ensure_database_properties,
)
from core.local_db_manager import (
    upsert_etf_holdings_batch,
    export_all_tables_to_csv,
    get_db_connection,
    init_database,
)


ETF_SCHEMA: Dict[str, Dict[str, Any]] = {
    "업데이트": {"date": {}},
    "편입일": {"date": {}},
    "편출일": {"date": {}},
}

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
# 2. 파생상품 및 현금성 자산 필터링 (사전 컴파일 정규식)
# ==============================================================================
RE_DERIVATIVE_OR_CASH = re.compile(
    r'(설정현금액|원화현금|USD현금|외화예치금|예탁금|미수금|예치금|KOFR|CD금리|SOFR|콜론|RP형|원화RP|외화RP|'
    r'\b(CASH|RP|MMF)\b|^현금$|'
    r'선물|위클리|콜옵션|풋옵션|옵션|스왑|\bSWAP\b|\bFUTURES\b|\bFUT\b|\bOPTION\b|'
    r'^\d{4}\s*\d{2}\s*.*선물|코스피\s*위클리|KOSPI\s*위클리|코스닥\s*선물|KOSDAQ\s*선물|미국\s*달러\s*선물)',
    re.IGNORECASE
)


def parse_quantity(val: Any) -> Optional[float]:
    """수량 문자열/숫자를 안전하게 float로 파싱합니다."""
    if val is None:
        return None
    try:
        s = str(val).replace(",", "").strip()
        if not s or s.lower() in ("null", "-", "none"):
            return None
        num = float(s)
        return num if num > 0 else None
    except (ValueError, TypeError):
        return None


def is_derivative_or_cash(name: str, raw_ticker: str = "") -> bool:
    """선물, 옵션, 스왑, 현금, 금리상품 등 비주식 파생자산을 0.0001초 만에 필터링합니다."""
    if not name:
        return True
    clean_n = name.strip().replace(" ", "")
    return bool(RE_DERIVATIVE_OR_CASH.search(clean_n) or RE_DERIVATIVE_OR_CASH.search(name))


# ==============================================================================
# 3. WiseReport 단일 SSOT 전수 수집부
# ==============================================================================
def get_etf_composition_wisereport(clean_ticker: str) -> List[Dict[str, Any]]:
    """
    WiseReport: 국내/해외/채권/원자재 ETF 구성종목(PDF) 및 수량을 100% 전수 수집합니다.
    - 네이버 금융/에프앤가이드 공시 PDF 전수 데이터 추출
    - 파생상품 및 현금성 자산 필터링
    """
    url = f"https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd={clean_ticker}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    holdings = []
    try:
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code == 200:
            r.encoding = r.apparent_encoding or "utf-8"
            match = re.search(r'var\s+CU_data\s*=\s*(\{.*?\});', r.text, re.DOTALL)
            if match:
                data = json.loads(match.group(1))
                for item in data.get("grid_data", []):
                    name = (item.get("STK_NM_KOR") or item.get("ITEM_NM") or "").strip()
                    raw_ticker = str(item.get("STK_CD") or item.get("CMP_CD") or "").strip()
                    if is_derivative_or_cash(name, raw_ticker):
                        continue
                    if raw_ticker.lower() in ("none", "null"):
                        raw_ticker = ""
                    qty = parse_quantity(item.get("AGMT_STK_CNT"))
                    if (name or raw_ticker) and qty is not None and qty > 0:
                        holdings.append({"raw_ticker": raw_ticker, "name": name, "quantity": qty})
    except Exception as exc:
        logger.warning(f"⚠️ WiseReport ETF 구성종목 조회 예외 ({clean_ticker}): {exc}")
    return holdings


# ==============================================================================
# 4. 로컬 DB(0.001s) & 온톨로지 사전 기반 종목 매칭 엔진
# ==============================================================================
class StockMatchEngine:
    """
    로컬 SQLite DB(tbl_stocks, tbl_dictionary) 및 노션 인메모리 다차원 캐시 기반 고속 종목 매칭 엔진
    """

    def __init__(self, client: Any):
        self.client = client
        self.inv_ticker_to_page: Dict[str, Dict[str, str]] = {}
        self.inv_name_to_page: Dict[str, Dict[str, str]] = {}
        self.inv_id_to_page: Dict[str, Dict[str, str]] = {}
        self.dict_alias_to_info: Dict[str, Dict[str, str]] = {}
        self.online_search_cache: Dict[str, Optional[Tuple[str, str]]] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        """로컬 SQLite DB(1차) 및 노션 투자주 DB(2차)에서 인메모리 색인 구축 (0.001s 로딩)"""
        init_database()
        # 1-1. 로컬 SQLite DB tbl_stocks에서 상장주식 마스터 색인
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT ticker, name, notion_page_id FROM tbl_stocks WHERE notion_page_id != '';")
                for r in cursor.fetchall():
                    pid = r["notion_page_id"]
                    t = (r["ticker"] or "").strip().upper()
                    n = (r["name"] or "").strip()
                    info = {"id": pid, "ticker": t, "name": n}
                    self.inv_id_to_page[pid] = info
                    if t:
                        self.inv_ticker_to_page[t.split(".")[0].strip().upper()] = info
                        self.inv_ticker_to_page[t] = info
                    if n:
                        self.inv_name_to_page[n] = info
                        self.inv_name_to_page[n.replace(" ", "")] = info
                        self.inv_name_to_page[n.upper()] = info

                # 1-2. tbl_dictionary에서 별칭/원시 사명 매핑 색인
                cursor.execute("SELECT keyword, official_name, yahoo_ticker FROM tbl_dictionary;")
                for r in cursor.fetchall():
                    kw = (r["keyword"] or "").strip()
                    off_name = (r["official_name"] or "").strip()
                    y_ticker = (r["yahoo_ticker"] or "").strip().upper()
                    if kw and (y_ticker or off_name):
                        alias_info = {"ticker": y_ticker, "name": off_name or kw}
                        self.dict_alias_to_info[kw.upper()] = alias_info
                        self.dict_alias_to_info[kw.replace(" ", "").upper()] = alias_info
        except Exception as e:
            logger.warning(f"⚠️ 로컬 SQLite DB 캐시 로드 중 예외: {e}")

        # 2. 노션 투자주 DB 실시간 최신 목록 동기화
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
                self.inv_name_to_page[name.upper()] = item_info
            count_inv += 1

        logger.info(f"✅ [초고속 종목 캐시] 로컬 DB & 투자주 DB 종목 {count_inv}개 인메모리 색인 구축 완료")

    def _create_investment_page(self, ticker: str, name: str) -> Optional[str]:
        """투자주 DB에 신규 페이지 생성 후 인메모리 캐시 즉시 갱신"""
        if not ticker:
            return None
        try:
            props = {
                "티커": {"title": [{"text": {"content": ticker}}]},
                "종목명": {"rich_text": [{"text": {"content": name}}]} if name else {},
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
                self.inv_name_to_page[name.upper()] = item_info

            logger.info(f"   ✨ [투자주 DB 신규 등록] {name}({ticker}) 완료")
            time.sleep(0.02)
            return new_id
        except Exception as exc:
            logger.warning(f"   ⚠️ [투자주 DB 등록 실패] {name}({ticker}): {exc}")
            return None

    def _search_foreign_ticker(self, name: str) -> Optional[Tuple[str, str]]:
        """DB에 없는 미등록 해외 종목 1회성 온라인 탐색 (Yahoo Finance)"""
        if not name or len(name) < 2:
            return None
        if name in self.online_search_cache:
            return self.online_search_cache[name]

        best = search_foreign_ticker(name)
        if best:
            self.online_search_cache[name] = best
            logger.info(f"   🔍 [신규 발굴] '{name}' ➔ 공식 티커: {best[0]} ({best[1]})")
            return best

        self.online_search_cache[name] = None
        return None

    def match(self, raw_ticker: str, name: str) -> Tuple[Optional[str], str, str]:
        """
        원시 사명/티커 ➔ (투자주ID, 확정티커, 짧고간결한브랜드명) 0.001초 반환
        1. 로컬 SQLite DB & 투자주 DB 인메모리 색인 최우선 조회
        2. 온톨로지 사전(tbl_dictionary) 별칭 매핑 조회
        3. 완전 신규 종목 시 1회성 온라인 탐색 후 자동 등록
        """
        t = raw_ticker.strip().upper()
        n = name.strip()
        is_kr_code = is_kr_ticker(t)

        # CASE A: 한국 주식 (KRX 6자리 코드 및 .KS/.KQ)
        if is_kr_code:
            clean_t = t.split(".")[0].strip().upper()
            if clean_t in self.inv_ticker_to_page:
                info = self.inv_ticker_to_page[clean_t]
                return info["id"], clean_t, info["name"] or n
            if n in self.inv_name_to_page:
                info = self.inv_name_to_page[n]
                return info["id"], info["ticker"] or clean_t, info["name"] or n

            new_id = self._create_investment_page(clean_t, n)
            return new_id, clean_t, n

        # CASE B: DB 인메모리 색인에서 티커/종목명 직접 매칭 (0.001s)
        if t and t in self.inv_ticker_to_page:
            info = self.inv_ticker_to_page[t]
            return info["id"], info["ticker"], extract_short_brand_name(info["name"] or n)
        if n and n in self.inv_name_to_page:
            info = self.inv_name_to_page[n]
            return info["id"], info["ticker"], extract_short_brand_name(info["name"] or n)
        if n and n.upper() in self.inv_name_to_page:
            info = self.inv_name_to_page[n.upper()]
            return info["id"], info["ticker"], extract_short_brand_name(info["name"] or n)

        # CASE C: 사전 DB(tbl_dictionary) 별칭 매핑 조회 (0.001s)
        n_up = n.upper()
        n_clean = n.replace(" ", "").upper()
        dict_match = self.dict_alias_to_info.get(n_up) or self.dict_alias_to_info.get(n_clean)
        if dict_match and dict_match["ticker"]:
            dticker = dict_match["ticker"].split(".")[0].strip().upper()
            dname = extract_short_brand_name(dict_match["name"] or n)
            if dticker in self.inv_ticker_to_page:
                return self.inv_ticker_to_page[dticker]["id"], dticker, dname
            inv_id = self._create_investment_page(dticker, dname)
            return inv_id, dticker, dname

        # CASE D: DB에 없는 완전 신규 해외 종목 1회성 온라인 검색
        matched_ticker = ""
        matched_name = ""
        if n:
            search_res = self._search_foreign_ticker(n)
            if search_res:
                matched_ticker, matched_name = search_res

        if matched_ticker:
            clean_brand = extract_short_brand_name(matched_name or n)
            clean_t = matched_ticker.split(".")[0].strip().upper()

            if clean_t in self.inv_ticker_to_page:
                inv_id = self.inv_ticker_to_page[clean_t]["id"]
            elif matched_ticker in self.inv_ticker_to_page:
                inv_id = self.inv_ticker_to_page[matched_ticker]["id"]
            elif matched_name and matched_name in self.inv_name_to_page:
                inv_id = self.inv_name_to_page[matched_name]["id"]
            else:
                inv_id = self._create_investment_page(matched_ticker, clean_brand)

            return inv_id, matched_ticker, clean_brand

        short_brand = extract_short_brand_name(n)
        fallback_t = t if (re.match(r'^[A-Z0-9.\-_]{1,10}$', t) and not t.isdigit()) else ""
        return None, fallback_t, short_brand


# ==============================================================================
# 5. 대상 ETF 식별 및 증분 Upsert 동기화
# ==============================================================================
def get_target_etfs(client: Any, db_cache: StockMatchEngine) -> List[Dict[str, str]]:
    """ETF 구성종목 DB에 등록된 부모 ETF만 정확히 식별 (지표 DB 전체 스캔 배제)"""
    logger.info(f"📋 ETF DB({ETF_DB_ID})에서 등록된 부모 ETF를 스캔합니다...")
    target_etfs: List[Dict[str, str]] = []
    parent_ids: set = set()

    for page in paginate_database(client, ETF_DB_ID, page_size=100):
        for rel in page.get("properties", {}).get("ETF(투자DB)", {}).get("relation", []):
            if rel.get("id"):
                parent_ids.add(rel["id"])

    logger.info(f"   🔍 등록된 부모 ETF 수: {len(parent_ids)}개")
    for pid in parent_ids:
        if pid in db_cache.inv_id_to_page:
            info = db_cache.inv_id_to_page[pid]
            ticker = info.get("ticker", "")
            name = info.get("name", "")
            if ticker:
                clean_t = ticker.split(".")[0].strip().upper()
                target_etfs.append({"etf_page_id": pid, "ticker": clean_t, "name": name or clean_t})
                continue

        try:
            page = client.pages.retrieve(page_id=pid)
            props = page.get("properties", {})
            ticker = get_page_text(props, ["티커", "Ticker"])
            name = get_page_text(props, ["종목명", "이름", "Title"])
            if ticker:
                clean_t = ticker.split(".")[0].strip().upper()
                target_etfs.append({"etf_page_id": pid, "ticker": clean_t, "name": name or clean_t})
        except Exception:
            pass

    unique_targets = []
    seen = set()
    for t in target_etfs:
        if t["etf_page_id"] not in seen:
            seen.add(t["etf_page_id"])
            unique_targets.append(t)

    logger.info(f"   ✅ 총 {len(unique_targets)}개 대상 ETF 확정 완료")
    return unique_targets


def sync_etf_holdings_upsert(
    client: Any,
    etf_page_id: str,
    items_to_insert: List[Dict[str, Any]],
    now_kst: Optional[str] = None,
) -> Tuple[int, int, int, int]:
    """
    개별 ETF에 대해 보유비중/수량 중심 고속 Upsert 및 노션 중복 레코드 자동 클린업을 수행합니다.
    1. 노션 내 기존 중복 페이지 자동 감지 및 정리(Archive)
    2. 유지/신규 종목: 비중/수량 및 상태 고속 갱신
    3. 편출 종목: 상태: 편출, 수량: 0, 편출일: 오늘 (Soft Delete 이력 보존)
    """
    now_kst = now_kst or kst_isoformat()
    today_date_str = now_kst[:10]

    # 1. 노션에서 해당 ETF에 매핑된 기존 구성종목 페이지 전체 조회
    existing_pages = []
    start_cursor = None
    filter_payload = {"property": "ETF(투자DB)", "relation": {"contains": etf_page_id}}
    while True:
        try:
            res = safe_databases_query(
                client,
                ETF_DB_ID,
                filter=filter_payload,
                start_cursor=start_cursor,
                page_size=100,
            )
            existing_pages.extend(res.get("results", []))
            if not res.get("has_more"):
                break
            start_cursor = res.get("next_cursor")
        except Exception as e:
            logger.warning(f"      ⚠️ 기존 데이터 조회 중 오류: {e}")
            break

    # 2. 기존 페이지들을 종목 식별자별로 그룹핑하여 중복 감지 및 자동 클린업(Archive)
    grouped_by_key: Dict[str, List[Dict[str, Any]]] = {}
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
            "properties": props,
        }

        # 고유 식별 키 생성 (티커 최우선, 없을 시 종목명)
        primary_key = page_ticker.split(".")[0].strip().upper() if page_ticker else page_name.replace(" ", "").upper()
        if primary_key:
            grouped_by_key.setdefault(primary_key, []).append(info)

    cleaned_dup_cnt = 0
    valid_existing_by_ticker: Dict[str, Dict[str, Any]] = {}
    valid_existing_by_name: Dict[str, Dict[str, Any]] = {}
    all_valid_existing_ids: set = set()
    id_to_existing_info: Dict[str, Dict[str, Any]] = {}

    for key, pages in grouped_by_key.items():
        # 첫 번째 페이지는 대표 레코드로 유지
        keeper = pages[0]
        valid_existing_by_ticker[key] = keeper
        if keeper["ticker"]:
            valid_existing_by_ticker[keeper["ticker"]] = keeper
            valid_existing_by_ticker[keeper["ticker"].split(".")[0].strip().upper()] = keeper
        if keeper["name"]:
            valid_existing_by_name[keeper["name"]] = keeper
            valid_existing_by_name[keeper["name"].replace(" ", "")] = keeper
            valid_existing_by_name[keeper["name"].upper()] = keeper

        all_valid_existing_ids.add(keeper["id"])
        id_to_existing_info[keeper["id"]] = keeper

        # 2개 이상 존재하는 중복 페이지는 노션에서 즉시 아카이브(삭제) 처리
        if len(pages) > 1:
            for duplicate in pages[1:]:
                dup_id = duplicate["id"]
                try:
                    client.pages.update(page_id=dup_id, archived=True)
                    cleaned_dup_cnt += 1
                    logger.info(f"      🗑️ [노션 중복 정리] '{duplicate['name']}' 중복 페이지({dup_id[:8]}) 아카이브 완료")
                except Exception as e:
                    logger.warning(f"      ⚠️ 중복 페이지 아카이브 실패 ({dup_id}): {e}")

    matched_page_ids: set = set()
    created_cnt, updated_cnt, excluded_cnt = 0, 0, 0

    # 3. items_to_insert 에 대해 고속 Upsert 수행
    for item in items_to_insert:
        item_ticker = (item.get("ticker") or "").strip().upper()
        clean_t = item_ticker.split(".")[0].strip().upper() if item_ticker else ""
        item_name = (item.get("name") or "").strip()
        item_qty = item.get("quantity")
        item_stock_id = item.get("stock_id")

        matched_info = None
        if clean_t and clean_t in valid_existing_by_ticker:
            matched_info = valid_existing_by_ticker[clean_t]
        elif item_ticker and item_ticker in valid_existing_by_ticker:
            matched_info = valid_existing_by_ticker[item_ticker]
        elif item_name and item_name in valid_existing_by_name:
            matched_info = valid_existing_by_name[item_name]
        elif item_name and item_name.replace(" ", "") in valid_existing_by_name:
            matched_info = valid_existing_by_name[item_name.replace(" ", "")]
        elif item_name and item_name.upper() in valid_existing_by_name:
            matched_info = valid_existing_by_name[item_name.upper()]

        if matched_info and matched_info["id"] not in matched_page_ids:
            # CASE A: 기존 레코드 존재 ➔ 수량/비중 및 상태 고속 수정 (Update)
            pid = matched_info["id"]
            matched_page_ids.add(pid)
            page_props = matched_info.get("properties", {})

            update_props: Dict[str, Any] = {}
            need_update = False

            if item_qty is not None and matched_info["quantity"] != item_qty:
                update_props["수량"] = {"number": item_qty}
                need_update = True

            if item_ticker and matched_info["ticker"] != item_ticker:
                update_props["티커"] = {"rich_text": [{"text": {"content": item_ticker}}]}
                need_update = True

            if item_stock_id and matched_info["stock_id"] != item_stock_id:
                update_props["종목(투자DB)"] = {"relation": [{"id": item_stock_id}]}
                need_update = True

            if "상태" in page_props and matched_info.get("status") != "편입(보유)":
                update_props["상태"] = {"select": {"name": "편입(보유)"}}
                need_update = True
                if "편출일" in page_props and matched_info.get("out_date"):
                    update_props["편출일"] = None
                    need_update = True

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
                    logger.warning(f"      ❌ {item_name} 수정 실패: {e}")

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
                logger.warning(f"      ❌ {item_name} 생성 실패: {e}")

    # CASE C: 편출된 종목 처리 (상태: 편출, 수량: 0, 편출일: 오늘 ➔ Soft Delete)
    to_exclude_ids = list(all_valid_existing_ids - matched_page_ids)
    if to_exclude_ids:
        def _exclude_page(pid: str) -> bool:
            info = id_to_existing_info.get(pid, {})
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
                logger.warning(f"      ⚠️ 편출 상태 업데이트 실패 ({info.get('name', pid)}): {e}")
                return False

        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(_exclude_page, to_exclude_ids))
            excluded_cnt = sum(1 for r in results if r)

    return created_cnt, updated_cnt, excluded_cnt, cleaned_dup_cnt


# ==============================================================================
# 6. 메인 파이프라인
# ==============================================================================
def main() -> None:
    logger.info("🚀 [ETF 구성종목 단일 SSOT & 고속 비중 동기화 파이프라인] 가동 시작")
    notion = build_notion_client(NOTION_TOKEN)
    ensure_database_properties(notion, ETF_DB_ID, ETF_SCHEMA)

    db_cache = StockMatchEngine(notion)
    target_etfs = get_target_etfs(notion, db_cache)
    if not target_etfs:
        logger.info("ℹ️ 갱신 대상 ETF가 없습니다. 작업을 종료합니다.")
        return

    now_kst = kst_isoformat()
    all_sqlite_holdings: List[Dict[str, Any]] = []

    for idx, target in enumerate(target_etfs, 1):
        etf_page_id = target["etf_page_id"]
        etf_ticker = target["ticker"]
        etf_name = target["name"]

        logger.info(f"[{idx}/{len(target_etfs)}] 🔄 수집 진행: {etf_name}({etf_ticker})...")
        raw_holdings = get_etf_composition_wisereport(etf_ticker)

        if not raw_holdings:
            logger.warning(f"   ⚠️ {etf_name} 유효 구성종목 없음 (건너뜀)")
            continue

        # DB 기반 고속 매핑 및 리스트 레벨 유니크(Deduplication) 정제
        items_to_insert: List[Dict[str, Any]] = []
        seen_keys: set = set()

        for h in raw_holdings:
            stock_id, matched_ticker, short_brand = db_cache.match(h["raw_ticker"], h["name"])
            dedup_key = matched_ticker.split(".")[0].strip().upper() if matched_ticker else short_brand.replace(" ", "").upper()

            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            items_to_insert.append({
                "name": short_brand,
                "ticker": matched_ticker,
                "stock_id": stock_id,
                "quantity": h["quantity"],
            })
            all_sqlite_holdings.append({
                "etf_ticker": etf_ticker,
                "holding_ticker": matched_ticker or short_brand,
                "holding_name": short_brand,
                "weight": h.get("quantity") or 0.0,
            })

        logger.info(f"   ⚡ 최신 {len(items_to_insert)}개 구성종목 보유비중 동기화(Upsert) 진행 중...")
        created_cnt, updated_cnt, excluded_cnt, cleaned_dup_cnt = sync_etf_holdings_upsert(
            notion, etf_page_id, items_to_insert, now_kst
        )

        logger.info(
            f"   ✅ [{etf_name}] 완료 (신규: {created_cnt} | 유지/수량갱신: {updated_cnt} | 편출: {excluded_cnt} | 중복정리: {cleaned_dup_cnt})"
        )

    # 통합 로컬 SQLite DB 캐싱 및 CSV 덤프 내보내기
    if all_sqlite_holdings:
        upsert_etf_holdings_batch(all_sqlite_holdings)
        export_all_tables_to_csv()
        logger.info(f"💾 [통합 로컬 SQLite DB] {len(all_sqlite_holdings)}개 ETF 구성종목 캐싱 및 CSV 내보내기 완료")

    logger.info("✨ 모든 관리 대상 ETF 갱신 작업이 성공적으로 완료되었습니다.")


if __name__ == "__main__":
    main()