# -*- coding: utf-8 -*-
"""
stock_registry.py
=================
[단일 진실 공급원(SSOT) 통합 종목 레지스트리 게이트웨이]
- 로컬 SQLite DB(tbl_stocks, tbl_dictionary) 0.001초 인메모리 색인
- 노션 상장주식 마스터 DB & 투자주 DB 실시간 동기화
- 3중 교차 검증 (1차 티커 정규화 ➔ 2차 종목명/브랜드 ➔ 3차 온톨로지 사전 별칭)
- 신규 종목 등록 시 로컬 DB & 인메모리 캐시 즉시 영구 적재로 중복 생성 100% 원천 차단
"""

import os
import re
import sys
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    kst_isoformat,
    resolve_stock_taxonomy,
    is_kr_ticker,
    extract_short_brand_name,
    paginate_database,
)
from core.local_db_manager import (
    get_db_connection,
    init_database,
    upsert_stocks_batch,
    export_all_tables_to_csv,
)

logger = logging.getLogger("StockRegistry")


def clean_ticker_key(ticker: str) -> str:
    """티커의 마켓 접미사(.T, .KS 등)를 보존하면서 공백과 대소문자를 표준화합니다."""
    if not ticker:
        return ""
    return str(ticker).strip().upper().replace(" ", "")


def clean_name_key(name: str) -> str:
    """종목명의 공백 및 특수문자를 제거하여 매칭용 키를 생성합니다."""
    if not name:
        return ""
    return re.sub(r'[\s\(\)\[\],\.\-_]', '', str(name).strip().upper())


class StockRegistryGateway:
    """
    상장주식 Master DB 및 투자주 DB의 중복 생성을 원천 차단하는 통합 검증 게이트웨이
    """

    def __init__(self, client: Any = None):
        self.notion_token = get_env_var("NOTION_TOKEN")
        self.client = client or build_notion_client(self.notion_token)
        self.master_db_id = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID"], required=False) or ""
        self.interest_db_id = get_db_id("DATABASE_ID", ["INTEREST_DATABASE_ID", "INTEREST_DB_ID"], required=False) or ""

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.notion_token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        })

        self.master_by_ticker: Dict[str, Dict[str, Any]] = {}
        self.master_by_name: Dict[str, Dict[str, Any]] = {}
        self.master_by_id: Dict[str, Dict[str, Any]] = {}

        self.invest_by_ticker: Dict[str, str] = {}
        self.invest_by_name: Dict[str, str] = {}
        self.invest_by_id: Dict[str, Dict[str, str]] = {}

        self.dict_alias_map: Dict[str, Dict[str, str]] = {}
        self._load_registry()

    def _load_registry(self) -> None:
        init_database()
        # 1. 로컬 SQLite DB (tbl_stocks & tbl_dictionary) 0.001s 색인
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # 1-1. tbl_stocks
                cursor.execute("SELECT ticker, name, market, country, notion_page_id FROM tbl_stocks WHERE notion_page_id != '';")
                for r in cursor.fetchall():
                    pid = r["notion_page_id"]
                    t = clean_ticker_key(r["ticker"])
                    n = (r["name"] or "").strip()
                    info = {
                        "id": pid,
                        "ticker": t,
                        "name": n,
                        "market": r["market"] or "",
                        "country": r["country"] or "",
                        "insight_status": []
                    }
                    self._index_master_info(t, n, info)

                # 1-2. tbl_dictionary
                cursor.execute("SELECT keyword, official_name, yahoo_ticker FROM tbl_dictionary;")
                for r in cursor.fetchall():
                    kw = (r["keyword"] or "").strip()
                    off_n = (r["official_name"] or "").strip()
                    yt = clean_ticker_key(r["yahoo_ticker"] or "")
                    if kw:
                        self.dict_alias_map[clean_name_key(kw)] = {"ticker": yt, "name": off_n or kw}
                        self.dict_alias_map[clean_ticker_key(kw)] = {"ticker": yt, "name": off_n or kw}
        except Exception as e:
            logger.warning(f"⚠️ 로컬 DB 색인 로드 예외: {e}")

        # 2. 노션 상장주식 Master DB 실시간 동기화
        if self.master_db_id:
            try:
                for p in paginate_database(self.client, self.master_db_id, page_size=100):
                    pid = p["id"]
                    props = p.get("properties", {})
                    t_prop = props.get("티커", {}).get("title", [])
                    t = clean_ticker_key(t_prop[0]["plain_text"]) if t_prop else ""
                    
                    name = ""
                    for k in ["종목명", "이름", "Name"]:
                        if k in props:
                            val = props[k]
                            if val.get("type") == "formula":
                                name = str(val.get("formula", {}).get("string") or "").strip()
                            elif val.get("type") in ["rich_text", "title"] and val.get(val["type"]):
                                name = val[val["type"]][0]["plain_text"].strip()
                    if not name:
                        name = t

                    market = (props.get("Market", {}).get("select") or {}).get("name", "") if "Market" in props else ""
                    country = (props.get("국가", {}).get("select") or {}).get("name", "") if "국가" in props else ""
                    status = [x["name"] for x in (props.get("인사이트상태", {}).get("multi_select") or [])] if "인사이트상태" in props else []

                    info = {"id": pid, "ticker": t, "name": name, "market": market, "country": country, "insight_status": status}
                    self._index_master_info(t, name, info)
            except Exception as e:
                logger.warning(f"⚠️ 노션 마스터 DB 동기화 예외: {e}")

        # 3. 노션 투자주/관심주 DB 실시간 동기화
        if self.interest_db_id:
            try:
                for p in paginate_database(self.client, self.interest_db_id, page_size=100):
                    pid = p["id"]
                    props = p.get("properties", {})
                    t_prop = props.get("티커", {}).get("title", [])
                    t = clean_ticker_key(t_prop[0]["plain_text"]) if t_prop else ""
                    n = ""
                    for k in ["종목명", "이름", "Name"]:
                        if k in props:
                            val = props[k]
                            if val.get("type") == "formula":
                                n = str(val.get("formula", {}).get("string") or "").strip()
                            elif val.get("type") in ["rich_text", "title"] and val.get(val["type"]):
                                n = val[val["type"]][0]["plain_text"].strip()

                    inv_info = {"id": pid, "ticker": t, "name": n}
                    self.invest_by_id[pid] = inv_info
                    if t:
                        self.invest_by_ticker[t] = pid
                        self.invest_by_ticker[t.split(".")[0]] = pid
                    if n:
                        self.invest_by_name[clean_name_key(n)] = pid
                        brand = extract_short_brand_name(n)
                        if brand:
                            self.invest_by_name[clean_name_key(brand)] = pid
            except Exception as e:
                logger.warning(f"⚠️ 노션 투자주 DB 동기화 예외: {e}")

        # 하위 호환성 별칭
        self.inv_id_to_page = self.invest_by_id

        logger.info(
            f"✅ [StockRegistryGateway] 상장주식 {len(self.master_by_id)}개 / 투자주 {len(set(self.invest_by_ticker.values()))}개 색인 활성화"
        )

    def _index_master_info(self, ticker: str, name: str, info: Dict[str, Any]) -> None:
        """상장주식 마스터 인메모리 다차원 색인"""
        pid = info["id"]
        self.master_by_id[pid] = info
        if ticker:
            self.master_by_ticker[ticker] = info
            self.master_by_ticker[ticker.split(".")[0]] = info
            if is_kr_ticker(ticker) and len(ticker) == 6:
                self.master_by_ticker[f"{ticker}.KS"] = info
                self.master_by_ticker[f"{ticker}.KQ"] = info
        if name:
            self.master_by_name[clean_name_key(name)] = info
            brand = extract_short_brand_name(name)
            if brand:
                self.master_by_name[clean_name_key(brand)] = info

    def find_master_stock(self, raw_ticker: str, raw_name: str = "") -> Optional[Dict[str, Any]]:
        """3중 교차 검증으로 상장주식 마스터 종목 조회 (0.001s)"""
        t = clean_ticker_key(raw_ticker)
        n = (raw_name or "").strip()

        # 1차: 티커 직접 조회
        if t in self.master_by_ticker:
            return self.master_by_ticker[t]
        if t.split(".")[0] in self.master_by_ticker:
            return self.master_by_ticker[t.split(".")[0]]

        # 2차: 종목명 / 브랜드명 조회
        if n:
            nk = clean_name_key(n)
            if nk in self.master_by_name:
                return self.master_by_name[nk]
            brand = extract_short_brand_name(n)
            if brand and clean_name_key(brand) in self.master_by_name:
                return self.master_by_name[clean_name_key(brand)]

        # 3차: 온톨로지 사전 별칭 조회
        if t in self.dict_alias_map:
            alias_info = self.dict_alias_map[t]
            yt = alias_info.get("ticker")
            if yt and yt in self.master_by_ticker:
                return self.master_by_ticker[yt]
        if n and clean_name_key(n) in self.dict_alias_map:
            alias_info = self.dict_alias_map[clean_name_key(n)]
            yt = alias_info.get("ticker")
            if yt and yt in self.master_by_ticker:
                return self.master_by_ticker[yt]

        return None

    def find_invest_id(self, raw_ticker: str, raw_name: str = "") -> Optional[str]:
        """투자주 DB 존재 여부 3중 교차 검증 (0.001s)"""
        t = clean_ticker_key(raw_ticker)
        n = (raw_name or "").strip()

        if t in self.invest_by_ticker:
            return self.invest_by_ticker[t]
        if t.split(".")[0] in self.invest_by_ticker:
            return self.invest_by_ticker[t.split(".")[0]]

        if n:
            nk = clean_name_key(n)
            if nk in self.invest_by_name:
                return self.invest_by_name[nk]
            brand = extract_short_brand_name(n)
            if brand and clean_name_key(brand) in self.invest_by_name:
                return self.invest_by_name[clean_name_key(brand)]

        return None

    def register_invest_stock(self, ticker: str, name: str, master_id: str, country: str, fx_id: Optional[str]) -> Optional[str]:
        """투자주 DB 신규 등록 후 게이트웨이 인메모리 즉시 반영 (중복 생성 원천 차단)"""
        clean_t = clean_ticker_key(ticker)
        existing_id = self.find_invest_id(clean_t, name)
        if existing_id:
            logger.info(f"   🎯 [중복 방지] 이미 투자주 DB에 존재하는 종목: {name}({clean_t}) -> 기존 ID 재사용")
            return existing_id

        props: Dict[str, Any] = {
            "티커": {"title": [{"text": {"content": clean_t}}]},
            "상장주식DB 전체": {"relation": [{"id": master_id}]},
            "투자여부": {"multi_select": [{"name": "관심"}]},
            "업데이트 일자": {"date": {"start": kst_isoformat()}}
        }
        if country:
            props["국가"] = {"select": {"name": country}}
        if fx_id:
            props["환율전환"] = {"relation": [{"id": fx_id}]}

        try:
            res = self.session.post(
                "https://api.notion.com/v1/pages",
                json={"parent": {"database_id": self.interest_db_id}, "properties": props}
            )
            if res.status_code in (200, 201):
                new_id = res.json()["id"]
                self.invest_by_ticker[clean_t] = new_id
                self.invest_by_ticker[clean_t.split(".")[0]] = new_id
                if name:
                    self.invest_by_name[clean_name_key(name)] = new_id
                logger.info(f"   ✨ [투자주 승격 완료] {name}({clean_t}) -> ID: {new_id[:8]}")
                return new_id
            else:
                logger.warning(f"   ❌ [투자주 등록 실패] {clean_t}: {res.text}")
                return None
        except Exception as e:
            logger.error(f"   🚨 [투자주 생성 예외] {clean_t}: {e}")
            return None

    def register_master_stock(self, ticker: str, name: str) -> Optional[Dict[str, Any]]:
        """상장주식 Master DB 신규 등록 후 로컬 DB & 인메모리 즉시 반영"""
        clean_t = clean_ticker_key(ticker)
        existing = self.find_master_stock(clean_t, name)
        if existing:
            logger.info(f"   🎯 [중복 방지] 이미 마스터 DB에 존재하는 종목: {name}({clean_t}) -> 기존 ID 재사용")
            return existing

        tax = resolve_stock_taxonomy(clean_t, name)
        props: Dict[str, Any] = {
            "티커": {"title": [{"text": {"content": clean_t}}]},
            "종목명": {"rich_text": [{"text": {"content": name}}]},
            "Market": {"select": {"name": tax.get("market", "KOSPI")}},
            "국가": {"select": {"name": tax.get("country", "한국")}},
            "상품유형": {"select": {"name": tax.get("product_type", "개별기업주식")}},
            "자산군": {"select": {"name": tax.get("asset_class", "국내주식밸류")}},
            "인사이트상태": {"multi_select": [{"name": "💡 유튜브발굴"}, {"name": "🔭 관찰대상"}]},
            "업데이트 일자": {"date": {"start": kst_isoformat()}}
        }

        try:
            res = self.session.post(
                "https://api.notion.com/v1/pages",
                json={"parent": {"database_id": self.master_db_id}, "properties": props}
            )
            if res.status_code in (200, 201):
                new_id = res.json()["id"]
                info = {
                    "id": new_id,
                    "ticker": clean_t,
                    "name": name,
                    "market": tax.get("market"),
                    "country": tax.get("country"),
                    "insight_status": ["💡 유튜브발굴", "🔭 관찰대상"]
                }
                self._index_master_info(clean_t, name, info)

                upsert_stocks_batch([{
                    "ticker": clean_t,
                    "name": name,
                    "market": tax.get("market"),
                    "country": tax.get("country"),
                    "product_type": tax.get("product_type"),
                    "asset_class": tax.get("asset_class"),
                    "notion_page_id": new_id
                }])
                logger.info(f"   🔭 [마스터 신규 등록] {name}({clean_t}) -> ID: {new_id[:8]} (로컬 DB 영구 캐싱)")
                return info
            else:
                logger.warning(f"   ❌ [마스터 등록 실패] {clean_t}: {res.text}")
                return None
        except Exception as e:
            logger.error(f"   🚨 [마스터 생성 예외] {clean_t}: {e}")
            return None

    def match_and_resolve(self, raw_ticker: str, name: str) -> Tuple[Optional[str], str, str]:
        """
        원시 사명/티커 ➔ (투자주ID, 확정티커, 짧고간결한브랜드명) 0.001초 반환
        1. 로컬 SQLite DB & 투자주 DB 인메모리 색인 최우선 조회
        2. 온톨로지 사전(tbl_dictionary) 별칭 매핑 조회
        3. 완전 신규 종목 시 1회성 온라인 탐색(Yahoo Finance) 후 자동 등록
        """
        from core.notion_utils import search_foreign_ticker

        t = clean_ticker_key(raw_ticker)
        n = (name or "").strip()
        is_kr_code = is_kr_ticker(t)

        # 1. 한국 주식 (KRX 6자리 코드 및 .KS/.KQ)
        if is_kr_code:
            clean_t = t.split(".")[0].strip().upper()
            invest_id = self.find_invest_id(clean_t, n)
            if invest_id:
                return invest_id, clean_t, n

            m_info = self.find_master_stock(clean_t, n)
            master_id = m_info["id"] if m_info else ""
            if not master_id:
                new_m = self.register_master_stock(clean_t, n)
                master_id = new_m["id"] if new_m else ""

            new_id = self.register_invest_stock(clean_t, n, master_id, "한국", None)
            return new_id, clean_t, n

        # 2. 투자주 DB 인메모리 색인 조회 (0.001s)
        invest_id = self.find_invest_id(t, n)
        if invest_id:
            m_info = self.find_master_stock(t, n)
            confirmed_t = m_info["ticker"] if m_info else t
            brand_n = extract_short_brand_name((m_info["name"] if m_info else "") or n)
            return invest_id, confirmed_t, brand_n

        # 3. 상장주식 마스터 및 온톨로지 사전 매칭 조회 (0.001s)
        m_info = self.find_master_stock(t, n)
        if m_info:
            confirmed_t = m_info["ticker"]
            brand_n = extract_short_brand_name(m_info["name"] or n)
            country = m_info.get("country", "미국")
            new_id = self.register_invest_stock(confirmed_t, brand_n, m_info["id"], country, None)
            return new_id, confirmed_t, brand_n

        # 4. 사전에도 없는 완전 신규 해외 종목 1회성 온라인 탐색
        matched_ticker = ""
        matched_name = ""
        if n:
            search_res = search_foreign_ticker(n)
            if search_res:
                matched_ticker, matched_name = search_res

        if matched_ticker:
            clean_brand = extract_short_brand_name(matched_name or n)
            clean_t = clean_ticker_key(matched_ticker)

            new_m = self.register_master_stock(clean_t, clean_brand)
            master_id = new_m["id"] if new_m else ""
            country = "일본" if clean_t.endswith(".T") else "미국"
            new_id = self.register_invest_stock(clean_t, clean_brand, master_id, country, None)
            return new_id, clean_t, clean_brand

        short_brand = extract_short_brand_name(n)
        fallback_t = t if (re.match(r'^[A-Z0-9.\-_]{1,10}$', t) and not t.isdigit()) else ""
        return None, fallback_t, short_brand

    def get_master_map(self) -> Dict[str, str]:
        """티커 ➔ notion_page_id 매핑 딕셔너리 반환"""
        return {t: info["id"] for t, info in self.master_by_ticker.items()}
