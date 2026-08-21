# -*- coding: utf-8 -*-
"""
link_master_db.py
=================
[미정리 종목 DB]에 적재된 유튜브/외부 수집 종목에 대해:
1. 실시간 주요 환율(USD/KRW, JPY/KRW, TWD/KRW, ILS/KRW) 크롤링 및 현재가 업데이트
2. 상장주식 Master DB(상장주식DB 전체)와의 티커 매칭 및 Relation 자동 연결
3. 사용자가 노션에서 '정리' 체크박스를 체크한 경우:
   - [통합 특이사항 DB]로 핵심언급내용을 이관 생성 (투자주 DB Relation 자동 연결)
   - [미정리 종목 DB]의 원본 행을 삭제하여 깔끔한 수동 검토 워크플로우를 완결합니다.
"""

import os
import re
import sys
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import requests
import yfinance as yf
from dotenv import load_dotenv

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    get_kst_str,
    paginate_database,
    get_prop_value,
)

# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LinkMasterDB")

load_dotenv()

# ==============================================================================
# 1. 환경 변수 및 DB ID 로드
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID", "2d8f59dbdb5b807aac70d3711b5b6e93"], required=True)
MASTER_DB_ID = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID", "2f0f59dbdb5b80e5bc5fe1ffdd3b941a"], required=True)
INTEREST_DB_ID = get_db_id("DATABASE_ID", ["INTEREST_DATABASE_ID", "2a9f59dbdb5b80fbab45dea3b3cbe9f4"], required=True)
UNIFIED_NOTES_DB_ID = get_db_id("UNIFIED_NOTES_DATABASE_ID", ["UNIFIED_NOTES_DB_ID", "2f8f59dbdb5b804e8318e9a3f0efaf9d"], required=True)

headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

session = requests.Session()
session.headers.update(headers)


def normalize(ticker: str) -> str:
    """티커 문자열에서 특수문자를 제거하고 대문자 표준 포맷으로 정규화합니다."""
    if not ticker:
        return ""
    return re.sub(r'[^0-9A-Z]', '', str(ticker).strip().upper())


# ==============================================================================
# 2. 환율 시세 크롤링
# ==============================================================================
def get_exchange_rates() -> Dict[str, float]:
    """yfinance를 통해 실시간 환율을 조회합니다."""
    rates: Dict[str, float] = {}
    direct_tickers = {"USDKRW": "USDKRW=X", "JPYKRW": "JPYKRW=X", "TWDKRW": "TWDKRW=X"}
    for notion_ticker, yf_ticker in direct_tickers.items():
        try:
            hist = yf.Ticker(yf_ticker).history(period="1d")
            if not hist.empty:
                rate = float(hist['Close'].iloc[-1])
                if notion_ticker == "JPYKRW":
                    rate *= 100
                rates[notion_ticker] = round(rate, 2)
        except Exception:
            pass
    try:
        usd_krw = rates.get("USDKRW")
        hist_ils = yf.Ticker("ILS=X").history(period="1d")
        if usd_krw and not hist_ils.empty:
            rates["ILSKRW"] = round(usd_krw / float(hist_ils['Close'].iloc[-1]), 2)
    except Exception:
        pass
    return rates


def update_notion_rate(page_id: str, rate: float) -> None:
    """노션 페이지의 현재가(Number) 속성을 업데이트합니다."""
    session.patch(f"https://api.notion.com/v1/pages/{page_id}", json={"properties": {"현재가": {"number": rate}}})


def get_prop(props: Dict[str, Any], key: str) -> Any:
    """노션 프로퍼티 값을 추출합니다."""
    p = props.get(key)
    if not p:
        return None
    dtype = p.get('type')
    if dtype == "title":
        return p.get("title", [])[0].get("plain_text", "") if p.get("title") else ""
    elif dtype == "rich_text":
        return p.get("rich_text", [])[0].get("plain_text", "") if p.get("rich_text") else ""
    elif dtype == "checkbox":
        return p.get("checkbox", False)
    elif dtype == "date":
        return p.get("date", {}).get("start") if p.get("date") else None
    elif dtype == "number":
        return p.get("number")
    return None


def get_all_pages(db_id: str) -> List[Dict[str, Any]]:
    """노션 DB의 모든 페이지를 페이지네이션하여 가져옵니다."""
    results = []
    has_more, cursor = True, None
    while has_more:
        payload: Dict[str, Any] = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        res = session.post(f"https://api.notion.com/v1/databases/{db_id}/query", json=payload)
        data = res.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
    return results


# ==============================================================================
# 3. 미정리 종목 개별 처리기
# ==============================================================================
def process_unorganized_page(p: Dict[str, Any], exchange_rates: Dict[str, float], master_map: Dict[str, str], interest_map: Dict[str, str]) -> None:
    try:
        p_id = p["id"]
        props = p["properties"]

        is_checked = get_prop(props, "정리")
        ticker_raw = get_prop(props, "티커")

        if not ticker_raw:
            if is_checked:
                print(f"   ⚠️ [스킵됨] '정리'에 체크는 되어있으나 '티커'가 입력되지 않았습니다.")
            return

        clean_ticker = normalize(ticker_raw)

        # 1. 환율 업데이트
        if clean_ticker in exchange_rates:
            current_rate = exchange_rates[clean_ticker]
            update_notion_rate(p_id, current_rate)
            print(f"   ✅ [환율] {clean_ticker} -> {current_rate:,.2f}원")

        # 2. 상장주식 DB 연결
        has_master_rel = bool(props.get("상장주식DB", {}).get("relation"))
        if not has_master_rel and clean_ticker in master_map:
            r1 = session.patch(
                f"https://api.notion.com/v1/pages/{p_id}",
                json={"properties": {"상장주식DB": {"relation": [{"id": master_map[clean_ticker]}]}}}
            )
            if r1.status_code == 200:
                print(f"   🔗 [연결] {clean_ticker} 상장주식 Master DB 매칭 완료")
            else:
                print(f"   ❌ [연결 에러] {clean_ticker}: {r1.text}")

        # 3. '정리' 체크 시 통합 특이사항 DB로 이관 후 원본 삭제
        if is_checked:
            post_date = get_prop(props, "게시일") or datetime.now().strftime("%Y-%m-%d")
            dest_props: Dict[str, Any] = {
                "[티커] 날짜 요약": {"title": [{"text": {"content": f"[{ticker_raw}] {post_date[2:10].replace('-','.')}"}}]},
                "날짜": {"date": {"start": post_date}},
                "특이사항": {"rich_text": props.get("핵심언급내용(Context - Korean)", {}).get("rich_text", [])}
            }
            if clean_ticker in interest_map:
                dest_props["투자주 DB"] = {"relation": [{"id": interest_map[clean_ticker]}]}

            res = session.post(
                "https://api.notion.com/v1/pages",
                json={"parent": {"database_id": UNIFIED_NOTES_DB_ID}, "properties": dest_props}
            )

            if res.status_code == 200:
                session.delete(f"https://api.notion.com/v1/blocks/{p_id}")
                print(f"   📦 [이관 성공] {clean_ticker} 통합 특이사항 DB로 이동 및 미정리 DB 삭제 완료")
            else:
                print(f"   ❌ [이관 에러] {clean_ticker} 이동 실패: {res.text}")

    except Exception as e:
        print(f"   🚨 [코드 에러] {e}")


# ==============================================================================
# 4. 메인 실행 함수
# ==============================================================================
def main() -> None:
    if not NOTION_TOKEN:
        print("❌ NOTION_TOKEN이 설정되지 않았습니다.")
        return

    print("=" * 80)
    print("🚀 [Link Master DB] 미정리 종목 분석, 환율 업데이트 및 통합 특이사항 DB 이관 시작")
    print("=" * 80)

    print("💵 실시간 주요 환율 크롤링 중...")
    exchange_rates = get_exchange_rates()
    print(f"   ✅ 수집된 환율: {exchange_rates}")

    print("🔍 노션 데이터베이스 색인 생성 중...")
    master_pages = get_all_pages(MASTER_DB_ID)
    master_map = {normalize(get_prop(p['properties'], "티커")): p['id'] for p in master_pages if get_prop(p['properties'], "티커")}
    print(f"   ✅ 상장주식 Master DB: {len(master_map)}개 티커 색인")

    interest_pages = get_all_pages(INTEREST_DB_ID)
    interest_map = {normalize(get_prop(p['properties'], "티커")): p['id'] for p in interest_pages if get_prop(p['properties'], "티커")}
    print(f"   ✅ 투자주 DB: {len(interest_map)}개 티커 색인")

    print("\n🚀 미정리 종목 처리 및 매칭 시작...")
    unorganized_pages = get_all_pages(UNORGANIZED_DB_ID)
    print(f"   📋 총 {len(unorganized_pages)}개의 미정리 항목 처리 중...")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for p in unorganized_pages:
            executor.submit(process_unorganized_page, p, exchange_rates, master_map, interest_map)

    print("\n🎉 모든 자동화 작업이 성공적으로 완료되었습니다.")


if __name__ == "__main__":
    main()
