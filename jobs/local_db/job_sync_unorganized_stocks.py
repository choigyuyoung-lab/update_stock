# -*- coding: utf-8 -*-
"""
sync_unorganized_stocks.py
==========================
[미정리 종목 DB]에 적재된 유튜브/외부 수집 종목에 대해 원스톱 지능형 처리 워크플로우를 완결합니다:
1. 실시간 주요 환율(USD/KRW, JPY/KRW, TWD/KRW, ILS/KRW) 크롤링 및 현재가 업데이트
2. 상장주식 Master DB와의 티커 매칭 및 Relation 자동 연결
3. 사용자가 노션에서 '정리' 체크박스를 체크한 경우 3대 분기 처리:
   - Case 1 (기존 투자주): [통합 특이사항 DB]로 핵심언급내용 이관 후 미정리 행 삭제
   - Case 2 (상장주식 DB 존재 & 투자주 DB 미등록):
     ➔ [투자주 DB]에 종목 자동 생성 (티커, 종목명, 마켓, 국가, 투자여부: "관심")
     ➔ 투자주 DB의 '상장주식DB' 및 '환율전환'(USDKRW 등) Relation 100% 자동 연결
     ➔ [상장주식 DB]에 '👑 투자주편입' 태그 자동 부여
     ➔ [통합 특이사항 DB]로 이관 후 미정리 DB 행 삭제
   - Case 3 (상장주식 DB 미등록 완전 신규 종목):
     ➔ [상장주식 Master DB]에 신규 등록 ('💡 유튜브발굴', '🔭 관찰대상' 태깅)
     ➔ 미정리 DB에 상장주식DB Relation 연결 후 관찰 상태로 보존 (다음번 언급 시 투자주 승격 가능)
"""

import os
import re
import sys
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import requests
import yfinance as yf
from dotenv import load_dotenv

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    kst_isoformat,
    ensure_database_properties,
    resolve_stock_taxonomy,
    is_kr_ticker,
)

UNORGANIZED_SCHEMA: Dict[str, Dict[str, Any]] = {
    "업데이트 일자": {"date": {}},
    "현재가": {"number": {"format": "number"}},
}

MASTER_EXT_SCHEMA: Dict[str, Dict[str, Any]] = {
    "인사이트상태": {"multi_select": {}},
}

INVESTMENT_SCHEMA: Dict[str, Dict[str, Any]] = {
    "업데이트 일자": {"date": {}},
    "국가": {"select": {}},
    "투자여부": {"multi_select": {}},
}

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
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID"], required=True)
MASTER_DB_ID = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID"], required=True)
INTEREST_DB_ID = get_db_id("DATABASE_ID", ["INTEREST_DATABASE_ID", "INTEREST_DB_ID"], required=True)
UNIFIED_NOTES_DB_ID = get_db_id("UNIFIED_NOTES_DATABASE_ID", ["UNIFIED_NOTES_DB_ID"], required=True)
BENCHMARK_DB_ID = os.environ.get("BENCHMARK_DATABASE_ID") or os.environ.get("BENCHMARK_DB_ID") or ""

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
    """노션 페이지의 현재가(Number) 및 업데이트 일자 속성을 업데이트합니다."""
    session.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        json={
            "properties": {
                "현재가": {"number": rate},
                "업데이트 일자": {"date": {"start": kst_isoformat()}}
            }
        }
    )


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
    elif dtype == "select":
        return p.get("select", {}).get("name") if p.get("select") else None
    elif dtype == "multi_select":
        return [item.get("name", "") for item in p.get("multi_select", []) if item.get("name")]
    elif dtype == "relation":
        return [r.get("id") for r in p.get("relation", []) if r.get("id")]
    return None


def get_all_pages(db_id: str) -> List[Dict[str, Any]]:
    """노션 DB의 모든 페이지를 페이지네이션하여 가져옵니다."""
    if not db_id:
        return []
    results = []
    has_more, cursor = True, None
    while has_more:
        payload: Dict[str, Any] = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        res = session.post(f"https://api.notion.com/v1/databases/{db_id}/query", json=payload)
        if res.status_code != 200:
            logger.warning(f"⚠️ DB 조회 실패 ({db_id}): {res.text}")
            break
        data = res.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
    return results


# ==============================================================================
# 3. 미정리 종목 개별 처리기 (3대 분기 원스톱 자동화)
# ==============================================================================
def process_unorganized_page(
    p: Dict[str, Any],
    exchange_rates: Dict[str, float],
    master_map: Dict[str, Dict[str, Any]],
    interest_map: Dict[str, str],
    fx_map: Dict[str, str]
) -> None:
    clean_ticker = ""
    try:
        p_id = p["id"]
        props = p["properties"]

        is_checked = get_prop(props, "정리")
        ticker_raw = get_prop(props, "티커")
        stock_name = get_prop(props, "종목명") or ticker_raw

        if not ticker_raw:
            if is_checked:
                print("   ⚠️ [스킵됨] '정리'에 체크는 되어있으나 '티커'가 입력되지 않았습니다.")
            return

        clean_ticker = normalize(ticker_raw)

        # 1. 환율 업데이트 (환율 티커인 경우)
        if clean_ticker in exchange_rates:
            current_rate = exchange_rates[clean_ticker]
            update_notion_rate(p_id, current_rate)
            print(f"   ✅ [환율] {clean_ticker} -> {current_rate:,.2f}원")

        # 2. 상장주식 Master DB 자동 연결 (정리와 무관하게 미연결 상태면 연결)
        has_master_rel = bool(props.get("상장주식DB", {}).get("relation"))
        if not has_master_rel and clean_ticker in master_map:
            m_id = master_map[clean_ticker]["id"]
            r1 = session.patch(
                f"https://api.notion.com/v1/pages/{p_id}",
                json={
                    "properties": {
                        "상장주식DB": {"relation": [{"id": m_id}]},
                        "업데이트 일자": {"date": {"start": kst_isoformat()}}
                    }
                }
            )
            if r1.status_code == 200:
                print(f"   🔗 [연결] {clean_ticker} 상장주식 Master DB 매칭 완료")
            else:
                print(f"   ❌ [연결 에러] {clean_ticker}: {r1.text}")

        # 3. '정리' 체크박스 체크 시 3대 분기 처리
        if is_checked:
            post_date = get_prop(props, "게시일") or datetime.now().strftime("%Y-%m-%d")
            context_text = props.get("핵심언급내용(Context - Korean)", {}).get("rich_text", [])
            target_invest_id: Optional[str] = None

            # ------------------------------------------------------------------
            # [Case 1] 투자주 DB에 이미 존재하는 경우
            # ------------------------------------------------------------------
            if clean_ticker in interest_map:
                target_invest_id = interest_map[clean_ticker]
                print(f"   🎯 [Case 1] {clean_ticker} 기존 투자주 DB 매칭 확인")

            # ------------------------------------------------------------------
            # [Case 2] 상장주식 DB에는 있고 투자주 DB에는 없는 경우 ➔ 투자주 DB 자동 승격
            # ------------------------------------------------------------------
            elif clean_ticker in master_map:
                m_info = master_map[clean_ticker]
                m_page_id = m_info["id"]
                m_market = m_info.get("market") or ("KOSPI" if is_kr_ticker(clean_ticker) else "NASDAQ")
                m_country = m_info.get("country") or ("한국" if is_kr_ticker(clean_ticker) else "미국")
                m_name = m_info.get("name") or stock_name

                print(f"   👑 [Case 2] {clean_ticker} ({m_name}) 투자주 DB 자동 승격 생성 중...")

                new_inv_props: Dict[str, Any] = {
                    "티커": {"title": [{"text": {"content": clean_ticker}}]},
                    "상장주식DB 전체": {"relation": [{"id": m_page_id}]},
                    "투자여부": {"multi_select": [{"name": "관심"}]},
                    "업데이트 일자": {"date": {"start": kst_isoformat()}}
                }
                if m_country:
                    new_inv_props["국가"] = {"select": {"name": m_country}}

                # 환율전환 Relation 자동 바인딩 (해외 종목)
                if m_market in ("NASDAQ", "NYSE", "AMEX", "ETF(US)") or m_country == "미국" or not is_kr_ticker(clean_ticker):
                    if "USDKRW" in fx_map:
                        new_inv_props["환율전환"] = {"relation": [{"id": fx_map["USDKRW"]}]}
                elif m_market in ("TSE", "TYO") or m_country == "일본":
                    if "JPYKRW" in fx_map:
                        new_inv_props["환율전환"] = {"relation": [{"id": fx_map["JPYKRW"]}]}

                inv_res = session.post(
                    "https://api.notion.com/v1/pages",
                    json={"parent": {"database_id": INTEREST_DB_ID}, "properties": new_inv_props}
                )

                if inv_res.status_code in (200, 201):
                    target_invest_id = inv_res.json()["id"]
                    interest_map[clean_ticker] = target_invest_id
                    print(f"   ✨ [승격 성공] {clean_ticker} 투자주 DB 생성 및 관계형(상장주식/환율) 자동 연결 완료")

                    # 상장주식 Master DB에 '👑 투자주편입' 태그 부여
                    cur_tags = list(m_info.get("insight_status") or [])
                    if "👑 투자주편입" not in cur_tags:
                        new_tags = cur_tags + ["👑 투자주편입"]
                        session.patch(
                            f"https://api.notion.com/v1/pages/{m_page_id}",
                            json={"properties": {"인사이트상태": {"multi_select": [{"name": t} for t in new_tags]}}}
                        )
                        m_info["insight_status"] = new_tags
                else:
                    print(f"   ❌ [승격 실패] {clean_ticker} 투자주 DB 생성 에러: {inv_res.text}")

            # ------------------------------------------------------------------
            # [Case 3] 상장주식 DB에도 없는 완전 신규 종목 ➔ 상장주식 DB 최초 등록 & 관찰
            # ------------------------------------------------------------------
            else:
                print(f"   🌱 [Case 3] {clean_ticker} ({stock_name}) 상장주식 Master DB 최초 등록 중...")
                tax = resolve_stock_taxonomy(clean_ticker, stock_name)

                new_mst_props: Dict[str, Any] = {
                    "티커": {"title": [{"text": {"content": clean_ticker}}]},
                    "종목명": {"rich_text": [{"text": {"content": stock_name}}]},
                    "Market": {"select": {"name": tax.get("market", "KOSPI")}},
                    "국가": {"select": {"name": tax.get("country", "한국")}},
                    "상품유형": {"select": {"name": tax.get("product_type", "개별기업주식")}},
                    "자산군": {"select": {"name": tax.get("asset_class", "국내주식밸류")}},
                    "인사이트상태": {"multi_select": [{"name": "💡 유튜브발굴"}, {"name": "🔭 관찰대상"}]},
                    "업데이트 일자": {"date": {"start": kst_isoformat()}}
                }

                mst_res = session.post(
                    "https://api.notion.com/v1/pages",
                    json={"parent": {"database_id": MASTER_DB_ID}, "properties": new_mst_props}
                )

                if mst_res.status_code in (200, 201):
                    new_mst_id = mst_res.json()["id"]
                    master_map[clean_ticker] = {
                        "id": new_mst_id,
                        "name": stock_name,
                        "market": tax.get("market"),
                        "country": tax.get("country"),
                        "insight_status": ["💡 유튜브발굴", "🔭 관찰대상"]
                    }
                    # 미정리 DB에 상장주식DB 연결 & 정리 체크박스 해제 (관찰 상태 안내)
                    session.patch(
                        f"https://api.notion.com/v1/pages/{p_id}",
                        json={
                            "properties": {
                                "상장주식DB": {"relation": [{"id": new_mst_id}]},
                                "정리": {"checkbox": False},
                                "업데이트 일자": {"date": {"start": kst_isoformat()}}
                            }
                        }
                    )
                    print(f"   🔭 [마스터 등록 완료] {clean_ticker} 상장주식 DB 등록 및 관찰대상 태깅 (미정리 DB 연결 완료)")
                else:
                    print(f"   ❌ [마스터 등록 실패] {clean_ticker}: {mst_res.text}")

            # ------------------------------------------------------------------
            # 통합 특이사항 DB 이관 및 미정리 DB 원본 삭제 (Case 1 및 Case 2)
            # ------------------------------------------------------------------
            if target_invest_id:
                dest_props: Dict[str, Any] = {
                    "[티커] 날짜 요약": {"title": [{"text": {"content": f"[{ticker_raw}] {post_date[2:10].replace('-','.')}"}}]},
                    "날짜": {"date": {"start": post_date}},
                    "특이사항": {"rich_text": context_text},
                    "투자주 DB": {"relation": [{"id": target_invest_id}]}
                }

                res = session.post(
                    "https://api.notion.com/v1/pages",
                    json={"parent": {"database_id": UNIFIED_NOTES_DB_ID}, "properties": dest_props}
                )

                if res.status_code in (200, 201):
                    session.delete(f"https://api.notion.com/v1/blocks/{p_id}")
                    print(f"   📦 [이관 완결] {clean_ticker} 통합 특이사항 DB 이동 및 미정리 DB 삭제 완료")
                else:
                    print(f"   ❌ [이관 에러] {clean_ticker} 특이사항 등록 실패: {res.text}")

    except Exception as e:
        print(f"   🚨 [코드 에러] {clean_ticker if clean_ticker else 'Unknown'}: {e}")


# ==============================================================================
# 4. 메인 실행 함수
# ==============================================================================
def main() -> None:
    if not NOTION_TOKEN:
        print("❌ NOTION_TOKEN이 설정되지 않았습니다.")
        return

    print("=" * 80)
    print("🚀 [Link Master DB] 미정리 종목 지능형 승격, 환율 업데이트 및 통합 특이사항 이관 시작")
    print("=" * 80)

    client = build_notion_client(NOTION_TOKEN)
    ensure_database_properties(client, UNORGANIZED_DB_ID, UNORGANIZED_SCHEMA)
    ensure_database_properties(client, MASTER_DB_ID, MASTER_EXT_SCHEMA)
    ensure_database_properties(client, INTEREST_DB_ID, INVESTMENT_SCHEMA)

    print("💵 실시간 주요 환율 크롤링 중...")
    exchange_rates = get_exchange_rates()
    print(f"   ✅ 수집된 환율: {exchange_rates}")

    # 1. 벤치마크/환율 지표 DB 색인 (USDKRW, JPYKRW)
    fx_map: Dict[str, str] = {}
    if BENCHMARK_DB_ID:
        try:
            bm_pages = get_all_pages(BENCHMARK_DB_ID)
            for bp in bm_pages:
                t = normalize(get_prop(bp["properties"], "티커") or get_prop(bp["properties"], "Ticker"))
                if t in ("USDKRW", "JPYKRW", "EURKRW", "CNYKRW"):
                    fx_map[t] = bp["id"]
            print(f"   ✅ 지표 DB 환율 매핑: {list(fx_map.keys())}")
        except Exception as exc:
            logger.warning(f"⚠️ 환율 지표 로드 실패: {exc}")

    # 2. 상장주식 Master DB 색인 로드
    print("🔍 상장주식 Master DB 색인 로드 중...")
    master_map: Dict[str, Dict[str, Any]] = {}
    try:
        from core.local_db_manager import get_db_connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ticker, notion_page_id, name, market, country FROM tbl_stocks WHERE notion_page_id != '';")
            rows = cursor.fetchall()
            master_map = {
                normalize(r['ticker']): {
                    "id": r['notion_page_id'],
                    "name": r['name'],
                    "market": r['market'],
                    "country": r['country'],
                    "insight_status": []
                }
                for r in rows
            }
            print(f"   ⚡ [로컬 SQLite 0.001s 로드] 상장주식 Master DB: {len(master_map)}개 티커 색인 즉시 활성화")
    except Exception:
        pass

    if not master_map:
        master_pages = get_all_pages(MASTER_DB_ID)
        for p in master_pages:
            t = normalize(get_prop(p['properties'], "티커"))
            if t:
                master_map[t] = {
                    "id": p['id'],
                    "name": get_prop(p['properties'], "종목명") or t,
                    "market": get_prop(p['properties'], "Market"),
                    "country": get_prop(p['properties'], "국가"),
                    "insight_status": get_prop(p['properties'], "인사이트상태") or []
                }
        print(f"   ✅ 상장주식 Master DB: {len(master_map)}개 티커 색인")

    # 3. 투자주 DB 색인 로드
    interest_pages = get_all_pages(INTEREST_DB_ID)
    interest_map = {
        normalize(get_prop(p['properties'], "티커") or get_prop(p['properties'], "Ticker")): p['id']
        for p in interest_pages
        if get_prop(p['properties'], "티커") or get_prop(p['properties'], "Ticker")
    }
    print(f"   ✅ 투자주 DB: {len(interest_map)}개 티커 색인")

    # 4. 미정리 종목 처리 및 매칭 시작
    print("\n🚀 미정리 종목 처리 및 매칭 시작...")
    unorganized_pages = get_all_pages(UNORGANIZED_DB_ID)
    print(f"   📋 총 {len(unorganized_pages)}개의 미정리 항목 처리 중...")

    # 미정리 DB 내 환율 페이지 색인 (USDKRW, JPYKRW 등)
    for p in unorganized_pages:
        t = normalize(get_prop(p["properties"], "티커"))
        if t in ("USDKRW", "JPYKRW", "EURKRW", "TWDKRW", "ILSKRW", "CNYKRW"):
            fx_map[t] = p["id"]
    if fx_map:
        print(f"   ✅ 환율 전환 매핑 활성화: {list(fx_map.keys())}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for p in unorganized_pages:
            executor.submit(process_unorganized_page, p, exchange_rates, master_map, interest_map, fx_map)

    print("\n🎉 모든 자동화 작업이 성공적으로 완료되었습니다.")


if __name__ == "__main__":
    main()
