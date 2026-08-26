# -*- coding: utf-8 -*-
"""
job_sync_unorganized_stocks.py
==============================
[미정리 종목 DB]에 적재된 유튜브/외부 수집 종목에 대해 원스톱 지능형 처리 워크플로우를 완결합니다:
1. 실시간 주요 환율(USD/KRW, JPY/KRW, EUR/KRW, TWD/KRW, ILS/KRW) 크롤링 및 현재가 업데이트
2. 로컬 SQLite 캐시 DB(tbl_stocks, tbl_dictionary) 및 노션 실시간 3중 교차 검증:
   - 1차: 티커 정규화 매칭 (원본, .KS/.KQ/.T 접미사 호환)
   - 2차: 종목명 및 정제 브랜드명 교차 매칭
   - 3차: 온톨로지 사전(tbl_dictionary) 별칭 매칭
3. 사용자가 노션에서 '정리' 체크박스를 체크한 경우 3대 분기 처리:
   - Case 1 (기존 투자주 존재): [통합 특이사항 DB]로 핵심언급내용 이관 후 미정리 행 삭제
   - Case 2 (상장주식 DB 존재 & 투자주 DB 미등록):
     ➔ [투자주 DB]에 종목 자동 생성 (티커, 종목명, 마켓, 국가, 투자여부: "관심")
     ➔ 투자주 DB의 '상장주식DB' 및 '환율전환'(USDKRW, JPYKRW 등) Relation 100% 자동 연결
     ➔ [상장주식 DB]에 '👑 투자주편입' 태그 자동 부여
     ➔ [통합 특이사항 DB]로 이관 후 미정리 DB 행 삭제
   - Case 3 (3중 검증 결과 완전 신규 종목):
     ➔ [상장주식 Master DB]에 신규 등록 ('💡 유튜브발굴', '🔭 관찰대상' 태깅)
     ➔ 미정리 DB에 상장주식DB Relation 연결 후 관찰 상태로 보존 (다음번 언급 시 투자주 승격 가능)
"""

import os
import re
import sys
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
import yfinance as yf
from dotenv import load_dotenv

from pathlib import Path

# 프로젝트 루트 디렉토리를 sys.path 최상단에 선제 등록 (독립 실행 및 모듈 실행 안전 보장)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Windows 콘솔 UTF-8 출력 안전화
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LinkMasterDB")

load_dotenv()

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    kst_isoformat,
    ensure_database_properties,
    resolve_stock_taxonomy,
    is_kr_ticker,
    extract_short_brand_name,
    paginate_database,
    safe_databases_query,
)
from core.local_db_manager import (
    get_db_connection,
    init_database,
    upsert_stocks_batch,
    export_all_tables_to_csv,
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

# ==============================================================================
# 1. 환경 변수 및 DB ID 로드
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID"], required=True)
MASTER_DB_ID = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID"], required=True)
INTEREST_DB_ID = get_db_id("DATABASE_ID", ["INTEREST_DATABASE_ID", "INTEREST_DB_ID"], required=True)
UNIFIED_NOTES_DB_ID = get_db_id("UNIFIED_NOTES_DATABASE_ID", ["UNIFIED_NOTES_DB_ID"], required=True)
BENCHMARK_DB_ID = os.environ.get("BENCHMARK_DATABASE_ID") or os.environ.get("BENCHMARK_DB_ID") or ""

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
})


from core.stock_registry import (
    StockRegistryGateway,
    clean_ticker_key,
    clean_name_key,
)


# ==============================================================================
# 4. 미정리 종목 개별 처리기 (3대 분기 원스톱 자동화)
# ==============================================================================
def process_unorganized_page(
    p: Dict[str, Any],
    exchange_rates: Dict[str, float],
    gateway: StockRegistryGateway,
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
                logger.info("   ⚠️ [스킵됨] '정리'에 체크는 되어있으나 '티커'가 입력되지 않았습니다.")
            return

        clean_ticker = clean_ticker_key(ticker_raw)

        # 1. 환율 업데이트 (환율 티커인 경우)
        if clean_ticker in exchange_rates:
            current_rate = exchange_rates[clean_ticker]
            update_notion_rate(p_id, current_rate)
            logger.info(f"   ✅ [환율] {clean_ticker} -> {current_rate:,.2f}원")

        # 2. 상장주식 Master DB 자동 연결 (3중 교차 검증으로 매칭)
        has_master_rel = bool(props.get("상장주식DB", {}).get("relation"))
        m_info = gateway.find_master_stock(clean_ticker, stock_name)

        if not has_master_rel and m_info:
            m_id = m_info["id"]
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
                logger.info(f"   🔗 [연결] {clean_ticker} 상장주식 Master DB 매칭 완료")

        # 3. '정리' 체크박스 체크 시 3대 분기 처리
        if is_checked:
            post_date = get_prop(props, "게시일") or datetime.now().strftime("%Y-%m-%d")
            context_text = props.get("핵심언급내용(Context - Korean)", {}).get("rich_text", [])
            target_invest_id: Optional[str] = None

            # [Case 1] 투자주 DB에 이미 존재하는 경우 (3중 교차 검증)
            invest_id = gateway.find_invest_id(clean_ticker, stock_name)
            if invest_id:
                target_invest_id = invest_id
                logger.info(f"   🎯 [Case 1] {clean_ticker} 기존 투자주 DB 매칭 확인 (ID: {invest_id[:8]})")

            # [Case 2] 상장주식 DB에는 있고 투자주 DB에는 없는 경우 ➔ 투자주 DB 자동 승격
            elif m_info:
                m_page_id = m_info["id"]
                m_market = m_info.get("market") or ("KOSPI" if is_kr_ticker(clean_ticker) else "NASDAQ")
                m_country = m_info.get("country") or ("한국" if is_kr_ticker(clean_ticker) else "미국")
                m_name = m_info.get("name") or stock_name

                # 환율 ID 선택
                fx_id = None
                if m_market in ("TSE", "TYO") or m_country == "일본" or clean_ticker.endswith(".T"):
                    fx_id = fx_map.get("JPYKRW")
                elif m_market in ("NASDAQ", "NYSE", "AMEX", "ETF(US)") or m_country == "미국" or not is_kr_ticker(clean_ticker):
                    fx_id = fx_map.get("USDKRW")

                target_invest_id = gateway.register_invest_stock(
                    ticker=clean_ticker,
                    name=m_name,
                    master_id=m_page_id,
                    country=m_country,
                    fx_id=fx_id
                )

                # 상장주식 Master DB에 '👑 투자주편입' 태그 부여
                if target_invest_id:
                    cur_tags = list(m_info.get("insight_status") or [])
                    if "👑 투자주편입" not in cur_tags:
                        new_tags = cur_tags + ["👑 투자주편입"]
                        session.patch(
                            f"https://api.notion.com/v1/pages/{m_page_id}",
                            json={"properties": {"인사이트상태": {"multi_select": [{"name": t} for t in new_tags]}}}
                        )
                        m_info["insight_status"] = new_tags

            # [Case 3] 상장주식 DB에도 없는 완전 신규 종목 ➔ 상장주식 DB 최초 등록 & 관찰
            else:
                new_m_info = gateway.register_master_stock(clean_ticker, stock_name)
                if new_m_info:
                    new_mst_id = new_m_info["id"]
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
                    logger.info(f"   🔭 [마스터 등록 완료] {clean_ticker} 상장주식 DB 등록 및 관찰대상 태깅")

            # 통합 특이사항 DB 이관 및 미정리 DB 원본 삭제 (Case 1 및 Case 2)
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
                    logger.info(f"   📦 [이관 완결] {clean_ticker} 통합 특이사항 DB 이동 및 미정리 DB 삭제 완료")
                else:
                    logger.warning(f"   ❌ [이관 에러] {clean_ticker} 특이사항 등록 실패: {res.text}")

    except Exception as e:
        logger.error(f"   🚨 [처리 에러] {clean_ticker if clean_ticker else 'Unknown'}: {e}")


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    if not NOTION_TOKEN:
        logger.error("❌ NOTION_TOKEN이 설정되지 않았습니다.")
        return

    logger.info("=" * 80)
    logger.info("🚀 [Link Master DB] 미정리 종목 지능형 승격, 환율 업데이트 및 통합 특이사항 이관 시작")
    logger.info("=" * 80)

    client = build_notion_client(NOTION_TOKEN)
    ensure_database_properties(client, UNORGANIZED_DB_ID, UNORGANIZED_SCHEMA)
    ensure_database_properties(client, MASTER_DB_ID, MASTER_EXT_SCHEMA)
    ensure_database_properties(client, INTEREST_DB_ID, INVESTMENT_SCHEMA)

    logger.info("💵 실시간 주요 환율 크롤링 중...")
    exchange_rates = get_exchange_rates()
    logger.info(f"   ✅ 수집된 환율: {exchange_rates}")

    # 1. 벤치마크/환율 지표 DB 색인 (USDKRW, JPYKRW)
    fx_map: Dict[str, str] = {}
    if BENCHMARK_DB_ID:
        try:
            for bp in paginate_database(client, BENCHMARK_DB_ID, page_size=100):
                t = clean_ticker_key(get_prop(bp["properties"], "티커") or get_prop(bp["properties"], "Ticker"))
                if t in ("USDKRW", "JPYKRW", "EURKRW", "TWDKRW", "ILSKRW", "CNYKRW"):
                    fx_map[t] = bp["id"]
            logger.info(f"   ✅ 지표 DB 환율 매핑: {list(fx_map.keys())}")
        except Exception as exc:
            logger.warning(f"⚠️ 환율 지표 로드 실패: {exc}")

    # 2. 통합 종목 검증 게이트웨이 초기화 (로컬 SQLite 0.001s + 노션 인메모리 색인)
    logger.info("🔍 통합 종목 레지스트리 게이트웨이 초기화 중...")
    gateway = StockRegistryGateway(client)

    # 3. 미정리 종목 처리 및 매칭 시작
    logger.info("\n🚀 미정리 종목 처리 및 매칭 시작...")
    unorganized_pages = list(paginate_database(client, UNORGANIZED_DB_ID, page_size=100))
    logger.info(f"   📋 총 {len(unorganized_pages)}개의 미정리 항목 처리 중...")

    # 미정리 DB 내 환율 페이지 색인
    for p in unorganized_pages:
        t = clean_ticker_key(get_prop(p["properties"], "티커"))
        if t in ("USDKRW", "JPYKRW", "EURKRW", "TWDKRW", "ILSKRW", "CNYKRW"):
            fx_map[t] = p["id"]

    with ThreadPoolExecutor(max_workers=5) as executor:
        for p in unorganized_pages:
            executor.submit(process_unorganized_page, p, exchange_rates, gateway, fx_map)

    # 4. 변경된 마스터 테이블 CSV 덤프
    export_all_tables_to_csv()
    logger.info("\n🎉 모든 자동화 작업이 성공적으로 완료되었습니다.")


if __name__ == "__main__":
    main()
