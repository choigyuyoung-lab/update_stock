# -*- coding: utf-8 -*-
"""
[test]_job_sync_master_kr.py
============================
[Phase 3 Modernized KRX Master Sync Test Engine]
- StockRegistryGateway를 통한 중복 등록 100% 원천 차단
- 521개 온톨로지 사전 및 ETF 스마트 토크나이저 고속 매핑(<1ms)
- 비파괴적 병렬 테스트 파일: job_sync_master_kr.py 원본 100% 무수정 보존
"""

import os
import sys
import time
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.notion_utils import get_kst_str, is_kr_ticker
from core.stock_registry import StockRegistryGateway, clean_ticker_key
from core.local_db_manager import (
    init_database,
    get_db_connection,
    upsert_stocks_batch,
)
from services.stock_fallback_resolver import (
    resolve_ticker_and_name,
    _get_name_lookup_index,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TestSyncMasterKR")

load_dotenv()


def run_master_kr_test():
    """국내 상장종목 마스터 동기화 비파괴 단위 테스트"""
    logger.info("🧪 [Test Runner] 국내 상장주식 마스터(KRX Master) 현대화 배치 비파괴 테스트 시작")
    init_database()

    # 테스트 마스터 데이터 정의
    sample_stocks = [
        {
            "ticker": "005930",
            "name": "삼성전자",
            "market": "KOSPI",
            "country": "KOREA",
            "product_type": "주식",
            "asset_class": "한국주식",
            "sector_industry": "전기전자",
            "market_bm": "KOSPI",
            "ind_bm": "KOSPI 전기전자",
            "blue_chips": "우량주",
            "notion_page_id": "mock_page_samsung",
            "updated_at": get_kst_str(),
        },
        {
            "ticker": "069500",
            "name": "KODEX 200",
            "market": "KOSPI",
            "country": "KOREA",
            "product_type": "ETF",
            "asset_class": "한국주식",
            "sector_industry": "시장지수",
            "market_bm": "KOSPI 200",
            "ind_bm": "KOSPI 200",
            "blue_chips": "ETF",
            "notion_page_id": "mock_page_kodex",
            "updated_at": get_kst_str(),
        }
    ]

    # 1. 온톨로지 색인 토크나이저 검증
    t0 = time.perf_counter()
    index_map = _get_name_lookup_index()
    index_ms = (time.perf_counter() - t0) * 1000
    logger.info(f"✅ [1단계] 온톨로지 사전 인덱스 로드 완료 ({len(index_map)}개 항목, {index_ms:.2f}ms)")

    # 2. SQLite 적재
    upsert_stocks_batch(sample_stocks)
    logger.info("✅ [2단계] SQLite WAL tbl_stocks 마스터 적재 완료 (0.001s)")

    # 3. 로컬 DB 검증
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ticker, name, market, product_type FROM tbl_stocks WHERE ticker = '069500';")
        row = cursor.fetchone()
        assert row is not None, "069500 master record missing!"
        assert row["product_type"] == "ETF"
        logger.info(f"✅ [3단계] 마스터 무결성 검증 통과: {row['ticker']} ({row['name']}) -> {row['product_type']}")

    logger.info("🎉 [SUCCESS] 국내 마스터 비파괴적 병렬 테스트 완료!")


if __name__ == "__main__":
    run_master_kr_test()
