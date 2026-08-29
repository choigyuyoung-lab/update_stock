# -*- coding: utf-8 -*-
"""
[test]_job_sync_benchmark.py
============================
[Phase 3 Modernized Macro Benchmarks Sync Test Engine]
- 글로벌 54개 거시경제 지표(환율, 금리, 유가, 금, 주요 지수) 동기화
- FDR -> Yahoo Finance -> Stooq 3단계 폴백
- 비파괴적 병렬 테스트 파일: job_sync_benchmark.py 원본 100% 무수정 보존
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

from core.notion_utils import get_kst_str
from core.local_db_manager import (
    init_database,
    get_db_connection,
    upsert_benchmarks_batch,
)
from core.stock_registry import clean_ticker_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TestSyncBenchmark")

load_dotenv()


def run_benchmark_macro_test():
    """글로벌 거시 지표 동기화 비파괴 단위 테스트"""
    logger.info("🧪 [Test Runner] 글로벌 거시경제 벤치마크(Macro Benchmark) 현대화 배치 비파괴 테스트 시작")
    init_database()

    sample_benchmarks = [
        {
            "ticker": "USDKRW",
            "summary": "원/달러 환율 (KRW/USD)",
            "category": "환율",
            "country": "글로벌",
            "keywords": "달러,원화,환율,FX",
            "notion_page_id": "mock_page_usdkrw",
            "updated_at": get_kst_str(),
        },
        {
            "ticker": "US10Y",
            "summary": "미국채 10년물 금리 (US 10-Year Treasury Yield)",
            "category": "금리",
            "country": "미국",
            "keywords": "국채,금리,10년물,이자율",
            "notion_page_id": "mock_page_us10y",
            "updated_at": get_kst_str(),
        },
        {
            "ticker": "WTI",
            "summary": "서부 텍사스산 원유 (WTI Crude Oil)",
            "category": "원자재",
            "country": "글로벌",
            "keywords": "유가,원유,오일,에너지",
            "notion_page_id": "mock_page_wti",
            "updated_at": get_kst_str(),
        }
    ]

    # 1. 거시 벤치마크 SQLite 적재
    t0 = time.perf_counter()
    upsert_benchmarks_batch(sample_benchmarks)
    upsert_ms = (time.perf_counter() - t0) * 1000
    logger.info(f"✅ [1단계] {len(sample_benchmarks)}개 거시 벤치마크 SQLite 적재 완료 ({upsert_ms:.2f}ms)")

    # 2. 로컬 DB 검증
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ticker, summary, category FROM tbl_benchmarks WHERE ticker = 'USDKRW';")
        row = cursor.fetchone()
        assert row is not None, "USDKRW benchmark missing!"
        assert row["category"] == "환율"
        logger.info(f"✅ [2단계] 거시 지표 무결성 검증 통과: {row['ticker']} -> {row['summary']} ({row['category']})")

    logger.info("🎉 [SUCCESS] 글로벌 거시 벤치마크 비파괴적 병렬 테스트 완료!")


if __name__ == "__main__":
    run_benchmark_macro_test()
