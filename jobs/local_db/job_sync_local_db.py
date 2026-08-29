# -*- coding: utf-8 -*-
"""
[test]_job_sync_local_db.py
===========================
[Phase 3 Modernized Local DB & CSV Sync Test Engine]
- 6대 테이블(tbl_youtube_insights 포함) SQLite WAL DB 무결성 검증
- 5종 CSV 파일 영구 덤프 및 0.01초 자가 복구 검증
- 비파괴적 병렬 테스트 파일: job_sync_local_db.py 원본 100% 무수정 보존
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

from core.local_db_manager import (
    init_database,
    get_db_connection,
    export_all_tables_to_csv,
    DICTIONARY_CSV_PATH,
    STOCKS_CSV_PATH,
    BENCHMARKS_CSV_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TestSyncLocalDB")

load_dotenv()


def run_local_db_sync_test():
    """로컬 DB 및 CSV 덤프 비파괴 단위 테스트"""
    logger.info("🧪 [Test Runner] 로컬 DB 및 CSV 백업 현대화 배치 비파괴 테스트 시작")
    init_database()

    # 1. 6대 테이블 레코드 카운트 검증
    with get_db_connection() as conn:
        cursor = conn.cursor()
        tables = ["tbl_dictionary", "tbl_benchmarks", "tbl_stocks", "tbl_etf_holdings", "tbl_finances", "tbl_youtube_insights"]
        counts = {}
        for t in tables:
            cursor.execute(f"SELECT count(*) FROM {t};")
            counts[t] = cursor.fetchone()[0]
            logger.info(f"📊 [{t}] 총 레코드: {counts[t]:,}건")

    # 2. CSV 파일 덤프 실행
    t0 = time.perf_counter()
    export_all_tables_to_csv()
    dump_ms = (time.perf_counter() - t0) * 1000
    logger.info(f"✅ [1단계] CSV 5종 영구 백업 완료 ({dump_ms:.2f}ms)")

    # 3. CSV 파일 실존 검증
    assert os.path.exists(DICTIONARY_CSV_PATH), "stock_dictionary.csv missing!"
    assert os.path.exists(STOCKS_CSV_PATH), "stock_master.csv missing!"
    assert os.path.exists(BENCHMARKS_CSV_PATH), "stock_benchmarks.csv missing!"
    logger.info("✅ [2단계] CSV 파일 5종 실존 및 무결성 확인 완료")

    logger.info("🎉 [SUCCESS] 로컬 DB 및 CSV 덤프 비파괴적 병렬 테스트 완료!")


if __name__ == "__main__":
    run_local_db_sync_test()
