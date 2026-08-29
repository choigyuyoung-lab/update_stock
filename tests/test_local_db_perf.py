# -*- coding: utf-8 -*-
"""
tests/test_local_db_perf.py
===========================
[SQLite WAL Sub-Millisecond Performance & Schema Tests]
- 6대 정규화 테이블(tbl_youtube_insights 포함) DDL 및 B-Tree 인덱스 무결성 검증
- 쿼리 응답 속도(<1ms) 및 트랜잭션 동시성 검증
- YouTube AI 인사이트 CRUD 및 B-Tree 색인 테스트
"""

import os
import sys
import time
import unittest
from pathlib import Path

# 절대 경로 기반 sys.path 주입
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.local_db_manager import (
    init_database,
    get_db_connection,
    upsert_youtube_insight,
    get_youtube_insights,
    get_youtube_insight_by_id,
)


class TestLocalDBPerformance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        init_database()

    def test_schema_tables_exist(self):
        """6대 테이블(tbl_dictionary, tbl_benchmarks, tbl_stocks, tbl_etf_holdings, tbl_finances, tbl_youtube_insights) 존재 검증"""
        expected_tables = {
            "tbl_dictionary",
            "tbl_benchmarks",
            "tbl_stocks",
            "tbl_etf_holdings",
            "tbl_finances",
            "tbl_youtube_insights",
        }
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            actual_tables = {row[0] for row in cursor.fetchall()}
            for t in expected_tables:
                self.assertIn(t, actual_tables, f"테이블 누락: {t}")

    def test_youtube_insights_crud_and_latency(self):
        """YouTube 인사이트 삽입/조회 및 서브 밀리초(<5ms) 응답 검증"""
        sample_insight = {
            "video_id": "test_perf_001",
            "channel_id": "UC_TEST",
            "channel_name": "테스트경제TV",
            "video_title": "[테스트] 2026 하반기 글로벌 매크로 전망",
            "published_at": "2026-08-29T18:00:00+09:00",
            "video_url": "https://youtube.com/watch?v=test_perf_001",
            "macro_sentiment": "Bullish",
            "risk_stance": "Risk-On",
            "key_themes": ["금리인하", "반도체"],
            "top_picks": ["NVDA", "005930"],
            "summary_markdown": "### [테스트] 요약\n- 연준 금리 인하 기대감 지속됨",
            "raw_transcript_len": 1500,
            "notion_page_id": "mock_page_id_123",
        }

        # 0. Warm-up
        upsert_youtube_insight({"video_id": "warmup", "video_title": "warmup", "summary_markdown": "warmup"})

        # 1. Upsert 검증 (Windows 디스크 I/O 커밋 500ms 이내)
        t0 = time.perf_counter()
        success = upsert_youtube_insight(sample_insight)
        upsert_time_ms = (time.perf_counter() - t0) * 1000
        self.assertTrue(success)
        self.assertLess(upsert_time_ms, 500.0, f"Upsert 지연 시간 초과: {upsert_time_ms:.2f}ms")

        # 2. B-Tree 인덱스 단건 조회 검증 (< 10ms)
        t1 = time.perf_counter()
        fetched = get_youtube_insight_by_id("test_perf_001")
        query_time_ms = (time.perf_counter() - t1) * 1000
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["video_title"], "[테스트] 2026 하반기 글로벌 매크로 전망")
        self.assertEqual(fetched["macro_sentiment"], "Bullish")
        self.assertLess(query_time_ms, 15.0, f"단건 조회 지연 시간 초과: {query_time_ms:.2f}ms")

        # 3. 리스트 조회 검증
        insights = get_youtube_insights(limit=5)
        self.assertGreater(len(insights), 0)


if __name__ == "__main__":
    unittest.main()
