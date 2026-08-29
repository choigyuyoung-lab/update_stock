# -*- coding: utf-8 -*-
"""
tools/mcp_server.py
===================
[FastMCP Standardized Financial ETL & Market Master Server]
- Antigravity IDE, Claude Desktop, Cursor 등 AI 에이전트 표준 도구(MCP) 연동
- 로컬 SQLite WAL DB(0.001s) 및 521개 온톨로지 사전 고속 질의 인터페이스 제공
"""

import sys
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

# 절대 경로 기반 sys.path 주입
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.local_db_manager import (
    get_db_connection,
    get_actual_db_path,
    get_youtube_insights,
    get_youtube_insight_by_id,
    load_etf_holdings_from_sqlite,
)
from core.stock_registry import clean_ticker_key

logger = logging.getLogger("FastMCPServer")

# FastMCP 서버 인스턴스 초기화 (mcp 패키지 임포트 방어 로직)
try:
    from mcp.server.fastmcp import FastMCP
    mcp = FastMCP("update_stock_data_service")
except ImportError:
    mcp = None
    logger.warning("⚠️ 'mcp' 패키지가 설치되지 않았습니다. pip install mcp 필요.")


if mcp:
    @mcp.tool()
    def get_stock_quote(ticker: str) -> Dict[str, Any]:
        """
        특정 종목 티커의 실시간 현재가, PER, PBR, 52주 고저점 및 5대 퀀트 팩터를 로컬 SQLite 캐시(0.001s)에서 조회합니다.
        - ticker: 표준화 종목코드 (예: 005930, AAPL, 7203.T, TSLA, QQQ)
        """
        clean_t = clean_ticker_key(ticker)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tbl_finances WHERE ticker = ?;", (clean_t,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            # tbl_stocks 기본 정보 조회
            cursor.execute("SELECT * FROM tbl_stocks WHERE ticker = ?;", (clean_t,))
            stock_row = cursor.fetchone()
            if stock_row:
                return {"status": "basic_info_only", "data": dict(stock_row)}
            return {"error": f"티커 [{clean_t}]에 해당하는 재무/시세 데이터를 찾을 수 없음."}

    @mcp.tool()
    def search_ontology_keyword(keyword: str) -> List[Dict[str, Any]]:
        """
        521개 온톨로지 사전에서 특정 키워드에 대한 표준 섹터, 시장 벤치마크, 대형 우량주 여부 및 자산 분류 룰을 조회합니다.
        - keyword: 탐색할 종목명, 산업명 또는 키워드 (예: 반도체, 2차전지, S&P500, TSMC)
        """
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM tbl_dictionary WHERE keyword LIKE ? OR official_name LIKE ? ORDER BY priority DESC LIMIT 10;",
                (f"%{keyword}%", f"%{keyword}%")
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    @mcp.tool()
    def get_macro_benchmark(ticker: str) -> Dict[str, Any]:
        """
        54개 글로벌 거시경제 지표 및 벤치마크(원달러 환율, 미 국채금리, WTI, 금, S&P500 등) 정보를 조회합니다.
        - ticker: 벤치마크 티커 (예: US10Y, USDKRW, US500, WTI, GOLD, KOSPI)
        """
        clean_t = clean_ticker_key(ticker)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tbl_benchmarks WHERE ticker = ?;", (clean_t,))
            row = cursor.fetchone()
            return dict(row) if row else {"error": f"벤치마크 [{clean_t}] 데이터를 찾을 수 없음."}

    @mcp.tool()
    def get_latest_youtube_insights(limit: int = 5) -> List[Dict[str, Any]]:
        """
        최근 AI 분석이 완료된 유튜브 경제/투자 시황 요약 및 시장 스탠스(Bullish/Bearish/Neutral)를 조회합니다.
        - limit: 조회할 최근 영상 수 (기본값: 5, 최대: 20)
        """
        clamped_limit = max(1, min(limit, 20))
        return get_youtube_insights(limit=clamped_limit)

    @mcp.tool()
    def get_etf_holdings(etf_ticker: str) -> List[Dict[str, Any]]:
        """
        특정 ETF의 상위 10대 편입 구성종목(PDF) 및 보유 비중(Weight)을 조회합니다.
        - etf_ticker: ETF 티커 (예: 069500, SPY, QQQ, 379800)
        """
        clean_t = clean_ticker_key(etf_ticker)
        return load_etf_holdings_from_sqlite(clean_t)


def main():
    """CLI 직접 실행 시 FastMCP 서버 기동"""
    if not mcp:
        print("❌ FastMCP를 초기화할 수 없습니다. 'pip install mcp'를 실행하십시오.")
        sys.exit(1)
    print("🚀 [FastMCP Server] update_stock 금융 데이터 ETL 도구 서버 기동 중...")
    mcp.run()


if __name__ == "__main__":
    main()
