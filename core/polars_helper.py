# -*- coding: utf-8 -*-
"""
polars_helper.py
================
AI 테크 레이더 추천 고성능 데이터 처리 모듈:
SQLite stock_master.db 및 CSV 데이터를 Polars를 통해 초고속(Zero-Copy)으로 로드합니다.
"""
from typing import Optional, Dict, Any

def is_polars_available() -> bool:
    try:
        import polars as pl
        return True
    except ImportError:
        return False

def read_stocks_with_polars(db_path: str):
    """Polars를 통한 초고속 주식 마스터 테이블 스캔"""
    import polars as pl
    import sqlite3
    conn = sqlite3.connect(db_path)
    df = pl.read_database("SELECT * FROM tbl_stocks;", conn)
    conn.close()
    return df
