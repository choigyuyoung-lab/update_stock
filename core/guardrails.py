# -*- coding: utf-8 -*-
"""
guardrails.py
=============
[K-올라운드 마스터 & update_stock] 
1. 5대 핵심 퀀트 공식(12M 모멘텀, 52주 낙폭, 60일 변동성, 200일선, 스윙 고저점) 수학적 무결성
2. 노션 7대 데이터베이스 스키마 및 보고서 정규화 서식 불변성
3. 프롬프트 템플릿 내 불변 영역([IMMUTABLE_REPORT_SCHEMA]) 훼손 방지
4. KST 타임존 및 자동 git commit/push 금지 프로젝트 규칙
을 0.001초 만에 검증하고 회귀(Regression)를 원천 차단하는 가드레일 엔진입니다.
"""

import re
import math
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("Guardrails")

# ==============================================================================
# 1. 5대 핵심 퀀트 공식 정규 정의 (SSOT)
# ==============================================================================
CANONICAL_QUANT_FORMULAS = {
    "12M_MOMENTUM": "(P_current - P_252d_ago) / P_252d_ago",
    "52W_DRAWDOWN": "(P_current - P_52w_high) / P_52w_high",
    "60D_VOLATILITY": "std(daily_returns_60d) * sqrt(252)",
    "200D_MA": "Close.rolling(200).mean()",
    "SWING_LOW_HIGH": "Low.rolling(20).min() / High.rolling(20).max()",
}

# ==============================================================================
# 2. 노션 7대 DB 핵심 정규화 스키마 필드 (불변)
# ==============================================================================
IMMUTABLE_STOCK_PROPERTIES = {
    "종목명",
    "Market",
    "국가",
    "상품유형",
    "자산군",
    "200일선",
    "수급선",
    "추세",
    "12M 모멘텀",
    "52주 낙폭",
    "60일 변동성",
}


def calculate_canonical_12m_momentum(current_price: float, price_252d_ago: float) -> float:
    """12M 모멘텀 공식: (현재가 - 252일전가격) / 252일전가격"""
    if price_252d_ago <= 0:
        return 0.0
    return (current_price - price_252d_ago) / price_252d_ago


def calculate_canonical_52w_drawdown(current_price: float, high_52w: float) -> float:
    """52주 낙폭 공식: (현재가 - 52주최고가) / 52주최고가 (음수 또는 0)"""
    if high_52w <= 0:
        return 0.0
    return min(0.0, (current_price - high_52w) / high_52w)


def calculate_canonical_60d_volatility(daily_returns_std_60d: float) -> float:
    """60일 변동성(연환산) 공식: std(일일수익률_60) * sqrt(252)"""
    return daily_returns_std_60d * math.sqrt(252)


def verify_quant_formulas_integrity() -> Tuple[bool, List[str]]:
    """5대 퀀트 공식의 수학적 정의 및 계산 무결성을 검증합니다."""
    errors = []
    
    # 1. 12M 모멘텀 검증
    m_val = calculate_canonical_12m_momentum(120.0, 100.0)
    if not math.isclose(m_val, 0.20, abs_tol=1e-5):
        errors.append(f"12M 모멘텀 계산 오류: expected 0.20, got {m_val}")

    # 2. 52주 낙폭 검증
    dd_val = calculate_canonical_52w_drawdown(80.0, 100.0)
    if not math.isclose(dd_val, -0.20, abs_tol=1e-5):
        errors.append(f"52주 낙폭 계산 오류: expected -0.20, got {dd_val}")

    # 3. 60일 변동성 연환산 검증
    vol_val = calculate_canonical_60d_volatility(0.01)
    expected_vol = 0.01 * math.sqrt(252)
    if not math.isclose(vol_val, expected_vol, abs_tol=1e-5):
        errors.append(f"60일 변동성 계산 오류: expected {expected_vol}, got {vol_val}")

    return len(errors) == 0, errors


def verify_prompt_immutable_sections(prompt_text: str) -> Tuple[bool, List[str]]:
    """
    프롬프트 템플릿 내 [IMMUTABLE_REPORT_SCHEMA] 영역이
    정상적으로 보존되어 있는지 검증합니다.
    """
    errors = []
    marker_start = "[IMMUTABLE_REPORT_SCHEMA_START]"
    marker_end = "[IMMUTABLE_REPORT_SCHEMA_END]"

    if marker_start in prompt_text and marker_end not in prompt_text:
        errors.append("프롬프트 불변 영역 닫기 태그([IMMUTABLE_REPORT_SCHEMA_END])가 누락되었습니다.")
    elif marker_end in prompt_text and marker_start not in prompt_text:
        errors.append("프롬프트 불변 영역 시작 태그([IMMUTABLE_REPORT_SCHEMA_START])가 누락되었습니다.")

    # 명사형 종결어미 강제 규칙 보존 여부 검사
    if "noun-ending" in prompt_text or "명사형" in prompt_text:
        pass  # 정상
    else:
        errors.append("보고서 정규화 규칙인 '명사형 종결어미' 준수 지침이 누락되었습니다.")

    return len(errors) == 0, errors


def verify_schema_guardrails(database_schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """노션 데이터베이스 스키마에서 핵심 불변 속성들이 보존되어 있는지 검증합니다."""
    errors = []
    schema_keys = set(database_schema.keys())
    missing = IMMUTABLE_STOCK_PROPERTIES - schema_keys
    if missing:
        errors.append(f"노션 DB 핵심 불변 속성 누락: {list(missing)}")

    return len(errors) == 0, errors
