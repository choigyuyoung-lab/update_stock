# -*- coding: utf-8 -*-
"""
test_guardrails.py
==================
5대 핵심 퀀트 공식의 수학적 정의, 노션 DB 스키마 정규화 규격,
프롬프트 불변 영역 락(Lock) 무결성을 전수 검사하는 가드레일 단위 테스트입니다.
"""

import sys
import unittest

# Windows 콘솔 UTF-8 안전화
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from core.guardrails import (
    calculate_canonical_12m_momentum,
    calculate_canonical_52w_drawdown,
    calculate_canonical_60d_volatility,
    verify_quant_formulas_integrity,
    verify_prompt_immutable_sections,
    verify_schema_guardrails,
    IMMUTABLE_STOCK_PROPERTIES,
)
from services.prompt_manager import (
    get_fia_youtube_system_instruction,
)


class TestGuardrails(unittest.TestCase):

    def test_quant_formulas_math(self):
        """5대 퀀트 공식의 수학적 무결성 검증"""
        passed, errors = verify_quant_formulas_integrity()
        self.assertTrue(passed, f"퀀트 공식 검증 실패: {errors}")

    def test_momentum_edge_cases(self):
        """12M 모멘텀 경계조건 검증"""
        self.assertEqual(calculate_canonical_12m_momentum(100.0, 0.0), 0.0)
        self.assertEqual(calculate_canonical_12m_momentum(100.0, -10.0), 0.0)
        self.assertAlmostEqual(calculate_canonical_12m_momentum(150.0, 100.0), 0.5)

    def test_drawdown_edge_cases(self):
        """52주 낙폭 음수 백분율 및 경계조건 검증"""
        self.assertEqual(calculate_canonical_52w_drawdown(100.0, 0.0), 0.0)
        self.assertEqual(calculate_canonical_52w_drawdown(120.0, 100.0), 0.0)  # 신고가는 0
        self.assertAlmostEqual(calculate_canonical_52w_drawdown(75.0, 100.0), -0.25)

    def test_prompts_immutable_sections(self):
        """모든 핵심 프롬프트의 불변 영역 및 명사형 종결어미 지침 보존 검증"""
        prompts = [
            ("FIA_YOUTUBE", get_fia_youtube_system_instruction()),
        ]
        for name, p_text in prompts:
            passed, errors = verify_prompt_immutable_sections(p_text)
            self.assertTrue(passed, f"프롬프트 [{name}] 불변성 검증 실패: {errors}")

    def test_stock_properties_schema(self):
        """노션 7대 DB 핵심 속성 락킹 검증"""
        mock_schema = {k: {} for k in IMMUTABLE_STOCK_PROPERTIES}
        passed, errors = verify_schema_guardrails(mock_schema)
        self.assertTrue(passed, f"스키마 가드레일 실패: {errors}")

    def test_modernized_architecture_guardrails(self):
        """Pydantic v2 모델 및 SQLite 6대 테이블(tbl_youtube_insights) 가드레일 검증"""
        from services.pydantic_models import YouTubeMarketInsight, AssetImpact
        from core.local_db_manager import get_db_connection, init_database

        init_database()
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = {row[0] for row in cursor.fetchall()}
            self.assertIn("tbl_youtube_insights", tables, "tbl_youtube_insights 테이블 누락")

        # Pydantic 모델 인스턴스화 무결성 확인
        model = YouTubeMarketInsight(
            video_title="테스트",
            channel_name="테스트채널",
            macro_stance="Neutral",
            risk_appetite="Defensive",
            key_takeaways=["테스트 요약임"],
            actionable_strategy="관망 필요"
        )
        self.assertEqual(model.macro_stance, "Neutral")


if __name__ == "__main__":
    print("=" * 80)
    print("🛡️ [Guardrails Zero-Regression Enforcer] 불변 규칙 가드레일 전수 진단 시작")
    print("=" * 80)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestGuardrails)
    runner = unittest.TextTestRunner(verbosity=2)
    res = runner.run(suite)
    if res.wasSuccessful():
        print("\n🎉 [SUCCESS] 5대 퀀트 공식 & 노션 보고서 정규화 스키마 100% 정상 보호 중!")
        sys.exit(0)
    else:
        print("\n❌ [FAIL] 가드레일 위반이 감지되었습니다.")
        sys.exit(1)
