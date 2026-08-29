# -*- coding: utf-8 -*-
"""
tests/test_pydantic_schemas.py
==============================
[Pydantic v2 Structured Output Schemas Unit Tests]
- AI 시황 구조화 출력 모델의 유효성 검증
- 한국어 기관형 명사형 종결어미(~함, ~임, ~필요, ~권고) 정규화 검증
- 마크다운 및 노션 직렬화 무결성 테스트
"""

import sys
import unittest
from pathlib import Path

# 절대 경로 기반 sys.path 주입
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.pydantic_models import (
    AssetImpact,
    YouTubeMarketInsight,
    StockValuationItem,
)


class TestPydanticSchemas(unittest.TestCase):

    def test_asset_impact_tone_normalization(self):
        """대화체 어미가 기관형 명사형(~함)으로 정상 정규화되는지 검증"""
        item = AssetImpact(
            ticker_or_asset="NVDA",
            direction="UP",
            catalyst="차세대 AI 칩 수요가 급증합니다"
        )
        self.assertEqual(item.direction, "UP")
        self.assertTrue(item.catalyst.endswith("함"), f"종결어미 미준수: {item.catalyst}")

    def test_youtube_market_insight_validation(self):
        """YouTubeMarketInsight 모델의 정상 인스턴스화 및 검증"""
        insight = YouTubeMarketInsight(
            video_id="abc12345678",
            video_title="[특징주] 연준 금리 인하 수혜주 및 글로벌 반도체 동향",
            channel_name="삼프로TV",
            published_at="2026-08-29T18:00:00+09:00",
            macro_stance="Bullish",
            risk_appetite="Risk-On",
            key_takeaways=[
                "미 연준의 완화적 통화정책 기조 유지 전망임",
                "국내 반도체 대형주 외국인 순매수 지속됨",
            ],
            asset_impacts=[
                AssetImpact(ticker_or_asset="005930", direction="UP", catalyst="HBM3E 공급 가시화로 실적 개선 기대됨"),
                AssetImpact(ticker_or_asset="USDKRW", direction="DOWN", catalyst="금리차 축소에 따른 원화 강세 압력 확대됨"),
            ],
            top_picks=["005930", "000660", "NVDA"],
            actionable_strategy="기술주 비중 3%p 확대 및 환율 밴드 하단 분할 매수 권고합니다"
        )
        self.assertEqual(insight.macro_stance, "Bullish")
        self.assertTrue(insight.actionable_strategy.endswith("권고"), f"전략 종결어미 미준수: {insight.actionable_strategy}")

        # 마크다운 요약 문자열 검증
        md_text = insight.to_markdown_summary()
        self.assertIn("삼프로TV", md_text)
        self.assertIn("005930", md_text)
        self.assertIn("대응 전략", md_text)

    def test_stock_valuation_item_parsing(self):
        """StockValuationItem 모델 파싱 및 유효성 검증"""
        item = StockValuationItem(
            ticker="005930",
            name="삼성전자",
            current_price=82000.0,
            per=14.5,
            pbr=1.3,
            ma_200=78000.0,
            trend="상승추세 (Bull)",
            momentum_12m=0.25,
            drop_52w=-0.08,
            volatility_60d=0.22,
            risk_grade="A"
        )
        self.assertEqual(item.ticker, "005930")
        self.assertEqual(item.trend, "상승추세 (Bull)")
        self.assertAlmostEqual(item.momentum_12m, 0.25)


if __name__ == "__main__":
    unittest.main()
