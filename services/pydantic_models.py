# -*- coding: utf-8 -*-
"""
services/pydantic_models.py
===========================
[Pydantic v2 Structured Output Schemas & Validation Engine]
- YouTube 시황 AI 분석 결과의 결정론적 JSON 스키마 유효성 검증
- 한국어 기관형 명사형 종결어미(~함, ~임, ~필요, ~권고) 자동 검증 및 정규화
- 노션 네이티브 블록(Callout, Bulleted List, Heading) 직렬화 지원
"""

from typing import Any, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator


class AssetImpact(BaseModel):
    """자산별 수혜/피해 영향 분석 모델"""
    ticker_or_asset: str = Field(description="수혜/영향 자산 또는 종목명 (예: NVDA, 삼성전자, KODEX 200, 금)")
    direction: Literal["UP", "DOWN", "NEUTRAL"] = Field(description="예상 방향성: UP(상승/수혜), DOWN(하락/피해), NEUTRAL(중립)")
    catalyst: str = Field(description="기관형 명사형 종결어미(~함, ~임)로 기술된 직접적 촉매 요인")

    @field_validator("catalyst", mode="after")
    @classmethod
    def validate_catalyst_tone(cls, v: str) -> str:
        text = v.strip()
        if not text:
            return text
        # 대화체 어미 방어 및 기관형 종결어미 권장
        for ban in ["합니다", "해요", "됩니다", "돼요", "입니다"]:
            if text.endswith(ban):
                text = text[:-len(ban)] + "함"
        return text


class YouTubeMarketInsight(BaseModel):
    """유튜브 데일리 시황 AI 구조화 분석 모델"""
    video_id: str = Field(default="", description="YouTube Video ID (11글자 고유 식별자)")
    video_title: str = Field(description="유튜브 영상 원본 제목")
    channel_name: str = Field(description="유튜브 채널명")
    published_at: str = Field(default="", description="영상 발행 일시 (ISO 8601 or YYYY-MM-DD)")
    macro_stance: Literal["Bullish", "Bearish", "Neutral"] = Field(description="거시 시장 전반 스탠스")
    risk_appetite: Literal["Risk-On", "Risk-Off", "Defensive"] = Field(description="위험 선호도 / 포트폴리오 기조")
    key_takeaways: List[str] = Field(description="~함/~임으로 종결되는 핵심 시황 분석 포인트 리스트 (3~5개)")
    asset_impacts: List[AssetImpact] = Field(default_factory=list, description="언급된 주요 자산/종목별 영향 분석")
    top_picks: List[str] = Field(default_factory=list, description="영상에서 특별히 강조되거나 추천된 티커/종목명 리스트")
    actionable_strategy: str = Field(description="~필요/~권고로 종결되는 구체적 퀀트 포트폴리오 대응 전략")

    @field_validator("actionable_strategy", mode="after")
    @classmethod
    def validate_action_tone(cls, v: str) -> str:
        text = v.strip()
        if not text:
            return text
        for ban in ["합니다", "해요", "하십시오", "하세요", "바랍니다"]:
            if text.endswith(ban):
                text = text[:-len(ban)] + "권고"
        return text

    def to_markdown_summary(self) -> str:
        """노션 본문 및 로컬 DB 적재용 구조화 마크다운 텍스트 생성"""
        lines = [
            f"### 📺 [{self.channel_name}] {self.video_title}",
            f"- **시장 스탠스**: `{self.macro_stance}` | **위험 성향**: `{self.risk_appetite}`",
            "",
            "#### 📌 핵심 요약 (Key Takeaways)",
        ]
        for t in self.key_takeaways:
            lines.append(f"- {t}")

        if self.asset_impacts:
            lines.append("")
            lines.append("#### 📊 주요 자산 및 섹터 영향 (Asset Impacts)")
            for a in self.asset_impacts:
                icon = "🔺" if a.direction == "UP" else ("🔻" if a.direction == "DOWN" else "🔹")
                lines.append(f"- {icon} **{a.ticker_or_asset}** ({a.direction}): {a.catalyst}")

        if self.top_picks:
            lines.append("")
            lines.append(f"🎯 **주요 언급 종목**: {', '.join(self.top_picks)}")

        lines.append("")
        lines.append(f"💡 **대응 전략**: {self.actionable_strategy}")
        return "\n".join(lines)


class StockValuationItem(BaseModel):
    """정규화 종목 밸류에이션 및 기술 지표 모델"""
    ticker: str = Field(description="표준화 티커 (e.g., 005930, AAPL, 7203.T)")
    name: str = Field(description="종목 공식 국문/영문명")
    current_price: Optional[float] = None
    per: Optional[float] = None
    forward_per: Optional[float] = None
    pbr: Optional[float] = None
    eps: Optional[float] = None
    bps: Optional[float] = None
    dividend_yield: Optional[float] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    ma_200: Optional[float] = None
    trend: Optional[str] = None
    momentum_12m: Optional[float] = None
    drop_52w: Optional[float] = None
    volatility_60d: Optional[float] = None
    risk_grade: Optional[str] = None
