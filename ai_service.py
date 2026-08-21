# -*- coding: utf-8 -*-
"""
ai_service.py
=============
Google Gemini API (google-genai 최신 SDK)를 활용하여 시황 분석, 종목 파싱,
Pydantic 기반 Structured Outputs(response_schema)를 지원하는 공통 AI 서비스 모듈입니다.
- 프롬프트 관리: prompt_manager.py (prompts/*.en.md) 중앙 집중형 아키텍처
- FIA_SYSTEM_INSTRUCTION: 영문 추론(CoT) + 한국어 명사형 종결어미(~함, ~임, ~필요, ~권고) 강제
- YouTubeAnalysisResult: Pydantic 100% Deterministic 구조화 출력 보장
- Google AI Studio 무료 티어(0원) 완벽 호환
"""

import os
import sys
import time
import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Google GenAI 최신 SDK 안전 임포트
try:
    from google import genai
    from google.genai import types
except ImportError:
    genai = None
    types = None

from prompt_manager import get_fia_youtube_system_instruction

logger = logging.getLogger("AIService")

DEFAULT_MAX_RETRIES = 2
DEFAULT_BASE_DELAY = 3.0


# ==============================================================================
# 1. Pydantic 기반 유튜브 시황 및 종목 분석 스키마 (Structured Outputs)
# ==============================================================================
class YouTubeAssetItem(BaseModel):
    ticker: str = Field(description="표준 6자리 국내 종목코드 (예: 005930) 또는 미국 티커 (예: NVDA, AAPL)")
    name: str = Field(description="종목명 또는 자산명 (예: 삼성전자, 엔비디아, 금선물)")
    context: str = Field(description="영상에서 언급된 구체적 투자 논리, 실적 전망, 목표가/손절가 또는 매수 타점 요약 (명사형 종결어미 ~함, ~임 사용)")
    opinion: str = Field(description="투자의견: 매수 / 관망 / 비중축소 / 중립 중 택1")
    link_url: Optional[str] = Field(default="", description="관련 웹 링크 또는 빈 문자열")


class YouTubeAnalysisResult(BaseModel):
    summarized_title_for_notion: str = Field(description="노션 DB용 간결하고 핵심적인 한 줄 제목 (명사형 종결)")
    publish_date: str = Field(description="게시일자 (YYYY-MM-DD 형식)")
    overall_summary: str = Field(description="영상 전체의 핵심 매크로 시황 및 시장 방향성 요약 (3~5문장, 명사형 종결어미 ~함, ~임 필수)")
    key_takeaways: List[str] = Field(description="핵심 시장 시사점 목록 (각 문장 명사형 종결어미 필수)")
    assets: List[YouTubeAssetItem] = Field(default_factory=list, description="영상에서 핵심 투자 논리가 언급된 주요 종목/자산 리스트 (최대 5개)")


# ==============================================================================
# 2. Google GenAI 클라이언트 및 서비스 클래스
# ==============================================================================
class AIService:
    """Google GenAI SDK 기반 AI 서비스 클라이언트 (Structured Outputs 지원)"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = (
            api_key
            or os.environ.get("GEMINI_API_KEY")
            or os.environ.get("GOOGLE_API_KEY")
            or ""
        ).strip()
        self.client: Any = None
        self._init_client()

    def _init_client(self) -> None:
        """google-genai 클라이언트를 초기화합니다."""
        if not self.api_key:
            logger.warning("⚠️ GEMINI_API_KEY가 설정되지 않았습니다.")
            return

        if genai is None or types is None:
            logger.error("❌ 'google-genai' 패키지가 설치되지 않았습니다. pip install google-genai 를 실행하세요.")
            self.client = None
            return

        try:
            self.client = genai.Client(api_key=self.api_key)
            logger.info("✅ Google GenAI 클라이언트 초기화 완료")
        except Exception as e:
            logger.error(f"❌ Google GenAI 클라이언트 초기화 실패: {e}")
            self.client = None

    def is_available(self) -> bool:
        """API 키 및 클라이언트 사용 가능 여부를 확인합니다."""
        return bool(self.api_key and self.client is not None)

    def analyze_youtube_transcript(
        self,
        transcript_text: str,
        video_meta: Dict[str, Any],
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY
    ) -> Optional[YouTubeAnalysisResult]:
        """
        유튜브 자막 텍스트와 메타데이터를 입력받아
        Pydantic YouTubeAnalysisResult 스키마를 강제한 Structured Outputs로 정밀 분석합니다.
        """
        if not self.is_available():
            logger.error("❌ Gemini API 클라이언트를 사용할 수 없습니다. GEMINI_API_KEY를 확인하세요.")
            return None

        fia_system_prompt = get_fia_youtube_system_instruction()

        user_content = f"""[영상 기본 정보]
- 채널명: {video_meta.get('channel_name', '')}
- 영상 원제목: {video_meta.get('title', '')}
- 영상 URL: {video_meta.get('url', '')}
- 게시일자: {video_meta.get('publish_date', '')}

[자막 스크립트 전문]
{transcript_text[:12000]}
"""

        models_to_try = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-3.5-flash-lite"]

        for model_name in models_to_try:
            for attempt in range(1, max_retries + 1):
                try:
                    config = types.GenerateContentConfig(
                        system_instruction=fia_system_prompt,
                        response_mime_type="application/json",
                        response_schema=YouTubeAnalysisResult,
                        temperature=0.1,
                    )

                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=user_content,
                        config=config,
                    )

                    if response and response.text:
                        parsed_result = YouTubeAnalysisResult.model_validate_json(response.text.strip())
                        if not parsed_result.publish_date or parsed_result.publish_date.lower() == "null":
                            parsed_result.publish_date = str(video_meta.get("publish_date", ""))
                        logger.info(f"✅ [Gemini AI] Structured Output 파싱 성공 (모델: {model_name})")
                        return parsed_result

                except Exception as exc:
                    err_str = str(exc)
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        logger.warning(f"⚠️ [Gemini AI] 쿼터 제한(429) 감지 -> 다음 모델로 전환합니다.")
                        time.sleep(3.0)
                        break
                    if "404" in err_str or "NOT_FOUND" in err_str:
                        break

                    logger.warning(f"⚠️ [Gemini AI] 호출 오류 ({model_name}, 시도 {attempt}/{max_retries}): {err_str}")
                    if attempt < max_retries:
                        time.sleep(base_delay * attempt)

        logger.error("❌ YouTube 자막 Structured Output 분석에 최종 실패하였습니다.")
        return None
