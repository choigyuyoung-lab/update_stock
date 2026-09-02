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
from pathlib import Path
from dotenv import load_dotenv

# .env 환경변수 로드
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

# 프로젝트 루트 디렉토리를 sys.path에 등록하여 core 모듈 접근 보장
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.gemini_router import GeminiSafeExecutor, DynamicGeminiModelManager
from services.prompt_manager import get_fia_youtube_system_instruction

import warnings
warnings.filterwarnings("ignore", message=".*automatic function calling.*")
warnings.filterwarnings("ignore", message=".*Support for Python version.*")

logger = logging.getLogger("AIService")
logging.getLogger("google").setLevel(logging.ERROR)
logging.getLogger("google_genai").setLevel(logging.ERROR)
logging.getLogger("google.genai").setLevel(logging.ERROR)

DEFAULT_MAX_RETRIES = 1
DEFAULT_BASE_DELAY = 0.5


# ==============================================================================
# 1. Pydantic 기반 유튜브 시황 및 종목 분석 스키마 (Structured Outputs)
# ==============================================================================
class YouTubeAssetItem(BaseModel):
    ticker: str = Field(description="표준 거래소 티커 심볼 또는 6자리 종목코드 (비상장 기업은 UNLISTED)")
    name: str = Field(description="언급된 공식 종목명 또는 자산명")
    context: str = Field(description="영상에서 언급된 구체적 투자 논리, 실적 전망, 목표가/손절가 또는 매수 타점 요약 (명사형 종결어미 ~함, ~임 필수)")
    opinion: str = Field(description="투자의견: 매수 / 관망 / 비중축소 / 중립 중 택1")
    link_url: Optional[str] = Field(default="", description="관련 웹 링크 또는 빈 문자열")


class YouTubeAnalysisResult(BaseModel):
    summarized_title_for_notion: str = Field(
        description="노션 DB용 '[주요테마/섹터] 핵심 시황/방향성 헤드라인' 형식의 한 줄 제목 (명사형 종결, 예: [반도체/AI] HBM 공급망 수혜 및 대형주 상승세 지속 전망)"
    )
    publish_date: str = Field(description="게시일자 (YYYY-MM-DD 형식)")
    market_sentiment: str = Field(
        default="중립",
        description="시장 심리 및 방향성 진단: '강세' / '중립' / '변동성확대' / '조정' 중 택1"
    )
    leading_sectors: List[str] = Field(
        default_factory=list,
        description="영상에서 핵심 주도주/수혜 섹터로 언급된 1~3개 섹터/테마명 (예: ['반도체/HBM', '전력인프라'])"
    )
    one_line_summary: str = Field(
        default="",
        description="노션 프로퍼티용 전체 시황을 관통하는 임팩트 있는 '단 1줄' 요약 (명사형 종결 필수, 1문장)"
    )
    key_takeaways: List[str] = Field(
        description=(
            "노션 프로퍼티용 3대 표준 축으로 구성된 정확히 '3개'의 간결한 핵심 시사점 목록 (각 항목 1줄, 명사형 종결 필수):\n"
            "1. [매크로/금리] 거시경제/금리/환율 핵심 시사점\n"
            "2. [산업/종목] 핵심 산업/수혜 종목 실적 모멘텀 시사점\n"
            "3. [전략/리스크] 자산배분 대응 전략 또는 모니터링 리스크 시사점"
        )
    )
    overall_summary: str = Field(
        description=(
            "노션 본문(Body)용 3대 표준 축으로 구성된 심층 상세 시황 분석 (각 단락 2~3문장, 명사형 종결 필수):\n"
            "[매크로 & 시장방향] 거시경제, 금리/환율, 통화정책 및 지수 수급 심층 분석 (2~3문장)\n\n"
            "[주도섹터 & 핵심이슈] 주도 섹터, 기업 실적 모멘텀, 공급망 및 정책 이슈 심층 분석 (2~3문장)\n\n"
            "[투자전략 & 리스크] 자산배분 관점의 구체적 대응 전략 및 가격 타점/리스크 심층 분석 (2~3문장)"
        )
    )
    assets: List[YouTubeAssetItem] = Field(
        default_factory=list,
        description="영상 전체 자막을 전수 스캔하여 실질적 투자 논리, 실적 전망, 매수/매도 타점이 논의된 주요 종목/자산 리스트 (최대 5개, 중요도/비중 순). 구어체 약칭(삼전, 삼전우, 물산, 하이닉스 등)은 반드시 공식 법인명(삼성전자, 삼성전자우, 삼성물산, SK하이닉스 등)으로 정규화하여 name에 기재."
    )


# ==============================================================================
# 2. Google GenAI 클라이언트 및 서비스 클래스
# ==============================================================================
class AIService:
    """공통 스마트 라우터(GeminiSafeExecutor) 기반 AI 서비스 클라이언트 (Structured Outputs 지원)"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = (
            api_key
            or os.environ.get("GEMINI_API_KEY")
            or os.environ.get("GOOGLE_API_KEY")
            or ""
        ).strip()
        self.executor = GeminiSafeExecutor(api_key=self.api_key)

    def is_available(self) -> bool:
        """API 키 및 실행 엔진 사용 가능 여부를 확인합니다."""
        return self.executor.is_available()

    def analyze_youtube_transcript(
        self,
        transcript_text: str,
        video_meta: Dict[str, Any],
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY
    ) -> Optional[YouTubeAnalysisResult]:
        """
        유튜브 자막 텍스트와 메타데이터를 입력받아
        공통 스마트 라우터를 통해 Pydantic YouTubeAnalysisResult 스키마를 강제한 Structured Outputs로 정밀 분석합니다.
        (TPM 250K 방어를 위한 45,000자 자동 다이어트 및 Lite 500회 우선 라우팅)
        """
        if not self.is_available():
            logger.error("❌ Gemini API 클라이언트를 사용할 수 없습니다. GEMINI_API_KEY를 확인하세요.")
            return None

        fia_system_prompt = get_fia_youtube_system_instruction()

        # TPM 250K 한도 초과 방지를 위한 45,000자 안전 슬라이싱
        safe_transcript = transcript_text[:45000] if transcript_text else ""

        user_content = f"""[영상 기본 정보]
- 채널명: {video_meta.get('channel_name', '')}
- 영상 원제목: {video_meta.get('title', '')}
- 영상 URL: {video_meta.get('url', '')}
- 게시일자: {video_meta.get('publish_date', '')}

[자막 스크립트 전문 (핵심 구간 분석)]
{safe_transcript}
"""

        parsed_result = self.executor.safe_generate_structured(
            contents=user_content,
            system_instruction=fia_system_prompt,
            response_schema=YouTubeAnalysisResult,
            max_input_chars=45000,
            temperature=0.0,
            max_retries_per_model=max_retries,
            base_delay=base_delay,
        )

        if parsed_result and isinstance(parsed_result, YouTubeAnalysisResult):
            meta_pub_date = str(video_meta.get("publish_date", "")).strip()
            if meta_pub_date and meta_pub_date.lower() not in ["null", "none"]:
                parsed_result.publish_date = meta_pub_date
            elif not parsed_result.publish_date or parsed_result.publish_date.lower() in ["null", "none"]:
                parsed_result.publish_date = meta_pub_date

            # 티커 기본 정규화 (대문자 및 6자리 zfill)
            for item in (parsed_result.assets or []):
                t_clean = (item.ticker or "").strip().upper()
                if t_clean.isdigit() and 1 <= len(t_clean) <= 6:
                    item.ticker = t_clean.zfill(6)
                else:
                    item.ticker = t_clean

            logger.info("✅ [Gemini AI] Structured Output 파싱 및 정규화 완료")
            return parsed_result

        logger.error("❌ YouTube 자막 Structured Output 분석에 최종 실패하였습니다.")
        return None

    def analyze_youtube_multimodal(
        self,
        video_url_or_id: str,
        video_meta: Dict[str, Any],
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY
    ) -> Optional[YouTubeAnalysisResult]:
        """
        유튜브 텍스트 자막을 추출할 수 없는 경우(IP 차단, 자막 비활성화 등),
        Google Gemini Multimodal API 및 메타데이터를 직접 분석하여
        동일한 Pydantic YouTubeAnalysisResult 스키마로 구조화된 시황 인텔리전스를 반환합니다.
        """
        if not self.is_available():
            logger.error("❌ Gemini API 클라이언트를 사용할 수 없습니다. GEMINI_API_KEY를 확인하세요.")
            return None

        fia_system_prompt = get_fia_youtube_system_instruction()

        raw_id = video_meta.get("video_id") or video_url_or_id
        if str(raw_id).startswith("http"):
            video_url = str(raw_id)
        else:
            video_url = f"https://www.youtube.com/watch?v={raw_id}"

        user_content_text = f"""[영상 기본 정보]
- 채널명: {video_meta.get('channel_name', '')}
- 영상 원제목: {video_meta.get('title', '')}
- 영상 URL: {video_url}
- 게시일자: {video_meta.get('publish_date', '')}

[상세 설명란 및 메타데이터]
{str(video_meta.get('description', '') or '')[:5000]}

[멀티모달 시황 분석 지시]
영상 원문/오디오 및 제공된 메타데이터를 종합 분석하여 시장 심리, 3대 핵심 시사점, 심층 상세 분석, 언급된 주요 종목(티커)을 Pydantic 구조화 스키마(YouTubeAnalysisResult)에 맞춰 한국어 명사형 종결어미(~함, ~임, ~필요, ~권고, ~전망)로 추출하십시오.
"""

        parsed_result = self.executor.safe_generate_structured(
            contents=user_content_text,
            system_instruction=fia_system_prompt,
            response_schema=YouTubeAnalysisResult,
            max_input_chars=45000,
            temperature=0.0,
            max_retries_per_model=max_retries,
            base_delay=base_delay,
        )

        if parsed_result and isinstance(parsed_result, YouTubeAnalysisResult):
            meta_pub_date = str(video_meta.get("publish_date", "")).strip()
            if meta_pub_date and meta_pub_date.lower() not in ["null", "none"]:
                parsed_result.publish_date = meta_pub_date
            elif not parsed_result.publish_date or parsed_result.publish_date.lower() in ["null", "none"]:
                parsed_result.publish_date = meta_pub_date

            # 티커 기본 정규화 (대문자 및 6자리 zfill)
            for item in (parsed_result.assets or []):
                t_clean = (item.ticker or "").strip().upper()
                if t_clean.isdigit() and 1 <= len(t_clean) <= 6:
                    item.ticker = t_clean.zfill(6)
                else:
                    item.ticker = t_clean

            logger.info("✅ [Gemini AI 멀티모달 Fallback] Structured Output 파싱 성공")
            return parsed_result

        logger.error("❌ YouTube 멀티모달 Structured Output 분석에 최종 실패하였습니다.")
        return None

