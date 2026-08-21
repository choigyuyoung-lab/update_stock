# -*- coding: utf-8 -*-
"""
ai_service.py
=============
Google Gemini API (google-genai 최신 SDK)를 활용하여 시황 분석, 종목 파싱, 
포트폴리오 리포트 진단을 지원하는 공통 AI 서비스 모듈입니다.
- Google AI Studio 무료 티어(0원) 완벽 호환
- 지수 백오프 기반 재시도 및 페일오버
"""

import os
import sys
import time
import logging
from typing import Any, Dict, Optional

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

logger = logging.getLogger("AIService")

DEFAULT_MAX_RETRIES = 2
DEFAULT_BASE_DELAY = 3.0


class AIService:
    """Google GenAI SDK 기반 AI 서비스 클라이언트"""

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
