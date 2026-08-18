"""
ai_service.py
=============
Google Gemini API (google-genai SDK)를 활용하여
포트폴리오 자산배분 데이터를 분석하고 전문적인 올웨더 진단 리포트를 생성하는 AI 서비스 모듈입니다.
"""

import os
import sys
import time
import logging
from typing import Any, Dict, Optional

from dotenv import load_dotenv

# .env 환경변수 로드
load_dotenv()

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from config_portfolio import (
    GEMINI_MODEL_NAME,
    GEMINI_FALLBACK_MODEL,
    SYSTEM_PROMPT,
    USER_PROMPT_TEMPLATE,
)

logger = logging.getLogger("AIService")


class AIService:
    """Google GenAI SDK 기반 자산배분 진단 AI 서비스"""

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

        try:
            from google import genai
            self.client = genai.Client(api_key=self.api_key)
            logger.info("✅ Google GenAI 클라이언트 초기화 완료")
        except ImportError:
            logger.error("❌ 'google-genai' 패키지가 설치되지 않았습니다. pip install google-genai 를 실행하세요.")
            self.client = None
        except Exception as e:
            logger.error(f"❌ Google GenAI 클라이언트 초기화 실패: {e}")
            self.client = None

    def is_available(self) -> bool:
        """API 키 및 클라이언트 사용 가능 여부를 확인합니다."""
        return bool(self.api_key and self.client is not None)

    def generate_portfolio_diagnosis(
        self,
        portfolio_summary: Dict[str, Any],
        max_retries: int = 3,
        base_delay: float = 3.0
    ) -> str:
        """
        포트폴리오 요약 데이터를 입력받아 Gemini 2.5 Flash를 통해 올웨더 진단 리포트를 생성합니다.
        
        :param portfolio_summary: 포트폴리오 집계 데이터 딕셔너리
        :return: 마크다운 형식의 진단 리포트 문자열
        """
        if not self.is_available():
            raise EnvironmentError(
                "GEMINI_API_KEY가 설정되지 않았거나 google-genai 클라이언트가 초기화되지 않았습니다. "
                ".env 파일 또는 환경 변수에 GEMINI_API_KEY를 설정해 주세요."
            )

        # 사용자 프롬프트 구성
        format_kwargs = {
            "analysis_date": portfolio_summary.get("analysis_date", "현재"),
            "total_eval_krw": portfolio_summary.get("total_eval_krw", 0.0),
            "stock_total_krw": portfolio_summary.get("stock_total_krw", 0.0),
            "cash_total_krw": portfolio_summary.get("cash_total_krw", 0.0),
            "total_positions_count": portfolio_summary.get("total_positions_count", 0),
            "monitoring_count": portfolio_summary.get("monitoring_count", 0),
            "prev_report_summary_text": portfolio_summary.get("prev_report_summary_text", "- **비교 기준**: 직전 리포트 없음"),
            "account_summary_text": portfolio_summary.get("account_summary_text", ""),
            "theme_summary_text": portfolio_summary.get("theme_summary_text", ""),
            "asset_summary_table": portfolio_summary.get("asset_summary_table", ""),
            "holdings_detail_text": portfolio_summary.get("holdings_detail_text", ""),
        }
        user_prompt = USER_PROMPT_TEMPLATE.format(**format_kwargs)



        from google.genai import types

        # 1차 시도: gemini-2.5-flash, 실패 시 gemini-2.0-flash 전환
        models_to_try = [GEMINI_MODEL_NAME, GEMINI_FALLBACK_MODEL]

        for model_name in models_to_try:
            print(f"🤖 [Gemini AI] 모델 '{model_name}' 호출 중...")
            
            for attempt in range(1, max_retries + 1):
                try:
                    config_kwargs: Dict[str, Any] = {
                        "system_instruction": SYSTEM_PROMPT,
                        "temperature": 0.2,
                        "max_output_tokens": 8192,
                    }
                    if "2.5" in model_name:
                        config_kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=1024)

                    config = types.GenerateContentConfig(**config_kwargs)
                    
                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=user_prompt,
                        config=config,
                    )
                    
                    candidate = response.candidates[0] if response.candidates else None
                    finish_reason = getattr(candidate, "finish_reason", None)
                    
                    report_text = response.text if hasattr(response, "text") else str(response)
                    if report_text and len(report_text.strip()) > 100:
                        print(f"✅ [Gemini AI] 리포트 생성 완료 (총 {len(report_text):,}자 수신, Finish reason: {finish_reason})")
                        return report_text.strip()
                    else:
                        print(f"   ⚠️ [Gemini AI] 응답 텍스트가 너무 짧습니다. 재시도 {attempt}/{max_retries}")

                except Exception as exc:
                    err_str = str(exc)
                    print(f"   ⚠️ [Gemini AI] API 호출 오류 (시도 {attempt}/{max_retries} - {model_name}): {err_str}")
                    
                    # 429 또는 일시적 통신 오류 시 지수 백오프
                    if attempt < max_retries:
                        delay = base_delay * (2 ** (attempt - 1))
                        time.sleep(delay)
                        continue

            print(f"⚠️ [Gemini AI] '{model_name}' 모델 실패, 다음 모델로 전환합니다...")

        raise RuntimeError("Google Gemini API를 통한 포트폴리오 진단 리포트 생성에 최종 실패하였습니다.")
