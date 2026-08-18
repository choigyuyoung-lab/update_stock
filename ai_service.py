# -*- coding: utf-8 -*-
"""
ai_service.py
=============
Google Gemini API (google-genai 최신 SDK)를 활용하여 포트폴리오 자산배분 데이터 및 실시간 매크로 지표를 분석하고
전문적인 「K-올라운드 마스터」 진단 및 리밸런싱 리포트를 자동 생성하는 AI 서비스 모듈입니다.
- 핵심 기능: 실시간 Google Search Grounding 연동 (환각 방지 및 최신 시장 팩트체크)
- 모델 풀: 4단계 지능형 폴백 풀 (gemini-3.6-flash -> gemini-2.5-pro -> gemini-2.5-flash -> gemini-3.5-flash-lite)
- 비용 최적화: Google AI Studio 무료 티어(0원) 완벽 호환
- 안정성: 지수 백오프 기반 재시도(3초 기본 대기), 429/404 즉시 페일오버
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
    GEMINI_MODEL_POOL,
    SYSTEM_PROMPT,
    USER_PROMPT_TEMPLATE,
)

logger = logging.getLogger("AIService")


# ==============================================================================
# 1. AI 서비스 설정 및 기본 상수
# ==============================================================================
DEFAULT_MAX_RETRIES = 2
DEFAULT_BASE_DELAY = 3.0
DEFAULT_THINKING_BUDGET = 2048
DEFAULT_MAX_OUTPUT_TOKENS = 8192


# ==============================================================================
# 2. Google GenAI 클라이언트 및 서비스 클래스
# ==============================================================================
class AIService:
    """Google GenAI SDK 기반 자산배분 진단 AI 서비스 (Google Search Grounding 지원)"""

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
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY
    ) -> str:
        """
        포트폴리오 집계 데이터 및 실시간 매크로 지표를 입력받아
        Google Search Grounding 도구를 활성화한 4단계 모델 풀을 순차 호출하여
        「K-올라운드 마스터」 진단 리포트를 생성합니다.
        
        :param portfolio_summary: 포트폴리오 집계 데이터 + 매크로 스냅샷 딕셔너리
        :param max_retries: 모델별 최대 재시도 횟수
        :param base_delay: 기본 대기 시간(초)
        :return: 마크다운 형식의 진단 리포트 문자열
        """
        if not self.is_available():
            raise EnvironmentError(
                "GEMINI_API_KEY가 설정되지 않았거나 google-genai 클라이언트가 초기화되지 않았습니다. "
                ".env 파일 또는 환경 변수에 GEMINI_API_KEY를 설정해 주세요."
            )

        # 사용자 프롬프트 템플릿 포맷팅
        format_kwargs = {
            "as_of_date": portfolio_summary.get("macro_as_of_date", portfolio_summary.get("analysis_date", "현재")),
            "macro_table_markdown": portfolio_summary.get("macro_table_markdown", "(실시간 매크로 데이터 수집 중)"),
            "fx_rule_status": portfolio_summary.get("fx_rule_status", "중립 환율 구간"),
            "fx_rate": portfolio_summary.get("fx_rate", 1400.0),
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

        # Google Search Grounding 도구 정의
        search_tool = types.Tool(google_search=types.GoogleSearch())

        # 4단계 지능형 폴백 모델 풀 순차 실행
        models_to_try = GEMINI_MODEL_POOL

        for rank, model_name in enumerate(models_to_try, start=1):
            print(f"🤖 [Gemini AI] [{rank}/{len(models_to_try)}순위] 모델 '{model_name}' 호출 중 (Google Search Grounding 활성화)...")
            
            for attempt in range(1, max_retries + 1):
                try:
                    config_kwargs: Dict[str, Any] = {
                        "system_instruction": SYSTEM_PROMPT,
                        "temperature": 0.2,
                        "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
                        "tools": [search_tool],
                    }

                    # 모델별 사고 토큰 예산 설정
                    if "2.5-flash" in model_name:
                        config_kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=DEFAULT_THINKING_BUDGET)
                    elif "2.5-pro" in model_name:
                        config_kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=1024)

                    config = types.GenerateContentConfig(**config_kwargs)
                    
                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=user_prompt,
                        config=config,
                    )
                    
                    candidate = response.candidates[0] if (response and response.candidates) else None
                    finish_reason = getattr(candidate, "finish_reason", None)
                    
                    report_text = response.text if hasattr(response, "text") and response.text else str(response)
                    if report_text and len(report_text.strip()) > 150:
                        print(f"✅ [Gemini AI] 리포트 생성 완료 (모델: {model_name}, 총 {len(report_text):,}자, Finish: {finish_reason})")
                        return report_text.strip()
                    else:
                        print(f"   ⚠️ [Gemini AI] 응답 텍스트가 너무 짧습니다 ({len(report_text)}자). 재시도 {attempt}/{max_retries}")

                except Exception as exc:
                    err_str = str(exc)
                    # 429 쿼터 초과 시 레이트 리밋 윈도우(RPM) 안정을 위해 5초 대기 후 다음 순위 모델로 전환
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str or "quota" in err_str.lower():
                        print(f"   ⚠️ [Gemini AI] '{model_name}' 모델 쿼터(429) 감지 -> 5초 대기 후 다음 순위 모델로 전환합니다.")
                        time.sleep(5.0)
                        break
                    
                    # 404 모델 미지원 시 즉시 다음 모델로 전환
                    if "404" in err_str or "NOT_FOUND" in err_str or "not found" in err_str.lower():
                        print(f"   ⚠️ [Gemini AI] '{model_name}' 모델 미지원(404) 감지 -> 다음 순위 모델로 전환합니다.")
                        break

                    print(f"   ⚠️ [Gemini AI] API 호출 오류 (시도 {attempt}/{max_retries} - {model_name}): {err_str}")
                    
                    # 일시적 통신 오류 시 지수 백오프 (기본 3초)
                    if attempt < max_retries:
                        delay = base_delay * (2 ** (attempt - 1))
                        time.sleep(delay)
                        continue

            print(f"⚠️ [Gemini AI] '{model_name}' 모델 실패, 다음 순위 모델로 전환합니다...")

        # 만약 Search Grounding 활성화 상태에서 전 모델 쿼터 소진 시, 검색 도구 없이 기본 분석으로 안전 생성 시도
        print("🔄 [Gemini AI] Search Grounding 쿼터 초과로 기본 분석 모드(Search-Free Safety Fallback)로 안전 전환합니다...")
        for model_name in ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-3.5-flash-lite"]:
            try:
                time.sleep(3.0)
                config = types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0.2,
                    max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
                )
                response = self.client.models.generate_content(
                    model=model_name,
                    contents=user_prompt,
                    config=config,
                )
                report_text = response.text if hasattr(response, "text") and response.text else str(response)
                if report_text and len(report_text.strip()) > 150:
                    print(f"✅ [Gemini AI] 안전 폴백 모드로 리포트 생성 완료 (모델: {model_name}, 총 {len(report_text):,}자)")
                    return report_text.strip()
            except Exception as e:
                print(f"   ⚠️ [Gemini AI] 안전 폴백 모델 '{model_name}' 시도 중 오류: {e}")

        raise RuntimeError("Google Gemini 4단계 모델 풀(Pool)을 통한 포트폴리오 진단 리포트 생성에 최종 실패하였습니다.")
