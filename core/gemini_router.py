# -*- coding: utf-8 -*-
"""
gemini_router.py
================
Google Gemini API (google-genai 최신 SDK)를 위한 단일 공통 스마트 라우터 모듈입니다.
1. client.models.list() 기반 가용 모델 실시간 자동 수집 (Auto-Discovery, 1시간 TTL 캐싱)
2. 공급 안정성(RPD 500 Lite 우선) 및 버전 최신도(3.5 > 3.1) 기반 스마트 스코어링 정렬
3. 서킷 브레이커: 429 쿼터 한도(60초 쿨다운), 404 미지원(블랙리스트 즉시 격리), 503(지수 백오프)
4. TPM 250K 초과 방지를 위한 텍스트 안전 슬라이싱 (기본 45,000자)
5. Pydantic Structured Outputs (response_schema) 및 Search Grounding 호환
"""

import os
import re
import sys
import time
import logging
from typing import Any, Dict, List, Optional, Type, Union
from pydantic import BaseModel

logger = logging.getLogger("GeminiRouter")

# Google GenAI 최신 SDK 안전 임포트
try:
    from google import genai
    from google.genai import types
except ImportError:
    genai = None
    types = None


# ==============================================================================
# 1. 동적 모델 관리자 & 서킷 브레이커 (DynamicGeminiModelManager)
# ==============================================================================
class DynamicGeminiModelManager:
    """Gemini 가용 모델 실시간 자동 수집 및 공급 안정성 기반 스마트 라우터"""

    def __init__(self, client: Any, cache_ttl_seconds: int = 3600):
        self.client = client
        self.cache_ttl = cache_ttl_seconds
        self.last_fetched_time = 0.0
        self.cached_models: List[str] = []
        self.cooldown_tracker: Dict[str, float] = {}  # {model_name: cooldown_until_timestamp}
        self.blacklist: set = set()

        # API 실패 시 비상용 기본 정적 풀 (공급 안정성 우선 순서)
        self.default_fallback_pool = [
            "gemini-3.5-flash-lite",
            "gemini-3.1-flash-lite",
            "gemini-2.5-flash-lite",
            "gemini-3.7-flash",
            "gemini-3.6-flash",
            "gemini-3.5-flash",
            "gemini-2.5-flash",
        ]

    def _compute_model_score(self, model_name: str) -> float:
        """
        공급 안정성 및 버전 최신도 기반 스코어링 공식
        Score = 용량등급(Lite +1000, Flash +100) + 버전(3.5->35점) - 쿨다운 페널티
        """
        name_lower = model_name.lower()
        score = 0.0

        # 1. 용량 등급 가중치 (Lite/8B 계열 RPD 500~1500회 최우선)
        if "lite" in name_lower or "8b" in name_lower:
            score += 1000.0
        elif "flash" in name_lower:
            score += 100.0
        elif "pro" in name_lower:
            score += 10.0
        else:
            score += 5.0

        # 2. 버전 최신도 가산점 (동일 등급 내 최신 지능 우선 사용: 3.7 > 3.6 > 3.5 > 3.1 > 2.5)
        version_match = re.search(r'(\d+(?:\.\d+)?)', name_lower)
        if version_match:
            try:
                score += float(version_match.group(1)) * 10.0
            except ValueError:
                pass

        # 3. 실시간 쿨다운 페널티
        now = time.time()
        if now < self.cooldown_tracker.get(model_name, 0.0):
            score -= 10000.0

        return score

    def refresh_models_from_api(self) -> None:
        """client.models.list()로 계정에 실제 배정된 활성 모델을 조회하고 스코어링 정렬"""
        if not self.client:
            self.cached_models = list(self.default_fallback_pool)
            return

        try:
            logger.info("🔄 [Gemini Router] 최신 가용 모델 목록을 Google API에서 실시간 조회 중...")
            all_models = list(self.client.models.list())
            discovered = []

            EXCLUDE_PATTERNS = [
                "image", "tts", "lyria", "nano", "deep-research", "embedding",
                "pro", "omni", "customtools"
            ]

            for m in all_models:
                m_name = getattr(m, "name", "") or str(m)
                clean_name = m_name.replace("models/", "").strip()
                clean_lower = clean_name.lower()
                
                # 무겁거나 저쿼터/특수 전용 모델 배제
                if any(pat in clean_lower for pat in EXCLUDE_PATTERNS):
                    continue

                # 지원 액션 검증
                actions = getattr(m, "supported_actions", []) or getattr(m, "supported_generation_methods", [])
                
                # 텍스트 생성이 가능한 Flash/Lite 모델만 선별
                if "flash" in clean_lower and (not actions or "generateContent" in actions):
                    discovered.append(clean_name)

            if discovered:
                # 스코어 높은 순(Lite 최우선 -> 최신 버전 순)으로 정렬
                discovered.sort(key=self._compute_model_score, reverse=True)
                self.cached_models = discovered
                self.last_fetched_time = time.time()
                logger.info(f"✅ [Gemini Router] 최적 모델 풀 자동 구성 완료: {self.cached_models}")
            else:
                self.cached_models = list(self.default_fallback_pool)
        except Exception as e:
            logger.warning(f"⚠️ [Gemini Router] 모델 자동 수집 실패 (기본 풀 사용): {e}")
            self.cached_models = list(self.default_fallback_pool)
            self.last_fetched_time = time.time()

    def get_ordered_models(self) -> List[str]:
        """현재 호출 가능한 모델 목록을 우선순위 순으로 반환"""
        now = time.time()
        if not self.cached_models or (now - self.last_fetched_time > self.cache_ttl):
            self.refresh_models_from_api()

        # 블랙리스트 제외 및 스코어 재정렬 (쿨다운 반영)
        active_models = [m for m in self.cached_models if m not in self.blacklist]
        active_models.sort(key=self._compute_model_score, reverse=True)

        return active_models if active_models else list(self.default_fallback_pool)

    def record_failure(self, model_name: str, error_str: str) -> None:
        """에러 유형에 따라 쿨다운 및 블랙리스트 등록"""
        now = time.time()
        err_upper = error_str.upper()

        if "429" in err_upper or "RESOURCE_EXHAUSTED" in err_upper or "QUOTA" in err_upper:
            # 429 쿼터 초과는 60초간 쿨다운
            self.cooldown_tracker[model_name] = now + 60.0
            logger.warning(f"🛑 [Gemini Router] [{model_name}] 429 쿼터 한도 감지 -> 60초 쿨다운 등록 및 즉시 우회")
        elif "404" in err_upper or "NOT_FOUND" in err_upper:
            # 404는 존재하지 않는 모델이므로 영구 제외
            self.blacklist.add(model_name)
            logger.error(f"❌ [Gemini Router] [{model_name}] 404 미지원 모델 감지 -> 블랙리스트 등록 (영구 제외)")
        elif "503" in err_upper or "UNAVAILABLE" in err_upper:
            # 503은 일시적 과부하이므로 10초 쿨다운
            self.cooldown_tracker[model_name] = now + 10.0
            logger.warning(f"⚠️ [Gemini Router] [{model_name}] 503 일시적 과부하 감지 -> 10초 쿨다운 등록")


# ==============================================================================
# 2. 단일 공통 실행 엔진 (GeminiSafeExecutor)
# ==============================================================================
class GeminiSafeExecutor:
    """모든 Gemini API 호출을 단일 공통 원칙으로 안전하게 실행하는 실행 엔진"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = (
            api_key
            or os.environ.get("GEMINI_API_KEY")
            or os.environ.get("GOOGLE_API_KEY")
            or ""
        ).strip()
        self.client: Any = None
        self.router: Optional[DynamicGeminiModelManager] = None
        self._init_engine()

    def _init_engine(self) -> None:
        if not self.api_key:
            logger.warning("⚠️ GEMINI_API_KEY가 설정되지 않았습니다.")
            return

        if genai is None or types is None:
            logger.error("❌ 'google-genai' 패키지가 설치되지 않았습니다.")
            return

        try:
            self.client = genai.Client(api_key=self.api_key)
            self.router = DynamicGeminiModelManager(client=self.client)
            logger.info("✅ Gemini Safe Executor 초기화 완료")
        except Exception as e:
            logger.error(f"❌ Gemini Safe Executor 초기화 실패: {e}")
            self.client = None
            self.router = None

    def is_available(self) -> bool:
        return bool(self.api_key and self.client is not None and self.router is not None)

    def safe_generate_structured(
        self,
        contents: Union[str, List[Any]],
        system_instruction: str,
        response_schema: Type[BaseModel],
        max_input_chars: int = 45000,
        temperature: float = 0.0,
        max_retries_per_model: int = 2,
        base_delay: float = 2.0,
    ) -> Optional[BaseModel]:
        """
        Pydantic 스키마(Structured Outputs)를 강제하여 구조화된 데이터를 안전하게 반환합니다.
        TPM 250K 안전 슬라이싱, 모델 자동 라우팅, 서킷 브레이커, 지수 백오프 자동 처리
        """
        if not self.is_available():
            logger.error("❌ Gemini Safe Executor를 사용할 수 없습니다.")
            return None

        # 1. 텍스트 컨텐츠인 경우 TPM 250K 방어를 위한 안전 슬라이싱
        safe_contents = contents
        if isinstance(contents, str) and len(contents) > max_input_chars:
            safe_contents = contents[:max_input_chars]
            logger.info(f"✂️ [Gemini Router] 입력 텍스트 안전 다이어트 적용 ({len(contents):,}자 -> {max_input_chars:,}자)")

        models_to_try = self.router.get_ordered_models()

        for model_name in models_to_try:
            for attempt in range(1, max_retries_per_model + 1):
                try:
                    config = types.GenerateContentConfig(
                        system_instruction=system_instruction,
                        response_mime_type="application/json",
                        response_schema=response_schema,
                        temperature=temperature,
                    )

                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=safe_contents,
                        config=config,
                    )

                    if response and response.text:
                        parsed_obj = response_schema.model_validate_json(response.text.strip())
                        logger.info(f"✅ [Gemini Router] Structured Output 성공 (모델: {model_name})")
                        return parsed_obj

                except Exception as exc:
                    err_str = str(exc)
                    self.router.record_failure(model_name, err_str)

                    # 503은 같은 모델에서 지수 백오프 대기 후 재시도
                    if "503" in err_str or "UNAVAILABLE" in err_str or "high demand" in err_str.lower():
                        if attempt < max_retries_per_model:
                            delay = base_delay * (2 ** (attempt - 1))
                            logger.warning(f"⚠️ [Gemini Router] [{model_name}] 503 과부하 감지 -> {delay:.1f}초 대기 후 재시도 ({attempt}/{max_retries_per_model})")
                            time.sleep(delay)
                            continue
                        else:
                            time.sleep(1.5)
                            break

                    # 429 또는 404는 즉시 다음 순위 모델로 전환 (1.0초 대기)
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str or "404" in err_str or "NOT_FOUND" in err_str:
                        time.sleep(1.0)
                        break

                    logger.warning(f"⚠️ [Gemini Router] 호출 오류 ({model_name}): {err_str}")
                    time.sleep(1.0)
                    break

        logger.error("❌ 모든 가용 모델 풀에서 Structured Output 생성에 최종 실패하였습니다.")
        return None

    def safe_generate_text(
        self,
        contents: Union[str, List[Any]],
        system_instruction: str,
        tools: Optional[List[Any]] = None,
        max_output_tokens: int = 8192,
        temperature: float = 0.2,
        max_input_chars: int = 45000,
        max_retries_per_model: int = 2,
        base_delay: float = 2.0,
    ) -> Optional[str]:
        """
        Search Grounding 또는 일반 텍스트 리포트 생성을 안전하게 실행합니다.
        """
        if not self.is_available():
            logger.error("❌ Gemini Safe Executor를 사용할 수 없습니다.")
            return None

        safe_contents = contents
        if isinstance(contents, str) and len(contents) > max_input_chars:
            safe_contents = contents[:max_input_chars]

        models_to_try = self.router.get_ordered_models()

        for model_name in models_to_try:
            for attempt in range(1, max_retries_per_model + 1):
                try:
                    config_kwargs: Dict[str, Any] = {
                        "system_instruction": system_instruction,
                        "temperature": temperature,
                        "max_output_tokens": max_output_tokens,
                    }
                    if tools:
                        config_kwargs["tools"] = tools

                    config = types.GenerateContentConfig(**config_kwargs)

                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=safe_contents,
                        config=config,
                    )

                    report_text = ""
                    if hasattr(response, "text") and response.text:
                        report_text = response.text.strip()
                    elif hasattr(response, "candidates") and response.candidates:
                        cand = response.candidates[0]
                        if cand.content and cand.content.parts:
                            parts = [p.text.strip() for p in cand.content.parts if getattr(p, "text", None)]
                            report_text = "\n\n".join(parts).strip()

                    if report_text and len(report_text) > 50:
                        logger.info(f"✅ [Gemini Router] 텍스트 생성 성공 (모델: {model_name}, 길이: {len(report_text):,}자)")
                        return report_text

                except Exception as exc:
                    err_str = str(exc)
                    self.router.record_failure(model_name, err_str)

                    if "503" in err_str or "UNAVAILABLE" in err_str:
                        if attempt < max_retries_per_model:
                            delay = base_delay * (2 ** (attempt - 1))
                            time.sleep(delay)
                            continue
                        break

                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str or "404" in err_str or "NOT_FOUND" in err_str:
                        time.sleep(1.0)
                        break

                    logger.warning(f"⚠️ [Gemini Router] 텍스트 생성 오류 ({model_name}): {err_str}")
                    time.sleep(1.0)
                    break

        logger.error("❌ 모든 가용 모델 풀에서 텍스트 리포트 생성에 최종 실패하였습니다.")
        return None
