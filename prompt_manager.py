# -*- coding: utf-8 -*-
"""
prompt_manager.py
=================
영문 원문 및 한국어 주석이 포함된 마크다운 템플릿(prompts/*.en.md)을 중앙에서 로드, 캐싱 및 렌더링하는
중앙 집중형 프롬프트 매니저(Single Source of Truth) 모듈입니다.
"""

import os
import re
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("PromptManager")

PROMPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompts")

_PROMPT_CACHE: Dict[str, str] = {}


def load_prompt_from_md(filename: str, fallback: str = "") -> str:
    """
    prompts/ 디렉토리 내 마크다운 파일에서 [PROMPT_START] ~ [PROMPT_END] 사이의
    순수 영문 프롬프트 본문을 추출하여 캐싱 후 반환합니다.
    """
    if filename in _PROMPT_CACHE:
        return _PROMPT_CACHE[filename]

    filepath = os.path.join(PROMPTS_DIR, filename)
    if not os.path.exists(filepath):
        logger.warning(f"⚠️ 프롬프트 파일이 존재하지 않습니다: {filepath}. 기본 폴백 프롬프트를 사용합니다.")
        return fallback

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        match = re.search(r'###\s*\[PROMPT_START\](.*?)###\s*\[PROMPT_END\]', content, flags=re.DOTALL)
        if match:
            prompt_body = match.group(1).strip()
        else:
            parts = content.split("---")
            prompt_body = parts[0].strip() if parts else content.strip()

        _PROMPT_CACHE[filename] = prompt_body
        logger.debug(f"✅ 프롬프트 파일 로드 성공: {filename}")
        return prompt_body
    except Exception as e:
        logger.error(f"❌ 프롬프트 파일 읽기 실패 ({filename}): {e}")
        return fallback


def get_fia_youtube_system_instruction() -> str:
    """유튜브 자막 분석용 FIA 시스템 인스트럭션을 로드합니다."""
    return load_prompt_from_md(
        "system_fia_youtube.en.md",
        fallback="""You are the Financial Intelligence Architect (FIA) and Senior Quant Portfolio Strategist.
Reason in English. Output JSON matching YouTubeAnalysisResult. Enforce Korean noun-ending verbs (~함, ~임, ~필요)."""
    )
