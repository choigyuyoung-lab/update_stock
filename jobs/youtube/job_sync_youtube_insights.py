# -*- coding: utf-8 -*-
"""
sync_youtube_insights.py
========================
노션 [Youtube 주소가이드 DB](YOUTUBE_GUIDE_DATABASE_ID)에서 활성화(체크박스)된
유튜브 채널/재생목록/영상을 자동으로 로드하여 영속 대기열(FIFO Queue) 기반으로 AI 시황을 분석하고 적재합니다.

[핵심 아키텍처]
1. Pure In-Memory 소스 식별: 웹 스크래핑 없이 재생목록(PL)/채널(UC/@)/단일영상을 0.001초 만에 자동 분류
2. 2-Phase 분할 배치 & 영속 대기열: 1단계 자막 사전 수집 -> 2단계 Gemini 초고속 구조화 분석 (영상당 3~5초)
3. 3중 교차 검증 게이트웨이 연동: StockRegistryGateway를 통해 상장주식 Master Relation 100% 자동 바인딩
"""

import os
import sys
import json
import re
import time
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import yt_dlp
except ImportError:
    yt_dlp = None

try:
    from youtube_transcript_api import YouTubeTranscriptApi
except ImportError:
    YouTubeTranscriptApi = None

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    get_kst_now,
    get_kst_str,
    paginate_database,
    get_page_text,
    get_prop_value,
)
from core.stock_registry import clean_ticker_key, StockRegistryGateway
from jobs.youtube.ai_service import AIService, YouTubeAnalysisResult
from services.stock_fallback_resolver import (
    resolve_ticker_and_name,
    _get_name_lookup_index,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SyncYouTubeInsights")

load_dotenv()

# ==============================================================================
# 1. 환경 설정 및 영속 캐시/대기열 경로
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN", required=False)
YOUTUBE_DB_ID = get_db_id("YOUTUBE_DATABASE_ID", ["YOUTUBE_DB_ID"], required=False)
YOUTUBE_GUIDE_DB_ID = get_db_id("YOUTUBE_GUIDE_DATABASE_ID", ["YOUTUBE_GUIDE_DB_ID"], required=False)
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID"], required=False)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOCAL_DIR = Path(__file__).resolve().parent

CACHE_PATHS = [PROJECT_ROOT / ".processed_youtube_videos.json", LOCAL_DIR / ".processed_youtube_videos.json"]
QUEUE_PATHS = [PROJECT_ROOT / ".youtube_pending_queue.json", LOCAL_DIR / ".youtube_pending_queue.json"]
GUIDE_CACHE_PATHS = [PROJECT_ROOT / ".youtube_guide_sources.json", LOCAL_DIR / ".youtube_guide_sources.json"]
DETECTED_CACHE_PATHS = [PROJECT_ROOT / ".new_youtube_videos_detected.json", LOCAL_DIR / ".new_youtube_videos_detected.json"]


# ==============================================================================
# 2. 캐시 및 영속 대기열(FIFO Queue) 관리자
# ==============================================================================
def save_detected_new_videos(videos: List[Dict[str, Any]]) -> None:
    """새롭게 감지된 미수집 동영상 목록을 임시 캐시 파일에 저장합니다."""
    for dp in DETECTED_CACHE_PATHS:
        try:
            dp.parent.mkdir(parents=True, exist_ok=True)
            with open(dp, "w", encoding="utf-8") as f:
                json.dump(videos, f, ensure_ascii=False, indent=2, default=_json_serial_default)
        except Exception as e:
            logger.warning(f"⚠️ 신규 영상 캐시 저장 실패 ({dp}): {e}")


def load_detected_new_videos() -> List[Dict[str, Any]]:
    """사전 감지된 미수집 동영상 목록을 로컬 캐시에서 로드합니다."""
    for dp in DETECTED_CACHE_PATHS:
        if dp.exists():
            try:
                with open(dp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
    return []

def load_cached_guide_sources() -> List[Dict[str, Any]]:
    """로컬에 백업/캐시된 유튜브 주소가이드 활성 목록을 로드합니다."""
    for gp in GUIDE_CACHE_PATHS:
        if gp.exists():
            try:
                with open(gp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and data:
                        return data
            except Exception as e:
                logger.warning(f"⚠️ 주소가이드 캐시 읽기 실패 ({gp}): {e}")
    return []


def save_guide_sources_cache(sources: List[Dict[str, Any]]) -> None:
    """노션에서 로드된 활성화된 주소가이드 목록을 로컬 영속 캐시에 저장합니다."""
    if not sources:
        return
    for gp in GUIDE_CACHE_PATHS:
        try:
            gp.parent.mkdir(parents=True, exist_ok=True)
            with open(gp, "w", encoding="utf-8") as f:
                json.dump(sources, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ 주소가이드 캐시 저장 실패 ({gp}): {e}")


def load_processed_videos() -> Set[str]:
    """이미 처리 완료된 유튜브 비디오 ID 집합을 로드합니다."""
    processed = set()
    for cp in CACHE_PATHS:
        if cp.exists():
            try:
                with open(cp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    processed.update(data if isinstance(data, list) else data.keys())
            except Exception as e:
                logger.warning(f"⚠️ 캐시 읽기 실패 ({cp}): {e}")
    return processed


def save_processed_videos(processed_ids: Set[str]) -> None:
    """노션에 성공적으로 적재된 비디오 ID 목록을 캐시에 저장합니다."""
    sorted_ids = sorted(list(processed_ids))
    for cp in CACHE_PATHS:
        try:
            cp.parent.mkdir(parents=True, exist_ok=True)
            with open(cp, "w", encoding="utf-8") as f:
                json.dump(sorted_ids, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ 캐시 저장 실패 ({cp}): {e}")


def load_pending_queue() -> List[Dict[str, Any]]:
    """
    모든 영속 대기열 경로(PROJECT_ROOT, LOCAL_DIR)에서 데이터를 안전하게 로드하고 지능형으로 병합합니다.
    - 단일 빈 파일([])이 다른 경로의 정상 스크립트를 덮어쓰거나 지우지 못하도록 보호
    - 스크립트(transcript)가 채워진 객체를 최우선으로 보존
    """
    merged_map: Dict[str, Dict[str, Any]] = {}
    for qp in QUEUE_PATHS:
        if qp.exists():
            try:
                with open(qp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            if not isinstance(item, dict):
                                continue
                            vid = item.get("video_id") or extract_video_id(item.get("url", ""))
                            if not vid:
                                continue
                            if vid not in merged_map:
                                merged_map[vid] = item
                            else:
                                # 기존 항목에 자막이 없는데 새 항목에 자막이 있는 경우 자막 버전으로 보존
                                if not merged_map[vid].get("transcript") and item.get("transcript"):
                                    merged_map[vid] = item
            except Exception as e:
                logger.warning(f"⚠️ 대기열 읽기 실패 ({qp}): {e}")
    return list(merged_map.values())


def _json_serial_default(obj: Any) -> Any:
    """JSON 직렬화 불가능한 객체(datetime 등)를 안전하게 변환합니다."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def save_pending_queue(queue_items: List[Dict[str, Any]]) -> None:
    """잔여 대기열 목록을 저장합니다."""
    for qp in QUEUE_PATHS:
        try:
            qp.parent.mkdir(parents=True, exist_ok=True)
            with open(qp, "w", encoding="utf-8") as f:
                json.dump(queue_items, f, ensure_ascii=False, indent=2, default=_json_serial_default)
        except Exception as e:
            logger.warning(f"⚠️ 대기열 저장 실패 ({qp}): {e}")


def sort_pending_queue_by_publish_time(queue_items: List[Dict[str, Any]], order: str = "asc") -> List[Dict[str, Any]]:
    """
    대기열(Queue) 내 동영상들을 한국 표준시 게시일시(publish_time_kst) 기준으로 정렬합니다.
    - order='asc': 과거 -> 최신순 (오름차순, 시황 타임라인 순차 분석, 기본값)
    - order='desc': 최신 -> 과거순 (내림차순, 최신 시황 우선 분석)
    """
    if not queue_items:
        return []

    def _get_sort_key(item: Dict[str, Any]) -> str:
        # 1. 분 단위 정밀 KST 일시 (YYYY-MM-DD HH:MM)
        p_time = str(item.get("publish_time_kst") or "").strip()
        if len(p_time) >= 10:
            return p_time
        # 2. 일자 (YYYY-MM-DD)
        p_date = str(item.get("publish_date") or "").strip()
        if len(p_date) >= 10:
            return f"{p_date} 00:00"
        return "9999-99-99 99:99" if order.lower() == "asc" else "0000-00-00 00:00"

    reverse_sort = (order.lower() == "desc")
    return sorted(queue_items, key=_get_sort_key, reverse=reverse_sort)


# ==============================================================================
# 3. 채널 및 영상 식별 유틸리티 (Pure In-Memory)
# ==============================================================================
def extract_video_id(video_input: str) -> Optional[str]:
    """유튜브 URL 또는 텍스트에서 11자리 Video ID를 추출합니다."""
    s = (video_input or "").strip()
    if re.match(r'^[A-Za-z0-9_-]{11}$', s):
        return s
    m = re.search(r'(?:v=|\/v\/|youtu\.be\/|\/embed\/|\/shorts\/)([A-Za-z0-9_-]{11})', s) or re.search(r'[\?&]v=([A-Za-z0-9_-]{11})', s)
    return m.group(1) if m else None


def extract_playlist_id(playlist_input: str) -> Optional[str]:
    """유튜브 URL 또는 텍스트에서 Playlist ID를 추출합니다."""
    s = (playlist_input or "").strip()
    if re.match(r'^(?:PL|UU|FL|RD|OLAK5uy_)[A-Za-z0-9_-]+$', s):
        return s
    m = re.search(r'[?&]list=([A-Za-z0-9_-]+)', s)
    return m.group(1) if m else None


def extract_channel_id(channel_input: str) -> str:
    """유튜브 URL 또는 텍스트에서 채널 ID(UC...) 또는 핸들(@...)을 정제 추출합니다."""
    s = (channel_input or "").strip()
    clean_s = s.split("?")[0].rstrip("/")
    m_uc = re.search(r'(UC[A-Za-z0-9_-]{20,24})', s)
    if m_uc:
        return m_uc.group(1)
    m_handle = re.search(r'(@[A-Za-z0-9_.-]+)', s)
    if m_handle:
        return m_handle.group(1)
    if re.match(r'^UC[A-Za-z0-9_-]{20,24}$', clean_s):
        return clean_s
    return clean_s


def resolve_channel_target(channel_input: str, max_videos: int = 3) -> Dict[str, Any]:
    """CLI 또는 입력값(채널 ID, 재생목록 ID, URL, @핸들)을 표준 소스 규격으로 변환합니다."""
    s = (channel_input or "").strip()
    pl_id = extract_playlist_id(s)
    v_id = extract_video_id(s) if any(x in s for x in ["watch", "youtu.be", "shorts"]) else None

    if pl_id:
        return {"page_id": "", "name": s, "type": "재생목록", "url": f"https://www.youtube.com/playlist?list={pl_id}", "channel_id": pl_id, "max_videos": max_videos}
    if v_id:
        return {"page_id": "", "name": s, "type": "단일영상", "url": f"https://www.youtube.com/watch?v={v_id}", "channel_id": v_id, "max_videos": max_videos}

    clean_ch = extract_channel_id(s)
    return {
        "page_id": "",
        "name": s,
        "type": "채널",
        "url": s if s.startswith("http") else (f"https://www.youtube.com/{clean_ch}" if clean_ch.startswith("@") else f"https://www.youtube.com/channel/{clean_ch}"),
        "channel_id": clean_ch,
        "max_videos": max_videos,
    }


class _YtDlpSilentLogger:
    def debug(self, msg: str) -> None: pass
    def warning(self, msg: str) -> None: pass
    def error(self, msg: str) -> None: pass


def _parse_video_publish_date(
    raw_date: Optional[str] = None,
    timestamp: Optional[Any] = None,
    iso_str: Optional[str] = None
) -> Tuple[str, str]:
    """
    다양한 포맷(YYYYMMDD, Unix timestamp, ISO 8601 UTC)의 동영상 게시일시를
    한국 표준시(KST) 기준 (YYYY-MM-DD, YYYY-MM-DD HH:MM) 튜플로 정밀 변환합니다.
    """
    tz_kst = ZoneInfo("Asia/Seoul")
    now_kst = get_kst_now()

    # 1. ISO 8601 형식 문자열 (예: RSS의 2026-08-27T14:30:00+00:00 또는 2026-08-27T14:30:00Z)
    if iso_str and isinstance(iso_str, str):
        try:
            clean_iso = iso_str.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_iso)
            dt_kst = dt.astimezone(tz_kst)
            return dt_kst.strftime("%Y-%m-%d"), dt_kst.strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass

    # 2. Unix Timestamp (초 단위 또는 밀리초)
    if timestamp is not None:
        try:
            ts_val = float(timestamp)
            if ts_val > 1e11:  # 밀리초인 경우
                ts_val /= 1000.0
            if ts_val > 0:
                dt_kst = datetime.fromtimestamp(ts_val, tz=tz_kst)
                return dt_kst.strftime("%Y-%m-%d"), dt_kst.strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass

    # 3. YYYYMMDD 또는 YYYY-MM-DD 형식 문자열
    if raw_date and isinstance(raw_date, str):
        s = raw_date.replace("-", "").strip()
        if len(s) == 8 and s.isdigit():
            date_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            return date_str, f"{date_str} 00:00"
        if len(raw_date.strip()) == 10 and raw_date.count("-") == 2:
            return raw_date.strip(), f"{raw_date.strip()} 00:00"

    today_str = now_kst.strftime("%Y-%m-%d")
    return today_str, now_kst.strftime("%Y-%m-%d %H:%M")


def resolve_video_info(video_input: str) -> Optional[Dict[str, Any]]:
    """단일 유튜브 영상 URL/ID의 메타데이터를 수집합니다."""
    vid = extract_video_id(video_input)
    if not vid:
        logger.warning(f"⚠️ 유효한 유튜브 Video ID를 찾을 수 없습니다: {video_input}")
        return None

    video_url = f"https://www.youtube.com/watch?v={vid}"
    title, channel_name, description = f"YouTube Video ({vid})", "YouTube", ""
    publish_date, publish_time_kst = "", ""

    if yt_dlp:
        try:
            ydl_opts = {
                "skip_download": True,
                "quiet": True,
                "no_warnings": True,
                "ignoreerrors": True,
                "nocheckcertificate": True,
                "logger": _YtDlpSilentLogger(),
                "extract_flat": "in_playlist",
                "extractor_args": {"youtube": {"player_client": ["android", "ios", "mweb"], "skip": ["dash", "hls"]}}
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                if info:
                    title = info.get("title") or title
                    channel_name = info.get("channel") or info.get("uploader") or channel_name
                    description = info.get("description") or ""
                    
                    ud = info.get("upload_date")
                    ts = info.get("timestamp") or info.get("release_timestamp")
                    publish_date, publish_time_kst = _parse_video_publish_date(raw_date=ud, timestamp=ts)
        except Exception as e:
            logger.debug(f"yt-dlp 메타데이터 조회 생략 ({vid}): {e}")

    if not publish_date:
        publish_date, publish_time_kst = _parse_video_publish_date()

    return {
        "video_id": vid,
        "title": title,
        "url": video_url,
        "publish_date": publish_date,
        "publish_time_kst": publish_time_kst,
        "channel_name": channel_name,
        "description": description,
    }


# ==============================================================================
# 4. [Youtube 주소가이드] 노션 DB 연동 관리자
# ==============================================================================
def load_active_sources_from_notion(client: Any, guide_db_id: str, use_cache_fallback: bool = True) -> List[Dict[str, Any]]:
    """노션 [Youtube 주소가이드 DB]에서 활성화된 채널/재생목록/영상을 로드합니다.
    DNS 또는 네트워크 장애 시 로컬 백업 캐시(.youtube_guide_sources.json)로 자동 폴백합니다."""
    if not guide_db_id:
        logger.warning("⚠️ YOUTUBE_GUIDE_DATABASE_ID가 설정되지 않았습니다.")
        if use_cache_fallback:
            return load_cached_guide_sources()
        return []

    sources = []
    try:
        for page in paginate_database(client, guide_db_id, page_size=100):
            props = page.get("properties", {})
            if not (props.get("활성화") or {}).get("checkbox", False):
                continue

            name = get_page_text(props, ["채널명 / 제목", "이름", "Title", "Name"]).strip()
            url_val = props.get("URL / 채널핸들", {}).get("url") or get_page_text(props, ["URL / 채널핸들", "URL", "Link"]).strip()
            ch_val = get_page_text(props, ["Channel ID", "ChannelID", "ID"]).strip()

            raw_target = ch_val or url_val
            if not raw_target:
                continue

            max_v = (props.get("최대 수집 개수") or {}).get("number")
            max_videos = int(max_v) if max_v is not None and max_v > 0 else 3

            pl_id = extract_playlist_id(raw_target)
            v_id = extract_video_id(raw_target) if any(x in raw_target for x in ["watch", "youtu.be", "shorts"]) else None

            if pl_id:
                src_type, target_id, target_url = "재생목록", pl_id, url_val or f"https://www.youtube.com/playlist?list={pl_id}"
            elif v_id:
                src_type, target_id, target_url = "단일영상", v_id, url_val or f"https://www.youtube.com/watch?v={v_id}"
            else:
                src_type = "채널"
                target_id = extract_channel_id(raw_target)
                target_url = url_val or (f"https://www.youtube.com/{target_id}" if target_id.startswith("@") else f"https://www.youtube.com/channel/{target_id}")

            sources.append({
                "page_id": page.get("id", ""),
                "name": name or target_id,
                "type": src_type,
                "url": target_url,
                "channel_id": target_id,
                "max_videos": max_videos,
            })

        if sources:
            save_guide_sources_cache(sources)
            logger.info(f"   💾 [주소가이드 캐시 갱신] 활성화 채널/영상 {len(sources)}개 캐시 저장 완료")
        elif use_cache_fallback:
            cached = load_cached_guide_sources()
            if cached:
                logger.warning(f"⚠️ [Youtube 주소가이드 DB] 활성화 항목이 0건으로 반환됨 -> 기존 로컬 캐시({len(cached)}개) 유지")
                return cached

    except Exception as e:
        logger.error(f"❌ [Youtube 주소가이드 DB] 로드 중 오류: {e}")
        if use_cache_fallback:
            cached = load_cached_guide_sources()
            if cached:
                print(f"   🛡️ [네트워크/DNS 장애 대응] 로컬 캐시된 주소가이드({len(cached)}개)로 복구하여 스캔을 계속 진행합니다.")
                return cached
    return sources


def prefetch_guide_sources_to_cache(notion_client: Any, guide_db_id: str) -> bool:
    """일반망 상태에서 노션 [Youtube 주소가이드 DB]의 활성 채널 목록을 미리 가져와 로컬 캐시에 저장합니다."""
    print("=" * 80)
    print("📥 [Step 1: 주소가이드 사전 동기화] 노션 DB에서 활성화된 유튜브 채널/영상 목록 선제 수집")
    print("=" * 80)
    if not notion_client or not guide_db_id:
        print("❌ 노션 토큰(NOTION_TOKEN) 또는 YOUTUBE_GUIDE_DATABASE_ID가 설정되지 않았습니다.")
        return False

    sources = load_active_sources_from_notion(notion_client, guide_db_id, use_cache_fallback=False)
    if sources:
        save_guide_sources_cache(sources)
        print(f"\n✅ [주소가이드 캐시 동기화 완료] 총 {len(sources)}개 활성 채널/영상이 성공적으로 캐싱되었습니다.")
        for idx, s in enumerate(sources, 1):
            print(f"   {idx}. [{s.get('type')}] {s.get('name')} (ID: {s.get('channel_id')}, 최대 {s.get('max_videos')}개)")
        print("-" * 80)
        return True
    else:
        print("⚠️ 노션에서 활성화된 채널/영상을 찾지 못했거나 로드하지 못했습니다.")
        return False


def update_guide_last_scanned(client: Any, page_id: str) -> None:
    """노션 [Youtube 주소가이드 DB] 해당 항목의 '최근 수집일'을 오늘(KST)로 갱신합니다."""
    if not page_id or not client:
        return
    try:
        client.pages.update(page_id=page_id, properties={"최근 수집일": {"date": {"start": get_kst_str("%Y-%m-%d")}}})
    except Exception as e:
        logger.debug(f"최근 수집일 갱신 실패 ({page_id}): {e}")


def append_video_history_to_guide_page(
    client: Any,
    page_id: str,
    video_meta: Dict[str, Any],
    analyzed: Optional[YouTubeAnalysisResult] = None,
    study_page_id: Optional[str] = None
) -> None:
    """[Youtube 주소가이드] 해당 페이지 본문에 수집된 영상 이력을 표(Table)로 누적 적재합니다."""
    if not page_id or not client:
        return

    vid = video_meta.get("video_id", "")
    title = video_meta.get("title", "YouTube Video")
    url = video_meta.get("url", f"https://www.youtube.com/watch?v={vid}")
    pub_time = video_meta.get("publish_time_kst") or video_meta.get("publish_date", get_kst_str("%Y-%m-%d"))
    summary_short = (analyzed.overall_summary.strip().split("\n")[0][:100]) if analyzed and analyzed.overall_summary else ""

    assets_str = ", ".join([f"{a.ticker or a.name}" for a in (analyzed.assets or []) if a.ticker or a.name][:3]) if analyzed else "-"
    notion_url = f"https://notion.so/{study_page_id.replace('-', '')}" if study_page_id else ""

    data_row = {
        "object": "block",
        "type": "table_row",
        "table_row": {
            "cells": [
                [{"type": "text", "text": {"content": pub_time}, "annotations": {"code": True}}],
                [{"type": "text", "text": {"content": title[:35] + ("..." if len(title) > 35 else ""), "link": {"url": url}}, "annotations": {"bold": True}}],
                [{"type": "text", "text": {"content": "📄 리포트 열기" if notion_url else "-", **({"link": {"url": notion_url}} if notion_url else {})}, "annotations": {"bold": True, "color": "blue"}}],
                [{"type": "text", "text": {"content": assets_str[:40]}}],
                [{"type": "text", "text": {"content": summary_short[:80]}}],
            ]
        }
    }

    try:
        blocks = client.blocks.children.list(block_id=page_id).get("results", [])
        existing_table_id = next((b.get("id") for b in blocks if b.get("type") == "table"), None)
        existing_text = "".join([rt.get("plain_text", "") + (rt.get("href") or "") for b in blocks for rt in b.get(b.get("type", ""), {}).get("rich_text", [])])

        if vid and (vid in existing_text or url in existing_text):
            return

        if existing_table_id:
            client.blocks.children.append(block_id=existing_table_id, children=[data_row])
            logger.info(f"   📊 [투자가이드 표 행 추가] '{title[:25]}' ({pub_time})")
            return

        header_row = {
            "object": "block",
            "type": "table_row",
            "table_row": {
                "cells": [
                    [{"type": "text", "text": {"content": "게시일시 (KST)"}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": "유튜브 영상"}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": "📑 투자공부 리포트"}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": "언급 종목"}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": "AI 핵심 요약"}, "annotations": {"bold": True}}],
                ]
            }
        }
        client.blocks.children.append(block_id=page_id, children=[
            {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"type": "text", "text": {"content": "🎬 수집된 동영상 이력 (KST 최신순)"}}]}},
            {"object": "block", "type": "table", "table": {"table_width": 5, "has_column_header": True, "has_row_header": False, "children": [header_row, data_row]}}
        ])
        logger.info(f"   📊 [투자가이드 표 신규 생성] '{title}' ({pub_time})")
    except Exception as e:
        logger.warning(f"   ⚠️ [투자가이드 본문 기록 실패]: {e}")


# ==============================================================================
# 5. 유튜브 최신 영상 및 자막 추출 엔진 (고속 RSS 병렬 스캔 + yt-dlp 자막 엔진)
# ==============================================================================
def fetch_videos_via_rss(channel_or_playlist_id: str, channel_name: str = "", max_videos: int = 5, is_playlist: bool = False) -> List[Dict[str, Any]]:
    """YouTube 공식 RSS 피드를 통해 외부 의존성 없이 최신 영상 목록을 초고속 수집합니다."""
    clean_id = (channel_or_playlist_id or "").strip()
    if not clean_id:
        return []

    is_pl = is_playlist or any(clean_id.startswith(p) for p in ["PL", "UU", "FL", "RD", "OLAK5uy_"])
    if is_pl:
        rss_url = f"https://www.youtube.com/feeds/videos.xml?playlist_id={clean_id}"
    elif clean_id.startswith("UC"):
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={clean_id}"
    else:
        # URL 또는 @핸들 형태 시 channel_id 정제 시도
        extracted = extract_channel_id(clean_id)
        if extracted.startswith("UC"):
            rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={extracted}"
        else:
            return []

    try:
        res = requests.get(rss_url, timeout=5)
        if res.status_code != 200:
            return []

        import xml.etree.ElementTree as ET
        root = ET.fromstring(res.text)
        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "yt": "http://www.youtube.com/xml/schemas/2015",
            "media": "http://search.yahoo.com/mrss/"
        }

        videos = []

        for entry in root.findall("atom:entry", ns):
            vid_elem = entry.find("yt:videoId", ns)
            title_elem = entry.find("atom:title", ns)
            pub_elem = entry.find("atom:published", ns)
            author_elem = entry.find("atom:author/atom:name", ns)

            vid = vid_elem.text.strip() if vid_elem is not None and vid_elem.text else ""
            vtitle = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
            pub_iso = pub_elem.text.strip() if pub_elem is not None and pub_elem.text else ""
            pub_str, pub_time_kst = _parse_video_publish_date(iso_str=pub_iso)
            ch_name = author_elem.text.strip() if author_elem is not None and author_elem.text else (channel_name or "YouTube")

            if vid and vtitle:
                videos.append({
                    "video_id": vid,
                    "title": vtitle,
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "publish_date": pub_str,
                    "publish_time_kst": pub_time_kst,
                    "publish_dt": datetime.now(ZoneInfo("Asia/Seoul")),
                    "channel_name": ch_name,
                    "guide_name": channel_name or ch_name,
                })
                if len(videos) >= max_videos:
                    break

        return videos
    except Exception as e:
        logger.debug(f"RSS 피드 수집 실패 ({clean_id}): {e}")
        return []


def fetch_all_active_sources_via_rss_parallel(sources: List[Dict[str, Any]], max_workers: int = 8) -> List[Dict[str, Any]]:
    """모든 활성 채널/재생목록의 RSS 피드를 멀티스레드 병렬(0.3초)로 초고속 수집합니다."""
    all_videos = []
    if not sources:
        return all_videos

    def _fetch_one(s: Dict[str, Any]) -> List[Dict[str, Any]]:
        src_name = str(s.get("name") or "")
        src_type = str(s.get("type") or "")
        src_url = str(s.get("url") or "")
        src_ch_id = str(s.get("channel_id") or "")
        src_max_v = int(s.get("max_videos", 3) or 3)
        guide_page_id = s.get("page_id")

        if src_type == "단일영상" or (src_url and extract_video_id(src_url) and not src_ch_id):
            vid = extract_video_id(src_url)
            if vid:
                return [{
                    "video_id": vid,
                    "title": src_name or f"YouTube Video ({vid})",
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "publish_date": get_kst_str("%Y-%m-%d"),
                    "publish_time_kst": get_kst_str("%Y-%m-%d %H:%M"),
                    "channel_name": src_name or "YouTube",
                    "guide_name": src_name,
                    "guide_page_id": guide_page_id,
                }]
            return []

        if not src_ch_id:
            return []

        is_pl = (src_type == "재생목록") or any(src_ch_id.startswith(p) for p in ["PL", "UU", "FL"])
        videos = fetch_videos_via_rss(src_ch_id, channel_name=src_name, max_videos=src_max_v, is_playlist=is_pl)
        for v in videos:
            v["guide_page_id"] = guide_page_id
            v["guide_name"] = src_name
        return videos

    worker_count = min(max_workers, len(sources) or 1)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(_fetch_one, s): s for s in sources}
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    all_videos.extend(res)
            except Exception as e:
                logger.debug(f"RSS 병렬 수집 중 예외: {e}")

    return all_videos


def _set_github_action_outputs(has_new: bool, new_count: int, new_ids: List[str]) -> None:
    """GitHub Actions 단계 간 통신을 위해 GITHUB_OUTPUT에 변수를 작성합니다."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        try:
            with open(output_file, "a", encoding="utf-8") as f:
                f.write(f"has_new={'true' if has_new else 'false'}\n")
                f.write(f"new_count={new_count}\n")
                f.write(f"new_ids={','.join(new_ids)}\n")
        except Exception as e:
            logger.debug(f"GITHUB_OUTPUT 기록 실패: {e}")


def detect_new_videos_pipeline(notion_client: Any = None) -> List[Dict[str, Any]]:
    """
    [2. 유튜브 목록에서 새로운 동영상이 있는지 파악 -> 신규 동영상 있는 채널에서 주소 확보]
    일반 인터넷망(초고속)에서 RSS 피드를 0.3초 만에 병렬 스캔하여
    이미 처리 완료되었거나 대기열에 담긴 영상을 제외한 '진짜 신규 동영상 목록'을 추출합니다.
    """
    print("=" * 80)
    print("📡 [신규 동영상 고속 탐지] 일반 인터넷망에서 YouTube RSS 피드 초고속 병렬 스캔")
    print("=" * 80)

    # 1. 활성 주소가이드 목록 로드 (캐시 우선, 없으면 노션 연동)
    sources = load_cached_guide_sources()
    if not sources and notion_client and YOUTUBE_GUIDE_DB_ID:
        sources = load_active_sources_from_notion(notion_client, YOUTUBE_GUIDE_DB_ID, use_cache_fallback=False)
        if sources:
            save_guide_sources_cache(sources)

    if not sources:
        print("⚠️ 활성화된 유튜브 채널/영상 주소가이드가 없습니다.")
        _set_github_action_outputs(has_new=False, new_count=0, new_ids=[])
        return []

    processed_ids = load_processed_videos()
    pending_queue = load_pending_queue()
    queued_ids = {str(item.get("video_id", "")) for item in pending_queue if item.get("video_id")}

    print(f"📖 활성 주소가이드: {len(sources)}개 채널/목록 | 기존 완료: {len(processed_ids)}개 | 현재 큐: {len(queued_ids)}개")

    # 2. RSS 피드 병렬 스캔 (0.3초)
    t0 = time.time()
    all_recent_videos = fetch_all_active_sources_via_rss_parallel(sources)
    scan_duration = time.time() - t0
    print(f"⚡ [RSS 스캔 완료] 총 {len(all_recent_videos)}개 최신 영상 탐색 ({scan_duration:.2f}초 소요)")

    # 3. 신규 동영상 필터링 (완료 캐시 및 큐에 없는 영상)
    new_videos = []
    seen_vids = set()

    for v in all_recent_videos:
        vid = v.get("video_id")
        if not vid or vid in seen_vids:
            continue
        seen_vids.add(vid)

        if vid in processed_ids:
            continue
        if vid in queued_ids:
            continue

        new_videos.append(v)

    save_detected_new_videos(new_videos)

    has_new = len(new_videos) > 0
    new_ids = [v["video_id"] for v in new_videos]

    print("\n" + "-" * 80)
    print(f"📊 [신규 동영상 탐지 결과 요약]")
    print(f"   • 신규 수집 대상 영상: {len(new_videos)}개")
    if new_videos:
        for idx, nv in enumerate(new_videos, 1):
            print(f"     {idx}. [{nv.get('guide_name')}] {nv.get('title')[:45]} (ID: {nv.get('video_id')}, KST: {nv.get('publish_time_kst')})")
        print("\n   ➔ Tailscale 스마트폰 Exit Node를 연결하여 자막을 수집합니다.")
    else:
        print("   ✨ 새로운 동영상이 없습니다 -> Tailscale 연결을 생략합니다 (0초 소모).")
    print("-" * 80)

    # GitHub Action Output 설정
    _set_github_action_outputs(has_new=has_new, new_count=len(new_videos), new_ids=new_ids)

    return new_videos


def fetch_recent_videos(channel_or_playlist_id: str, channel_name: str = "", max_videos: int = 5, is_playlist: bool = False) -> List[Dict[str, Any]]:
    """YouTube RSS 피드를 우선 사용하여 최신 영상 목록을 고속 수집합니다."""
    return fetch_videos_via_rss(channel_or_playlist_id, channel_name=channel_name, max_videos=max_videos, is_playlist=is_playlist)


def format_snippets_to_text(items: List[Any]) -> str:
    """타임스탬프([MM:SS]) 마커를 약 60초 간격으로 삽입하여 정제된 자막 전문을 생성합니다."""
    if not items:
        return ""
    formatted_chunks, current_words, last_marker_sec = [], [], -999.0

    for item in items:
        t = getattr(item, "text", "") if hasattr(item, "text") else (item.get("text", "") if isinstance(item, dict) else "")
        start_sec = getattr(item, "start", 0.0) if hasattr(item, "start") else (item.get("start", 0.0) if isinstance(item, dict) else 0.0)
        t = str(t).strip()
        if not t:
            continue

        if (float(start_sec) - last_marker_sec) >= 60.0:
            if current_words:
                formatted_chunks.append(" ".join(current_words))
                current_words = []
            minutes, seconds = int(float(start_sec) // 60), int(float(start_sec) % 60)
            formatted_chunks.append(f"\n[{minutes:02d}:{seconds:02d}]")
            last_marker_sec = float(start_sec)

        current_words.append(t)

    if current_words:
        formatted_chunks.append(" ".join(current_words))
    return " ".join(formatted_chunks).strip()


def extract_transcript_via_ytdlp(video_id: str) -> Tuple[Optional[str], str, Dict[str, Any]]:
    """
    [4. yt-dlp만 사용해서 자막 수집, 안되면 pass]
    타임아웃 10초 설정으로 신속하게 자막 및 메타데이터를 추출합니다.
    """
    if not yt_dlp:
        return None, "", {}

    url = f"https://www.youtube.com/watch?v={video_id}"
    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("TAILSCALE_PROXY")

    ydl_opts = {
        "skip_download": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": ["ko", "ko-KR", "ko-orig", "en", "en-US", "auto"],
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "nocheckcertificate": True,
        "socket_timeout": 10,
        "logger": _YtDlpSilentLogger(),
        "extractor_args": {"youtube": {"player_client": ["android", "ios", "mweb"], "skip": ["dash", "hls"]}}
    }
    if proxy_url:
        ydl_opts["proxy"] = proxy_url

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False) or {}
            subs = info.get("subtitles", {}) or {}
            auto_subs = info.get("automatic_captions", {}) or {}

            cand_subs, selected_lang = None, "unknown"
            for lang in ["ko", "ko-KR", "ko-orig", "en", "en-US"]:
                if lang in subs and subs[lang]:
                    cand_subs, selected_lang = subs[lang], f"manual-{lang}"
                    break
            if not cand_subs:
                for lang in ["ko", "ko-KR", "ko-orig", "en", "en-US"]:
                    if lang in auto_subs and auto_subs[lang]:
                        cand_subs, selected_lang = auto_subs[lang], f"auto-{lang}"
                        break

            if cand_subs:
                sub_url = next((f.get("url") for f in cand_subs if f.get("ext") == "json3"), None)
                if not sub_url:
                    sub_url = next((f.get("url") for f in cand_subs if f.get("ext") in ["srv3", "vtt", "ttml"]), cand_subs[0].get("url"))

                if sub_url:
                    res = requests.get(sub_url, timeout=10)
                    if res.status_code == 200:
                        if "json3" in sub_url or res.headers.get("content-type", "").startswith("application/json"):
                            try:
                                data = res.json()
                                snippets = [
                                    {"text": "".join([s.get("utf8", "") for s in ev.get("segs", []) if "utf8" in s]).strip(), "start": ev.get("tStartMs", 0) / 1000.0}
                                    for ev in data.get("events", [])
                                ]
                                formatted = format_snippets_to_text([s for s in snippets if s["text"] and s["text"] != "\n"])
                                if len(formatted) >= 50:
                                    return formatted, f"yt-dlp ({selected_lang})", info
                            except Exception:
                                pass

                        lines = [line.strip() for line in res.text.split("\n") if line.strip() and "-->" not in line and not line.startswith("WEBVTT") and not line.isdigit()]
                        formatted = " ".join(lines).strip()
                        if len(formatted) >= 50:
                            return formatted, f"yt-dlp-vtt ({selected_lang})", info

            return None, "", info
    except Exception as e:
        logger.debug(f"yt-dlp 자막 추출 pass ({video_id}): {e}")
        return None, "", {}


def extract_transcript_via_youtube_transcript_api(video_id: str) -> Tuple[Optional[str], str]:
    """youtube-transcript-api 기반 보조 자막 추출 (옵션)."""
    if not YouTubeTranscriptApi:
        return None, ""

    preferred_languages = ["ko", "ko-KR", "a.ko", "en", "a.en", "en-US"]
    try:
        yta = YouTubeTranscriptApi()
        t_list = yta.list(video_id) if hasattr(yta, "list") else (YouTubeTranscriptApi.list_transcripts(video_id) if hasattr(YouTubeTranscriptApi, "list_transcripts") else None)
        if t_list:
            target = None
            try:
                target = t_list.find_transcript(preferred_languages)
            except Exception:
                pass
            if not target and hasattr(t_list, "__iter__"):
                for t in t_list:
                    if getattr(t, "language_code", "") in preferred_languages:
                        target = t
                        break
            if target:
                items = getattr(target.fetch(), "snippets", target.fetch())
                text = format_snippets_to_text(items)
                lang_code = getattr(target, "language_code", "unknown")
                if len(text) >= 50:
                    return text, f"transcript-api ({lang_code})"
    except Exception as e:
        logger.debug(f"youtube-transcript-api 패스 ({video_id}): {e}")
    return None, ""


def clean_channel_name(raw_channel: str) -> str:
    """유튜브 채널명 정제 (@제거 및 공백 정리)."""
    return re.sub(r'^@', '', (raw_channel or "").strip()).strip() or "기타"


# ==============================================================================
# 6. 노션 적재 엔진 (정규화 포맷 및 400 에러 방지 스키마 가드)
# ==============================================================================
_KNOWN_YOUTUBE_DB_PROPS: Optional[Set[str]] = None


def _get_youtube_db_valid_properties(client: Any, db_id: str) -> Set[str]:
    """투자공부 DB의 실제 프로퍼티 이름을 캐싱하여 400 Bad Request 에러를 사전에 방지합니다."""
    global _KNOWN_YOUTUBE_DB_PROPS
    if _KNOWN_YOUTUBE_DB_PROPS is not None:
        return _KNOWN_YOUTUBE_DB_PROPS

    try:
        db_meta = client.databases.retrieve(database_id=db_id)
        _KNOWN_YOUTUBE_DB_PROPS = set((db_meta.get("properties") or {}).keys())
    except Exception:
        _KNOWN_YOUTUBE_DB_PROPS = {"Title", "URL", "Summary", "Key Takeaways", "Date", "선택"}

    return _KNOWN_YOUTUBE_DB_PROPS


def create_youtube_summary_notion_page(client: Any, db_id: str, analyzed: YouTubeAnalysisResult, video_meta: Dict[str, Any]) -> Optional[str]:
    """분석된 시황 및 자산 테이블을 [투자공부 by Youtube DB]에 적재합니다."""
    if not db_id:
        return None

    title = analyzed.summarized_title_for_notion or video_meta.get("title", "유튜브 시황 분석 리포트")
    url = video_meta.get("url", "")
    
    pub_date_str = video_meta.get("publish_date") or analyzed.publish_date or get_kst_str("%Y-%m-%d")
    pub_time_kst = video_meta.get("publish_time_kst", f"{pub_date_str} 00:00")
    now_kst = get_kst_str("%Y-%m-%d %H:%M")
    today_date_str = now_kst[:10]

    one_line = (getattr(analyzed, "one_line_summary", "") or "").strip()
    if not one_line and analyzed.overall_summary:
        one_line = analyzed.overall_summary.strip().split("\n")[0].replace("[매크로 & 시장방향]", "").strip()

    takeaways_3 = (analyzed.key_takeaways or [])[:3]
    takeaways_text = "\n".join([f"• {pt}" if not pt.strip().startswith("•") else pt.strip() for pt in takeaways_3])
    detailed_summary = (analyzed.overall_summary or "").strip()
    sentiment = (getattr(analyzed, "market_sentiment", "") or "중립").strip()
    sectors_str = ", ".join(getattr(analyzed, "leading_sectors", []) or []) or "전반/혼조"
    guide_title = (video_meta.get("guide_name") or "").strip()
    channel_name = (video_meta.get("channel_name") or "").strip()
    channel_tag = clean_channel_name(guide_title or channel_name)

    valid_props = _get_youtube_db_valid_properties(client, db_id)

    page_props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title}}]},
        "URL": {"url": url},
        "Summary": {"rich_text": [{"text": {"content": one_line[:2000]}}]},
        "Key Takeaways": {"rich_text": [{"text": {"content": takeaways_text[:2000]}}]},
    }
    if channel_tag and "선택" in valid_props:
        page_props["선택"] = {"select": {"name": channel_tag}}
    if pub_date_str and "Date" in valid_props:
        page_props["Date"] = {"date": {"start": pub_date_str}}
    if pub_date_str and "게시일" in valid_props:
        page_props["게시일"] = {"date": {"start": pub_date_str}}
    if "분석일시" in valid_props:
        page_props["분석일시"] = {"date": {"start": today_date_str}}

    blocks: List[Dict[str, Any]] = [
        {
            "object": "block", "type": "callout",
            "callout": {
                "icon": {"type": "emoji", "emoji": "📺"}, "color": "blue_background",
                "rich_text": [{"type": "text", "text": {"content": f"📺 출처: {guide_title or channel_name or 'YouTube'} | 영상: {video_meta.get('title', title)} ({url})\n📅 영상 게시일시: {pub_time_kst} (KST) | 🤖 AI 분석일시: {now_kst} (KST) | 🏷️ 시장 심리: {sentiment}\n\n📌 핵심 요약 (1줄):\n{one_line}"[:2000]}}]
            }
        }
    ]

    if detailed_summary:
        blocks.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "📋 종합 시황 & 심층 분석 (In-depth Analysis)"}}]}})
        for para in [p.strip() for p in detailed_summary.split("\n\n") if p.strip()] or [detailed_summary]:
            blocks.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": para}}]}})

    if takeaways_3:
        blocks.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "💡 핵심 시장 시사점 (Key Takeaways)"}}]}})
        for point in takeaways_3:
            blocks.append({"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": re.sub(r'^[•\-\d\.]+\s*', '', point).strip()}}]}})

    assets = analyzed.assets or []
    if assets:
        blocks.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "📊 언급 종목 & 매매 타점 분석"}}]}})
        table_rows = [{
            "object": "block", "type": "table_row",
            "table_row": {"cells": [[{"type": "text", "text": {"content": h}, "annotations": {"bold": True}}] for h in ["티커", "종목명", "투자의견", "핵심 분석 내용 & 타점"]]}
        }]
        for a in assets:
            table_rows.append({
                "object": "block", "type": "table_row",
                "table_row": {"cells": [
                    [{"type": "text", "text": {"content": (a.ticker or "-").strip()}, "annotations": {"code": True}}],
                    [{"type": "text", "text": {"content": (a.name or "-").strip()}}],
                    [{"type": "text", "text": {"content": (a.opinion or "중립").strip()}}],
                    [{"type": "text", "text": {"content": (a.context or "-").strip()}}],
                ]}
            })
        blocks.append({"object": "block", "type": "table", "table": {"table_width": 4, "has_column_header": True, "has_row_header": False, "children": table_rows}})

    blocks.extend([
        {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "🧭 주간 리포트 연계 퀀트 인덱스 (Macro Signals)"}}]}},
        {"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"동영상 기준 게시일시: {pub_time_kst} (KST)"}, "annotations": {"bold": True}}]}},
        {"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"시장 심리 / 방향성: {sentiment}"}, "annotations": {"bold": True}}]}},
        {"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"주요 주도 섹터: {sectors_str}"}}]}},
        {"object": "block", "type": "bulleted_list_item", "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"AI 분석 및 노션 동기화 일시: {now_kst} (KST)"}}]}},
    ])

    removable_props = ["분석일시", "게시일", "선택", "Date"]
    current_props = dict(page_props)

    while True:
        try:
            new_page = client.pages.create(parent={"database_id": db_id}, properties=current_props, children=blocks)
            page_id = new_page.get("id")
            logger.info(f"   ✅ [Notion 생성 성공] {title} (게시일: {pub_date_str}, 분석일: {today_date_str})")
            return page_id
        except Exception as e:
            err_msg = str(e)
            dropped = False
            for p_cand in removable_props:
                if p_cand in current_props and ("is not a property that exists" in err_msg or p_cand in err_msg):
                    current_props.pop(p_cand, None)
                    dropped = True
                    break
            if dropped:
                continue
            logger.error(f"   ❌ [Notion 생성 실패] {title}: {e}")
            return None


def create_unorganized_stock_items(
    client: Any,
    db_id: str,
    analyzed: YouTubeAnalysisResult,
    video_meta: Dict[str, Any],
    gateway: Optional[StockRegistryGateway] = None
) -> int:
    """추출된 개별 종목을 [미정리 종목 DB]에 적재하고 StockRegistryGateway로 상장주식 Master Relation을 바인딩합니다."""
    if not db_id:
        return 0

    pub_date_str = video_meta.get("publish_date") or analyzed.publish_date or get_kst_str("%Y-%m-%d")
    count = 0

    for asset in (analyzed.assets or []):
        raw_ticker = (asset.ticker or "").strip()
        name = (asset.name or "").strip()
        if not raw_ticker:
            continue

        clean_ticker = clean_ticker_key(raw_ticker)
        full_context = f"[{asset.opinion}] {asset.context}" if asset.opinion else (asset.context or "").strip()

        props: Dict[str, Any] = {
            "티커": {"title": [{"text": {"content": raw_ticker}}]},
            "종목명": {"rich_text": [{"text": {"content": name}}]},
            "핵심언급내용(Context - Korean)": {"rich_text": [{"text": {"content": full_context[:2000]}}]},
            "정리": {"checkbox": False},
        }
        if pub_date_str:
            props["게시일"] = {"date": {"start": pub_date_str}}

        if gateway:
            master_info = gateway.find_master_stock(clean_ticker, name)
            if master_info and master_info.get("id"):
                props["상장주식DB"] = {"relation": [{"id": master_info["id"]}]}

        try:
            client.pages.create(parent={"database_id": db_id}, properties=props)
            count += 1
            logger.info(f"      🥬 [미정리 종목 추가] {raw_ticker} ({name})")
        except Exception as e:
            if "게시일" in props and "is not a property that exists" in str(e):
                props.pop("게시일", None)
                try:
                    client.pages.create(parent={"database_id": db_id}, properties=props)
                    count += 1
                    continue
                except Exception:
                    pass
            logger.warning(f"      ⚠️ [미정리 종목 생성 실패] {raw_ticker}: {e}")

    return count


def prepare_video_payload_for_queue(v: Dict[str, Any], guide_page_id: Optional[str] = None, guide_name: Optional[str] = None, verbose: bool = True) -> Dict[str, Any]:
    """신규 동영상 메타데이터와 자막 전문을 수집 단계에서 추출하여 대기열(Queue) Payload로 패키징합니다."""
    vid = v.get("video_id") or extract_video_id(v.get("url", "")) or ""
    v["video_id"], v["guide_page_id"] = vid, guide_page_id
    if guide_name:
        v["guide_name"] = guide_name
    elif not v.get("guide_name"):
        v["guide_name"] = v.get("channel_name", "")

    if verbose:
        print(f"      ⏳ [스크립트 추출 중] yt-dlp 자막 조회 (ID: {vid})...")

    t_text, sub_src, r_meta = extract_transcript_via_ytdlp(vid)

    if r_meta:
        if r_meta.get("title") and (not v.get("title") or v.get("title").startswith("YouTube Video")):
            v["title"] = r_meta["title"]
        if r_meta.get("channel") and (not v.get("channel_name") or v.get("channel_name") == "YouTube"):
            v["channel_name"] = r_meta["channel"]
        if r_meta.get("description"):
            v["description"] = r_meta["description"]
        
        ud = r_meta.get("upload_date")
        ts = r_meta.get("timestamp") or r_meta.get("release_timestamp")
        if ud or ts:
            p_date, p_time = _parse_video_publish_date(raw_date=ud, timestamp=ts)
            v["publish_date"] = p_date
            v["publish_time_kst"] = p_time

    v["transcript"] = t_text or ""
    v["sub_source"] = sub_src or ""
    return v


# ==============================================================================
# 7. 단일 영상 통합 처리 파이프라인
# ==============================================================================
def process_single_video_item(
    v: Dict[str, Any],
    notion_client: Any,
    ai_service: AIService,
    gateway: StockRegistryGateway,
    processed_ids: Set[str],
    guide_page_id: Optional[str] = None,
    force: bool = False
) -> bool:
    """단일 유튜브 영상 메타데이터를 기반으로 AI 분석을 수행하고 노션에 적재합니다."""
    vid = v["video_id"]
    vtitle = v.get("title", "")
    pub_date = v.get("publish_time_kst") or v.get("publish_date", "")

    if vid in processed_ids and not force:
        print(f"   ⚡ [기존 완료] '{vtitle[:30]}...' -> 스킵")
        return False

    print(f"\n🎬 [영상 분석 시작] '{vtitle}' ({pub_date})")

    analyzed: Optional[YouTubeAnalysisResult] = None
    transcript = v.get("transcript") or ""
    sub_source = v.get("sub_source") or ""

    if not transcript:
        print(f"   ⏳ [스크립트 재조회] 대기열 내 자막 부재로 즉시 자막 추출 시도...")
        transcript, sub_source, _ = extract_transcript_via_ytdlp(vid)

    # 1. 자막(수동/자동생성) 전문 기반 Gemini AI 초고속 텍스트 분석 (1차)
    if transcript and len(transcript) >= 50:
        print(f"   📜 [스크립트 전달] 확보된 스크립트 {len(transcript):,}자 ({sub_source}) -> Gemini AI 1차 텍스트 분석")
        print(f"   🧠 [AI 추론 진행 중] 시장 심리 / 3대 시사점 / 언급 종목(티커) 구조화 분석...")
        analyzed = ai_service.analyze_youtube_transcript(transcript, v)
    else:
        # 2. 자막 부재 시 🎥 Gemini Multimodal(비디오/오디오) 3차 Fallback 분석 가동
        print(f"   ⚠️ [텍스트 자막 부재] 자막 추출 불가 ({sub_source or '무자막'}) -> 🎥 Gemini 멀티모달 Fallback 분석 가동...")
        print(f"   🧠 [AI 멀티모달 추론] YouTube 영상/오디오 및 메타데이터 종합 분석...")
        analyzed = ai_service.analyze_youtube_multimodal(vid, v)

    if not analyzed:
        print(f"   ❌ [분석 최종 실패] 영상 '{vtitle}'의 텍스트 및 멀티모달 분석에 모두 실패하여 건너뜁니다.")
        return False

    # 마스터 DB & 온톨로지 사전 기반 티커 보정
    for asset in (analyzed.assets or []):
        corrected_t, corrected_n = resolve_ticker_and_name(asset.ticker, asset.name)
        asset.ticker = corrected_t
        if corrected_n and not asset.name:
            asset.name = corrected_n

    # 1) [투자공부 by Youtube DB]에 시황 리포트 적재
    page_id = create_youtube_summary_notion_page(client=notion_client, db_id=YOUTUBE_DB_ID, analyzed=analyzed, video_meta=v)

    # [현대화 아키텍처] 로컬 SQLite B-Tree tbl_youtube_insights 듀얼 적재
    try:
        from core.local_db_manager import upsert_youtube_insight
        upsert_youtube_insight({
            "video_id": vid,
            "channel_id": v.get("channel_id", "UC_YOUTUBE"),
            "channel_name": v.get("channel_name", "YouTube"),
            "video_title": vtitle,
            "published_at": v.get("publish_date", get_kst_now().strftime("%Y-%m-%d")),
            "video_url": f"https://youtube.com/watch?v={vid}",
            "macro_sentiment": getattr(analyzed, "market_sentiment", "중립"),
            "risk_stance": "Risk-On" if getattr(analyzed, "market_sentiment", "") == "강세" else "Neutral",
            "key_themes": getattr(analyzed, "leading_sectors", []),
            "top_picks": [a.name for a in (analyzed.assets or []) if a.name],
            "summary_markdown": getattr(analyzed, "overall_summary", ""),
            "raw_transcript_len": len(v.get("transcript", "") or ""),
            "notion_page_id": page_id or "",
        })
    except Exception as e_sql:
        logger.warning(f"⚠️ SQLite tbl_youtube_insights 적재 예외: {e_sql}")

    # 2) [미정리 종목 DB]에 개별 종목 적재
    if UNORGANIZED_DB_ID:
        create_unorganized_stock_items(client=notion_client, db_id=UNORGANIZED_DB_ID, analyzed=analyzed, video_meta=v, gateway=gateway)

    # 3) [Youtube 주소가이드] 페이지 본문에 이력 누적 적재
    target_guide_page_id = guide_page_id or v.get("guide_page_id")
    if target_guide_page_id:
        append_video_history_to_guide_page(client=notion_client, page_id=target_guide_page_id, video_meta=v, analyzed=analyzed, study_page_id=page_id)
        if notion_client:
            update_guide_last_scanned(notion_client, target_guide_page_id)

    if page_id:
        processed_ids.add(vid)
        save_processed_videos(processed_ids)
        print(f"   💾 [완료 캐시 등록] Video ID '{vid}' 영속 캐시 저장 완료")
        time.sleep(1.0)
        return True

    return False


# ==============================================================================
# 8. 노션 DB 90일(3개월) 자동 아카이빙 및 유지보수 엔진
# ==============================================================================
def cleanup_old_youtube_insights(client: Any, db_id: str, retention_days: int = 90) -> int:
    """
    [투자공부 by Youtube DB]에서 기준일자가 retention_days(기본 90일)를
    초과한 과거 시황 분석 페이지를 자동으로 아카이브(휴지통 이동)합니다.
    """
    if not db_id or not client or retention_days <= 0:
        return 0

    now_kst = get_kst_now()
    cutoff_date = (now_kst - timedelta(days=retention_days)).strftime("%Y-%m-%d")
    print(f"\n🧹 [노션 3개월 아카이빙 점검] {retention_days}일(기준일 {cutoff_date} 이전) 초과 과거 시황 리포트 탐색 시작...")

    archived_count = 0
    try:
        pages = list(paginate_database(client, db_id, page_size=100, retry_delay=0.1))
        for p in pages:
            props = p.get("properties", {})
            page_id = p.get("id", "")
            raw_date = get_prop_value(props, ["게시일", "기준일자", "Date", "날짜", "업데이트 일자"]) or ""
            date_val = str(raw_date)[:10] if raw_date else ""
            title = get_prop_value(props, ["Title", "제목", "이름"]) or "과거 영상"

            if date_val and date_val < cutoff_date:
                try:
                    client.pages.update(page_id=page_id, archived=True)
                    archived_count += 1
                    logger.info(f"   🗑️ [3개월 초과 아카이빙] '{title[:30]}' (게시일: {date_val}) -> 노션 휴지통 이동")
                except Exception as e_del:
                    logger.warning(f"   ⚠️ 페이지 아카이브 실패 ({page_id}): {e_del}")

        if archived_count > 0:
            print(f"   ✅ [노션 정리 완료] {retention_days}일(기준일 {cutoff_date} 이전) 초과 과거 리포트 총 {archived_count}건 노션 아카이브(휴지통) 처리 완료")
        else:
            print(f"   ✨ {retention_days}일 초과 아카이빙 대상 과거 리포트가 없습니다 (최신 3개월 상태 유지 중).")
    except Exception as e:
        logger.warning(f"⚠️ 과거 유튜브 리포트 정리 중 오류: {e}")

    return archived_count


# ==============================================================================
# 9. 고속 3단계 파이프라인 (대기열 분석 -> 신규 탐지 -> yt-dlp 자막 수집)
# ==============================================================================
def process_pending_queue_pipeline(
    notion_client: Any,
    batch_limit: int = 2,
    sort_order: str = "asc",
    force: bool = False,
    retention_days: int = 90,
    skip_cleanup: bool = False,
) -> int:
    """
    [1. 스크립트 저장된 동영상 분석 먼저. 분석이 모두 끝나면 제미나이 API 종료]
    일반 초고속 인터넷망에서 대기열에 저장된 자막 스크립트를 Gemini AI로 분석하고 노션에 적재합니다.
    """
    print("=" * 80)
    print("🚀 [1단계: 대기열 AI 시황 분석 및 노션 적재] (일반 초고속 인터넷)")
    print("=" * 80)

    processed_ids = load_processed_videos()
    pending_queue = sort_pending_queue_by_publish_time(load_pending_queue(), order=sort_order)

    print(f"💾 기존 완료 캐시: {len(processed_ids)}개, 현재 대기열 미처리: {len(pending_queue)}개 (정렬: {sort_order})")

    if not pending_queue:
        print("\n✨ [대기열 비어있음] 분석 대기 중인 자막 스크립트가 없습니다. AI 단계를 즉시 종료합니다.")
        return 0

    batch_limit = max(batch_limit, 1)
    items_to_process = pending_queue[:batch_limit]
    remaining_queue = pending_queue[batch_limit:]

    print(f"\n🚀 [Gemini AI 구조화 분석 실행] 이번 목표: 최대 {len(items_to_process)}개 (대기열 총 {len(pending_queue)}개 중)")

    ai_service = AIService()
    _get_name_lookup_index()
    gateway = StockRegistryGateway(client=notion_client)

    total_new_processed = 0
    processed_in_this_run, failed_in_this_run = [], []

    for idx, v in enumerate(items_to_process, 1):
        vid, vtitle = v.get("video_id", ""), v.get("title", "")
        pub_time = v.get("publish_time_kst") or v.get("publish_date", "미상")
        print(f"\n🎯 [AI 분석 {idx}/{len(items_to_process)}] '{vtitle}' (게시일시: {pub_time}, ID: {vid})")

        if process_single_video_item(
            v=v,
            notion_client=notion_client,
            ai_service=ai_service,
            gateway=gateway,
            processed_ids=processed_ids,
            guide_page_id=v.get("guide_page_id"),
            force=force
        ):
            total_new_processed += 1
            processed_in_this_run.append(vid)
        else:
            failed_in_this_run.append(v)

    new_pending_queue = remaining_queue + failed_in_this_run
    new_pending_queue = sort_pending_queue_by_publish_time(new_pending_queue, order=sort_order)
    save_pending_queue(new_pending_queue)

    print(f"\n💾 [대기열 갱신] 이번 회차 완료: {len(processed_in_this_run)}개, 잔여 대기열: {len(new_pending_queue)}개")

    # 3. [유지보수] 90일(3개월) 초과 과거 시황 노션 자동 아카이빙
    if not skip_cleanup and YOUTUBE_DB_ID and notion_client:
        cleanup_old_youtube_insights(notion_client, YOUTUBE_DB_ID, retention_days=retention_days)

    return total_new_processed


def fetch_subtitles_for_targets(target_video_ids: Optional[List[str]] = None, sort_order: str = "asc") -> int:
    """
    [3. Tailscale 접속 -> 4. yt-dlp만 사용해서 자막 수집, 안되면 pass -> 5. 수집된 자막 대기열에 집어넣고 정렬]
    신규 동영상들에 대해서만 Tailscale 환경에서 yt-dlp로 다이렉트 자막 수집을 수행합니다.
    """
    print("=" * 80)
    print("📥 [자막 수집 단계: yt-dlp 전용] 스마트폰 Tailscale Exit Node 경유 자막 추출")
    print("=" * 80)

    detected_videos = load_detected_new_videos()
    detected_map = {v.get("video_id"): v for v in detected_videos if v.get("video_id")}

    if target_video_ids:
        target_vids = [vid.strip() for vid in target_video_ids if vid.strip()]
    else:
        target_vids = list(detected_map.keys())

    if not target_vids:
        print("ℹ️ 수집 대상 신규 영상 ID가 없습니다.")
        return 0

    pending_queue = load_pending_queue()
    existing_q_vids = {item.get("video_id") for item in pending_queue if item.get("video_id")}

    print(f"🎯 수집 대상: 총 {len(target_vids)}개 영상")
    success_count = 0

    for idx, vid in enumerate(target_vids, 1):
        if vid in existing_q_vids:
            print(f"   [{idx}/{len(target_vids)}] ⏳ (ID: {vid}) 이미 대기열에 존재함 -> 스킵")
            continue

        v_meta = detected_map.get(vid) or resolve_video_info(vid)
        if not v_meta:
            v_meta = {
                "video_id": vid,
                "title": f"YouTube Video ({vid})",
                "url": f"https://www.youtube.com/watch?v={vid}",
                "publish_date": get_kst_str("%Y-%m-%d"),
                "publish_time_kst": get_kst_str("%Y-%m-%d %H:%M"),
                "channel_name": "YouTube",
            }

        vtitle = v_meta.get("title", "")
        print(f"\n   [{idx}/{len(target_vids)}] ⏳ '{vtitle[:40]}' (ID: {vid}) yt-dlp 자막 추출 중...")

        # 4. yt-dlp만 사용해서 자막 수집, 안되면 pass
        t_text, sub_src, r_meta = extract_transcript_via_ytdlp(vid)
        if r_meta:
            if r_meta.get("title") and (not v_meta.get("title") or v_meta.get("title").startswith("YouTube Video")):
                v_meta["title"] = r_meta["title"]
            if r_meta.get("channel") and (not v_meta.get("channel_name") or v_meta.get("channel_name") == "YouTube"):
                v_meta["channel_name"] = r_meta["channel"]
            ud = r_meta.get("upload_date")
            ts = r_meta.get("timestamp") or r_meta.get("release_timestamp")
            if ud or ts:
                p_date, p_time = _parse_video_publish_date(raw_date=ud, timestamp=ts)
                v_meta["publish_date"] = p_date
                v_meta["publish_time_kst"] = p_time

        if t_text and len(t_text) >= 50:
            v_meta["transcript"] = t_text
            v_meta["sub_source"] = sub_src
            pending_queue.append(v_meta)
            existing_q_vids.add(vid)
            success_count += 1
            print(f"      📜 [자막 수집 성공] 총 {len(t_text):,}자 확보 완료! (출처: {sub_src}) -> 대기열 등록")
        else:
            print(f"      ⚠️ [자막 미확보] yt-dlp 자막 미제공 -> Pass (건너뜀)")

    # 5. 수집된 자막 대기열에 집어넣고 정렬
    pending_queue = sort_pending_queue_by_publish_time(pending_queue, order=sort_order)
    save_pending_queue(pending_queue)

    print("\n" + "-" * 80)
    print(f"📊 [자막 수집 및 대기열 갱신 완료]")
    print(f"   • 성공적으로 큐에 등록된 자막: {success_count}개")
    print(f"   • 총 대기열 미처리 스크립트: {len(pending_queue)}개")
    print("-" * 80)

    # 임시 신규 감지 파일 정리
    for dp in DETECTED_CACHE_PATHS:
        try:
            if dp.exists():
                dp.unlink()
        except Exception:
            pass

    return success_count


# ==============================================================================
# 10. CLI 인자 파서 및 메인 실행부
# ==============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="YouTube AI Insights Auto Sync & Notion Integration")
    parser.add_argument("-c", "--channels", nargs="+", help="지정할 유튜브 채널 목록 (@핸들, URL, 또는 ID)")
    parser.add_argument("-v", "--video", type=str, help="분석할 단일 유튜브 영상 URL 또는 Video ID")
    parser.add_argument("-m", "--max-videos", type=int, default=3, help="채널당 수집할 최근 영상 수")
    parser.add_argument("-b", "--batch-limit", type=int, default=2, help="1회 실행 시 AI로 분석할 최대 영상 수")
    parser.add_argument("-f", "--force", action="store_true", help="기존 처리 캐시를 무시하고 강제 재분석")
    parser.add_argument("--sort-order", type=str, choices=["asc", "desc"], default="asc", help="영상 게시일시 기준 분석 정렬 순서 (asc: 과거->최신, desc: 최신->과거, 기본값: asc)")
    parser.add_argument("--retention-days", type=int, default=90, help="노션 시황 리포트 보존 기간(일 단위, 기본: 90일/3개월)")
    parser.add_argument("--skip-cleanup", action="store_true", help="90일 초과 과거 리포트 자동 아카이빙(정리) 건너뛰기")
    parser.add_argument("--prefetch-sources", action="store_true", help="Step 1: 일반망에서 노션 주소가이드 목록을 선제 수집하여 로컬 캐시(.youtube_guide_sources.json)에 저장")
    parser.add_argument("--detect-new-videos", action="store_true", help="Step 2: 일반망에서 YouTube RSS 피드를 병렬 스캔하여 신규 동영상 탐지 & GitHub Action Output 설정")
    parser.add_argument("--fetch-target-videos", action="store_true", help="Step 3: 신규 감지된 영상에 대해서만 Tailscale Exit Node 경유 yt-dlp 자막 수집 및 대기열 등록")
    parser.add_argument("--target-video-ids", nargs="+", help="자막 수집할 특정 Video ID 목록 (지정 시 해당 ID만 수집)")
    parser.add_argument("--process-queue-only", action="store_true", help="Step 1: 대기열(Queue)에 있는 자막을 읽어 Gemini AI 분석 및 노션 적재 수행 후 종료")
    parser.add_argument("--process-only", action="store_true", help="기존 호환: 대기열 영상 Gemini AI 분석 및 노션 적재만 수행")
    parser.add_argument("--fetch-only", action="store_true", help="기존 호환: 유튜브 채널 스캔 및 자막 수집 수행")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    notion_client = build_notion_client(NOTION_TOKEN) if NOTION_TOKEN else None
    sort_order = getattr(args, "sort_order", "asc") or "asc"

    # 1. Step 1: 노션 주소가이드 사전 동기화 단독 모드
    if getattr(args, "prefetch_sources", False):
        success = prefetch_guide_sources_to_cache(notion_client, YOUTUBE_GUIDE_DB_ID)
        sys.exit(0 if success else 1)

    # 2. Step 2: 일반망 RSS 피드 기반 신규 동영상 고속 탐지 모드
    if getattr(args, "detect_new_videos", False):
        detect_new_videos_pipeline(notion_client=notion_client)
        return

    # 3. Step 3: yt-dlp 전용 자막 수집 모드 (Tailscale 구간)
    if getattr(args, "fetch_target_videos", False) or (args.fetch_only and not args.channels and not args.video):
        fetch_subtitles_for_targets(target_video_ids=args.target_video_ids, sort_order=sort_order)
        return

    # 4. Step 4: 대기열 AI 분석 및 노션 적재 모드 (일반망 구간)
    if getattr(args, "process_queue_only", False) or args.process_only:
        process_pending_queue_pipeline(
            notion_client=notion_client,
            batch_limit=args.batch_limit,
            sort_order=sort_order,
            force=args.force,
            retention_days=args.retention_days,
            skip_cleanup=args.skip_cleanup,
        )
        return

    # 5. CLI 단일 영상 직접 분석
    if args.video:
        print("=" * 80)
        print(f"🎯 [CLI 단일 영상 직접 처리] {args.video}...")
        print("=" * 80)
        v_meta = resolve_video_info(args.video)
        if not v_meta:
            print(f"❌ 유효한 유튜브 영상을 찾을 수 없습니다: {args.video}")
            sys.exit(1)

        payload = prepare_video_payload_for_queue(v_meta, verbose=True)
        vid = payload.get("video_id") or extract_video_id(args.video)

        if args.fetch_only:
            pending_queue = load_pending_queue()
            if vid not in [x.get("video_id") for x in pending_queue]:
                pending_queue.append(payload)
                pending_queue = sort_pending_queue_by_publish_time(pending_queue, order=sort_order)
                save_pending_queue(pending_queue)
                print(f"📥 [대기열 등록] 영상(ID: {vid}) 대기열 저장 완료!")
            else:
                print(f"⏳ [대기열 기등록] 이미 분석 대기열에 담겨 있음")
            return
        else:
            ai_service = AIService()
            _get_name_lookup_index()
            gateway = StockRegistryGateway(client=notion_client)
            processed_ids = load_processed_videos()
            process_single_video_item(v=payload, notion_client=notion_client, ai_service=ai_service, gateway=gateway, processed_ids=processed_ids, force=args.force)
            return

    # 6. 기본 실행 (하이브리드 파이프라인: 대기열 분석 -> 신규 탐지 -> 자막 수집)
    print("=" * 80)
    print(f"🚀 [Sync YouTube Insights] 통합 하이브리드 파이프라인 시작")
    print("=" * 80)

    # 1) 대기열에 남은 스크립트 먼저 분석
    process_pending_queue_pipeline(
        notion_client=notion_client,
        batch_limit=args.batch_limit,
        sort_order=sort_order,
        force=args.force,
        retention_days=args.retention_days,
        skip_cleanup=args.skip_cleanup,
    )

    # 2) 신규 동영상 탐지
    new_videos = detect_new_videos_pipeline(notion_client=notion_client)

    # 3) 신규 영상이 있다면 자막 수집
    if new_videos:
        fetch_subtitles_for_targets(sort_order=sort_order)

    print("\n" + "=" * 80)
    print("🎉 [동기화 완료] 유튜브 AI 시황 동기화 파이프라인이 정상 종료되었습니다.")
    print("=" * 80)


if __name__ == "__main__":
    main()
