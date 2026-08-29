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
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
YOUTUBE_DB_ID = get_db_id("YOUTUBE_DATABASE_ID", ["YOUTUBE_DB_ID"], required=False)
YOUTUBE_GUIDE_DB_ID = get_db_id("YOUTUBE_GUIDE_DATABASE_ID", ["YOUTUBE_GUIDE_DB_ID"], required=False)
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID"], required=False)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOCAL_DIR = Path(__file__).resolve().parent

CACHE_PATHS = [PROJECT_ROOT / ".processed_youtube_videos.json", LOCAL_DIR / ".processed_youtube_videos.json"]
QUEUE_PATHS = [PROJECT_ROOT / ".youtube_pending_queue.json", LOCAL_DIR / ".youtube_pending_queue.json"]


# ==============================================================================
# 2. 캐시 및 영속 대기열(FIFO Queue) 관리자
# ==============================================================================
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
def load_active_sources_from_notion(client: Any, guide_db_id: str) -> List[Dict[str, Any]]:
    """노션 [Youtube 주소가이드 DB]에서 활성화된 채널/재생목록/영상을 로드합니다."""
    if not guide_db_id:
        logger.warning("⚠️ YOUTUBE_GUIDE_DATABASE_ID가 설정되지 않았습니다.")
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
    except Exception as e:
        logger.error(f"❌ [Youtube 주소가이드 DB] 로드 중 오류: {e}")
    return sources


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
# 5. 유튜브 최신 영상 및 자막 추출 엔진 (yt-dlp 고속 메인 + YouTube RSS 2중 Fallback)
# ==============================================================================
def fetch_videos_via_rss(channel_or_playlist_id: str, channel_name: str = "", max_videos: int = 5, is_playlist: bool = False) -> List[Dict[str, Any]]:
    """YouTube 공식 RSS 피드를 통해 외부 의존성 없이 최신 영상 목록을 고속 수집합니다."""
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
        res = requests.get(rss_url, timeout=10)
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

        if videos:
            logger.info(f"   ✨ [RSS 피드 수집] [{channel_name or clean_id}] 최신 영상 {len(videos)}개 수집 성공")
        return videos
    except Exception as e:
        logger.debug(f"RSS 피드 수집 실패 ({clean_id}): {e}")
        return []


def fetch_recent_videos(channel_or_playlist_id: str, channel_name: str = "", max_videos: int = 5, is_playlist: bool = False) -> List[Dict[str, Any]]:
    """yt-dlp 및 YouTube RSS 피드 2중 엔진을 통해 최신 영상 목록을 고속으로 수집합니다."""
    if not channel_or_playlist_id:
        return []

    # 1. yt-dlp 패키지 미설치 시 즉시 RSS 피드 Fallback 실행
    if not yt_dlp:
        logger.warning(f"⚠️ [yt-dlp 미설치] [{channel_name}] YouTube RSS 피드로 대체 수집합니다.")
        return fetch_videos_via_rss(channel_or_playlist_id, channel_name=channel_name, max_videos=max_videos, is_playlist=is_playlist)

    is_pl = is_playlist or any(channel_or_playlist_id.startswith(p) for p in ["PL", "UU", "FL", "RD", "OLAK5uy_"])
    if is_pl:
        target_url = f"https://www.youtube.com/playlist?list={channel_or_playlist_id}"
    elif channel_or_playlist_id.startswith("http"):
        target_url = channel_or_playlist_id.split("?")[0]
        if not target_url.endswith("/videos") and not is_pl:
            target_url = f"{target_url.rstrip('/')}/videos"
    elif channel_or_playlist_id.startswith("@"):
        target_url = f"https://www.youtube.com/{channel_or_playlist_id}/videos"
    else:
        target_url = f"https://www.youtube.com/channel/{channel_or_playlist_id}/videos"

    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("TAILSCALE_PROXY")

    ydl_opts = {
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "nocheckcertificate": True,
        "extract_flat": "in_playlist",
        "playlistend": max(max_videos, 1),
        "logger": _YtDlpSilentLogger(),
        "extractor_args": {"youtube": {"player_client": ["android", "ios", "mweb"], "skip": ["dash", "hls"]}}
    }
    if proxy_url:
        ydl_opts["proxy"] = proxy_url


    videos, seen_vids = [], set()
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(target_url, download=False) or {}

            for e in (info.get("entries") or []):
                if not e:
                    continue
                raw_id = e.get("id") or ""
                vid = raw_id if len(raw_id) == 11 else (extract_video_id(e.get("url", "")) or raw_id)
                if not vid or vid in seen_vids:
                    continue

                seen_vids.add(vid)
                vtitle = (e.get("title") or "").strip()
                if not vtitle:
                    continue

                vurl = e.get("url") if (e.get("url") or "").startswith("http") else f"https://www.youtube.com/watch?v={vid}"
                ud = e.get("upload_date")
                ts = e.get("timestamp") or e.get("release_timestamp")
                pub_date, pub_time_kst = _parse_video_publish_date(raw_date=ud, timestamp=ts)

                videos.append({
                    "video_id": vid,
                    "title": vtitle,
                    "url": vurl,
                    "publish_date": pub_date,
                    "publish_time_kst": pub_time_kst,
                    "publish_dt": datetime.now(ZoneInfo("Asia/Seoul")),
                    "channel_name": e.get("channel") or e.get("uploader") or channel_name or channel_or_playlist_id,
                    "guide_name": channel_name or channel_or_playlist_id,
                })
                if len(videos) >= max_videos:
                    break

        if videos:
            logger.info(f"   ✨ [{channel_name}] 최신 영상 {len(videos)}개 수집 완료")
            return videos
    except Exception as e:
        logger.warning(f"   ⚠️ [{channel_name}] yt-dlp 영상 수집 중 예외: {e}")

    # 2. yt-dlp 수집 실패/0건 시 2차 RSS 피드 자동 Fallback
    rss_videos = fetch_videos_via_rss(channel_or_playlist_id, channel_name=channel_name, max_videos=max_videos, is_playlist=is_playlist)
    if rss_videos:
        return rss_videos

    return []


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
    """yt-dlp Innertube 모바일 클라이언트 스푸핑 기반 자막 및 메타데이터 추출."""
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
                    res = requests.get(sub_url, timeout=15)
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
        logger.debug(f"yt-dlp 자막 추출 생략 ({video_id}): {e}")
        return None, "", {}


def extract_transcript_via_youtube_transcript_api(video_id: str) -> Tuple[Optional[str], str]:
    """youtube-transcript-api 기반 보조 자막 추출."""
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
        logger.debug(f"youtube-transcript-api 시도 실패 ({video_id}): {e}")
    return None, ""


def clean_channel_name(raw_channel: str) -> str:
    """유튜브 채널명 정제 (@제거 및 공백 정리)."""
    return re.sub(r'^@', '', (raw_channel or "").strip()).strip() or "기타"


# ==============================================================================
# 6. 노션 적재 엔진 (정규화 포맷 및 블록 구성)
# ==============================================================================
def create_youtube_summary_notion_page(client: Any, db_id: str, analyzed: YouTubeAnalysisResult, video_meta: Dict[str, Any]) -> Optional[str]:
    """분석된 시황 및 자산 테이블을 [투자공부 by Youtube DB]에 적재합니다."""
    if not db_id:
        return None

    title = analyzed.summarized_title_for_notion or video_meta.get("title", "유튜브 시황 분석 리포트")
    url = video_meta.get("url", "")
    
    # 동영상 실제 게시일(기준일자) 우선 확보
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

    page_props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title}}]},
        "URL": {"url": url},
        "Summary": {"rich_text": [{"text": {"content": one_line[:2000]}}]},
        "Key Takeaways": {"rich_text": [{"text": {"content": takeaways_text[:2000]}}]},
    }
    if channel_tag:
        page_props["선택"] = {"select": {"name": channel_tag}}
    if pub_date_str:
        # Date 열: 동영상 실제 게시일 (주간 리포트 및 캘린더 조회 호환)
        page_props["Date"] = {"date": {"start": pub_date_str}}
        # 게시일 열: 동영상 실제 게시일 (명시적 분리 열)
        page_props["게시일"] = {"date": {"start": pub_date_str}}
    # 분석일시 열: AI 분석 및 노션 적재가 수행된 시각
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

    # 스키마 방어 생성: 미존재 프로퍼티 자동 감지 및 순차 제거 재시도
    removable_props = ["분석일시", "게시일", "선택"]
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
        print(f"      ⏳ [스크립트 추출 중] yt-dlp 및 모바일 클라이언트 자막 조회 (ID: {vid})...")

    t_text, sub_src, r_meta = extract_transcript_via_ytdlp(vid)
    if not t_text:
        if verbose:
            print(f"      🔄 [2차 시도] youtube-transcript-api 기반 보조 자막 조회 시도...")
        t_text, sub_src = extract_transcript_via_youtube_transcript_api(vid)

    if r_meta:
        if r_meta.get("title") and (not v.get("title") or v.get("title").startswith("YouTube Video")):
            v["title"] = r_meta["title"]
        if r_meta.get("channel") and (not v.get("channel_name") or v.get("channel_name") == "YouTube"):
            v["channel_name"] = r_meta["channel"]
        if r_meta.get("description"):
            v["description"] = r_meta["description"]
        
        # 실제 동영상 업로드일 메타데이터 갱신
        ud = r_meta.get("upload_date")
        ts = r_meta.get("timestamp") or r_meta.get("release_timestamp")
        if ud or ts:
            p_date, p_time = _parse_video_publish_date(raw_date=ud, timestamp=ts)
            v["publish_date"] = p_date
            v["publish_time_kst"] = p_time

    # 자막이 없거나 상세 설명란/게시일이 비어있는 경우 영상 메타데이터(설명란/챕터/업로드일) 자동 Fallback 확보
    if not v.get("description") or not v.get("publish_date"):
        full_info = resolve_video_info(vid)
        if full_info:
            if full_info.get("description") and not v.get("description"):
                v["description"] = full_info["description"]
            if full_info.get("title") and (not v.get("title") or v.get("title").startswith("YouTube Video")):
                v["title"] = full_info["title"]
            if full_info.get("channel_name") and (not v.get("channel_name") or v.get("channel_name") == "YouTube"):
                v["channel_name"] = full_info["channel_name"]
            if full_info.get("publish_date") and (not v.get("publish_date") or v.get("publish_date") == get_kst_str("%Y-%m-%d")):
                v["publish_date"] = full_info["publish_date"]
                v["publish_time_kst"] = full_info.get("publish_time_kst", f"{full_info['publish_date']} 00:00")

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
    rich_meta: Dict[str, Any] = {}
    transcript = v.get("transcript") or ""
    sub_source = v.get("sub_source") or ""

    if not transcript:
        print(f"   ⏳ [스크립트 재조회] 대기열 내 자막 부재로 즉시 자막 추출 시도...")
        transcript, sub_source, rich_meta = extract_transcript_via_ytdlp(vid)
        if not transcript:
            transcript, sub_source = extract_transcript_via_youtube_transcript_api(vid)

    if rich_meta:
        if rich_meta.get("title") and (not v.get("title") or v.get("title").startswith("YouTube Video")):
            v["title"] = rich_meta["title"]
        if rich_meta.get("channel") and (not v.get("channel_name") or v.get("channel_name") == "YouTube"):
            v["channel_name"] = rich_meta["channel"]
        if rich_meta.get("description"):
            v["description"] = rich_meta["description"]

    # 1. 자막(수동/자동생성) 전문 기반 Gemini AI 초고속 텍스트 분석 (1차)
    if transcript and len(transcript) >= 50:
        print(f"   📜 [스크립트 전달] 확보된 스크립트 {len(transcript):,}자 ({sub_source}) -> Gemini AI 1차 텍스트 분석")
        print(f"   🧠 [AI 추론 진행 중] 시장 심리 / 3대 시사점 / 언급 종목(티커) 구조화 분석...")
        analyzed = ai_service.analyze_youtube_transcript(transcript, v)
    else:
        # 2. 자막 부재 시 🎥 Gemini Multimodal(비디오/오디오) 3차 Fallback 분석 가동
        print(f"   ⚠️ [텍스트 자막 부재] 자막 추출 불가 ({sub_source or 'IP 차단/무자막'}) -> 🎥 Gemini 멀티모달 3차 Fallback 분석 가동...")
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

    if page_id:
        processed_ids.add(vid)
        save_processed_videos(processed_ids)
        print(f"   💾 [완료 캐시 등록] Video ID '{vid}' 영속 캐시 저장 완료")
        time.sleep(2.0)
        return True

    return False


# ==============================================================================
# 8. 노션 DB 90일(3개월) 자동 아카이빙 및 유지보수 엔진
# ==============================================================================
def cleanup_old_youtube_insights(client: Any, db_id: str, retention_days: int = 90) -> int:
    """
    [투자공부 by Youtube DB]에서 기준일자(게시일/Date)가 retention_days(기본 90일, 1개 분기)를
    초과한 과거 시황 분석 페이지를 자동으로 아카이브(휴지통 이동)하여 노션 DB를 최적화합니다.
    - 90일 초과 시황 데이터는 시의성을 상실하므로 노션 쿼리 속도 및 용량 최적화를 위해 정리
    - 처리 완료 캐시(.processed_youtube_videos.json)는 보존되어 과거 영상 재수집 방지
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
                    # 노션 공식 API: 페이지 아카이브(휴지통 이동)
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
# 9. CLI 인자 파서 및 메인 실행부
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
    parser.add_argument("--fetch-only", action="store_true", help="1단계: 유튜브 채널 스캔 및 자막/메타데이터 대기열(Queue) 수집만 수행 (Tailscale 연결 구간용)")
    parser.add_argument("--process-only", action="store_true", help="2단계: 대기열(Queue) 영상 Gemini AI 분석 및 노션 적재만 수행 (일반 인터넷 직통용)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mode_str = ""
    if args.fetch_only:
        mode_str = " [Phase 1: 자막 수집 모드 (Fetch-Only)]"
    elif args.process_only:
        mode_str = " [Phase 2: AI 분석 및 노션 적재 모드 (Process-Only)]"

    print("=" * 80)
    print(f"🚀 [Sync YouTube Insights]{mode_str} 'Youtube 주소가이드' 영속 대기열 기반 AI 시황 동기화 시작")
    print("=" * 80)

    notion_client = build_notion_client(NOTION_TOKEN) if NOTION_TOKEN else None

    processed_ids = load_processed_videos()
    sort_order = getattr(args, "sort_order", "asc") or "asc"
    pending_queue = sort_pending_queue_by_publish_time(load_pending_queue(), order=sort_order)
    print(f"💾 기존 완료 캐시: {len(processed_ids)}개, 현재 대기열 미처리: {len(pending_queue)}개 (정렬: {sort_order})")

    total_new_processed = 0

    # 1. CLI 단일 영상 직접 분석
    if args.video:
        print(f"\n🎯 [CLI 단일 영상 직접 처리] {args.video}...")
        if args.fetch_only:
            v_meta = resolve_video_info(args.video)
            if not v_meta:
                print(f"❌ 유효한 유튜브 영상을 찾을 수 없습니다: {args.video}")
                sys.exit(1)
            payload = prepare_video_payload_for_queue(v_meta, verbose=True)
            vid = payload.get("video_id") or extract_video_id(args.video)
            if vid not in [x.get("video_id") for x in pending_queue]:
                pending_queue.append(payload)
                pending_queue = sort_pending_queue_by_publish_time(pending_queue, order=sort_order)
                save_pending_queue(pending_queue)
                print(f"📥 [대기열 등록] 영상(ID: {vid}) 대기열 저장 완료!")
            else:
                print(f"⏳ [대기열 기등록] 이미 분석 대기열에 담겨 있음")
            print("\n✨ [Phase 1: Fetch-Only 완료] 1단계 단일 영상 자막 확보 및 대기열 저장이 완료되었습니다.")
            return
        else:
            v_meta = resolve_video_info(args.video)
            if not v_meta:
                print(f"❌ 유효한 유튜브 영상을 찾을 수 없습니다: {args.video}")
                sys.exit(1)

            ai_service = AIService()
            _get_name_lookup_index()
            gateway = StockRegistryGateway(client=notion_client)

            payload = prepare_video_payload_for_queue(v_meta, verbose=True)
            if process_single_video_item(v=payload, notion_client=notion_client, ai_service=ai_service, gateway=gateway, processed_ids=processed_ids, force=args.force):
                total_new_processed += 1

    # 2. 영속 대기열(Queue) 기반 수집 및 2-Phase 분할 배치 분석
    else:
        # [Phase 1: 채널 스캔 및 스크립트(자막) 수집] (--process-only 가 아닐 때 실행)
        if not args.process_only:
            print("\n📡 [1단계: 채널 스캔 및 스크립트(자막) 실시간 확보]")
            if args.channels:
                active_sources = [resolve_channel_target(ch_in, max_videos=args.max_videos) for ch_in in args.channels if ch_in.strip()]
            elif YOUTUBE_GUIDE_DB_ID and notion_client:
                print(f"📖 [노션 DB 모드] 'Youtube 주소가이드' DB 연동 확인 ({YOUTUBE_GUIDE_DB_ID[:8]}...)")
                active_sources = load_active_sources_from_notion(notion_client, YOUTUBE_GUIDE_DB_ID)
                if not active_sources:
                    print("   ℹ️ [Youtube 주소가이드 DB] '활성화' 체크된 채널/영상이 없습니다.")
            else:
                if not YOUTUBE_GUIDE_DB_ID:
                    logger.warning("⚠️ YOUTUBE_GUIDE_DATABASE_ID 미설정으로 CLI 인자(-c / -v)를 사용하세요.")
                active_sources = []

            queued_vids = {str(item.get("video_id", "")) for item in pending_queue if item.get("video_id")}
            newly_enqueued_count = 0
            skipped_processed_count = 0
            skipped_queued_count = 0
            skipped_no_transcript_count = 0

            for src in active_sources:
                src_name = str(src.get("name") or "")
                src_type = str(src.get("type") or "")
                src_url = str(src.get("url") or "")
                src_ch_id = str(src.get("channel_id") or "")
                src_page_id = str(src.get("page_id") or "")
                src_max_v = int(src.get("max_videos", args.max_videos) or args.max_videos)
                guide_page_id = src_page_id if src_page_id else None

                # 단일 영상
                if src_type == "단일영상" or (src_url and extract_video_id(src_url) and not src_ch_id):
                    vid = extract_video_id(src_url)
                    print(f"\n🎬 [단일영상] '{src_name}' 확인 중 (ID: {vid})...")
                    if not vid:
                        continue
                    if vid in processed_ids and not args.force:
                        print(f"   ⚡ [기존 완료] 이미 노션 DB에 분석 적재 완료된 영상 -> 스킵")
                        skipped_processed_count += 1
                    elif vid in queued_vids:
                        print(f"   ⏳ [대기열 기등록] 이미 분석 대기열에 담겨 있음 -> 스킵")
                        skipped_queued_count += 1
                    else:
                        v_meta = resolve_video_info(src_url)
                        if v_meta:
                            payload = prepare_video_payload_for_queue(v_meta, guide_page_id=guide_page_id, guide_name=src_name, verbose=True)
                            t_len = len(payload.get("transcript", "") or "")
                            sub_src = payload.get("sub_source", "yt-dlp")
                            if t_len >= 50:
                                pending_queue.append(payload)
                                queued_vids.add(vid)
                                newly_enqueued_count += 1
                                print(f"      📜 [스크립트 확보 성공] 총 {t_len:,}자 확보 완료! (출처: {sub_src})")
                                print(f"      📥 [대기열 등록] 분석 대기열(Queue)에 신규 등록 완료")
                            else:
                                payload["sub_source"] = "gemini-multimodal-fallback"
                                pending_queue.append(payload)
                                queued_vids.add(vid)
                                newly_enqueued_count += 1
                                print(f"      ⚠️ [자막 미확보 감지] 텍스트 자막 미제공/50자 미만 -> 🎥 Gemini 멀티모달 Fallback 대기열 등록 완료")

                    if guide_page_id and notion_client:
                        update_guide_last_scanned(notion_client, guide_page_id)

                # 채널 또는 재생목록
                else:
                    if not src_ch_id:
                        continue
                    is_pl = (src_type == "재생목록") or any(src_ch_id.startswith(p) for p in ["PL", "UU", "FL"])
                    print(f"\n{'📑' if is_pl else '📡'} [{'재생목록' if is_pl else '채널'}] '{src_name}' 스캔 중 (최대 {src_max_v}개)...")
                    fetched_videos = fetch_recent_videos(src_ch_id, channel_name=src_name, max_videos=src_max_v, is_playlist=is_pl)
                    if not fetched_videos:
                        print(f"   ℹ️ 최근 게시된 영상을 찾을 수 없습니다.")
                        continue

                    for v_idx, v in enumerate(fetched_videos, 1):
                        vid = v.get("video_id")
                        vtitle = v.get("title", "YouTube Video")
                        print(f"   🔍 [영상 탐색 {v_idx}/{len(fetched_videos)}] '{vtitle[:40]}' (ID: {vid})")

                        if not vid:
                            continue
                        if vid in processed_ids and not args.force:
                            print(f"      ⚡ [기존 완료] 이미 노션 DB에 분석 적재 완료된 영상 -> 스킵")
                            skipped_processed_count += 1
                        elif vid in queued_vids:
                            print(f"      ⏳ [대기열 기등록] 이미 분석 대기열에 담겨 있음 -> 스킵")
                            skipped_queued_count += 1
                        else:
                            payload = prepare_video_payload_for_queue(v, guide_page_id=guide_page_id, guide_name=src_name, verbose=True)
                            t_len = len(payload.get("transcript", "") or "")
                            sub_src = payload.get("sub_source", "yt-dlp")
                            if t_len >= 50:
                                pending_queue.append(payload)
                                queued_vids.add(vid)
                                newly_enqueued_count += 1
                                print(f"      📜 [스크립트 확보 성공] 총 {t_len:,}자 확보 완료! (출처: {sub_src})")
                                print(f"      📥 [대기열 등록] 분석 대기열(Queue)에 신규 등록 완료")
                            else:
                                payload["sub_source"] = "gemini-multimodal-fallback"
                                pending_queue.append(payload)
                                queued_vids.add(vid)
                                newly_enqueued_count += 1
                                print(f"      ⚠️ [자막 미확보 감지] 텍스트 자막 미제공/50자 미만 -> 🎥 Gemini 멀티모달 Fallback 대기열 등록 완료")

                    if guide_page_id and notion_client:
                        update_guide_last_scanned(notion_client, guide_page_id)

            # 대기열 게시일시(KST) 기준 글로벌 정렬 적용 (기본: 과거 -> 최신순)
            pending_queue = sort_pending_queue_by_publish_time(pending_queue, order=sort_order)
            save_pending_queue(pending_queue)
            
            sort_order_desc = "과거 ➡️ 최신 (오름차순, 시황 타임라인 순)" if sort_order == "asc" else "최신 ➡️ 과거 (내림차순, 최신성 우선)"
            print("\n" + "-" * 80)
            print(f"📊 [1단계 스크립트 확보 및 시계열 정렬 결과 요약]")
            print(f"   • 이번 스캔 신규 스크립트 확보: {newly_enqueued_count}개")
            print(f"   • 스크립트 미확보/제외: {skipped_no_transcript_count}개")
            print(f"   • 기존 완료/대기열 스킵: {skipped_processed_count + skipped_queued_count}개")
            print(f"   • 총 분석 대기 중인 스크립트(대기열): {len(pending_queue)}개")
            print(f"   • 📅 정렬 기준: 게시일시(KST) {sort_order_desc}")
            print("-" * 80)

            if args.fetch_only:
                print("\n✨ [Phase 1: Fetch-Only 완료] 자막 수집 및 대기열 저장이 완료되었습니다.")
                print("   ➔ Tailscale Exit Node 연결을 안전하게 종료하고, Phase 2에서 AI 분석을 진행합니다.")
                return

        # [Phase 2: 대기열 Dequeue & Gemini AI 분할 배치 분석] (--fetch-only 가 아닐 때 실행)
        batch_limit = max(args.batch_limit, 1)
        sort_order_desc = "과거 ➡️ 최신 (오름차순, 시황 타임라인 순)" if sort_order == "asc" else "최신 ➡️ 과거 (내림차순, 최신성 우선)"
        if not pending_queue:
            print("\n✨ [대기열 비어있음] 분석 대기 중인 신규 스크립트가 없습니다. 배치를 정상 종료합니다.")
        else:
            print(f"\n🚀 [2단계: Gemini AI 초고속 구조화 분석 실행] 이번 목표: 최대 {batch_limit}개 ({sort_order_desc}, 대기열 총 {len(pending_queue)}개 중)")

            ai_service = AIService()
            _get_name_lookup_index()
            gateway = StockRegistryGateway(client=notion_client)

            items_to_process = pending_queue[:batch_limit]
            remaining_queue = pending_queue[batch_limit:]
            processed_in_this_run, failed_in_this_run = [], []

            for idx, v in enumerate(items_to_process, 1):
                vid, vtitle = v.get("video_id", ""), v.get("title", "")
                pub_time = v.get("publish_time_kst") or v.get("publish_date", "미상")
                print(f"\n🎯 [AI 분석 {idx}/{len(items_to_process)}] '{vtitle}' (게시일시: {pub_time}, ID: {vid})")
                if process_single_video_item(v=v, notion_client=notion_client, ai_service=ai_service, gateway=gateway, processed_ids=processed_ids, guide_page_id=v.get("guide_page_id"), force=args.force):
                    total_new_processed += 1
                    processed_in_this_run.append(vid)
                else:
                    failed_in_this_run.append(v)

            new_pending_queue = remaining_queue + failed_in_this_run
            # 잔여 대기열도 시간순 정렬 상태 유지
            new_pending_queue = sort_pending_queue_by_publish_time(new_pending_queue, order=sort_order)
            save_pending_queue(new_pending_queue)
            print(f"\n💾 [대기열 갱신] 이번 회차 완료: {len(processed_in_this_run)}개, 다음 회차 잔여 스크립트 대기열: {len(new_pending_queue)}개")

    print("\n" + "=" * 80)
    print(f"🎉 [동기화 완료] 총 {total_new_processed}개의 신규 유튜브 영상 AI 분석 데이터가 노션에 적재되었습니다.")
    print("=" * 80)

    # 3. [유지보수] 90일(3개월) 초과 과거 시황 노션 자동 아카이빙(휴지통 이동)
    if not args.skip_cleanup and YOUTUBE_DB_ID and notion_client and not args.fetch_only:
        cleanup_old_youtube_insights(notion_client, YOUTUBE_DB_ID, retention_days=args.retention_days)


if __name__ == "__main__":
    main()
