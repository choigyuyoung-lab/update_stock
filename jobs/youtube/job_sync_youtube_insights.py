# -*- coding: utf-8 -*-
"""
sync_youtube_insights.py
========================
노션 [Youtube 투자가이드 DB](YOUTUBE_GUIDE_DATABASE_ID)에서 관리자가 등록 및 활성화(체크박스)한
유튜브 채널 및 추천 영상 목록을 자동으로 로드하여 무인 자동화(100% Non-interactive)로 동작합니다.

[핵심 엔진]
1. [Youtube 투자가이드 DB] 동적 로드: 활성화된 채널 RSS 및 개별 영상 자동 수집 & 최근 수집일 갱신
2. 자막 추출 & Gemini Pydantic AI 분석: API 쿼터 0 소모 RSS + youtube-transcript-api + Gemini Structured Outputs
3. 3단계 노션 동기화:
   - 1단계: [투자공부 by Youtube DB] - 전체 시황 리포트 및 자산 분석 테이블 적재
   - 2단계: [미정리 종목 DB] - 개별 종목별 행(Row) 생성 및 상장주식 Master Relation 연결
   - 3단계: [상장주식 Master DB 개별 페이지] - 하단 Callout 블록으로 유튜브 인사이트 자동 추가
"""

import os
import sys
import json
import re
import time
import logging
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

try:
    import yt_dlp
except ImportError:
    yt_dlp = None

try:
    from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
except ImportError:
    YouTubeTranscriptApi = None
    TranscriptsDisabled = Exception
    NoTranscriptFound = Exception

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    get_kst_str,
    paginate_database,
    get_prop_value,
    get_page_text,
)
from jobs.youtube.ai_service import AIService, YouTubeAnalysisResult
from services.stock_fallback_resolver import (
    resolve_ticker_and_name,
    _get_name_lookup_index,
)


def normalize_ticker(ticker: str) -> str:
    """티커 문자열에서 마켓 식별자(.T, .KS 등)를 보존하고 대문자 표준 포맷으로 정규화합니다."""
    if not ticker:
        return ""
    return str(ticker).strip().upper().replace(" ", "")


# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SyncYouTubeInsights")

# .env 환경변수 로드
load_dotenv()

# ==============================================================================
# 1. 환경 설정 및 상수 정의 (보안 조치: 하드코딩 제거 완료)
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
YOUTUBE_DB_ID = get_db_id("YOUTUBE_DATABASE_ID", ["YOUTUBE_DB_ID"], required=False)
YOUTUBE_GUIDE_DB_ID = get_db_id("YOUTUBE_GUIDE_DATABASE_ID", ["YOUTUBE_GUIDE_DB_ID"], required=False)
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID"], required=False)
MASTER_DB_ID = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID"], required=False)
INTEREST_DB_ID = get_db_id("DATABASE_ID", ["INTEREST_DATABASE_ID", "INTEREST_DB_ID"], required=False)

# 프로젝트 루트 및 jobs/youtube 디렉터리 양쪽 캐시 경로
REPO_ROOT_CACHE_FILE = Path(__file__).resolve().parent.parent.parent / ".processed_youtube_videos.json"
LOCAL_DIR_CACHE_FILE = Path(__file__).resolve().parent / ".processed_youtube_videos.json"

# 노션 DB 미연동 시 비상용 기본 채널 목록 (채널명 기반)
DEFAULT_CHANNELS: List[Dict[str, str]] = []


# ==============================================================================
# 2. 캐시 관리자 (중복 수집 및 AI 토큰 낭비 방지)
# ==============================================================================
def load_processed_videos() -> Set[str]:
    """이미 처리 완료된 유튜브 비디오 ID 집합을 로드합니다."""
    processed = set()
    for cache_path in [REPO_ROOT_CACHE_FILE, LOCAL_DIR_CACHE_FILE]:
        if cache_path.exists():
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    items = set(data if isinstance(data, list) else data.keys())
                    processed.update(items)
            except Exception as e:
                logger.warning(f"⚠️ 캐시 파일 읽기 실패 ({cache_path}): {e}")
    return processed


def save_processed_videos(processed_ids: Set[str]) -> None:
    """노션에 성공적으로 적재된 비디오 ID 목록을 프로젝트 루트 및 로컬 캐시에 저장합니다."""
    sorted_ids = sorted(list(processed_ids))
    for cache_path in [REPO_ROOT_CACHE_FILE, LOCAL_DIR_CACHE_FILE]:
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(sorted_ids, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ 캐시 파일 저장 실패 ({cache_path}): {e}")


# ==============================================================================
# 3. 지능형 채널 및 영상 식별 엔진 (Smart Channel / Video Resolver)
# ==============================================================================
def resolve_channel_info(channel_input: str) -> Optional[Dict[str, str]]:
    """
    유튜브 핸들(@아이디), 채널 URL, 또는 채널 ID를 채널 정보(ID 및 채널명)로 변환합니다.
    예:
      - @syukaworld -> {"channel_id": "UCJo6G1u0e_-wS-JQn3T-zEw", "name": "슈카월드"}
      - https://www.youtube.com/@3protv -> {"channel_id": "UChlv4GSd7OQl3js-jkLOnFA", "name": "삼프로TV"}
      - UChlv4GSd7OQl3js-jkLOnFA -> {"channel_id": "UChlv4GSd7OQl3js-jkLOnFA", "name": "삼프로TV"}
    """
    channel_input = channel_input.strip()
    if not channel_input:
        return None

    # 1. 이미 24자리 표준 채널 ID(UC...)인 경우
    if re.match(r'^UC[A-Za-z0-9_-]{22}$', channel_input):
        return {"channel_id": channel_input, "name": channel_input}

    # 2. URL 또는 핸들 형식 정규화
    if channel_input.startswith("@"):
        url = f"https://www.youtube.com/{channel_input}"
    elif channel_input.startswith("http://") or channel_input.startswith("https://"):
        url = channel_input
    else:
        url = f"https://www.youtube.com/@{channel_input}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            logger.warning(f"⚠️ 채널 페이지 접근 실패 (Status {res.status_code}): {url}")
            return None

        html = res.text

        # Channel ID 추출 패턴
        channel_id = None
        m_id = (
            re.search(r'<meta itemprop="channelId" content="([^"]+)"', html) or
            re.search(r'"channelId":"(UC[A-Za-z0-9_-]{22})"', html) or
            re.search(r'"externalId":"(UC[A-Za-z0-9_-]{22})"', html) or
            re.search(r'https://www.youtube.com/channel/(UC[A-Za-z0-9_-]{22})', html) or
            re.search(r'channel_id=(UC[A-Za-z0-9_-]{22})', html)
        )
        if m_id:
            channel_id = m_id.group(1)

        # 채널명 추출 패턴
        channel_name = channel_input
        m_name = (
            re.search(r'<meta property="og:title" content="([^"]+)"', html) or
            re.search(r'<title>([^<]+)</title>', html)
        )
        if m_name:
            raw_name = m_name.group(1).strip()
            channel_name = re.sub(r' - YouTube$', '', raw_name).strip()

        if channel_id:
            return {"channel_id": channel_id, "name": channel_name}
        else:
            logger.warning(f"⚠️ 채널 ID를 찾을 수 없습니다: {channel_input}")
            return None

    except Exception as e:
        logger.error(f"❌ 채널 식별 중 오류 발생 ({channel_input}): {e}")
        return None


def extract_video_id(video_input: str) -> Optional[str]:
    """유튜브 URL 또는 텍스트에서 11자리 Video ID를 추출합니다."""
    video_input = video_input.strip()
    if not video_input:
        return None

    # 이미 11자리 ID인 경우
    if re.match(r'^[A-Za-z0-9_-]{11}$', video_input):
        return video_input

    # URL 패턴 매칭 (watch?v=, youtu.be/, shorts/, embed/)
    patterns = [
        r'(?:v=|\/v\/|youtu\.be\/|\/embed\/|\/shorts\/)([A-Za-z0-9_-]{11})',
        r'[\?&]v=([A-Za-z0-9_-]{11})',
    ]
    for pattern in patterns:
        m = re.search(pattern, video_input)
        if m:
            return m.group(1)
    return None


def extract_playlist_id(playlist_input: str) -> Optional[str]:
    """유튜브 URL 또는 텍스트에서 Playlist ID를 추출합니다."""
    playlist_input = playlist_input.strip()
    if not playlist_input:
        return None

    # 이미 PL/UU/FL 등으로 시작하는 ID인 경우
    if re.match(r'^(?:PL|UU|FL|RD|OLAK5uy_)[A-Za-z0-9_-]+$', playlist_input):
        return playlist_input

    # URL에서 list= 파라미터 매칭
    m = re.search(r'[?&]list=([A-Za-z0-9_-]+)', playlist_input)
    if m:
        return m.group(1)
    return None


def resolve_playlist_info(playlist_input: str) -> Optional[Dict[str, str]]:
    """
    유튜브 재생목록 URL 또는 ID를 바탕으로 재생목록 ID와 제목을 수집합니다.
    """
    pid = extract_playlist_id(playlist_input)
    if not pid:
        return None

    url = f"https://www.youtube.com/playlist?list={pid}"
    title = f"YouTube Playlist ({pid})"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            m_title = (
                re.search(r'<meta property="og:title" content="([^"]+)"', res.text) or
                re.search(r'<title>([^<]+)</title>', res.text)
            )
            if m_title:
                raw_title = m_title.group(1).strip()
                title = re.sub(r' - YouTube$', '', raw_title).strip()
    except Exception as e:
        logger.debug(f"재생목록 메타데이터 조회 생략: {e}")

    return {
        "playlist_id": pid,
        "name": title,
    }


class _YtDlpSilentLogger:
    """yt-dlp 내부의 노이즈 및 봇 경고 로그를 정숙화하는 전용 로거"""
    def debug(self, msg: str) -> None: pass
    def warning(self, msg: str) -> None: pass
    def error(self, msg: str) -> None: pass


def resolve_video_info(video_input: str) -> Optional[Dict[str, Any]]:
    """
    유튜브 영상 URL 또는 Video ID를 바탕으로 영상 메타데이터(제목, 채널명, 설명란, 챕터)를 수집합니다.
    """
    vid = extract_video_id(video_input)
    if not vid:
        logger.warning(f"⚠️ 유효한 유튜브 Video ID를 찾을 수 없습니다: {video_input}")
        return None

    video_url = f"https://www.youtube.com/watch?v={vid}"
    title = f"YouTube Video ({vid})"
    channel_name = "YouTube"
    description = ""
    publish_date = ""

    # 1. yt-dlp 메타데이터 우선 조회 (모바일 클라이언트 스푸핑으로 봇 차단 방지)
    if yt_dlp is not None:
        try:
            ydl_opts = {
                "skip_download": True,
                "quiet": True,
                "no_warnings": True,
                "ignoreerrors": True,
                "nocheckcertificate": True,
                "logger": _YtDlpSilentLogger(),
                "extract_flat": "in_playlist",
                "extractor_args": {
                    "youtube": {
                        "player_client": ["android", "ios", "mweb"],
                        "skip": ["dash", "hls"]
                    }
                }
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                if info:
                    title = info.get("title") or title
                    channel_name = info.get("channel") or info.get("uploader") or channel_name
                    description = info.get("description") or ""
                    upload_date = info.get("upload_date")
                    if upload_date and len(upload_date) == 8:
                        publish_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
        except Exception as e:
            logger.debug(f"yt-dlp 메타데이터 조회 생략 ({vid}): {e}")

    # 2. oEmbed API를 통한 보조 조회
    if not description or title.startswith("YouTube Video"):
        try:
            oembed_url = f"https://www.youtube.com/oembed?url={video_url}&format=json"
            res = requests.get(oembed_url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                title = data.get("title", title)
                channel_name = data.get("author_name", channel_name)
        except Exception as e:
            logger.debug(f"oEmbed 메타데이터 조회 생략: {e}")

    now_kst = get_kst_str("%Y-%m-%d %H:%M")
    if not publish_date:
        publish_date = now_kst[:10]

    return {
        "video_id": vid,
        "title": title,
        "url": video_url,
        "publish_date": publish_date,
        "publish_time_kst": now_kst,
        "channel_name": channel_name,
        "description": description,
    }


# ==============================================================================
# 4. [Youtube 투자가이드] 노션 DB 연동 관리자
# ==============================================================================
def load_active_sources_from_notion(client: Any, guide_db_id: str) -> List[Dict[str, Any]]:
    """
    [Youtube 투자가이드] 노션 DB에서 '활성화' 체크된 모니터링 대상 채널 및 영상 목록을 로드합니다.
    Channel ID가 비어있는 경우 핸들/URL을 자동 분석하여 노션 DB에 업데이트(Self-Healing)합니다.
    """
    if not guide_db_id:
        logger.warning("⚠️ YOUTUBE_GUIDE_DATABASE_ID가 설정되지 않아 기본 채널 목록을 사용합니다.")
        return []

    sources: List[Dict[str, Any]] = []
    try:
        for page in paginate_database(client, guide_db_id, page_size=100):
            props = page.get("properties", {})
            page_id = page.get("id", "")

            # 1. 활성화 체크박스 확인
            is_active = props.get("활성화", {}).get("checkbox", False)
            if not is_active:
                continue

            # 2. 채널명 / 제목
            name = get_page_text(props, ["채널명 / 제목", "이름", "Title", "Name"]).strip()

            # 3. 구분 (채널(RSS), 단일영상, 재생목록)
            source_type = ""
            if props.get("구분", {}).get("select"):
                source_type = props.get("구분", {}).get("select", {}).get("name", "")
            if not source_type:
                source_type = "채널(RSS)"

            # 4. URL / 채널핸들
            url_or_handle = props.get("URL / 채널핸들", {}).get("url") or get_page_text(props, ["URL / 채널핸들", "URL", "Link"]).strip()

            # 5. Channel ID
            channel_id = get_page_text(props, ["Channel ID", "ChannelID", "ID"]).strip()

            # 6. 최대 수집 개수
            max_v = props.get("최대 수집 개수", {}).get("number")
            max_videos = int(max_v) if max_v is not None and max_v > 0 else 3

            # 7. 카테고리 태그
            categories = [
                opt.get("name", "")
                for opt in props.get("카테고리", {}).get("multi_select", [])
                if opt.get("name")
            ]

            # [Self-Healing] Channel ID 또는 Playlist ID가 누락된 경우 URL/핸들로 자동 판별 후 노션 업데이트
            is_playlist = (source_type == "재생목록") or (url_or_handle and extract_playlist_id(url_or_handle) is not None)
            if is_playlist:
                source_type = "재생목록"
                if not channel_id and url_or_handle:
                    logger.info(f"🔍 [{name or url_or_handle}] 재생목록 ID 누락 감지 -> 유튜브 URL 자동 해석 중...")
                    pl_info = resolve_playlist_info(url_or_handle)
                    if pl_info and pl_info.get("playlist_id"):
                        channel_id = pl_info["playlist_id"]
                        if not name or name == "Untitled":
                            name = pl_info.get("name", name)

                        update_payload: Dict[str, Any] = {
                            "Channel ID": {"rich_text": [{"text": {"content": channel_id}}]}
                        }
                        if name:
                            update_payload["채널명 / 제목"] = {"title": [{"text": {"content": name}}]}

                        try:
                            client.pages.update(page_id=page_id, properties=update_payload)
                            logger.info(f"   ✨ [Notion 자동 보정] Playlist ID 저장 완료: {channel_id}")
                        except Exception as ex:
                            logger.warning(f"   ⚠️ Notion Playlist ID 보정 저장 실패: {ex}")
            elif source_type == "채널(RSS)" and not channel_id and url_or_handle:
                logger.info(f"🔍 [{name or url_or_handle}] Channel ID 누락 감지 -> 유튜브 핸들/URL 자동 해석 중...")
                info = resolve_channel_info(url_or_handle)
                if info and info.get("channel_id"):
                    channel_id = info["channel_id"]
                    if not name or name == "Untitled":
                        name = info.get("name", name)

                    # 노션 DB 속성 자동 보정 저장
                    update_payload: Dict[str, Any] = {
                        "Channel ID": {"rich_text": [{"text": {"content": channel_id}}]}
                    }
                    if name:
                        update_payload["채널명 / 제목"] = {"title": [{"text": {"content": name}}]}

                    try:
                        client.pages.update(page_id=page_id, properties=update_payload)
                        logger.info(f"   ✨ [Notion 자동 보정] Channel ID 저장 완료: {channel_id}")
                    except Exception as ex:
                        logger.warning(f"   ⚠️ Notion Channel ID 보정 저장 실패: {ex}")

            sources.append({
                "page_id": page_id,
                "name": name or channel_id or url_or_handle,
                "type": source_type,
                "url": url_or_handle,
                "channel_id": channel_id,
                "max_videos": max_videos,
                "categories": categories,
            })

    except Exception as e:
        logger.error(f"❌ [Youtube 투자가이드 DB] 로드 중 오류 발생: {e}")

    return sources


def update_guide_last_scanned(client: Any, page_id: str) -> None:
    """노션 [Youtube 투자가이드 DB] 해당 항목의 '최근 수집일'을 오늘 날짜(KST)로 갱신합니다."""
    if not page_id or not client:
        return
    today_str = get_kst_str("%Y-%m-%d")
    try:
        client.pages.update(
            page_id=page_id,
            properties={"최근 수집일": {"date": {"start": today_str}}}
        )
        logger.debug(f"   📅 [최근 수집일 갱신] {today_str} (Page: {page_id})")
    except Exception as e:
        logger.debug(f"   ⚠️ 최근 수집일 갱신 실패 (Page: {page_id}): {e}")


def append_video_history_to_guide_page(
    client: Any,
    page_id: str,
    video_meta: Dict[str, Any],
    analyzed: Optional[YouTubeAnalysisResult] = None,
    study_page_id: Optional[str] = None
) -> None:
    """
    [Youtube 투자가이드]의 해당 채널/재생목록 페이지 본문에 수집된 동영상 이력을 표(Table) 형식으로 누적 기록하고
    [투자공부 by Youtube DB]의 상세 분석 리포트 페이지와 상호 연결합니다.
    """
    if not page_id or not client:
        return

    vid = video_meta.get("video_id", "")
    title = video_meta.get("title", "YouTube Video")
    url = video_meta.get("url", f"https://www.youtube.com/watch?v={vid}")
    pub_time_kst = video_meta.get("publish_time_kst") or video_meta.get("publish_date", get_kst_str("%Y-%m-%d"))
    summary_short = ""
    if analyzed and analyzed.overall_summary:
        first_line = analyzed.overall_summary.strip().split("\n")[0]
        summary_short = first_line[:100]

    # 언급 종목 요약 (최대 3개)
    assets_str = "-"
    if analyzed and analyzed.assets:
        tickers = [f"{a.ticker or a.name}" for a in analyzed.assets if a.ticker or a.name]
        if tickers:
            assets_str = ", ".join(tickers[:3])

    # 투자공부 리포트 노션 페이지 URL
    notion_report_url = f"https://notion.so/{study_page_id.replace('-', '')}" if study_page_id else ""

    # 셀 구성 (5개 열: 게시일시, 유튜브 영상, 투자공부 리포트, 언급 종목, AI 요약)
    cell_date = [{"type": "text", "text": {"content": pub_time_kst}, "annotations": {"code": True}}]
    cell_yt = [{"type": "text", "text": {"content": title[:35] + ("..." if len(title) > 35 else ""), "link": {"url": url}}, "annotations": {"bold": True}}]
    if notion_report_url:
        cell_report = [{"type": "text", "text": {"content": "📄 리포트 열기", "link": {"url": notion_report_url}}, "annotations": {"bold": True, "color": "blue"}}]
    else:
        cell_report = [{"type": "text", "text": {"content": "-"}}]
    cell_assets = [{"type": "text", "text": {"content": assets_str[:40]}}]
    cell_summary = [{"type": "text", "text": {"content": summary_short[:80]}}]

    data_row = {
        "object": "block",
        "type": "table_row",
        "table_row": {
            "cells": [cell_date, cell_yt, cell_report, cell_assets, cell_summary]
        }
    }

    try:
        # 기존 블록 목록 확인
        existing_blocks = client.blocks.children.list(block_id=page_id).get("results", [])
        existing_text = ""
        existing_table_id = None

        for b in existing_blocks:
            b_type = b.get("type", "")
            if b_type == "table":
                existing_table_id = b.get("id")
            rich_texts = b.get(b_type, {}).get("rich_text", [])
            for rt in rich_texts:
                existing_text += rt.get("plain_text", "") + " " + (rt.get("href") or "")

        # 이미 본문에 해당 영상 URL이나 Video ID가 적혀있다면 중복 추가 생략
        if vid and (vid in existing_text or url in existing_text):
            logger.debug(f"   ℹ️ [투자가이드 본문] 이미 기록된 영상입니다: {vid}")
            return

        # 1. 기존 테이블이 있는 경우: 해당 테이블에 행(Row)만 추가
        if existing_table_id:
            try:
                client.blocks.children.append(block_id=existing_table_id, children=[data_row])
                logger.info(f"   📊 [투자가이드 표 행 추가] '{title[:25]}' ({pub_time_kst})")
                return
            except Exception as ex:
                logger.warning(f"   ⚠️ 테이블 행 추가 실패 -> 신규 블록으로 재시도: {ex}")

        # 2. 기존 테이블이 없는 경우: 헤더와 함께 신규 표 생성
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

        new_blocks = [
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "🎬 수집된 동영상 이력 (KST 최신순)"}}]
                }
            },
            {
                "object": "block",
                "type": "table",
                "table": {
                    "table_width": 5,
                    "has_column_header": True,
                    "has_row_header": False,
                    "children": [header_row, data_row]
                }
            }
        ]

        client.blocks.children.append(block_id=page_id, children=new_blocks)
        logger.info(f"   📊 [투자가이드 표 신규 생성] '{title}' ({pub_time_kst}) 적재 완료")

    except Exception as e:
        logger.warning(f"   ⚠️ [투자가이드 본문 기록 실패] {e}")


# ==============================================================================
# 5. 유튜브 RSS 피드 파서 (API 쿼터 0 소모 & KST 시간 기준 정렬)
# ==============================================================================
def fetch_recent_videos_from_rss(channel_id: str, channel_name: str = "", max_videos: int = 5, is_playlist: bool = False) -> List[Dict[str, Any]]:
    """
    유튜브 채널 또는 재생목록 RSS 피드를 파싱하여 한국 시간(KST) 기준 최신 업로드 비디오 목록을 반환합니다.
    (YouTube Data API 쿼터를 전혀 소모하지 않음)
    """
    if is_playlist or channel_id.startswith("PL") or channel_id.startswith("UU") or channel_id.startswith("FL") or channel_id.startswith("RD") or channel_id.startswith("OLAK5uy_"):
        rss_url = f"https://www.youtube.com/feeds/videos.xml?playlist_id={channel_id}"
    else:
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    videos = []

    try:
        res = requests.get(rss_url, headers=headers, timeout=10)
        if res.status_code != 200:
            logger.warning(f"⚠️ [{channel_name}] RSS 피드 수신 실패 (Status {res.status_code})")
            return []

        root = ET.fromstring(res.content)
        ns = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}

        entries = root.findall("atom:entry", ns)
        for entry in entries:
            video_id_elem = entry.find("yt:videoId", ns)
            title_elem = entry.find("atom:title", ns)
            published_elem = entry.find("atom:published", ns)
            link_elem = entry.find("atom:link", ns)

            if video_id_elem is not None and title_elem is not None:
                vid = (video_id_elem.text or "").strip()
                vtitle = (title_elem.text or "").strip()
                vpub = (published_elem.text or "").strip() if published_elem is not None else ""
                vurl = link_elem.attrib.get("href", f"https://www.youtube.com/watch?v={vid}") if link_elem is not None else f"https://www.youtube.com/watch?v={vid}"

                pub_dt = None
                pub_date = ""
                pub_time_kst = ""
                if vpub:
                    try:
                        # UTC/ISO 날짜를 한국 표준시(KST, Asia/Seoul)로 변환
                        utc_dt = datetime.fromisoformat(vpub.replace("Z", "+00:00"))
                        pub_dt = utc_dt.astimezone(ZoneInfo("Asia/Seoul"))
                        pub_date = pub_dt.strftime("%Y-%m-%d")
                        pub_time_kst = pub_dt.strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        pub_date = vpub[:10]
                        pub_time_kst = vpub[:16]

                videos.append({
                    "video_id": vid,
                    "title": vtitle,
                    "url": vurl,
                    "publish_date": pub_date,
                    "publish_time_kst": pub_time_kst,
                    "publish_dt": pub_dt or datetime.min.replace(tzinfo=ZoneInfo("Asia/Seoul")),
                    "channel_name": channel_name or channel_id,
                })

        # 한국 시간(KST) 기준 최신 발행일시 역순(최신순) 엄격 정렬
        videos.sort(key=lambda x: x["publish_dt"], reverse=True)

        return videos[:max_videos]

    except Exception as e:
        logger.error(f"❌ [{channel_name}] RSS 파싱 에러: {e}")
        return []


def format_snippets_to_text(items: List[Any]) -> str:
    """타임스탬프([MM:SS]) 마커를 약 60초 간격으로 삽입하여 정제된 자막 전문을 생성합니다."""
    if not items:
        return ""
    formatted_chunks = []
    last_marker_sec = -999.0
    current_words = []

    for item in items:
        t = getattr(item, "text", "") if hasattr(item, "text") else (item.get("text", "") if isinstance(item, dict) else "")
        start_sec = getattr(item, "start", 0.0) if hasattr(item, "start") else (item.get("start", 0.0) if isinstance(item, dict) else 0.0)

        t = str(t).strip()
        if not t:
            continue

        # 60초 경과 시 타임스탬프 마커 삽입
        if (float(start_sec) - last_marker_sec) >= 60.0:
            if current_words:
                formatted_chunks.append(" ".join(current_words))
                current_words = []
            minutes = int(float(start_sec) // 60)
            seconds = int(float(start_sec) % 60)
            formatted_chunks.append(f"\n[{minutes:02d}:{seconds:02d}]")
            last_marker_sec = float(start_sec)

        current_words.append(t)

    if current_words:
        formatted_chunks.append(" ".join(current_words))

    return " ".join(formatted_chunks).strip()


def extract_transcript_via_ytdlp(video_id: str) -> Tuple[Optional[str], str, Dict[str, Any]]:
    """
    [1순위 (2번)] yt-dlp Android/iOS Innertube 클라이언트 스푸핑 기반 자막 및 메타데이터 추출.
    클라우드(AWS/Azure/GCP) IP 차단을 안전하게 우회하여 수동/자동생성 자막을 수집합니다.
    """
    if yt_dlp is None:
        logger.debug("yt-dlp 패키지가 설치되지 않았습니다.")
        return None, "", {}

    url = f"https://www.youtube.com/watch?v={video_id}"

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
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "ios", "mweb"],
                "skip": ["dash", "hls"]
            }
        }
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                return None, "", {}

            subtitles = info.get("subtitles", {}) or {}
            auto_subtitles = info.get("automatic_captions", {}) or {}

            candidate_subs = None
            selected_lang = "unknown"

            # 1. 수동 등록 자막 우선 탐색
            for lang in ["ko", "ko-KR", "ko-orig", "en", "en-US"]:
                if lang in subtitles and subtitles[lang]:
                    candidate_subs = subtitles[lang]
                    selected_lang = f"manual-{lang}"
                    break

            # 2. 자동 생성 자막 탐색
            if not candidate_subs:
                for lang in ["ko", "ko-KR", "ko-orig", "en", "en-US"]:
                    if lang in auto_subtitles and auto_subtitles[lang]:
                        candidate_subs = auto_subtitles[lang]
                        selected_lang = f"auto-{lang}"
                        break

            if candidate_subs:
                # JSON3 > SRV3 > VTT > TTML 순으로 URL 탐색
                sub_url = None
                for fmt in candidate_subs:
                    if fmt.get("ext") == "json3":
                        sub_url = fmt.get("url")
                        break
                if not sub_url:
                    for fmt in candidate_subs:
                        if fmt.get("ext") in ["srv3", "vtt", "ttml"]:
                            sub_url = fmt.get("url")
                            break
                if not sub_url and candidate_subs:
                    sub_url = candidate_subs[0].get("url")

                if sub_url:
                    res = requests.get(sub_url, timeout=15)
                    if res.status_code == 200:
                        # JSON3 파싱
                        if "json3" in sub_url or res.headers.get("content-type", "").startswith("application/json"):
                            try:
                                data = res.json()
                                events = data.get("events", [])
                                snippets = []
                                for ev in events:
                                    start_ms = ev.get("tStartMs", 0)
                                    segs = ev.get("segs", [])
                                    text = "".join([s.get("utf8", "") for s in segs if "utf8" in s]).strip()
                                    if text and text != "\n":
                                        snippets.append({"text": text, "start": start_ms / 1000.0})
                                formatted = format_snippets_to_text(snippets)
                                if len(formatted) >= 50:
                                    logger.info(f"   ✨ [yt-dlp 자막 추출 성공] Video ID '{video_id}' ({selected_lang}, {len(formatted):,}자)")
                                    return formatted, f"yt-dlp ({selected_lang})", info
                            except Exception:
                                pass

                        # VTT / 일반 텍스트 파싱 폴백
                        lines = [
                            line.strip() for line in res.text.split("\n")
                            if line.strip() and "-->" not in line and not line.startswith("WEBVTT") and not line.isdigit()
                        ]
                        formatted = " ".join(lines).strip()
                        if len(formatted) >= 50:
                            logger.info(f"   ✨ [yt-dlp VTT 자막 추출 성공] Video ID '{video_id}' ({selected_lang}, {len(formatted):,}자)")
                            return formatted, f"yt-dlp-vtt ({selected_lang})", info

            return None, "", info

    except Exception as e:
        logger.debug(f"yt-dlp 자막 추출 생략 ({video_id}): {e}")
        return None, "", {}


def extract_transcript_via_youtube_transcript_api(video_id: str) -> Optional[str]:
    """
    [4순위 (3번)] youtube-transcript-api 기반 보조 자막 추출 (v1.2+ 및 v0.x 하이브리드 지원).
    """
    if YouTubeTranscriptApi is None:
        return None

    preferred_languages = ["ko", "ko-KR", "a.ko", "en", "a.en", "en-US"]
    try:
        yta = YouTubeTranscriptApi()
        if hasattr(yta, "list"):
            transcript_list = yta.list(video_id)
            target_transcript = None
            try:
                target_transcript = transcript_list.find_transcript(preferred_languages)
            except Exception:
                pass
            if not target_transcript:
                for t in transcript_list:
                    if not getattr(t, "is_generated", False) and getattr(t, "language_code", "") in preferred_languages:
                        target_transcript = t
                        break
            if not target_transcript:
                for t in transcript_list:
                    if getattr(t, "is_generated", False) and getattr(t, "language_code", "") in preferred_languages:
                        target_transcript = t
                        break
            if not target_transcript:
                for t in transcript_list:
                    if getattr(t, "is_translatable", False):
                        try:
                            target_transcript = t.translate("ko")
                            break
                        except Exception:
                            pass
            if not target_transcript:
                for t in transcript_list:
                    target_transcript = t
                    break
            if target_transcript:
                items = target_transcript.fetch()
                items = getattr(items, "snippets", items)
                text = format_snippets_to_text(items)
                if len(text) >= 50:
                    return text
        elif hasattr(YouTubeTranscriptApi, "list_transcripts"):
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            target_transcript = transcript_list.find_transcript(preferred_languages)
            items = target_transcript.fetch()
            text = format_snippets_to_text(items)
            if len(text) >= 50:
                return text
    except Exception as e:
        logger.debug(f"youtube-transcript-api 시도 실패 ({video_id}): {e}")
    return None


def get_video_transcript(video_id: str) -> Optional[str]:
    """
    자막 추출 통합 인터페이스: yt-dlp 우선 시도 후 youtube-transcript-api로 폴백합니다.
    """
    t_ytdlp, _, _ = extract_transcript_via_ytdlp(video_id)
    if t_ytdlp:
        return t_ytdlp

    return extract_transcript_via_youtube_transcript_api(video_id)


def extract_concise_channel_tag(raw_channel: str) -> str:
    """
    유튜브 채널명/코너명에서 불필요한 접두사, 특수문자, 긴 수식어를 제거하여
    노션 [선택] 프로퍼티용 간결하고 일관된 태그(예: 핀플, 클로징벨라이브, 김장열 반도체, 삼프로TV, 슈카월드 등)로 정제합니다.
    """
    if not raw_channel:
        return "기타"
    tag = raw_channel.strip()
    tag = re.sub(r'^@', '', tag)
    tag = re.sub(r'[\(\[\{].*?[\)\]\}]', '', tag)  # 괄호 안 내용 제거
    # 자주 쓰이는 긴 채널 수식어 간소화
    tag = re.sub(r'\s*[-_/|].*$', '', tag)  # 구분자 이후 제거
    lower_tag = tag.lower()
    # 대표 채널 축약 룰
    if "삼프로" in tag or "3pro" in lower_tag:
        return "삼프로TV"
    if "슈카" in tag or "syuka" in lower_tag:
        return "슈카월드"
    if "월가월부" in tag:
        return "매경 월가월부"
    if "클로징" in tag or "closing" in lower_tag:
        return "클로징벨라이브"
    if "김장열" in tag:
        return "김장열 반도체"
    if "핀플" in tag or "finflow" in lower_tag:
        return "핀플"
    tag = re.sub(r'\s*(공식채널|Official|경제의신과함께|TV|티비)\b', '', tag, flags=re.IGNORECASE).strip()
    return tag or raw_channel[:15].strip()


# ==============================================================================
# 7. 노션 적재 엔진 (100% 정규화 포맷 준수)
# ==============================================================================
def create_youtube_summary_notion_page(
    client: Any,
    db_id: str,
    analyzed: YouTubeAnalysisResult,
    video_meta: Dict[str, Any]
) -> Optional[str]:
    """
    분석된 유튜브 시황 및 추천 자산 테이블을 [투자공부 by Youtube DB]에 정규화된 규격으로 적재합니다.
    (Summary 프로퍼티 1줄, Key Takeaways 3줄, 본문 블록 심층 상세 분석)
    """
    if not db_id:
        return None

    title = analyzed.summarized_title_for_notion or video_meta.get("title", "유튜브 시황 분석 리포트")
    url = video_meta.get("url", "")
    pub_date_str = analyzed.publish_date or video_meta.get("publish_date", get_kst_str("%Y-%m-%d"))
    pub_time_kst = video_meta.get("publish_time_kst", pub_date_str)
    
    # 1. 1줄 요약 (노션 DB 프로퍼티용)
    one_line = (getattr(analyzed, "one_line_summary", "") or "").strip()
    if not one_line and analyzed.overall_summary:
        one_line = analyzed.overall_summary.strip().split("\n")[0].replace("[매크로 & 시장방향]", "").strip()

    # 2. 3줄 핵심 시사점 (노션 DB 프로퍼티용)
    takeaways = analyzed.key_takeaways or []
    takeaways_3 = takeaways[:3]
    takeaways_text = "\n".join([f"• {pt}" if not pt.strip().startswith("•") else pt.strip() for pt in takeaways_3]) if takeaways_3 else ""

    # 3. 본문용 상세 심층 분석 (3단락)
    detailed_summary = (analyzed.overall_summary or "").strip()
    
    assets = analyzed.assets or []
    sentiment = (getattr(analyzed, "market_sentiment", "") or "중립").strip()
    sectors = getattr(analyzed, "leading_sectors", []) or []
    sectors_str = ", ".join(sectors) if sectors else "전반/혼조"

    channel_tag = extract_concise_channel_tag(video_meta.get("channel_name", "") or video_meta.get("guide_name", ""))

    page_props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title}}]},
        "URL": {"url": url},
        "Summary": {"rich_text": [{"text": {"content": one_line[:2000]}}]},
        "Key Takeaways": {"rich_text": [{"text": {"content": takeaways_text[:2000]}}]},
    }
    if channel_tag:
        page_props["선택"] = {"select": {"name": channel_tag}}
    if pub_date_str:
        page_props["Date"] = {"date": {"start": pub_date_str}}

    blocks: List[Dict[str, Any]] = []

    # 1. 콜아웃: 영상 출처 메타정보 & 1줄 핵심 요약
    callout_content = (
        f"📺 출처: {video_meta.get('channel_name', 'YouTube')} | 영상: {video_meta.get('title', title)} ({url})\n"
        f"📅 게시일시: {pub_time_kst} (KST) | 🏷️ 시장 심리: {sentiment}\n\n"
        f"📌 핵심 요약 (1줄):\n{one_line}"
    )
    blocks.append({
        "object": "block",
        "type": "callout",
        "callout": {
            "icon": {"type": "emoji", "emoji": "📺"},
            "color": "blue_background",
            "rich_text": [{"type": "text", "text": {"content": callout_content[:2000]}}]
        }
    })

    # 2. 종합 시황 & 심층 분석 H2 (본문 상세 분석 블록)
    if detailed_summary:
        blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"text": {"content": "📋 종합 시황 & 심층 분석 (In-depth Analysis)"}}]}
        })
        # 3개 섹션별로 문단 블록 생성
        summary_paragraphs = [p.strip() for p in detailed_summary.split("\n\n") if p.strip()]
        if not summary_paragraphs:
            summary_paragraphs = [detailed_summary]
        for para in summary_paragraphs:
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": para}}]}
            })

    # 3. 핵심 시장 시사점 H2 (3줄 표준 불릿)
    if takeaways_3:
        blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"text": {"content": "💡 핵심 시장 시사점 (Key Takeaways)"}}]}
        })
        for point in takeaways_3:
            clean_pt = re.sub(r'^[•\-\d\.]+\s*', '', point).strip()
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": clean_pt}}]}
            })

    # 4. 자산 및 티커 분석 테이블 (4열 표준)
    if assets:
        blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"text": {"content": "📊 언급 종목 & 매매 타점 분석"}}]}
        })

        table_rows = []
        table_rows.append({
            "object": "block",
            "type": "table_row",
            "table_row": {
                "cells": [
                    [{"type": "text", "text": {"content": "티커"}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": "종목명"}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": "투자의견"}, "annotations": {"bold": True}}],
                    [{"type": "text", "text": {"content": "핵심 분석 내용 & 타점"}, "annotations": {"bold": True}}],
                ]
            }
        })

        for asset in assets:
            t = (asset.ticker or "-").strip()
            n = (asset.name or "-").strip()
            op = (asset.opinion or "중립").strip()
            ctx = (asset.context or "-").strip()

            table_rows.append({
                "object": "block",
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "text": {"content": t}, "annotations": {"code": True}}],
                        [{"type": "text", "text": {"content": n}}],
                        [{"type": "text", "text": {"content": op}}],
                        [{"type": "text", "text": {"content": ctx}}],
                    ]
                }
            })

        blocks.append({
            "object": "block",
            "type": "table",
            "table": {
                "table_width": 4,
                "has_column_header": True,
                "has_row_header": False,
                "children": table_rows
            }
        })

    # 5. 주간 리포트 연계 퀀트 인덱스 (Macro Signals)
    now_kst = get_kst_str("%Y-%m-%d %H:%M")
    blocks.append({
        "object": "block",
        "type": "heading_2",
        "heading_2": {"rich_text": [{"text": {"content": "🧭 주간 리포트 연계 퀀트 인덱스 (Macro Signals)"}}]}
    })
    blocks.append({
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"시장 심리 / 방향성: {sentiment}"}, "annotations": {"bold": True}}]}
    })
    blocks.append({
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"주요 주도 섹터: {sectors_str}"}}]}
    })
    blocks.append({
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": f"분석 및 동기화 일시: {now_kst} (KST)"}}]}
    })

    try:
        new_page = client.pages.create(
            parent={"database_id": db_id},
            properties=page_props,
            children=blocks
        )
        page_id = new_page.get("id")
        page_url = new_page.get("url", f"https://notion.so/{page_id.replace('-', '')}")
        logger.info(f"   ✅ [Notion 생성 성공] {title} (URL: {page_url})")
        return page_id
    except Exception as e:
        # 만약 '선택' 속성 이름 불일치로 실패 시, 안전하게 대체 시도
        if "선택" in page_props and "is not a property that exists" in str(e):
            logger.warning("   ⚠️ '선택' 속성 미존재 감지 -> 속성 제외 후 재시도")
            page_props.pop("선택", None)
            try:
                new_page = client.pages.create(parent={"database_id": db_id}, properties=page_props, children=blocks)
                return new_page.get("id")
            except Exception as e2:
                logger.error(f"   ❌ [Notion 생성 재시도 실패] {title}: {e2}")
                return None
        logger.error(f"   ❌ [Notion 생성 실패] {title}: {e}")
        return None


def create_unorganized_stock_items(
    client: Any,
    db_id: str,
    analyzed: YouTubeAnalysisResult,
    video_meta: Dict[str, Any],
    master_map: Dict[str, str]
) -> int:
    """
    영상에서 추출된 개별 종목/자산을 [미정리 종목 DB]에 적재하고
    상장주식 Master DB Relation을 자동 바인딩합니다.
    """
    if not db_id:
        return 0

    assets = analyzed.assets or []
    pub_date_str = analyzed.publish_date or video_meta.get("publish_date", get_kst_str("%Y-%m-%d"))
    count = 0

    for asset in assets:
        raw_ticker = (asset.ticker or "").strip()
        name = (asset.name or "").strip()
        context = (asset.context or "").strip()
        opinion = (asset.opinion or "").strip()

        if not raw_ticker:
            continue

        clean_ticker = normalize_ticker(raw_ticker)
        full_context = f"[{opinion}] {context}" if opinion else context

        props: Dict[str, Any] = {
            "티커": {"title": [{"text": {"content": raw_ticker}}]},
            "종목명": {"rich_text": [{"text": {"content": name}}]},
            "핵심언급내용(Context - Korean)": {"rich_text": [{"text": {"content": full_context[:2000]}}]},
            "정리": {"checkbox": False},
        }

        if pub_date_str:
            props["게시일"] = {"date": {"start": pub_date_str}}

        # 상장주식 Master DB Relation 자동 바인딩
        if clean_ticker in master_map:
            props["상장주식DB"] = {"relation": [{"id": master_map[clean_ticker]}]}

        try:
            client.pages.create(parent={"database_id": db_id}, properties=props)
            count += 1
            logger.info(f"      🥬 [미정리 종목 추가] {raw_ticker} ({name}) -> 미정리 DB 적재 완료")
        except Exception as e:
            logger.warning(f"      ⚠️ [미정리 종목 생성 실패] {raw_ticker}: {e}")

    return count


# ==============================================================================
# 8. 단일 영상 통합 처리 파이프라인
# ==============================================================================
def process_single_video_item(
    v: Dict[str, Any],
    notion_client: Any,
    ai_service: AIService,
    master_map: Dict[str, str],
    processed_ids: Set[str],
    guide_page_id: Optional[str] = None,
    force: bool = False
) -> bool:
    """
    단일 유튜브 영상 메타데이터를 기반으로
    다계층 파이프라인(2번 yt-dlp ➔ 4번 설명란/챕터 ➔ 1번 Gemini 비디오 URL ➔ 3번 레거시 자막)을 실행하고
    노션에 최종 적재가 성공했을 때만 완료 캐시에 등록합니다.
    """
    vid = v["video_id"]
    vtitle = v.get("title", "")
    pub_date = v.get("publish_time_kst") or v.get("publish_date", "")

    if vid in processed_ids and not force:
        print(f"   ⚡ [이미 처리됨] '{vtitle[:30]}...' -> 스킵")
        return False

    print(f"\n🎬 [영상 분석 시작] '{vtitle}' ({pub_date})")

    analyzed: Optional[YouTubeAnalysisResult] = None
    rich_meta: Dict[str, Any] = {}

    # [1단계 / 사용자 제안 2번] yt-dlp 기반 자막 추출 (가장 빠름 & 최소 토큰)
    print("   ⏳ [1단계: yt-dlp] 자막 트랜스크립트 추출 중...")
    transcript, sub_source, rich_meta = extract_transcript_via_ytdlp(vid)
    if rich_meta:
        if rich_meta.get("title") and (not v.get("title") or v.get("title").startswith("YouTube Video")):
            v["title"] = rich_meta["title"]
        if rich_meta.get("channel") and (not v.get("channel_name") or v.get("channel_name") == "YouTube"):
            v["channel_name"] = rich_meta["channel"]
        if rich_meta.get("description"):
            v["description"] = rich_meta["description"]

    if transcript and len(transcript) >= 50:
        print(f"   🧠 [yt-dlp 자막 추출 성공] ({len(transcript):,} 글자, 소스: {sub_source}). Gemini AI 구조화 분석 중...")
        analyzed = ai_service.analyze_youtube_transcript(transcript, v)

    # [2단계 / 사용자 제안 4번] 영상 상세 설명란(Description) & 챕터 타임라인 기반 분석
    if not analyzed:
        desc = v.get("description") or (rich_meta.get("description") if rich_meta else "")
        if desc and len(desc.strip()) >= 50:
            print(f"   📋 [2단계: 설명란/챕터] 자막 미제공 -> 영상 상세 설명란({len(desc):,}자) 기반 AI 분석 진행 중...")
            formatted_desc = (
                f"[동영상 메타데이터 & 상세 챕터 타임라인 (자막 대체 폴백)]\n"
                f"- 영상 제목: {v.get('title')}\n"
                f"- 채널명: {v.get('channel_name')}\n"
                f"- 게시일자: {v.get('publish_date')}\n\n"
                f"[상세 설명 및 타임라인 전문]\n{desc[:30000]}"
            )
            analyzed = ai_service.analyze_youtube_transcript(formatted_desc, v)

    # [3단계 / 사용자 제안 1번] Gemini 네이티브 YouTube URL 직접 멀티모달 분석 (음성/화면 차트 시청)
    if not analyzed:
        video_url = v.get("url") or f"https://www.youtube.com/watch?v={vid}"
        print(f"   👑 [3단계: Gemini Video URL] 네이티브 멀티모달 분석 시도 중 ({video_url})...")
        analyzed = ai_service.analyze_youtube_video(video_url, v)

    # [4단계 / 사용자 제안 3번] youtube-transcript-api 레거시 보조 시도
    if not analyzed:
        print("   🔍 [4단계: youtube-transcript-api] 레거시 자막 추출 보조 시도 중...")
        legacy_sub = extract_transcript_via_youtube_transcript_api(vid)
        if legacy_sub and len(legacy_sub) >= 50:
            print(f"   🧠 [레거시 자막 취득 성공] ({len(legacy_sub):,} 글자). Gemini AI 분석 중...")
            analyzed = ai_service.analyze_youtube_transcript(legacy_sub, v)

    if not analyzed:
        print(f"   ❌ [분석 실패] 영상 '{vtitle}'에 대한 AI 분석에 최종 실패하였습니다. (재시도를 위해 캐시에 등록하지 않습니다)")
        return False

    # 🛡️ 공식 마스터 DB & 온톨로지 사전 기반 티커 2차 정밀 교차 검증 및 보정 (하드코딩 0%)
    for asset in (analyzed.assets or []):
        corrected_t, corrected_n = resolve_ticker_and_name(asset.ticker, asset.name)
        asset.ticker = corrected_t
        if corrected_n and not asset.name:
            asset.name = corrected_n

    # 1) 📹 [투자공부 by Youtube DB]에 전체 시황 적재
    print(f"   📥 [1/3] 투자공부 DB 저장 중: '{analyzed.summarized_title_for_notion}'...")
    page_id = create_youtube_summary_notion_page(
        client=notion_client,
        db_id=YOUTUBE_DB_ID,
        analyzed=analyzed,
        video_meta=v
    )

    # 2) 🥬 [미정리 종목 DB]에 개별 종목 적재
    if UNORGANIZED_DB_ID:
        print(f"   📥 [2/3] 미정리 종목 DB에 개별 자산({len(analyzed.assets)}개) 적재 중...")
        create_unorganized_stock_items(
            client=notion_client,
            db_id=UNORGANIZED_DB_ID,
            analyzed=analyzed,
            video_meta=v,
            master_map=master_map
        )

    # 3) 📑 [Youtube 투자가이드 DB 해당 채널/재생목록 페이지 본문] 수집된 동영상 이력 누적 적재
    if guide_page_id:
        print("   📥 [3/3] Youtube 투자가이드 페이지 본문에 동영상 수집 이력 적재 중...")
        append_video_history_to_guide_page(
            client=notion_client,
            page_id=guide_page_id,
            video_meta=v,
            analyzed=analyzed,
            study_page_id=page_id
        )

    # 💾 노션 생성 성공 시에만 캐시에 등록 (영구 패싱 방지)
    if page_id:
        processed_ids.add(vid)
        save_processed_videos(processed_ids)
        print(f"   💾 [완료 캐시 등록] Video ID '{vid}' 저장 완료")
        time.sleep(2.0)
        return True

    return False


# ==============================================================================
# 9. CLI 인자 파서
# ==============================================================================
def parse_args() -> argparse.Namespace:
    """CLI 인자를 정의하고 파싱합니다."""
    parser = argparse.ArgumentParser(
        description="YouTube AI Insights Auto Sync & Notion Integration (Youtube 투자가이드 연동)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
실행 예시:
  # 1. 노션 [Youtube 투자가이드 DB]에 활성화된 채널/영상 전체 자동 스캔 (기본값)
  python sync_youtube_insights.py

  # 2. 특정 채널만 임시 지정하여 스캔
  python sync_youtube_insights.py -c @syukaworld @3protv -m 2

  # 3. 특정 단일 영상 1개 즉시 분석
  python sync_youtube_insights.py -v "https://www.youtube.com/watch?v=XXXXX"

  # 4. 기존 처리 캐시를 무시하고 강제 재분석
  python sync_youtube_insights.py -f
        """
    )
    parser.add_argument(
        "-c", "--channels",
        nargs="+",
        help="임시 지정할 유튜브 채널 목록 (@핸들, 채널 URL, 또는 채널 ID)"
    )
    parser.add_argument(
        "-v", "--video",
        type=str,
        help="직접 분석할 단일 유튜브 영상 URL 또는 Video ID"
    )
    parser.add_argument(
        "-m", "--max-videos",
        type=int,
        default=3,
        help="채널당 수집할 최근 영상 수 (기본값: 3, 노션 DB 설정값이 우선)"
    )
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        help="이미 처리된 영상 캐시를 무시하고 강제 재분석"
    )
    return parser.parse_args()


# ==============================================================================
# 10. 메인 파이프라인 실행 엔진
# ==============================================================================
def main() -> None:
    args = parse_args()

    print("=" * 80)
    print("🚀 [Sync YouTube Insights] 'Youtube 투자가이드' 노션 DB 기반 AI 시황 동기화 시작")
    print("=" * 80)

    notion_client = build_notion_client(NOTION_TOKEN)
    ai_service = AIService()

    # 0. 공식 SQLite 마스터 DB 및 온톨로지 사전 색인 프리로딩 (하드코딩 0%)
    _get_name_lookup_index()

    # 1. 상장주식 Master DB 색인 로드 (로컬 SQLite 0.001s + 노션 동기화)
    master_map: Dict[str, str] = {}
    try:
        from core.local_db_manager import get_db_connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ticker, notion_page_id FROM tbl_stocks WHERE notion_page_id != '';")
            for r in cursor.fetchall():
                t = normalize_ticker(r["ticker"])
                pid = r["notion_page_id"]
                master_map[t] = pid
                master_map[t.split(".")[0]] = pid
    except Exception as e:
        logger.warning(f"⚠️ 로컬 DB 색인 로드 예외: {e}")

    if MASTER_DB_ID:
        try:
            for p in paginate_database(notion_client, MASTER_DB_ID, page_size=100):
                t_val = get_prop_value(p.get("properties", {}), ["티커", "Ticker"])
                if t_val:
                    t = normalize_ticker(str(t_val))
                    pid = p.get("id", "")
                    master_map[t] = pid
                    master_map[t.split(".")[0]] = pid
            logger.info(f"📋 Master DB {len(master_map)}개 티커 색인 완료")
        except Exception as e:
            logger.warning(f"⚠️ Master DB 인덱싱 실패: {e}")

    # 2. 처리 완료 캐시 로드
    processed_ids = load_processed_videos()
    logger.info(f"💾 기존 처리된 영상 캐시: {len(processed_ids)}개")

    total_new_processed = 0

    # 3. 분기 1: CLI 인자로 단일 영상이 직접 지정된 경우
    if args.video:
        print(f"\n🎯 [CLI 단일 영상 직접 분석] {args.video}...")
        v_meta = resolve_video_info(args.video)
        if not v_meta:
            print(f"❌ 유효한 유튜브 영상을 찾을 수 없습니다: {args.video}")
            sys.exit(1)

        success = process_single_video_item(
            v=v_meta,
            notion_client=notion_client,
            ai_service=ai_service,
            master_map=master_map,
            processed_ids=processed_ids,
            force=args.force
        )
        if success:
            total_new_processed += 1

    # 4. 분기 2: CLI 인자로 특정 채널들이 직접 지정된 경우
    elif args.channels:
        print(f"\n📡 [CLI 지정 채널 스캔] 총 {len(args.channels)}개 채널")
        resolved_channels = []
        for ch_in in args.channels:
            info = resolve_channel_info(ch_in)
            if info:
                resolved_channels.append(info)
                logger.info(f"✅ 채널 식별 완료: {info['name']} ({info['channel_id']})")
            else:
                logger.warning(f"⚠️ 채널 식별 실패: {ch_in}")

        for ch in resolved_channels:
            ch_name = ch["name"]
            ch_id = ch["channel_id"]
            print(f"\n📡 [{ch_name}] 신규 업로드 영상 RSS 스캔 중 (ID: {ch_id})...")
            recent_videos = fetch_recent_videos_from_rss(ch_id, channel_name=ch_name, max_videos=args.max_videos)
            for v in recent_videos:
                success = process_single_video_item(
                    v=v,
                    notion_client=notion_client,
                    ai_service=ai_service,
                    master_map=master_map,
                    processed_ids=processed_ids,
                    force=args.force
                )
                if success:
                    total_new_processed += 1

    # 5. 분기 3 (기본 모드): 노션 [Youtube 투자가이드 DB]에서 활성 소스 자동 로드
    else:
        if YOUTUBE_GUIDE_DB_ID:
            print(f"\n📖 [노션 DB 모드] 'Youtube 투자가이드' DB({YOUTUBE_GUIDE_DB_ID})에서 활성 목록 조회 중...")
            active_sources = load_active_sources_from_notion(notion_client, YOUTUBE_GUIDE_DB_ID)
            if not active_sources:
                print("   ℹ️ [Youtube 투자가이드 DB] '활성화' 체크된 채널/영상이 없습니다. 동기화를 정상 종료합니다.")
                return
        else:
            logger.info("ℹ️ YOUTUBE_GUIDE_DATABASE_ID 미설정으로 기본 채널(DEFAULT_CHANNELS)로 동작합니다.")
            active_sources = [
                {
                    "page_id": "",
                    "name": ch["name"],
                    "type": "채널(RSS)",
                    "url": f"https://www.youtube.com/channel/{ch['channel_id']}",
                    "channel_id": ch["channel_id"],
                    "max_videos": args.max_videos,
                    "categories": [],
                }
                for ch in DEFAULT_CHANNELS
            ]

        print(f"✅ 총 {len(active_sources)}개 활성 소스(체크박스 활성화됨) 수집 시작")

        for src in active_sources:
            src_name: str = str(src.get("name") or "")
            src_type: str = str(src.get("type") or "")
            src_url: str = str(src.get("url") or "")
            src_ch_id: str = str(src.get("channel_id") or "")
            src_page_id: str = str(src.get("page_id") or "")
            raw_max_v = src.get("max_videos", args.max_videos)
            src_max_v: int = int(raw_max_v) if isinstance(raw_max_v, (int, str)) and str(raw_max_v).isdigit() and int(raw_max_v) > 0 else (args.max_videos or 5)
            guide_page_id: Optional[str] = src_page_id if src_page_id else None

            # A. 단일 영상 소스
            if src_type == "단일영상" or (src_url and extract_video_id(src_url) and not src_ch_id):
                print(f"\n🎯 [단일 영상] '{src_name}' ({src_url})")
                v_meta = resolve_video_info(src_url)
                if v_meta:
                    success = process_single_video_item(
                        v=v_meta,
                        notion_client=notion_client,
                        ai_service=ai_service,
                        master_map=master_map,
                        processed_ids=processed_ids,
                        guide_page_id=guide_page_id,
                        force=args.force
                    )
                    if success:
                        total_new_processed += 1
                    if guide_page_id:
                        update_guide_last_scanned(notion_client, guide_page_id)
                else:
                    logger.warning(f"⚠️ 단일 영상 정보를 가져올 수 없습니다: {src_url}")

            # B. 채널 또는 재생목록 RSS 피드 소스
            else:
                if not src_ch_id:
                    logger.warning(f"⚠️ [{src_name}] Channel ID 또는 Playlist ID가 없어 스킵합니다.")
                    continue

                is_pl = (src_type == "재생목록") or src_ch_id.startswith("PL") or src_ch_id.startswith("UU") or src_ch_id.startswith("FL")
                label = "재생목록 RSS" if is_pl else "채널 RSS"
                icon = "📑" if is_pl else "📡"

                print(f"\n{icon} [{label}] '{src_name}' 스캔 중 (최대 {src_max_v}개 영상, ID: {src_ch_id})...")
                recent_videos = fetch_recent_videos_from_rss(src_ch_id, channel_name=src_name, max_videos=src_max_v, is_playlist=is_pl)

                if not recent_videos:
                    print("   ℹ️ 최근 게시된 영상을 찾을 수 없습니다.")
                else:
                    for v in recent_videos:
                        success = process_single_video_item(
                            v=v,
                            notion_client=notion_client,
                            ai_service=ai_service,
                            master_map=master_map,
                            processed_ids=processed_ids,
                            guide_page_id=guide_page_id,
                            force=args.force
                        )
                        if success:
                            total_new_processed += 1

                if guide_page_id:
                    update_guide_last_scanned(notion_client, guide_page_id)

    print("\n" + "=" * 80)
    print(f"🎉 [동기화 완료] 총 {total_new_processed}개의 신규 유튜브 영상 AI 분석 데이터가 노션에 적재되었습니다.")
    print("=" * 80)


if __name__ == "__main__":
    main()
