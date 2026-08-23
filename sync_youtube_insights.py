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
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    get_kst_str,
    paginate_database,
    get_prop_value,
    get_page_text,
)
from services.ai_service import AIService, YouTubeAnalysisResult


def normalize_ticker(ticker: str) -> str:
    """티커 문자열에서 특수문자를 제거하고 대문자 표준 포맷으로 정규화합니다."""
    if not ticker:
        return ""
    return re.sub(r'[^0-9A-Z]', '', str(ticker).strip().upper())


# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SyncYouTubeInsights")

load_dotenv()

# ==============================================================================
# 1. 환경 설정 및 상수 정의
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
YOUTUBE_DB_ID = get_db_id("YOUTUBE_DATABASE_ID", ["YOUTUBE_DB_ID", "2d0f59dbdb5b804891e4e054ef049d1c"], required=False)
YOUTUBE_GUIDE_DB_ID = get_db_id("YOUTUBE_GUIDE_DATABASE_ID", ["YOUTUBE_GUIDE_DB_ID", "3c4f59dbdb5b80d49fa9d884e3dc920b"], required=False)
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID", "2d8f59dbdb5b807aac70d3711b5b6e93"], required=False)
MASTER_DB_ID = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID", "2f0f59dbdb5b80e5bc5fe1ffdd3b941a"], required=False)
INTEREST_DB_ID = get_db_id("DATABASE_ID", ["INTEREST_DATABASE_ID", "2a9f59dbdb5b80fbab45dea3b3cbe9f4"], required=False)

PROCESSED_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".processed_youtube_videos.json")

# 노션 DB 미연동 시 비상용 기본 채널 목록
DEFAULT_CHANNELS = [
    {"name": "삼프로TV", "channel_id": "UChlv4GSd7OQl3js-jkLOnFA"},
    {"name": "슈카월드", "channel_id": "UCJo6G1u0e_-wS-JQn3T-zEw"},
    {"name": "매경 월가월부", "channel_id": "UCIipmgxpUxDmPP-ma3Ahvbw"},
]


# ==============================================================================
# 2. 캐시 관리자 (중복 수집 및 AI 토큰 낭비 방지)
# ==============================================================================
def load_processed_videos() -> Set[str]:
    """이미 처리된 유튜브 비디오 ID 집합을 로드합니다."""
    if os.path.exists(PROCESSED_CACHE_FILE):
        try:
            with open(PROCESSED_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data if isinstance(data, list) else data.keys())
        except Exception as e:
            logger.warning(f"⚠️ 캐시 파일 읽기 실패: {e}")
    return set()


def save_processed_videos(processed_ids: Set[str]) -> None:
    """처리된 비디오 ID 목록을 로컬 캐시에 저장합니다."""
    try:
        with open(PROCESSED_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(processed_ids)), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"⚠️ 캐시 파일 저장 실패: {e}")


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
    channel_input = str(channel_input).strip()
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
    video_input = str(video_input).strip()
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


def resolve_video_info(video_input: str) -> Optional[Dict[str, Any]]:
    """
    유튜브 영상 URL 또는 Video ID를 바탕으로 영상 메타데이터를 수집합니다.
    """
    vid = extract_video_id(video_input)
    if not vid:
        logger.warning(f"⚠️ 유효한 유튜브 Video ID를 찾을 수 없습니다: {video_input}")
        return None

    video_url = f"https://www.youtube.com/watch?v={vid}"
    title = f"YouTube Video ({vid})"
    channel_name = "YouTube"

    # oEmbed API를 통한 제목 및 채널명 조회 (무료, 쿼터 소모 0)
    try:
        oembed_url = f"https://www.youtube.com/oembed?url={video_url}&format=json"
        res = requests.get(oembed_url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            title = data.get("title", title)
            channel_name = data.get("author_name", channel_name)
    except Exception as e:
        logger.debug(f"oEmbed 메타데이터 조회 생략: {e}")

    return {
        "video_id": vid,
        "title": title,
        "url": video_url,
        "publish_date": get_kst_str("%Y-%m-%d"),
        "channel_name": channel_name,
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

            # [Self-Healing] Channel ID가 누락된 경우 URL/핸들로 자동 판별 후 노션 업데이트
            if source_type == "채널(RSS)" and not channel_id and url_or_handle:
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


# ==============================================================================
# 5. 유튜브 RSS 피드 파서 (API 쿼터 0 소모)
# ==============================================================================
def fetch_recent_videos_from_rss(channel_id: str, channel_name: str = "", max_videos: int = 5) -> List[Dict[str, Any]]:
    """
    유튜브 채널 RSS 피드를 파싱하여 최근 업로드된 비디오 목록을 반환합니다.
    (YouTube Data API 쿼터를 전혀 소모하지 않음)
    """
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
        for entry in entries[:max_videos]:
            video_id_elem = entry.find("yt:videoId", ns)
            title_elem = entry.find("atom:title", ns)
            published_elem = entry.find("atom:published", ns)
            link_elem = entry.find("atom:link", ns)

            if video_id_elem is not None and title_elem is not None:
                vid = video_id_elem.text.strip()
                vtitle = title_elem.text.strip()
                vpub = published_elem.text.strip() if published_elem is not None else ""
                vurl = link_elem.attrib.get("href", f"https://www.youtube.com/watch?v={vid}") if link_elem is not None else f"https://www.youtube.com/watch?v={vid}"

                pub_date = ""
                if vpub:
                    try:
                        pub_dt = datetime.fromisoformat(vpub.replace("Z", "+00:00")).astimezone(ZoneInfo("Asia/Seoul"))
                        pub_date = pub_dt.strftime("%Y-%m-%d")
                    except Exception:
                        pub_date = vpub[:10]

                videos.append({
                    "video_id": vid,
                    "title": vtitle,
                    "url": vurl,
                    "publish_date": pub_date,
                    "channel_name": channel_name or channel_id,
                })
    except Exception as e:
        logger.error(f"❌ [{channel_name}] RSS 파싱 에러: {e}")

    return videos


# ==============================================================================
# 6. 자막(Transcript) 추출 엔진
# ==============================================================================
def get_video_transcript(video_id: str) -> Optional[str]:
    """
    youtube-transcript-api를 활용하여 한국어/영어 자막 텍스트를 추출합니다.
    """
    languages = ["ko", "ko-KR", "en", "en-US", "auto"]
    try:
        if hasattr(YouTubeTranscriptApi, "fetch"):
            api = YouTubeTranscriptApi()
            fetched = api.fetch(video_id, languages=languages)
            snippets = getattr(fetched, "snippets", fetched)
            full_text = " ".join([
                getattr(item, "text", "") if hasattr(item, "text") else item.get("text", "")
                for item in snippets
            ])
        elif hasattr(YouTubeTranscriptApi, "get_transcript"):
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=languages)
            full_text = " ".join([item.get("text", "") for item in transcript_list])
        else:
            logger.warning("   ⚠️ YouTubeTranscriptApi 호환 메서드를 찾을 수 없습니다.")
            return None

        full_text = re.sub(r'\s+', ' ', full_text).strip()
        return full_text if len(full_text) >= 100 else None

    except (TranscriptsDisabled, NoTranscriptFound):
        logger.info(f"   ℹ️ [자막 없음] Video ID '{video_id}'에 사용 가능한 자막이 없습니다.")
        return None
    except Exception as e:
        logger.warning(f"   ⚠️ [자막 추출 오류] Video ID '{video_id}': {e}")
        return None


# ==============================================================================
# 7. 노션 적재 엔진
# ==============================================================================
def create_youtube_summary_notion_page(
    client: Any,
    db_id: str,
    analyzed: YouTubeAnalysisResult,
    video_meta: Dict[str, Any]
) -> Optional[str]:
    """
    분석된 유튜브 시황 및 추천 자산 테이블을 [투자공부 by Youtube DB]에 적재합니다.
    """
    if not db_id:
        return None

    title = analyzed.summarized_title_for_notion or video_meta.get("title", "유튜브 시황 분석 리포트")
    url = video_meta.get("url", "")
    pub_date_str = analyzed.publish_date or video_meta.get("publish_date", get_kst_str("%Y-%m-%d"))
    summary = analyzed.overall_summary or ""
    takeaways = analyzed.key_takeaways or []
    assets = analyzed.assets or []

    takeaways_text = "\n".join([f"• {point}" for point in takeaways]) if takeaways else ""

    page_props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title}}]},
        "URL": {"url": url},
        "Summary": {"rich_text": [{"text": {"content": summary[:2000]}}]},
        "Key Takeaways": {"rich_text": [{"text": {"content": takeaways_text[:2000]}}]},
    }
    if pub_date_str:
        page_props["Date"] = {"date": {"start": pub_date_str}}

    blocks: List[Dict[str, Any]] = []

    # 콜아웃: 한 줄 핵심 요약
    blocks.append({
        "object": "block",
        "type": "callout",
        "callout": {
            "icon": {"type": "emoji", "emoji": "📺"},
            "color": "blue_background",
            "rich_text": [{"type": "text", "text": {"content": f"출처: {video_meta.get('channel_name', 'YouTube')} ({url})\n{summary}"}}]
        }
    })

    # 핵심 시장 시사점 H2
    if takeaways:
        blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"text": {"content": "💡 핵심 시장 시사점 (Key Takeaways)"}}]}
        })
        for point in takeaways:
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [{"text": {"content": point}}]}
            })

    # 자산 및 티커 분석 테이블
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
            t = str(asset.ticker or "-").strip()
            n = str(asset.name or "-").strip()
            op = str(asset.opinion or "중립").strip()
            ctx = str(asset.context or "-").strip()

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
        raw_ticker = str(asset.ticker or "").strip()
        name = str(asset.name or "").strip()
        context = str(asset.context or "").strip()
        opinion = str(asset.opinion or "").strip()

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


def append_insight_callout_to_master_db(
    client: Any,
    master_map: Dict[str, str],
    analyzed: YouTubeAnalysisResult,
    video_meta: Dict[str, Any]
) -> int:
    """
    추출된 추천 종목(result.assets)이 상장주식 Master DB에 존재하는 경우,
    해당 종목 페이지 하단에 유튜브 분석 요약 Callout 블록을 자동으로 추가합니다.
    """
    assets = analyzed.assets or []
    pub_date = analyzed.publish_date or video_meta.get("publish_date", get_kst_str("%Y-%m-%d"))
    channel_name = video_meta.get("channel_name", "YouTube")
    video_title = video_meta.get("title", "")
    video_url = video_meta.get("url", "")
    appended_count = 0

    for asset in assets:
        raw_ticker = str(asset.ticker or "").strip()
        if not raw_ticker:
            continue

        clean_ticker = normalize_ticker(raw_ticker)
        if clean_ticker not in master_map:
            continue

        master_page_id = master_map[clean_ticker]
        opinion = str(asset.opinion or "중립").strip()
        context = str(asset.context or "").strip()

        callout_block = {
            "object": "block",
            "type": "callout",
            "callout": {
                "icon": {"type": "emoji", "emoji": "📺"},
                "color": "gray_background",
                "rich_text": [
                    {"type": "text", "text": {"content": f"[{pub_date}] {channel_name} 유튜브 인사이트 ({opinion})\n", "annotations": {"bold": True}}},
                    {"type": "text", "text": {"content": f"• 영상: {video_title}\n• 링크: {video_url}\n• 분석내용: {context}"}}
                ]
            }
        }

        try:
            client.blocks.children.append(block_id=master_page_id, children=[callout_block])
            appended_count += 1
            logger.info(f"      📌 [Master DB Callout 추가] {clean_ticker} 페이지에 유튜브 인사이트 블록 추가 완료")
        except Exception as e:
            logger.warning(f"      ⚠️ [Master DB Callout 추가 실패] {clean_ticker}: {e}")

    return appended_count


# ==============================================================================
# 8. 단일 영상 통합 처리 파이프라인
# ==============================================================================
def process_single_video_item(
    v: Dict[str, Any],
    notion_client: Any,
    ai_service: AIService,
    master_map: Dict[str, str],
    processed_ids: Set[str],
    force: bool = False
) -> bool:
    """
    단일 유튜브 영상 메타데이터를 기반으로 자막 추출 -> AI 분석 -> 노션 적재를 수행합니다.
    """
    vid = v["video_id"]
    vtitle = v.get("title", "")
    pub_date = v.get("publish_date", "")

    if vid in processed_ids and not force:
        print(f"   ⚡ [이미 처리됨] '{vtitle[:30]}...' -> 스킵")
        return False

    print(f"\n🎬 [영상 분석 시작] '{vtitle}' ({pub_date})")
    print("   ⏳ 자막 스크립트 추출 중...")
    transcript = get_video_transcript(vid)

    if not transcript:
        print("   ⚠️ 자막이 제공되지 않아 처리를 건너뜁니다.")
        processed_ids.add(vid)
        save_processed_videos(processed_ids)
        return False

    print(f"   🧠 자막 추출 완료 ({len(transcript):,} 글자). Gemini AI Pydantic 구조화 분석 중...")
    analyzed: Optional[YouTubeAnalysisResult] = ai_service.analyze_youtube_transcript(transcript, v)

    if not analyzed:
        print("   ❌ AI 분석 실패.")
        return False

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

    # 3) 📌 [상장주식 Master DB 개별 페이지] Callout 블록 적재
    if MASTER_DB_ID and analyzed.assets:
        print("   📥 [3/3] 상장주식 Master DB 개별 페이지에 Callout 인사이트 블록 추가 중...")
        append_insight_callout_to_master_db(
            client=notion_client,
            master_map=master_map,
            analyzed=analyzed,
            video_meta=v
        )

    if page_id:
        processed_ids.add(vid)
        save_processed_videos(processed_ids)
        time.sleep(1.0)
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

    # 1. 상장주식 Master DB 색인 로드
    master_map: Dict[str, str] = {}
    if MASTER_DB_ID:
        try:
            for p in paginate_database(notion_client, MASTER_DB_ID, page_size=100):
                t_val = get_prop_value(p.get("properties", {}), ["티커", "Ticker"])
                if t_val:
                    master_map[normalize_ticker(str(t_val))] = p.get("id", "")
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
        print(f"\n📖 [노션 DB 모드] 'Youtube 투자가이드' DB({YOUTUBE_GUIDE_DB_ID})에서 활성 목록 조회 중...")
        active_sources = load_active_sources_from_notion(notion_client, YOUTUBE_GUIDE_DB_ID)

        if not active_sources:
            logger.info("ℹ️ 노션 DB에 활성화된 채널이 없어 기본 채널(DEFAULT_CHANNELS)로 동작합니다.")
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

        print(f"✅ 총 {len(active_sources)}개 활성 소스(채널/영상) 수집 시작")

        for src in active_sources:
            src_name = src["name"]
            src_type = src["type"]
            src_url = src.get("url", "")
            src_ch_id = src.get("channel_id", "")
            src_page_id = src.get("page_id", "")
            src_max_v = src.get("max_videos", args.max_videos)

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
                        force=args.force
                    )
                    if success:
                        total_new_processed += 1
                    if src_page_id:
                        update_guide_last_scanned(notion_client, src_page_id)
                else:
                    logger.warning(f"⚠️ 단일 영상 정보를 가져올 수 없습니다: {src_url}")

            # B. 채널 RSS 피드 소스
            else:
                if not src_ch_id:
                    logger.warning(f"⚠️ [{src_name}] Channel ID가 없어 스킵합니다.")
                    continue

                print(f"\n📡 [채널 RSS] '{src_name}' 스캔 중 (최대 {src_max_v}개 영상, ID: {src_ch_id})...")
                recent_videos = fetch_recent_videos_from_rss(src_ch_id, channel_name=src_name, max_videos=src_max_v)

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
                            force=args.force
                        )
                        if success:
                            total_new_processed += 1

                if src_page_id:
                    update_guide_last_scanned(notion_client, src_page_id)

    print("\n" + "=" * 80)
    print(f"🎉 [동기화 완료] 총 {total_new_processed}개의 신규 유튜브 영상 AI 분석 데이터가 노션에 적재되었습니다.")
    print("=" * 80)


if __name__ == "__main__":
    main()
