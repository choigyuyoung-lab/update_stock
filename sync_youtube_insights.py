# -*- coding: utf-8 -*-
"""
sync_youtube_insights.py
========================
API 쿼터 소모가 없는 유튜브 채널 RSS 피드(https://www.youtube.com/feeds/videos.xml?channel_id=...)와
youtube-transcript-api를 결합하여 신규 영상의 자막 텍스트를 추출하고,
Google Gemini AI(ai_service.py)의 Pydantic Structured Outputs(response_schema) 엔진을 통해
시황 및 종목 분석 데이터를 100% Deterministic하게 추출합니다.

[핵심 적재 워크플로우]
1. [투자공부 by Youtube DB]: 전체 시황 요약, Key Takeaways, 자산 분석 테이블 적재
2. [미정리 종목 DB]: 개별 종목별 행(Row) 생성 및 상장주식 Master DB Relation 연결
3. [상장주식 Master DB 개별 페이지]: 추출된 추천 종목에 대해 페이지 하단 Callout 블록으로 유튜브 인사이트 자동 추가
"""

import os
import sys
import json
import re
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    get_kst_str,
    paginate_database,
    get_prop_value,
)
from ai_service import AIService, YouTubeAnalysisResult, YouTubeAssetItem


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
UNORGANIZED_DB_ID = get_db_id("UNORGANIZED_DATABASE_ID", ["UNORGANIZED_DB_ID", "2d8f59dbdb5b807aac70d3711b5b6e93"], required=False)
MASTER_DB_ID = get_db_id("MASTER_DATABASE_ID", ["MASTER_DB_ID", "2f0f59dbdb5b80e5bc5fe1ffdd3b941a"], required=False)
INTEREST_DB_ID = get_db_id("DATABASE_ID", ["INTEREST_DATABASE_ID", "2a9f59dbdb5b80fbab45dea3b3cbe9f4"], required=False)

PROCESSED_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".processed_youtube_videos.json")

# 기본 모니터링 유튜브 채널 목록
DEFAULT_CHANNELS = [
    {"name": "삼프로TV", "channel_id": "UChTDgvngP3A4OxNWv_gW0Pw"},
    {"name": "슈카월드", "channel_id": "UCsJ6RuBiTVWRX156FVbeaGg"},
    {"name": "매경 월가월부", "channel_id": "UC_YcKz2Fk9e8l6F5j7oR0sw"},
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
# 3. 유튜브 RSS 피드 파서 (API 쿼터 0 소모)
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
# 4. 자막(Transcript) 추출 엔진
# ==============================================================================
def get_video_transcript(video_id: str) -> Optional[str]:
    """
    youtube-transcript-api를 활용하여 한국어/영어 자막 텍스트를 추출합니다.
    """
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(
            video_id,
            languages=["ko", "ko-KR", "en", "en-US", "auto"]
        )
        if not transcript_list:
            return None

        full_text = " ".join([item.get("text", "") for item in transcript_list])
        full_text = re.sub(r'\s+', ' ', full_text).strip()
        return full_text if len(full_text) >= 100 else None

    except (TranscriptsDisabled, NoTranscriptFound):
        logger.info(f"   ℹ️ [자막 없음] Video ID '{video_id}'에 사용 가능한 자막이 없습니다.")
        return None
    except Exception as e:
        logger.warning(f"   ⚠️ [자막 추출 오류] Video ID '{video_id}': {e}")
        return None


# ==============================================================================
# 5. 노션 적재 엔진
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
# 6. 메인 파이프라인 실행 엔진
# ==============================================================================
def main() -> None:
    print("=" * 80)
    print("🚀 [Sync YouTube Insights] Pydantic Structured Outputs 기반 유튜브 AI 분석 시작")
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

    # 3. 채널 목록 순회
    channels = DEFAULT_CHANNELS
    total_new_processed = 0

    for ch in channels:
        ch_name = ch["name"]
        ch_id = ch["channel_id"]
        print(f"\n📡 [{ch_name}] 신규 업로드 영상 RSS 스캔 중...")

        recent_videos = fetch_recent_videos_from_rss(ch_id, channel_name=ch_name, max_videos=3)
        if not recent_videos:
            print(f"   ℹ️ 최근 게시된 영상을 찾을 수 없습니다.")
            continue

        for v in recent_videos:
            vid = v["video_id"]
            vtitle = v["title"]
            if vid in processed_ids:
                print(f"   ⚡ [이미 처리됨] '{vtitle[:30]}...' -> 스킵")
                continue

            print(f"\n🎬 [신규 영상 감지] '{vtitle}' ({v['publish_date']})")
            print(f"   ⏳ 자막 스크립트 추출 중...")
            transcript = get_video_transcript(vid)

            if not transcript:
                print(f"   ⚠️ 자막이 제공되지 않아 처리를 건너뜁니다.")
                processed_ids.add(vid)
                continue

            print(f"   🧠 자막 추출 완료 ({len(transcript):,} 글자). Gemini AI Pydantic 구조화 분석 중...")
            analyzed: Optional[YouTubeAnalysisResult] = ai_service.analyze_youtube_transcript(transcript, v)

            if not analyzed:
                print(f"   ❌ AI 분석 실패.")
                continue

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
                print(f"   📥 [3/3] 상장주식 Master DB 개별 페이지에 Callout 인사이트 블록 추가 중...")
                append_insight_callout_to_master_db(
                    client=notion_client,
                    master_map=master_map,
                    analyzed=analyzed,
                    video_meta=v
                )

            if page_id:
                processed_ids.add(vid)
                total_new_processed += 1
                save_processed_videos(processed_ids)
                time.sleep(1.0)

    print("\n" + "=" * 80)
    print(f"🎉 [완료] 총 {total_new_processed}개의 신규 유튜브 Pydantic 분석 결과가 노션에 적재되었습니다.")
    print("=" * 80)


if __name__ == "__main__":
    main()
