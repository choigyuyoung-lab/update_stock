# -*- coding: utf-8 -*-
"""
youtube_auto_collector.py
=========================
API 쿼터 소모가 없는 유튜브 채널 RSS 피드(https://www.youtube.com/feeds/videos.xml?channel_id=...)와
youtube-transcript-api를 결합하여 신규 영상의 자막 텍스트를 추출하고,
Google Gemini AI(ai_service.py)를 통해 시황 및 종목 분석 데이터를 구조화하여
노션 데이터베이스([투자공부 by Youtube] & [미정리 종목]) 및 상장주식 Master DB에
완전 무개입(Zero-Touch)으로 자동 적재하는 파이프라인 모듈입니다.
"""

import os
import sys
import json
import re
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

# notion_utils 및 ai_service 재사용
from notion_utils import (
    build_notion_client,
    get_env_var,
    get_db_id,
    get_kst_str,
    get_kst_now,
    kst_isoformat,
    safe_create_page,
    paginate_database,
    get_prop_value,
)
from ai_service import AIService


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
logger = logging.getLogger("YouTubeAutoCollector")

# .env 로드
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

# 기본 모니터링 유튜브 채널 목록 (환경변수 YOUTUBE_CHANNEL_IDS 또는 기본값)
DEFAULT_CHANNELS = [
    # (채널명, 채널 ID)
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
    (YouTube Data API 쿼터 전혀 소모하지 않음)
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
        # Atom 네임스페이스
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

                # 날짜 YYYY-MM-DD 변환
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
    (수동 자막 우선, 자동생성 자막 폴백 지원)
    """
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(
            video_id,
            languages=["ko", "ko-KR", "en", "en-US", "auto"]
        )
        if not transcript_list:
            return None

        # 텍스트 결합 (공백 정규화)
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
# 5. Gemini AI 구조화 분석 엔진 (시황 요약 + 종목/타점/팩터 추출)
# ==============================================================================
AI_EXTRACTION_PROMPT = """당신은 월스트리트 헤지펀드 시니어 퀀트 애널리스트이자 주식 시황 분석가입니다.
아래 제공된 유튜브 영상의 자막 스크립트를 정밀 분석하여, 투자자를 위한 핵심 시황 요약 및 언급된 종목/자산의 분석 데이터를 JSON 형식으로 추출하세요.

### [출력 형식 (반드시 아래 JSON 스키마만 순수하게 출력할 것)]
```json
{
  "summarized_title_for_notion": "노션 DB용 간결하고 핵심적인 한 줄 제목 (예: [삼프로TV] 8월 FOMC 금리인하 기대감 및 반도체 반등 논리)",
  "publish_date": "YYYY-MM-DD",
  "overall_summary": "영상 전체의 핵심 매크로 시황 및 시장 방향성 요약 (3~5문장 내외, 명사형 종결어미 ~함/~임 사용)",
  "key_takeaways": [
    "핵심 시사점 1 (명사형 종결)",
    "핵심 시사점 2 (명사형 종결)",
    "핵심 시사점 3 (명사형 종결)"
  ],
  "assets": [
    {
      "ticker": "표준 6자리 국내 종목코드(예: 005930) 또는 미국 티커(예: NVDA, AAPL)",
      "name": "종목명 또는 자산명 (예: 삼성전자, 엔비디아, 금선물)",
      "context": "영상에서 언급된 구체적 투자 논리, 실적 전망, 목표가/손절가 또는 매수 타점 요약",
      "opinion": "매수 / 관망 / 비중축소 / 중립 중 택1",
      "link_url": ""
    }
  ]
}
```

### [분석 지침]
1. 단순 언급된 종목이 아닌, 영상에서 실제 투자 논리나 전망이 언급된 핵심 종목 위주로 최대 5개까지 추출하세요.
2. 모든 요약 문장은 반드시 명사형 종결어미(~함, ~임, ~필요, ~권고)를 사용하세요.
3. 티커는 한국 주식의 경우 6자리 숫자 코드, 미국 주식은 대문자 심볼을 정확히 기재하세요.
4. 마크다운 코드블록(```json) 안에 JSON 데이터만 출력하세요.
"""


def analyze_transcript_with_gemini(ai_service: AIService, transcript_text: str, video_meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Gemini AI를 통해 자막 텍스트를 정밀 구조화 분석하여 JSON 사전으로 반환합니다."""
    if not ai_service.is_available():
        logger.error("❌ Gemini AI 서비스를 사용할 수 없습니다. GEMINI_API_KEY를 확인하세요.")
        return None

    user_content = f"""[영상 기본 정보]
- 채널명: {video_meta.get('channel_name', '')}
- 영상 원제목: {video_meta.get('title', '')}
- 영상 URL: {video_meta.get('url', '')}
- 게시일자: {video_meta.get('publish_date', '')}

[자막 스크립트 전문]
{transcript_text[:12000]}
"""

    try:
        prompt = f"{AI_EXTRACTION_PROMPT}\n\n{user_content}"
        res = ai_service.client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={"temperature": 0.2, "max_output_tokens": 4096}
        )
        res_text = res.text.strip()
        cleaned_json = re.sub(r'^```(?:json)?\s*', '', res_text, flags=re.MULTILINE)
        cleaned_json = re.sub(r'\s*```$', '', cleaned_json, flags=re.MULTILINE).strip()
        data = json.loads(cleaned_json)
        
        # URL 및 게시일자 보정
        if not data.get("url"):
            data["url"] = video_meta.get("url", "")
        if not data.get("publish_date") or data.get("publish_date") == "null":
            data["publish_date"] = video_meta.get("publish_date", get_kst_str("%Y-%m-%d"))

        return data
    except Exception as e:
        logger.error(f"❌ Gemini 분석 응답 파싱 실패: {e}")
        return None


# ==============================================================================
# 6. 노션 페이지 빌더 및 상장주식 Master DB 연동
# ==============================================================================
def create_youtube_summary_notion_page(client: Any, db_id: str, analyzed_data: Dict[str, Any], master_map: Dict[str, str], interest_map: Dict[str, str]) -> Optional[str]:
    """
    분석된 유튜브 시황 및 종목 테이블을 노션 DB에 정밀 블록 구조로 생성하고,
    상장주식 Master DB 및 투자주 DB Relation을 자동 바인딩합니다.
    """
    if not db_id:
        logger.warning("⚠️ YOUTUBE_DATABASE_ID가 설정되지 않아 노션 페이지 생성을 건너뜁니다.")
        return None

    title = analyzed_data.get("summarized_title_for_notion") or "유튜브 시황 분석 리포트"
    url = analyzed_data.get("url", "")
    pub_date_str = analyzed_data.get("publish_date", get_kst_str("%Y-%m-%d"))
    summary = analyzed_data.get("overall_summary", "")
    takeaways = analyzed_data.get("key_takeaways", [])
    assets = analyzed_data.get("assets", [])

    # 1. 속성 빌드
    takeaways_text = "\n".join([f"• {point}" for point in takeaways]) if takeaways else ""

    page_props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title}}]},
        "URL": {"url": url},
        "Summary": {"rich_text": [{"text": {"content": summary[:2000]}}]},
        "Key Takeaways": {"rich_text": [{"text": {"content": takeaways_text[:2000]}}]},
    }
    if pub_date_str:
        page_props["Date"] = {"date": {"start": pub_date_str}}

    # 2. 본문 블록 빌드
    blocks: List[Dict[str, Any]] = []

    # 콜아웃: 한 줄 핵심 요약
    blocks.append({
        "object": "block",
        "type": "callout",
        "callout": {
            "icon": {"type": "emoji", "emoji": "📺"},
            "color": "blue_background",
            "rich_text": [{"type": "text", "text": {"content": f"출처: {url}\n{summary}"}}]
        }
    })

    # 핵심 요약 H2
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
        # 테이블 헤더
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
            t = str(asset.get("ticker", "-")).strip()
            n = str(asset.get("name", "-")).strip()
            op = str(asset.get("opinion", "중립")).strip()
            ctx = str(asset.get("context", "-")).strip()

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

    # 노션 페이지 생성
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
    analyzed_data: Dict[str, Any],
    master_map: Dict[str, str],
    interest_map: Dict[str, str]
) -> int:
    """
    영상에서 추출된 개별 종목/자산을 [미정리 종목 DB]에 자동 생성하고
    상장주식 Master DB 및 투자주 DB Relation을 연결합니다.
    """
    if not db_id:
        return 0

    assets = analyzed_data.get("assets", [])
    pub_date_str = analyzed_data.get("publish_date", get_kst_str("%Y-%m-%d"))
    count = 0

    for asset in assets:
        raw_ticker = str(asset.get("ticker", "")).strip()
        name = str(asset.get("name", "")).strip()
        context = str(asset.get("context", "")).strip()
        opinion = str(asset.get("opinion", "")).strip()

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
            client.pages.create(
                parent={"database_id": db_id},
                properties=props
            )
            count += 1
            logger.info(f"      🥬 [미정리 종목 추가] {raw_ticker} ({name}) -> 미정리 DB 적재 완료")
        except Exception as e:
            logger.warning(f"      ⚠️ [미정리 종목 생성 실패] {raw_ticker}: {e}")

    return count


# ==============================================================================
# 7. 메인 파이프라인 실행 엔진
# ==============================================================================
def main() -> None:
    print("=" * 80)
    print("🚀 [YouTube Auto Collector] RSS 기반 매크로/종목 자동 추출 & 노션 동기화 시작")
    print("=" * 80)

    notion_client = build_notion_client(NOTION_TOKEN)
    ai_service = AIService()

    # 1. 인덱스 맵 로드 (상장주식 Master DB & 투자주 DB)
    master_map: Dict[str, str] = {}
    interest_map: Dict[str, str] = {}
    if MASTER_DB_ID:
        try:
            for p in paginate_database(notion_client, MASTER_DB_ID, page_size=100):
                t_val = get_prop_value(p.get("properties", {}), ["티커", "Ticker"])
                if t_val:
                    master_map[normalize_ticker(str(t_val))] = p.get("id", "")
            logger.info(f"📋 Master DB {len(master_map)}개 티커 색인 완료")
        except Exception as e:
            logger.warning(f"⚠️ Master DB 인덱싱 실패: {e}")

    if INTEREST_DB_ID:
        try:
            for p in paginate_database(notion_client, INTEREST_DB_ID, page_size=100):
                t_val = get_prop_value(p.get("properties", {}), ["티커", "Ticker"])
                if t_val:
                    interest_map[normalize_ticker(str(t_val))] = p.get("id", "")
            logger.info(f"📋 투자주 DB {len(interest_map)}개 티커 색인 완료")
        except Exception as e:
            logger.warning(f"⚠️ 투자주 DB 인덱싱 실패: {e}")

    # 2. 처리 완료 캐시 로드
    processed_ids = load_processed_videos()
    logger.info(f"💾 기존 처리된 영상 캐시: {len(processed_ids)}개")

    # 3. 채널 목록 탐색
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
                processed_ids.add(vid)  # 재시도 방지
                continue

            print(f"   🧠 자막 추출 완료 ({len(transcript):,} 글자). Gemini AI 정밀 구조화 분석 중...")
            analyzed = analyze_transcript_with_gemini(ai_service, transcript, v)

            if not analyzed:
                print(f"   ❌ AI 분석 실패.")
                continue

            # 1) 📹 [투자공부 by Youtube DB]에 전체 시황/리포트 적재
            print(f"   📥 [1/2] 투자공부 DB 저장 중: '{analyzed.get('summarized_title_for_notion')}'...")
            yt_db = YOUTUBE_DB_ID
            page_id = create_youtube_summary_notion_page(
                client=notion_client,
                db_id=yt_db,
                analyzed_data=analyzed,
                master_map=master_map,
                interest_map=interest_map
            )

            # 2) 🥬 [미정리 종목 DB]에 개별 종목 자동 적재 및 Master DB 연결
            unorg_db = UNORGANIZED_DB_ID
            if unorg_db:
                print(f"   📥 [2/2] 미정리 종목 DB에 개별 자산({len(analyzed.get('assets', []))}개) 적재 중...")
                create_unorganized_stock_items(
                    client=notion_client,
                    db_id=unorg_db,
                    analyzed_data=analyzed,
                    master_map=master_map,
                    interest_map=interest_map
                )

            if page_id:
                processed_ids.add(vid)
                total_new_processed += 1
                save_processed_videos(processed_ids)
                time.sleep(1.0)

    print("\n" + "=" * 80)
    print(f"🎉 [완료] 총 {total_new_processed}개의 신규 유튜브 분석 리포트가 노션에 적재되었습니다.")
    print("=" * 80)


if __name__ == "__main__":
    main()
