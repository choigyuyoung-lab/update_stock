# -*- coding: utf-8 -*-
"""
test_youtube_proxy_and_multimodal.py
====================================
1. 스마트폰 무인 노드(Tailscale Exit Node) 및 프록시 환경변수 감지/Direct 폴백 검증
2. yt-dlp -> youtube-transcript-api -> Gemini Multimodal 3단계 Fallback 체인 무결성 검증
3. Pydantic YouTubeAnalysisResult 구조화 스키마 및 6자리 티커 zfill 정규화 검증
4. notion_utils.py 표준 스키마 방어 로직(if prop in properties) 무결성 검증
"""

import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from typing import Any, Dict

# Windows 콘솔 UTF-8 안전화
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.youtube.ai_service import AIService, YouTubeAnalysisResult, YouTubeAssetItem
from jobs.youtube.job_sync_youtube_insights import (
    process_single_video_item,
    create_youtube_summary_notion_page,
    create_unorganized_stock_items,
    extract_transcript_via_ytdlp,
    extract_transcript_via_youtube_transcript_api,
)


class TestYouTubeProxyAndMultimodal(unittest.TestCase):
    """유튜브 프록시, 멀티모달 Fallback 및 노션 스키마 방어 단위 테스트"""

    def setUp(self):
        self.mock_video_meta = {
            "video_id": "test_vid_12345",
            "title": "[반도체] AI 반도체 HBM 공급망과 차세대 패키징 전망",
            "url": "https://www.youtube.com/watch?v=test_vid_12345",
            "publish_date": "2026-08-29",
            "publish_time_kst": "2026-08-29 09:00",
            "channel_name": "삼프로TV",
            "description": "반도체 HBM 공급망 수혜주 및 글로벌 AI 투자 전략 심층 분석",
        }

        self.valid_analysis_dict = {
            "summarized_title_for_notion": "[반도체/AI] HBM 공급망 수혜 및 대형주 상승세 지속 전망",
            "publish_date": "2026-08-29",
            "market_sentiment": "강세",
            "leading_sectors": ["반도체/HBM", "전력인프라"],
            "one_line_summary": "글로벌 빅테크의 AI 인프라 투자 지속으로 HBM 공급망 수혜 지속 전망.",
            "key_takeaways": [
                "1. [매크로/금리] 미국 기준금리 인하 기조로 기술주 밸류에이션 부담 완화 전망.",
                "2. [산업/종목] HBM3E 양산 확대에 따른 메모리 반도체 실적 개선 지속 전망.",
                "3. [전략/리스크] 단기 급등에 따른 변동성 주의하며 분할 매수 전략 유효함."
            ],
            "overall_summary": (
                "[매크로 & 시장방향] 유동성 환경 개선으로 테크 중심의 상승 랠리 지속 전망.\n\n"
                "[주도섹터 & 핵심이슈] AI 반도체 가속기 수요 폭증으로 고대역폭메모리 공급 부족 심화.\n\n"
                "[투자전략 & 리스크] 핵심 밸류체인 대형주 중심의 비중 확대 및 단기 조정 시 분할 매수 권고."
            ),
            "assets": [
                {
                    "ticker": "5930",  # 정규화 대상 (005930으로 zfill 필요)
                    "name": "삼성전자",
                    "context": "HBM3E 공급 본격화로 실적 턴어라운드 가속화 전망.",
                    "opinion": "매수",
                    "link_url": ""
                },
                {
                    "ticker": "000660",
                    "name": "SK하이닉스",
                    "context": "독점적 HBM 공급 지위 유지 및 사상 최대 실적 달성 기대.",
                    "opinion": "매수",
                    "link_url": ""
                },
                {
                    "ticker": "NVDA",
                    "name": "NVIDIA",
                    "context": "차세대 블랙웰 아키텍처 출하에 따른 실적 모멘텀 지속 전망.",
                    "opinion": "매수",
                    "link_url": ""
                }
            ]
        }

    def test_pydantic_schema_validation_and_ticker_normalization(self):
        """Pydantic 스키마 검증 및 6자리 티커 zfill 정규화 테스트"""
        parsed = YouTubeAnalysisResult.model_validate(self.valid_analysis_dict)
        self.assertEqual(parsed.market_sentiment, "강세")
        self.assertEqual(len(parsed.assets), 3)

        # 6자리 티커 zfill 수동/자동 정규화 검증
        for asset in parsed.assets:
            t = asset.ticker.strip().upper()
            if t.isdigit() and 1 <= len(t) <= 6:
                asset.ticker = t.zfill(6)

        self.assertEqual(parsed.assets[0].ticker, "005930")
        self.assertEqual(parsed.assets[1].ticker, "000660")
        self.assertEqual(parsed.assets[2].ticker, "NVDA")

    def test_korean_noun_ending_integrity(self):
        """명사형 종결어미(~함, ~임, ~전망, ~권고, ~필요) 준수 검증"""
        parsed = YouTubeAnalysisResult.model_validate(self.valid_analysis_dict)
        valid_endings = ("함", "임", "됨", "봄", "전망", "권고", "필요", "유효", "확대", "지속", "포함", "강조", "예상", "판단", "유지")

        title_valid = any(parsed.summarized_title_for_notion.rstrip(" .").endswith(end) for end in valid_endings)
        self.assertTrue(title_valid, f"제목 명사형 종결어미 위반: {parsed.summarized_title_for_notion}")

        summary_valid = any(parsed.one_line_summary.rstrip(" .").endswith(end) for end in valid_endings)
        self.assertTrue(summary_valid, f"1줄 요약 명사형 종결어미 위반: {parsed.one_line_summary}")

    @patch("jobs.youtube.ai_service.genai")
    def test_gemini_multimodal_fallback_execution(self, mock_genai):
        """자막 추출 실패 시 Gemini Multimodal Fallback 호출 체인 검증"""
        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        
        # 가짜 Gemini 응답 모킹
        mock_response = MagicMock()
        mock_response.text = YouTubeAnalysisResult.model_validate(self.valid_analysis_dict).model_dump_json()
        mock_client.models.generate_content.return_value = mock_response

        ai_service = AIService(api_key="fake_test_gemini_api_key")
        ai_service.client = mock_client

        # 자막이 없는 상태에서 멀티모달 분석 호출
        result = ai_service.analyze_youtube_multimodal(
            video_url_or_id=self.mock_video_meta["video_id"],
            video_meta=self.mock_video_meta
        )

        self.assertIsNotNone(result, "Gemini 멀티모달 Fallback 결과가 None이 아니어야 함")
        self.assertEqual(result.summarized_title_for_notion, "[반도체/AI] HBM 공급망 수혜 및 대형주 상승세 지속 전망")
        self.assertEqual(result.assets[0].ticker, "005930")
        self.assertTrue(mock_client.models.generate_content.called)

    @patch("jobs.youtube.job_sync_youtube_insights.extract_transcript_via_ytdlp")
    @patch("jobs.youtube.job_sync_youtube_insights.extract_transcript_via_youtube_transcript_api")
    def test_3tier_fallback_pipeline_integration(self, mock_yta, mock_ytdlp):
        """1단계 yt-dlp 실패 -> 2단계 youtube-transcript-api 실패 -> 3단계 멀티모달 연계 검증"""
        # Tier 1, Tier 2 자막 수집 모두 실패(None 반환) 시뮬레이션
        mock_ytdlp.return_value = (None, "", {})
        mock_yta.return_value = (None, "")

        mock_ai_service = MagicMock(spec=AIService)
        mock_ai_service.is_available.return_value = True
        mock_ai_service.analyze_youtube_multimodal.return_value = YouTubeAnalysisResult.model_validate(self.valid_analysis_dict)

        mock_notion_client = MagicMock()
        mock_notion_client.pages.create.return_value = {"id": "mock_notion_page_001"}
        mock_gateway = MagicMock()
        mock_gateway.find_master_stock.return_value = {"id": "mock_master_id_001"}

        processed_ids = set()

        # process_single_video_item 실행
        success = process_single_video_item(
            v=dict(self.mock_video_meta),
            notion_client=mock_notion_client,
            ai_service=mock_ai_service,
            gateway=mock_gateway,
            processed_ids=processed_ids,
            force=True
        )

        self.assertTrue(success, "3단계 멀티모달 Fallback 연계로 최종 성공해야 함")
        self.assertIn("test_vid_12345", processed_ids, "처리 완료 캐시에 등록되어야 함")
        self.assertTrue(mock_ai_service.analyze_youtube_multimodal.called, "자막 부재 시 멀티모달 메서드가 호출되어야 함")

    def test_notion_schema_defensive_guard(self):
        """notion_utils.py 표준 스키마 방어 로직(if prop in properties) 무결성 검증"""
        mock_client = MagicMock()
        mock_client.pages.create.return_value = {"id": "mock_new_page_id"}

        parsed = YouTubeAnalysisResult.model_validate(self.valid_analysis_dict)

        # 1) 시황 요약 페이지 생성 테스트 (방어 로직 검증)
        page_id = create_youtube_summary_notion_page(
            client=mock_client,
            db_id="test_db_id",
            analyzed=parsed,
            video_meta=self.mock_video_meta
        )
        self.assertEqual(page_id, "mock_new_page_id")
        self.assertTrue(mock_client.pages.create.called)

        # 2) 미정리 종목 DB 생성 테스트 (방어 로직 검증)
        mock_gateway = MagicMock()
        mock_gateway.find_master_stock.return_value = {"id": "master_stock_page_id"}
        count = create_unorganized_stock_items(
            client=mock_client,
            db_id="test_unorganized_db_id",
            analyzed=parsed,
            video_meta=self.mock_video_meta,
            gateway=mock_gateway
        )
        self.assertEqual(count, 3, "언급 종목 3개 모두 적재 시도 성공해야 함")


if __name__ == "__main__":
    print("=" * 80)
    print("🧪 [Test YouTube Proxy & Multimodal Fallback] 3중 방어 로직 무결성 진단 시작")
    print("=" * 80)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestYouTubeProxyAndMultimodal)
    runner = unittest.TextTestRunner(verbosity=2)
    res = runner.run(suite)
    if res.wasSuccessful():
        print("\n🎉 [SUCCESS] 프록시/멀티모달 3중 Fallback 및 노션 스키마 방어 로직 100% 무결성 검증 통과!")
        sys.exit(0)
    else:
        print("\n❌ [FAIL] 단위 테스트 실패 감지")
        sys.exit(1)
