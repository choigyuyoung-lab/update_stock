import os
import time
import re
import requests
import yfinance as yf
from notion_client import Client
from googleapiclient.discovery import build

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

# 노션 클라이언트 초기화
notion = Client(auth=NOTION_TOKEN)

# 산업분류 영-한 매핑 테이블
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

def clean_name(name: str) -> str:
    """비교를 위해 특수문자와 공백을 제거하고 대문자로 변환"""
    if not name: return ""
    return re.sub(r'[^a-zA-Z0-9가-힣]', '', str(name)).upper()

def google_search_verify(ticker: str, target_name: str) -> bool:
    """구글 검색 API를 사용하여 종목명과 티커의 연관성 검증"""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        return False
    try:
        service = build("customsearch", "v1", developerKey=GOOGLE_API_KEY)
        query = f"{ticker} {target_name} 주식"
        res = service.cse().list(q=query, cx=GOOGLE_CX, num=3).execute()
        
        items = res.get("items", [])
        combined_text = "".join([i.get("title", "") + i.get("snippet", "") for i in items])
        
        # 검색 결과 내에 티커와 종목명이 모두 발견되는지 확인
        return clean_name(target_name) in clean_name(combined_text)
    except Exception as e:
        print(f"   ⚠️ Google Search API 에러: {e}")
        return False

def main():
    print(f"🚀 [상장주식 DB 무결성 검증] 시작 (ID: {MASTER_DATABASE_ID})")
    google_api_count = 0
    next_cursor = None
    
    while True:
        # 노션 DB 쿼리 (AttributeError 방지를 위한 명시적 호출)
        query_kwargs = {
            "database_id": MASTER_DATABASE_ID,
            "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
            "page_size": 20
        }
        if next_cursor:
            query_kwargs["start_cursor"] = next_cursor
            
        response = notion.databases.query(**query_kwargs)
        pages = response.get("results", [])
        
        for page in pages:
            # 하루 구글 API 무료 한도(100건) 보호를 위해 90건에서 중단
            if google_api_count >= 90:
                print("⚠️ 오늘 자 구글 API 사용 한도에 도달했습니다.")
                return

            page_id = page["id"]
            props = page["properties"]
            
            # 티커 및 기존 이름 추출
            ticker_obj = props.get("티커", {}).get("title", [])
            ticker = ticker_obj[0].get("plain_text", "").strip().upper() if ticker_obj else ""
            
            existing_name_obj = props.get("종목명(기존)", {}).get("rich_text", [])
            existing_name = existing_name_obj[0].get("plain_text", "").strip() if existing_name_obj else ""
            
            if not ticker: continue
            
            print(f"🔍 검증 중: {ticker} ({existing_name})")
            is_kr = len(ticker) == 6 and ticker.isdigit()
            status = "🔍 검색대기"
            log_messages = []
            
            try:
                # 1. API 데이터 수집
                if is_kr:
                    api_res = requests.get(f"https://m.stock.naver.com/api/stock/{ticker}/integration", timeout=10).json()
                    item = api_res.get("result", {}).get("stockItem", {})
                    actual_name = item.get("stockName", "")
                    summary = item.get("description", "")
                    sector_orig = item.get("industryName", "")
                else:
                    stock = yf.Ticker(ticker)
                    actual_name = stock.info.get("longName") or stock.info.get("shortName", "")
                    summary = stock.info.get("longBusinessSummary", "")
                    sector_orig = stock.info.get("sector", "")

                # 2. 교차 검증 로직
                verified = False
                # 1차: 네이버/야후 이름과 노션 이름이 유사한지 확인
                if actual_name and (clean_name(existing_name) in clean_name(actual_name) or clean_name(actual_name) in clean_name(existing_name)):
                    verified = True
                    log_messages.append("✅ 1차 대조 성공")
                else:
                    # 2차: 불일치 시 구글 검색 동원
                    google_api_count += 1
                    if google_search_verify(ticker, existing_name):
                        verified = True
                        log_messages.append("✅ 2차 구글 검증 성공")
                    else:
                        status = "⚠️ 확인필요"
                        log_messages.append(f"❌ 검증 실패: 공식명칭({actual_name})")

                # 3. 노션 데이터 업데이트
                new_props = {
                    "데이터 상태": {"select": {"name": status}},
                    "검증로그": {"rich_text": [{"text": {"content": " | ".join(log_messages)}}]}
                }
                
                if verified:
                    new_props.update({
                        "종목명(텍스트)": {"rich_text": [{"text": {"content": actual_name}}]},
                        "산업분류(원문)": {"rich_text": [{"text": {"content": sector_orig}}]},
                        "산업분류(텍스트)": {"rich_text": [{"text": {"content": INDUSTRY_MAP.get(sector_orig, sector_orig)}}]},
                        "회사개요": {"rich_text": [{"text": {"content": summary[:1900]}}]} # 노션 글자수 제한 안전망
                    })
                
                notion.pages.update(page_id=page_id, properties=new_props)
                
            except Exception as e:
                print(f"   ❌ {ticker} 처리 중 오류: {e}")
            
            time.sleep(0.5) # API 부하 방지

        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print("✅ 오늘 자 작업을 마쳤습니다.")

if __name__ == "__main__":
    main()
