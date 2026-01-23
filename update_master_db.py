import os, time, re, requests, math
import yfinance as yf
from notion_client import Client
from googleapiclient.discovery import build

# 1. 환경 변수 설정 (GitHub Secrets에 등록 필요)
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

notion = Client(auth=NOTION_TOKEN)

# 산업분류 번역 매핑 테이블
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

def clean_name(name):
    """검증을 위한 이름 정규화 (공백, 특수문자 제거)"""
    if not name: return ""
    return re.sub(r'[^a-zA-Z0-9가-힣]', '', name).upper()

def google_search_verify(ticker, target_name):
    """구글 검색 API를 통한 2차 교차 검증"""
    if not GOOGLE_API_KEY or not GOOGLE_CX: return None
    try:
        service = build("customsearch", "v1", developerKey=GOOGLE_API_KEY)
        query = f"{ticker} {target_name} 주식"
        res = service.cse().list(q=query, cx=GOOGLE_CX, num=3).execute()
        items = res.get("items", [])
        combined_text = "".join([item.get("title", "") + item.get("snippet", "") for item in items])
        return clean_name(target_name) in clean_name(combined_text)
    except Exception as e:
        print(f"   ⚠️ Google Search API Error: {e}")
        return None

def main():
    print(f"🚀 [상장주식 DB 무결성 검증] 시작")
    count = 0
    next_cursor = None
    
    while True:
        # '데이터 상태'가 '✅ 검증완료'가 아닌 항목을 최대 90개까지 가져옴 (API 한도 관리)
        query_params = {
            "database_id": DATABASE_ID,
            "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
            "page_size": 50,
            "start_cursor": next_cursor
        } if next_cursor else {
            "database_id": DATABASE_ID,
            "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
            "page_size": 50
        }
        
        results = notion.databases.query(**query_params)
        
        for page in results.get("results", []):
            if count >= 90: break # 하루 구글 API 한도 준수
            
            page_id = page["id"]
            props = page["properties"]
            
            # 티커 및 기존 이름 추출
            ticker = props.get("티커", {}).get("title", [{}])[0].get("plain_text", "").strip().upper()
            existing_name = props.get("종목명(기존)", {}).get("rich_text", [{}])[0].get("plain_text", "").strip()
            
            if not ticker: continue
            
            print(f"🔍 검증 중: {ticker} ({existing_name})")
            is_kr = len(ticker) == 6 and ticker.isdigit()
            status = "🔍 검색대기"
            log = []
            
            try:
                # 1. API 데이터 수집
                if is_kr:
                    api_url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
                    api_res = requests.get(api_url, timeout=10).json()
                    item = api_res.get("result", {}).get("stockItem", {})
                    actual_name = item.get("stockName")
                    summary = item.get("description", "") # 네이버 요약
                    sector_orig = item.get("industryName", "") # 네이버 업종
                else:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    actual_name = info.get("longName") or info.get("shortName")
                    summary = info.get("longBusinessSummary", "")
                    sector_orig = info.get("sector", "")

                # 2. 3중 검증 로직
                verified = False
                if clean_name(existing_name) in clean_name(actual_name) or clean_name(actual_name) in clean_name(existing_name):
                    verified = True
                    log.append("✅ 1차 대조 성공 (네이버/야후 일치)")
                else:
                    count += 1
                    if google_search_verify(ticker, existing_name):
                        verified = True
                        log.append("✅ 2차 대조 성공 (구글 검색 일치)")
                    else:
                        status = "⚠️ 확인필요"
                        log.append(f"❌ 검증 실패: 실제명({actual_name})과 불일치")

                # 3. 데이터 업데이트
                if verified:
                    status = "✅ 검증완료"
                    upd_props = {
                        "종목명(텍스트)": {"rich_text": [{"text": {"content": actual_name}}]},
                        "산업분류(원문)": {"rich_text": [{"text": {"content": sector_orig}}]},
                        "산업분류(텍스트)": {"rich_text": [{"text": {"content": INDUSTRY_MAP.get(sector_orig, sector_orig)}}]},
                        "회사개요": {"rich_text": [{"text": {"content": summary[:2000]}}]}, # 노션 글자수 제한
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": " | ".join(log)}}]}
                    }
                    notion.pages.update(page_id=page_id, properties=upd_props)
                    print(f"   -> {status} 업데이트 완료")
                else:
                    notion.pages.update(page_id=page_id, properties={
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": " | ".join(log)}}]}
                    })

            except Exception as e:
                print(f"   ❌ 오류 발생: {e}")
            
            time.sleep(0.5)

        if not results.get("has_more") or count >= 90: break
        next_cursor = results.get("next_cursor")

if __name__ == "__main__":
    main()
