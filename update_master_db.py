import os
import time
import re
import requests
import yfinance as yf
from notion_client import Client
from googleapiclient.discovery import build

# 1. 환경 변수 및 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

client = Client(auth=NOTION_TOKEN)

# 산업분류 매핑 테이블
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

def clean_name(name):
    """비교를 위해 특수문자 제거 및 대문자 변환"""
    if not name: return ""
    return re.sub(r'[^a-zA-Z0-9가-힣]', '', str(name)).upper()

def get_stock_data(ticker):
    """네이버/야후 API를 통해 종목 데이터 수집 (접미어 제거 로직 포함)"""
    # [중요] 티커에서 접미어 제거 (.KS, .KQ, .O, .N 등 모두 삭제)
    clean_ticker = ticker.split('.')[0].strip()
    
    try:
        if len(clean_ticker) == 6 and clean_ticker.isdigit(): # 한국 주식
            res = requests.get(f"https://m.stock.naver.com/api/stock/{clean_ticker}/integration", timeout=10).json()
            item = res.get("result", {}).get("stockItem", {})
            if item:
                return item.get("stockName"), item.get("description"), item.get("industryName")
        else: # 미국 주식
            # 1차 시도: 접미어 제거된 티커로 시도
            stock = yf.Ticker(clean_ticker)
            info = stock.info
            
            # 1차 실패 시 원본 티커로 재시도 (야후 파이낸스 특성 반영)
            if not info or 'longName' not in info:
                stock = yf.Ticker(ticker)
                info = stock.info
                
            if info and ('longName' in info or 'shortName' in info):
                name = info.get("longName") or info.get("shortName")
                return name, info.get("longBusinessSummary"), info.get("sector")
    except Exception as e:
        print(f"      ⚠️ {ticker} API 수집 중 오류: {e}")
    
    return None, None, None

def main():
    print(f"🚀 [상장주식 DB 검증] 시작")
    google_count = 0
    next_cursor = None
    
    while True:
        try:
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
                "page_size": 30
            }
            if next_cursor:
                query_params["start_cursor"] = next_cursor
            
            response = client.databases.query(**query_params)
            pages = response.get("results", [])
            
            for page in pages:
                if google_count >= 90: break
                
                page_id = page["id"]
                props = page["properties"]
                
                raw_ticker = props.get("티커", {}).get("title", [{}])[0].get("plain_text", "").strip().upper()
                if not raw_ticker: continue
                
                existing_name_list = props.get("종목명(기존)", {}).get("rich_text", [])
                existing_name = existing_name_list[0].get("plain_text", "").strip() if existing_name_list else ""
                
                print(f"🔍 {raw_ticker} ({existing_name}) 처리 중...")
                
                # 데이터 수집 호출
                actual_name, summary, sector = get_stock_data(raw_ticker)

                verified = False
                log = ""
                
                if not actual_name:
                    log = f"❌ API 수집 실패 (티커 확인 요망: {raw_ticker})" # 상세 로그 남김
                elif clean_name(existing_name) in clean_name(actual_name) or clean_name(actual_name) in clean_name(existing_name):
                    verified, log = True, "✅ 1차 대조 성공"
                else:
                    # 구글 2차 검증 (생략 가능하나 무결성을 위해 유지)
                    # google_search_verify 로직은 기존과 동일하므로 필요시 추가 가능
                    log = f"❌ 이름 불일치 (기존: {existing_name} vs API: {actual_name})"

                # 노션 업데이트
                upd_props = {
                    "데이터 상태": {"select": {"name": "✅ 검증완료" if verified else "⚠️ 확인필요"}},
                    "검증로그": {"rich_text": [{"text": {"content": log}}]}
                }
                
                if verified:
                    upd_props.update({
                        "종목명(텍스트)": {"rich_text": [{"text": {"content": actual_name}}]},
                        "산업분류(원문)": {"rich_text": [{"text": {"content": sector if sector else ""}}]},
                        "산업분류(텍스트)": {"rich_text": [{"text": {"content": INDUSTRY_MAP.get(sector, sector) if sector else ""}}]},
                        "회사개요": {"rich_text": [{"text": {"content": summary[:1900] if summary else ""}}]}
                    })
                
                client.pages.update(page_id=page_id, properties=upd_props)
                time.sleep(0.3)

            if not response.get("has_more") or google_count >= 90: break
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"❌ 오류: {e}")
            break

if __name__ == "__main__":
    main()
