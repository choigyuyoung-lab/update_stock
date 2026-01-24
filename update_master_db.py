import os
import time
import re
import requests
import yfinance as yf
from notion_client import Client
from googleapiclient.discovery import build

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

client = Client(auth=NOTION_TOKEN)

# 산업분류 매핑
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

def clean_name(name):
    if not name: return ""
    return re.sub(r'[^a-zA-Z0-9가-힣]', '', str(name)).upper()

def get_stock_data(ticker):
    """네이버/야후 API 데이터 수집"""
    # 접미어 제거 (.KS, .KQ, .O 등)
    clean_ticker = ticker.split('.')[0].strip()
    
    try:
        if len(clean_ticker) == 6 and clean_ticker.isdigit(): # 한국
            # 네이버 API 헤더 추가 (차단 방지)
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(f"https://m.stock.naver.com/api/stock/{clean_ticker}/integration", headers=headers, timeout=10).json()
            item = res.get("result", {}).get("stockItem", {})
            if item:
                return item.get("stockName"), item.get("industryName")
        else: # 미국
            stock = yf.Ticker(clean_ticker)
            info = stock.info
            # 1차 실패 시 원본 티커로 재시도
            if not info or 'longName' not in info:
                stock = yf.Ticker(ticker)
                info = stock.info
            
            if info and ('longName' in info or 'shortName' in info):
                name = info.get("longName") or info.get("shortName")
                return name, info.get("sector")
    except Exception as e:
        print(f"      ⚠️ {ticker} 수집 에러: {e}")
    
    return None, None

def main():
    print(f"🚀 [상장주식 DB 검증] 시작 - 실제 열 이름 반영 버전")
    google_count = 0
    next_cursor = None
    
    while True:
        try:
            # 필터: '데이터 상태'가 '✅ 검증완료'가 아닌 것
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
                
                # 티커 추출
                raw_ticker = props.get("티커", {}).get("title", [{}])[0].get("plain_text", "").strip().upper()
                if not raw_ticker: continue
                
                # 기존 이름 추출
                existing_name_list = props.get("종목명(기존)", {}).get("rich_text", [])
                existing_name = existing_name_list[0].get("plain_text", "").strip() if existing_name_list else ""
                
                print(f"🔍 {raw_ticker} ({existing_name}) 처리 중...")
                
                # 데이터 수집 (회사개요 제외)
                actual_name, sector = get_stock_data(raw_ticker)

                verified = False
                log = ""
                
                if not actual_name:
                    log = f"❌ API 수집 실패 (티커: {raw_ticker})"
                elif clean_name(existing_name) in clean_name(actual_name) or clean_name(actual_name) in clean_name(existing_name):
                    verified, log = True, "✅ 1차 대조 성공"
                else:
                    # 구글 검색
                    try:
                        if GOOGLE_API_KEY and GOOGLE_CX:
                            service = build("customsearch", "v1", developerKey=GOOGLE_API_KEY)
                            res = service.cse().list(q=f"{raw_ticker} {existing_name} 주식", cx=GOOGLE_CX, num=3).execute()
                            items = res.get("items", [])
                            combined = "".join([i.get("title", "") + i.get("snippet", "") for i in items])
                            if clean_name(existing_name) in clean_name(combined):
                                google_count += 1
                                verified, log = True, "✅ 2차 구글 검증 성공"
                            else:
                                log = f"❌ 불일치 ({actual_name})"
                        else:
                             log = f"❌ 불일치 ({actual_name}) - 구글키 없음"
                    except:
                        log = f"❌ 불일치 ({actual_name}) - 검색 에러"

                # [수정됨] 실제 노션 열 이름('종목명', '산업분류') 사용
                upd_props = {
                    "데이터 상태": {"select": {"name": "✅ 검증완료" if verified else "⚠️ 확인필요"}},
                    "검증로그": {"rich_text": [{"text": {"content": log}}]}
                }
                
                if verified:
                    upd_props.update({
                        "종목명": {"rich_text": [{"text": {"content": actual_name}}]}, # (텍스트) 제거
                        "산업분류": {"rich_text": [{"text": {"content": INDUSTRY_MAP.get(sector, sector) if sector else ""}}]} # (텍스트) 제거
                        # 회사개요는 없으므로 삭제함
                    })
                
                client.pages.update(page_id=page_id, properties=upd_props)
                time.sleep(0.3)

            if not response.get("has_more") or google_count >= 90: break
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            break

if __name__ == "__main__":
    main()
