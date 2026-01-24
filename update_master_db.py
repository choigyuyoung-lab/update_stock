import os
import time
import re
import requests
import yfinance as yf
from notion_client import Client
from googleapiclient.discovery import build

# [설정] 디버깅하고 싶은 종목 리스트 (접미어 제외하고 입력)
DEBUG_TICKERS = ["005930", "000660", "TSLA", "AAPL", "NVDA"] 

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

client = Client(auth=NOTION_TOKEN)

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

def google_search_verify(ticker, target_name):
    if not GOOGLE_API_KEY or not GOOGLE_CX: return False
    try:
        service = build("customsearch", "v1", developerKey=GOOGLE_API_KEY)
        res = service.cse().list(q=f"{ticker} {target_name} 주식", cx=GOOGLE_CX, num=3).execute()
        items = res.get("items", [])
        combined = "".join([i.get("title", "") + i.get("snippet", "") for i in items])
        return clean_name(target_name) in clean_name(combined)
    except:
        return False

def main():
    print(f"🚀 [상장주식 DB 무결성 검증] 시작 (디버깅 모드 활성화)")
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
                ticker = raw_ticker.split('.')[0]
                existing_name = props.get("종목명(기존)", {}).get("rich_text", [{}])[0].get("plain_text", "").strip()
                
                # [디버깅] 대상 종목인 경우 콘솔에 상세 출력
                is_debug = ticker in DEBUG_TICKERS
                if is_debug:
                    print(f"\n--- 🛠️ DEBUG: {ticker} ---")
                    print(f"   [노션 데이터] 기존이름: {existing_name} / 원본티커: {raw_ticker}")

                try:
                    if len(ticker) == 6 and ticker.isdigit(): # 한국
                        res = requests.get(f"https://m.stock.naver.com/api/stock/{ticker}/integration", timeout=10).json()
                        item = res.get("result", {}).get("stockItem", {})
                        actual_name, summary, sector = item.get("stockName"), item.get("description"), item.get("industryName")
                    else: # 미국
                        info = yf.Ticker(ticker).info
                        actual_name = info.get("longName") or info.get("shortName")
                        summary, sector = info.get("longBusinessSummary"), info.get("sector")

                    if is_debug:
                        print(f"   [수집 데이터] 수집이름: {actual_name}")
                        print(f"   [수집 데이터] 섹터: {sector} / 요약길이: {len(summary) if summary else 0}")

                    verified, log = False, ""
                    if not actual_name:
                        log = "❌ API 데이터 없음"
                    elif clean_name(existing_name) in clean_name(actual_name) or clean_name(actual_name) in clean_name(existing_name):
                        verified, log = True, "✅ 1차 대조 성공"
                    else:
                        google_count += 1
                        if google_search_verify(ticker, existing_name):
                            verified, log = True, "✅ 2차 구글 검증 성공"
                        else:
                            log = f"❌ 불일치(API: {actual_name})"

                    if is_debug: print(f"   [검증 결과] {log}\n")

                    upd = {
                        "데이터 상태": {"select": {"name": "✅ 검증완료" if verified else "⚠️ 확인필요"}},
                        "검증로그": {"rich_text": [{"text": {"content": f"[DEBUG] {log}" if is_debug else log}}]}
                    }
                    if verified:
                        upd.update({
                            "종목명(텍스트)": {"rich_text": [{"text": {"content": actual_name}}]},
                            "산업분류(원문)": {"rich_text": [{"text": {"content": sector}}]},
                            "산업분류(텍스트)": {"rich_text": [{"text": {"content": INDUSTRY_MAP.get(sector, sector)}}]},
                            "회사개요": {"rich_text": [{"text": {"content": summary[:1900] if summary else ""}}]}
                        })
                    client.pages.update(page_id=page_id, properties=upd)
                except Exception as e:
                    if is_debug: print(f"   ❌ DEBUG 에러: {e}")
                    continue

            if not response.get("has_more") or google_count >= 90: break
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"❌ 중단: {e}")
            break
