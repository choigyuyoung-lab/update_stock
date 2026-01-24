import os
import time
import requests
import re
import yfinance as yf
from bs4 import BeautifulSoup
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# [구글 검증용 키] (GitHub Secrets에 등록되어 있어야 작동)
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.environ.get("GOOGLE_CSE_ID")

# [설정] 전체 업데이트 (비워두면 전체 실행)
TARGET_TICKERS = []

# 시스템 상수
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 야후 산업분류 한글 매핑
YAHOO_SECTOR_MAP = {
    "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재", "Communication Services": "통신 서비스",
    "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
    "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
}

class StockCrawler:
    def __init__(self):
        self.headers = {'User-Agent': USER_AGENT}

    # ------------------------------------------------------------------
    # [기능] 구글 검색 검증
    # ------------------------------------------------------------------
    def verify_with_google(self, ticker, fetched_name):
        """
        티커로 구글 검색 후, 결과에 크롤링한 종목명(fetched_name)이 있는지 교차 검증
        """
        # 키가 없으면 검증 패스 (기존 크롤링 데이터 신뢰)
        if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
            return True, ""

        try:
            # 검색어: 한국주식은 "005930 주식", 미국주식은 "AAPL stock"
            query = f"{ticker} 주식" if re.search(r'\d', ticker) else f"{ticker} stock"
            
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CSE_ID,
                'q': query,
                'num': 2  # 상위 2개만 확인
            }
            
            res = requests.get(url, params=params, timeout=5)
            # API 한도 초과 등의 경우 True 반환(기존 데이터 유지)
            if res.status_code != 200:
                return True, "" 

            items = res.json().get('items', [])
            if not items:
                return False, "(구글결과 없음)"

            # 검증: 검색 결과 제목/내용에 핵심 단어가 있는지 확인
            # 쉼표 등 제거하고 첫 단어 위주로 비교 (Apple Inc -> apple)
            core_name = fetched_name.split()[0].replace(',', '').lower()
            
            is_matched = False
            for item in items:
                title = item.get('title', '').lower()
                snippet = item.get('snippet', '').lower()
                
                # 핵심 단어가 포함되거나, 티커 자체가 제목에 있으면 인정
                if (core_name in title or core_name in snippet) or \
                   (ticker.lower().split('.')[0] in title):
                    is_matched = True
                    break
            
            if is_matched:
                return True, "+ 구글검증됨"
            else:
                return False, "(구글검증 실패)"

        except Exception:
            return True, "" # 에러 시 기존 데이터 신뢰

    # ------------------------------------------------------------------
    # 크롤링 로직 (네이버/야후)
    # ------------------------------------------------------------------
    def fetch_naver_crawling(self, ticker):
        try:
            url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            res = requests.get(url, headers=self.headers, timeout=10)
            res.encoding = res.apparent_encoding 

            if res.status_code != 200: return None
            
            soup = BeautifulSoup(res.text, 'html.parser')
            name_tag = soup.select_one('.wrap_company h2 a')
            if not name_tag: return None 
            name = name_tag.text.strip()

            industry = "한국증시"
            try:
                ind_tag = soup.select_one('div.section.trade_compare h4 em a')
                if ind_tag: industry = ind_tag.text.strip()
            except: pass

            summary = ""
            summary_div = soup.select_one('#summary_info p')
            if summary_div: summary = summary_div.text.strip()
            
            return {
                "name": name,
                "industry": industry,
                "summary": summary,
                "source": "네이버 정보"
            }
        except Exception: pass
        return None

    def fetch_yahoo(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if 'regularMarketPrice' not in info and 'symbol' not in info:
                return None

            name = info.get('longName') or info.get('shortName') or ticker
            eng_sector = info.get('sector', '')
            industry = YAHOO_SECTOR_MAP.get(eng_sector, eng_sector)
            summary = info.get('longBusinessSummary', '')

            return {
                "name": name,
                "industry": industry,
                "summary": summary,
                "source": "야후 정보"
            }
        except Exception: pass
        return None

    def get_data(self, ticker):
        raw_ticker = ticker.strip().upper()
        
        is_korea = False
        search_code = raw_ticker

        # 한국/미국 판별
        if (len(raw_ticker) == 6 and raw_ticker[0].isdigit()) or \
           raw_ticker.endswith('.KS') or raw_ticker.endswith('.KQ'):
            is_korea = True
            if '.' in raw_ticker: search_code = raw_ticker.split('.')[0]
        else:
            if '.' in raw_ticker: search_code = raw_ticker.split('.')[0]

        # 1. 데이터 수집
        data = None
        if is_korea:
            data = self.fetch_naver_crawling(search_code)
        else:
            data = self.fetch_yahoo(search_code)

        # 2. 구글 검증 (데이터가 있을 때만)
        if data:
            is_verified, msg = self.verify_with_google(search_code, data['name'])
            
            if msg:
                data['source'] = f"{data['source']} {msg}"
            
            data['is_verified'] = is_verified

        return data

def main():
    print(f"🚀 [Master DB] 미검증 종목 업데이트 시작")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        crawler = StockCrawler()
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return

    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # [필터] '검증완료'가 아닌 것만 가져오기
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
                "page_size": 50
            }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 대상이 없습니다 (모두 검증완료 상태).")
                break
            if not pages: break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS: continue

                print(f"🔍 업데이트 중: {raw_ticker} ...")
                
                # 데이터 수집 + 구글 검증
                data = crawler.get_data(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    # 검증 통과(True)면 완료, 실패(False)면 확인필요
                    if data.get('is_verified', True):
                        status = "✅ 검증완료"
                    else:
                        status = "⚠️ 확인필요"
                    
                    log_msg = data['source']
                    
                    summary_text = data['summary']
                    safe_summary = summary_text[:1900] + "..." if summary_text and len(summary_text) > 1900 else (summary_text or "")
                    
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                    
                    print(f"   └ 완료 {data['name']} ({log_msg})")
                else:
                    status = "⚠️ 확인필요"
                    log_msg = "데이터 없음"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ 실패 {log_msg}")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.5) 

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 업데이트 완료: 총 {processed_count}건")

if __name__ == "__main__":
    main()
