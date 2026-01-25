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

# GitHub Secrets 이름인 GOOGLE_CX를 그대로 사용
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

# [설정 1] True = 전체 강제 업데이트 (수동 실행용)
# [설정 1] False = '검증완료' 제외하고 업데이트 (스케줄 실행용)
IS_FULL_UPDATE = True 

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
    # [기능] 구글 검색 검증 (3단 상태 반환으로 수정)
    # ------------------------------------------------------------------
    def verify_with_google(self, ticker, fetched_name):
        """
        반환값: (상태코드, 로그메시지)
        - PASS: 검증 성공 (-> ✅ 검증완료)
        - SKIP: 할당량 초과 또는 키 없음 (-> ⏳ 검증대기)
        - FAIL: 검증 실패 (-> ⚠️ 확인필요)
        """
        if not GOOGLE_API_KEY or not GOOGLE_CX:
            return "SKIP", "(API키 없음/건너뜀)"

        try:
            query = f"{ticker} 주식" if re.search(r'\d', ticker) else f"{ticker} stock"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CX, 
                'q': query,
                'num': 2
            }
            
            res = requests.get(url, params=params, timeout=5)
            
            # [설정 2] 할당량 초과(429) 또는 권한 에러(403) 발생 시 -> 검증대기
            if res.status_code in [429, 403]:
                return "SKIP", f"(일일할당량 초과/대기: {res.status_code})"
            
            if res.status_code != 200:
                return "SKIP", f"(구글 에러 {res.status_code})"

            items = res.json().get('items', [])
            if not items:
                return "FAIL", "(구글결과 없음)"

            core_name = fetched_name.split()[0].replace(',', '').lower()
            is_matched = False
            for item in items:
                title = item.get('title', '').lower()
                snippet = item.get('snippet', '').lower()
                
                if (core_name in title or core_name in snippet) or \
                   (ticker.lower().split('.')[0] in title):
                    is_matched = True
                    break
            
            if is_matched:
                return "PASS", "+ 구글검증됨"
            else:
                return "FAIL", "(구글검증 실패)"

        except Exception as e:
            return "SKIP", f"(검증 에러: {str(e)})"

    # ------------------------------------------------------------------
    # 크롤링 로직 (기존 코드 유지)
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

            industry = "ETF"
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

        if (len(raw_ticker) == 6 and raw_ticker[0].isdigit()) or \
           raw_ticker.endswith('.KS') or raw_ticker.endswith('.KQ'):
            is_korea = True
            if '.' in raw_ticker: search_code = raw_ticker.split('.')[0]
        else:
            if '.' in raw_ticker: search_code = raw_ticker.split('.')[0]

        data = None
        if is_korea:
            data = self.fetch_naver_crawling(search_code)
        else:
            data = self.fetch_yahoo(search_code)

        # [수정됨] 검증 로직 호출 시 상태값 처리
        if data:
            v_status, msg = self.verify_with_google(search_code, data['name'])
            data['ver_status'] = v_status # PASS, SKIP, FAIL
            data['source'] = f"{data['source']} {msg}"

        return data

def main():
    mode_msg = "전체 강제 업데이트" if IS_FULL_UPDATE else "미검증 항목만 업데이트"
    print(f"🚀 [Master DB] 시작: {mode_msg}")
    
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
            # 기본 쿼리 파라미터
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "page_size": 50
            }

            # [설정 3] IS_FULL_UPDATE가 False일 때만 '검증완료' 제외 필터 적용
            if not IS_FULL_UPDATE:
                query_params["filter"] = {
                    "property": "데이터 상태", 
                    "select": {"does_not_equal": "✅ 검증완료"}
                }
            
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 대상이 없습니다.")
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
                
                data = crawler.get_data(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    # [설정 2] 상태값 매핑 (PASS->완료, SKIP->대기, FAIL->확인필요)
                    v_stat = data.get('ver_status', 'SKIP')
                    if v_stat == "PASS":
                        status = "✅ 검증완료"
                    elif v_stat == "SKIP":
                        status = "⏳ 검증대기"
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
                    
                    print(f"   └ {status}: {data['name']} ({log_msg})")
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
