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

# [변경] 구글 API 관련 설정 삭제됨

# [설정] 특정 티커만 테스트하고 싶을 때 채우세요 (비워두면 전체 실행)
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
    # [1] 네이버 해외주식(미국) 데이터 가져오기
    # ------------------------------------------------------------------
    def fetch_naver_us_stock(self, ticker):
        try:
            search_ticker = f"{ticker}.O"
            url = f"https://api.stock.naver.com/stock/{search_ticker}/basic"
            
            res = requests.get(url, headers=self.headers, timeout=5)
            
            if res.status_code != 200:
                url = f"https://api.stock.naver.com/stock/{ticker}/basic"
                res = requests.get(url, headers=self.headers, timeout=5)
                if res.status_code != 200:
                    return None

            data = res.json()
            stock_item = data.get('stockItem', {})
            
            # 1. 종목명 (한글 우선)
            kor_name = stock_item.get('stockName', ticker)
            eng_name = stock_item.get('engStockName', ticker)
            final_name = kor_name if kor_name else eng_name
            
            # 2. 산업분류
            industry_map = stock_item.get('industryCodeType', {})
            industry = industry_map.get('industryGroupKor', "미국주식") 

            # 3. 회사개요
            summary = stock_item.get('corpSummary', "")
            
            return {
                "name": final_name,
                "industry": industry,
                "summary": summary,
                "source": "네이버 해외주식"
            }

        except Exception:
            return None

    # ------------------------------------------------------------------
    # [2] 야후 파이낸스 (백업용)
    # ------------------------------------------------------------------
    def fetch_yahoo(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if 'regularMarketPrice' not in info and 'symbol' not in info: return None

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

    # ------------------------------------------------------------------
    # [3] 네이버 국내주식 크롤링
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

    # ------------------------------------------------------------------
    # [핵심] 데이터 수집 총괄 (하이브리드 + 구글검증 제거)
    # ------------------------------------------------------------------
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
            # [미국] 하이브리드 전략
            naver_data = self.fetch_naver_us_stock(search_code)
            
            is_naver_good = False
            if naver_data:
                if naver_data['summary'] and naver_data['industry'] != "미국주식":
                    is_naver_good = True
            
            if is_naver_good:
                data = naver_data
            else:
                yahoo_data = self.fetch_yahoo(search_code)
                if naver_data and yahoo_data:
                    data = {
                        "name": naver_data['name'],
                        "industry": naver_data['industry'] if naver_data['industry'] != "미국주식" else yahoo_data['industry'],
                        "summary": naver_data['summary'] if naver_data['summary'] else yahoo_data['summary'],
                        "source": "네이버(이름) + 야후(내용)"
                    }
                elif naver_data:
                    data = naver_data
                elif yahoo_data:
                    data = yahoo_data

        # [변경] 구글 검증 로직 완전히 제거됨 (무조건 검증됨으로 처리)
        if data:
            data['is_verified'] = True 

        return data

def main():
    print(f"🚀 [Master DB] 강제 전체 업데이트 모드 (구글검증 OFF)")
    
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
            # [핵심 변경] 필터 제거 -> 모든 데이터베이스 항목을 가져옵니다.
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                # "filter": ... <-- 삭제됨 (모든 데이터를 다시 씀)
                "page_size": 50
            }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 데이터베이스가 비어있습니다.")
                break
            if not pages: break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS: continue

                print(f"🔍 재설정 중: {raw_ticker} ...")
                
                data = crawler.get_data(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    # 구글 검증 없이 무조건 신뢰
                    status = "✅ 검증완료" 
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
                    
                    print(f"   └ 갱신 완료: {data['name']}")
                else:
                    status = "⚠️ 확인필요"
                    log_msg = "데이터 없음"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ 실패: 데이터 없음")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.5) 

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 전체 갱신 완료: 총 {processed_count}건")

if __name__ == "__main__":
    main()
