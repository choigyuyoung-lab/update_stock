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
    """
    네이버(한국) HTML 파싱 + 야후(미국) API 하이브리드 크롤러
    """
    def __init__(self):
        self.headers = {'User-Agent': USER_AGENT}

    def fetch_naver_crawling(self, ticker):
        """
        [1순위] 한국 주식: 네이버 금융 PC페이지 HTML 크롤링
        """
        try:
            url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            res = requests.get(url, headers=self.headers, timeout=10)
            
            # 인코딩 자동 감지 (한글 깨짐 방지)
            res.encoding = res.apparent_encoding 

            if res.status_code != 200: return None
            
            # HTML 파싱
            soup = BeautifulSoup(res.text, 'html.parser')

            # 1. 종목명
            name_tag = soup.select_one('.wrap_company h2 a')
            if not name_tag: return None 
            name = name_tag.text.strip()

            # 2. 산업분류
            industry = "한국증시"
            try:
                ind_tag = soup.select_one('div.section.trade_compare h4 em a')
                if ind_tag:
                    industry = ind_tag.text.strip()
            except: pass

            # 3. 회사개요
            summary = ""
            summary_div = soup.select_one('#summary_info p')
            if summary_div:
                summary = summary_div.text.strip()
            
            return {
                "name": name,
                "industry": industry,
                "summary": summary,
                "source": "네이버 정보" # [요청] 문장부호 없는 텍스트
            }

        except Exception:
            pass
        return None

    def fetch_yahoo(self, ticker):
        """
        [2순위] 미국 주식: yfinance 사용
        """
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
                "source": "야후 정보" # [요청] 문장부호 없는 텍스트
            }
        except Exception:
            pass
        return None

    def get_data(self, ticker):
        raw_ticker = ticker.strip().upper()
        
        # -----------------------------------------------------
        # [핵심 로직 수정] 한국/미국 판별 및 접미어 처리
        # -----------------------------------------------------
        is_korea = False
        search_code = raw_ticker

        # 1. 한국 주식 판별
        # 조건: 숫자로 시작하는 6자리 코드 (예: 005930, 0057H0)
        # 0057H0 처럼 영어가 섞여 있어도 첫 글자가 숫자이고 길이가 6이면 한국 주식으로 처리
        if len(raw_ticker) == 6 and raw_ticker[0].isdigit():
            is_korea = True
            search_code = raw_ticker
        
        # 조건: 접미어(.KS, .KQ)가 붙어있는 경우
        elif raw_ticker.endswith('.KS') or raw_ticker.endswith('.KQ'):
            is_korea = True
            search_code = raw_ticker.split('.')[0]

        # 2. 데이터 분기
        if is_korea:
            # 한국 주식 -> 네이버 크롤링
            return self.fetch_naver_crawling(search_code)
        else:
            # 미국 주식 -> 접미어 제거 후 야후 검색
            # [요청사항] 접미어가 있는 경우 삭제 (예: AAPL.O -> AAPL)
            if '.' in raw_ticker:
                search_code = raw_ticker.split('.')[0]
            
            return self.fetch_yahoo(search_code)

def main():
    print(f"🚀 [Master DB] 전체 종목 업데이트 시작")
    
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
            # 전체 검색 (필터 없음)
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "page_size": 50
            }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 데이터베이스가 비어있습니다.")
                break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS:
                    continue

                print(f"🔍 업데이트 중: {raw_ticker} ...")
                
                # 데이터 수집
                data = crawler.get_data(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    status = "✅ 검증완료"
                    log_msg = data['source'] # "네이버 정보" or "야후 정보"
                    
                    # 요약본 길이 제한
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
                        print(f"   └ 완료 {data['name']} {log_msg}")
                    else:
                        print(f"   └ 완료 {data['name']} 개요열없음")
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
            
    print(f"🏁 전체 업데이트 완료: 총 {processed_count}건")

if __name__ == "__main__":
    main()
