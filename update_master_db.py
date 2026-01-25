import os
import time
import requests
import re
import yfinance as yf
from bs4 import BeautifulSoup
from notion_client import Client
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

TARGET_TICKERS = []
IS_FULL_UPDATE = True 
MAX_WORKERS = 2  # 안정성을 위해 2개로 조정

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
YAHOO_SECTOR_MAP = {
    "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재", "Communication Services": "통신 서비스",
    "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
    "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
}

class StockCrawler:
    def __init__(self):
        self.headers = {'User-Agent': USER_AGENT}

    # [3단계] 구글 검색 검증 (100건 제한 대응)
    def verify_with_google(self, ticker, fetched_name):
        if not GOOGLE_API_KEY or not GOOGLE_CX:
            return "SKIP", "(API키 없음/건너뜀)"
        try:
            query = f"{ticker} 주식" if re.search(r'\d', ticker) else f"{ticker} stock"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 2}
            res = requests.get(url, params=params, timeout=5)
            if res.status_code in [429, 403]: return "SKIP", f"(할당량 초과: {res.status_code})"
            if res.status_code != 200: return "SKIP", f"(구글 에러 {res.status_code})"
            items = res.json().get('items', [])
            if not items: return "FAIL", "(결과 없음)"
            core_name = fetched_name.split()[0].replace(',', '').lower()
            is_matched = any(core_name in item.get('title', '').lower() for item in items)
            return ("PASS", "+ 구글검증됨") if is_matched else ("FAIL", "(검증 실패)")
        except: return "SKIP", "(검증 에러)"

    # [4단계] 한글 위키백과 상세 크롤링
    def fetch_wikipedia_data(self, company_name):
        clean_name = company_name.replace('(주)', '').strip()
        url = f"https://ko.wikipedia.org/wiki/{clean_name}"
        try:
            res = requests.get(url, headers=self.headers, timeout=10)
            if res.status_code != 200: return "정보 없음", "정보 없음"
            soup = BeautifulSoup(res.text, 'html.parser')
            infobox = soup.select_one('table.vcard, table.infobox')
            wiki_industry, wiki_service = "정보 없음", "정보 없음"
            if infobox:
                for row in infobox.find_all('tr'):
                    th, td = row.find('th'), row.find('td')
                    if th and td:
                        th_text = th.get_text(strip=True)
                        td_text = re.sub(r'\[.*?\]', '', td.get_text(separator=' ', strip=True))
                        if '산업 분야' in th_text: wiki_industry = td_text
                        elif '서비스' in th_text: wiki_service = td_text
            return wiki_industry, wiki_service
        except: return "정보 없음", "정보 없음"

    def fetch_naver_crawling(self, ticker):
        try:
            url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            res = requests.get(url, headers=self.headers, timeout=10)
            res.encoding = res.apparent_encoding 
            soup = BeautifulSoup(res.text, 'html.parser')
            name = soup.select_one('.wrap_company h2 a').text.strip()
            industry = soup.select_one('div.section.trade_compare h4 em a').text.strip() if soup.select_one('div.section.trade_compare h4 em a') else "ETF"
            wiki_ind, wiki_srv = self.fetch_wikipedia_data(name)
            return {"name": name, "industry": industry, "wiki_industry": wiki_ind, "service": wiki_srv, "source": "네이버+위키"}
        except: return None

    def fetch_yahoo(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            name = info.get('longName') or info.get('shortName') or ticker
            industry = YAHOO_SECTOR_MAP.get(info.get('sector', ''), info.get('sector', ''))
            wiki_ind, wiki_srv = self.fetch_wikipedia_data(name)
            return {"name": name, "industry": industry, "wiki_industry": wiki_ind, "service": wiki_srv, "source": "야후+위키"}
        except: return None

    def get_data(self, ticker):
        raw_ticker = ticker.strip().upper()
        is_korea = (len(raw_ticker) == 6 and raw_ticker[0].isdigit()) or raw_ticker.endswith(('.KS', '.KQ'))
        search_code = raw_ticker.split('.')[0]
        data = self.fetch_naver_crawling(search_code) if is_korea else self.fetch_yahoo(search_code)
        if data:
            v_status, msg = self.verify_with_google(search_code, data['name'])
            data['ver_status'], data['source'] = v_status, f"{data['source']} {msg}"
        return data

def process_page(page, crawler, notion):
    """개별 페이지 업데이트를 위한 스레드 작업"""
    try:
        page_id, props = page["id"], page["properties"]
        ticker_list = props.get("티커", {}).get("title", [])
        if not ticker_list: return
        raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
        
        data = crawler.get_data(raw_ticker)
        # ISO 8601 형식으로 시간 기록
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        
        if data:
            v_stat = data.get('ver_status', 'SKIP')
            status = "✅ 검증완료" if v_stat == "PASS" else ("⏳ 검증대기" if v_stat == "SKIP" else "⚠️ 확인필요")
            final_industry = data['wiki_industry'] if data['wiki_industry'] != "정보 없음" else data['industry']
            
            upd_props = {
                "데이터 상태": {"select": {"name": status}},
                "검증로그": {"rich_text": [{"text": {"content": data['source']}}]},
                "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                "산업분류": {"rich_text": [{"text": {"content": final_industry}}]},
                "업데이트 일자": {"date": {"start": now_iso}}
            }
            if "서비스" in props:
                upd_props["서비스"] = {"rich_text": [{"text": {"content": data['service']}}]}
        else:
            upd_props = {"데이터 상태": {"select": {"name": "⚠️ 확인필요"}}, "업데이트 일자": {"date": {"start": now_iso}}}
        
        notion.pages.update(page_id=page_id, properties=upd_props)
        print(f"✅ {raw_ticker} 업데이트 완료 ({now_iso})")
    except Exception as e:
        print(f"❌ 오류 ({raw_ticker}): {e}")

def main():
    print(f"🚀 [Master DB] 멀티스레딩({MAX_WORKERS} 스레드) 실행 시작")
    try:
        notion, crawler = Client(auth=NOTION_TOKEN), StockCrawler()
    except Exception as e:
        print(f"❌ 초기화 실패: {e}"); return

    query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
    if not IS_FULL_UPDATE:
        query_params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
    
    response = notion.databases.query(**query_params)
    pages = response.get("results", [])
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for page in pages:
            executor.submit(process_page, page, crawler, notion)
            time.sleep(0.4) # 노션 API 안정성을 위한 최소 간격

    print(f"🏁 전체 {len(pages)}건 작업 종료")

if __name__ == "__main__":
    main()
