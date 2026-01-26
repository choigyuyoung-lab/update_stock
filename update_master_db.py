import os
import re
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, Tuple

import requests
import yfinance as yf
from bs4 import BeautifulSoup
from notion_client import Client
from duckduckgo_search import DDGS

# ---------------------------------------------------------
# 1. 전역 설정 및 시니어급 로깅
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

# 환경 변수 문자열을 불리언으로 변환
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"
MAX_WORKERS = 2 
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

YAHOO_SECTOR_MAP = {
    "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재", "Communication Services": "통신 서비스",
    "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
    "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
}

class StockCrawlerOptimizer:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})

    def _clean_text(self, text: str) -> str:
        if not text: return ""
        cleaned = re.sub(r'\[.*?\]', '', text).strip()
        return "" if cleaned in ["정보 없음", "정보없음"] else cleaned

    def verify_ticker_hybrid(self, ticker: str) -> Tuple[str, str]:
        """[하이브리드 검증] DDG 우선, 구글 API 백업"""
        query = f"{ticker} stock"
        # 1. DDG 검증
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=3))
                if results and any(ticker.lower() in r.get('title', '').lower() for r in results):
                    return "PASS", "(DDG) +검증됨"
        except Exception: pass

        # 2. 구글 백업
        if not GOOGLE_API_KEY: return "SKIP", "(DDG) 지연"
        try:
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 3}
            res = self.session.get(url, params=params, timeout=5)
            if res.status_code in [429, 403]: return "SKIP", "(GOO) 할당량초과"
            items = res.json().get('items', [])
            if any(ticker.lower() in item.get('title', '').lower() for item in items):
                return "PASS", "(GOO) +검증됨"
            return "FAIL", "(GOO) 검증실패"
        except: return "SKIP", "(검증에러)"

    def fetch_wiki_url(self, ticker: str, is_korea: bool) -> Optional[str]:
        exchange = "KRX" if is_korea else "NASDAQ"
        url = f"https://www.google.com/finance/quote/{ticker}:{exchange}?hl=ko"
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'), string=re.compile(r'Wikipedia'))
            return wiki_link.get('href') if wiki_link else None
        except: return None

    def fetch_wiki_details(self, url: str) -> Dict[str, str]:
        data = {"wiki_industry": "", "wiki_service": ""}
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            infobox = soup.select_one('table.vcard, table.infobox')
            if infobox:
                for row in infobox.find_all('tr'):
                    th, td = row.find('th'), row.find('td')
                    if th and td:
                        lbl, val = th.get_text(strip=True), self._clean_text(td.get_text(separator=' ', strip=True))
                        if '산업 분야' in lbl: data["wiki_industry"] = val
                        elif '서비스' in lbl: data["wiki_service"] = val
        except: pass
        return data

    def get_data(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        ticker = raw_ticker.strip().upper()
        base = ticker.split('.')[0]
        is_korea = (len(base) == 6) or ticker.endswith(('.KS', '.KQ'))
        
        try:
            if is_korea:
                url = f"https://finance.naver.com/item/main.naver?code={base}"
                res = self.session.get(url, timeout=10); res.encoding = res.apparent_encoding
                soup = BeautifulSoup(res.text, 'html.parser')
                name = soup.select_one('.wrap_company h2 a').get_text(strip=True)
                industry = soup.select_one('div.section.trade_compare h4 em a').get_text(strip=True) if soup.select_one('div.section.trade_compare h4 em a') else "ETF"
                data = {"name": name, "industry": industry, "tag": "네이버"}
            else:
                stock = yf.Ticker(ticker.replace('.', '-'))
                name = stock.info.get('longName') or base
                industry = YAHOO_SECTOR_MAP.get(stock.info.get('sector', ''), stock.info.get('sector', ''))
                data = {"name": name, "industry": industry, "tag": "야후"}

            v_stat, v_log = self.verify_ticker_hybrid(base)
            data.update({"ver_status": v_stat, "ver_log": f"{data['tag']}{v_log}"})
            
            wiki_url = self.fetch_wiki_url(base, is_korea)
            data.update(self.fetch_wiki_details(wiki_url) if wiki_url else {"wiki_industry": "", "wiki_service": ""})
            return data
        except Exception: return None

def process_job(page, crawler, notion):
    try:
        pid, props = page["id"], page["properties"]
        ticker = props.get("티커", {}).get("title", [])[0].get("plain_text", "").strip()
        data = crawler.get_data(ticker)
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        
        if data:
            status = {"PASS": "✅ 검증완료", "SKIP": "⏳ 검증대기", "FAIL": "⚠️ 확인필요"}.get(data['ver_status'], "⚠️ 확인필요")
            upd = {
                "데이터 상태": {"select": {"name": status}},
                "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                "산업 분류": {"rich_text": [{"text": {"content": data['industry']}}]},
                "업데이트 일자": {"date": {"start": now}},
                "검증로그": {"rich_text": [{"text": {"content": data['ver_log']}}]}
            }
            if "산업 분야" in props: upd["산업 분야"] = {"rich_text": [{"text": {"content": data['wiki_industry']}}]}
            if "서비스" in props: upd["서비스"] = {"rich_text": [{"text": {"content": data['wiki_service']}}]}
        else:
            upd = {"데이터 상태": {"select": {"name": "⚠️ 확인필요"}}, "업데이트 일자": {"date": {"start": now}}}
        
        notion.pages.update(page_id=pid, properties=upd)
    except Exception: pass

def main():
    logger.info(f"Automation Start [Full Update: {IS_FULL_UPDATE}]")
    notion, crawler = Client(auth=NOTION_TOKEN), StockCrawlerOptimizer()
    
    cursor = None
    while True:
        # [교정] DatabasesEndpoint의 query 메서드 호출 (표준 문법)
        params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: params["start_cursor"] = cursor
        if not IS_FULL_UPDATE:
            params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
        
        # 100개씩 끊어서 계속 쿼리함 (Pagination)
        response = notion.databases.query(**params)
        pages = response.get("results", [])
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(process_job, page, crawler, notion)
                time.sleep(0.4)
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")
    logger.info("All Jobs Done.")

if __name__ == "__main__":
    main()
