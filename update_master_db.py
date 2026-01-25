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

# ---------------------------------------------------------
# 1. 전역 설정 및 로깅
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

MAX_WORKERS = 2  # 안정적인 병렬 처리를 위해 2개 유지
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

YAHOO_SECTOR_MAP = {
    "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재", "Communication Services": "통신 서비스",
    "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
    "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
}

class StockAutomationCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})

    def _clean_text(self, text: str) -> str:
        if not text: return "정보 없음"
        return re.sub(r'\[.*?\]', '', text).strip()

    def fetch_wiki_url_from_google(self, ticker: str, is_korea: bool) -> Optional[str]:
        """[교정] 메서드명 일치화 및 직통 링크 추출"""
        exchange = "KRX" if is_korea else "NASDAQ"
        url = f"https://www.google.com/finance/quote/{ticker}:{exchange}?hl=ko"
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'), string=re.compile(r'Wikipedia'))
            return wiki_link.get('href') if wiki_link else None
        except: return None

    def fetch_wikipedia_details(self, wiki_url: str) -> Dict[str, str]:
        data = {"wiki_industry": "정보 없음", "wiki_service": "정보 없음"}
        try:
            res = self.session.get(wiki_url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            infobox = soup.select_one('table.vcard, table.infobox')
            if infobox:
                for row in infobox.find_all('tr'):
                    th, td = row.find('th'), row.find('td')
                    if th and td:
                        label, value = th.get_text(strip=True), self._clean_text(td.get_text(separator=' ', strip=True))
                        if '산업 분야' in label: data["wiki_industry"] = value
                        elif '서비스' in label: data["wiki_service"] = value
        except: pass
        return data

    def verify_with_google_search(self, ticker: str, name: str) -> Tuple[str, str]:
        if not GOOGLE_API_KEY or not GOOGLE_CX: return "SKIP", "(API Key Missing)"
        try:
            query = f"{ticker} 주식" if re.search(r'\d', ticker) else f"{ticker} stock"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 2}
            res = self.session.get(url, params=params, timeout=5)
            items = res.json().get('items', [])
            match = any(name.split()[0].lower() in item.get('title', '').lower() for item in items)
            return ("PASS", "+ 구글검증됨") if match else ("FAIL", "(검증 실패)")
        except: return "SKIP", "(Verification Error)"

    def get_stock_data(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        ticker = raw_ticker.strip().upper()
        base_ticker = ticker.split('.')[0]
        is_korea = (len(base_ticker) == 6) or ticker.endswith(('.KS', '.KQ'))
        
        data = None
        try:
            if is_korea:
                url = f"https://finance.naver.com/item/main.naver?code={base_ticker}"
                res = self.session.get(url, timeout=10); res.encoding = res.apparent_encoding
                soup = BeautifulSoup(res.text, 'html.parser')
                name = soup.select_one('.wrap_company h2 a').get_text(strip=True)
                industry = soup.select_one('div.section.trade_compare h4 em a').get_text(strip=True) if soup.select_one('div.section.trade_compare h4 em a') else "ETF"
                data = {"name": name, "industry": industry, "source": "네이버"}
            else:
                stock = yf.Ticker(ticker.replace('.', '-'))
                info = stock.info
                name = info.get('longName') or info.get('shortName') or base_ticker
                industry = YAHOO_SECTOR_MAP.get(info.get('sector', ''), info.get('sector', '분류없음'))
                data = {"name": name, "industry": industry, "source": "야후"}
        except: return None

        if data:
            v_status, v_msg = self.verify_with_google_search(base_ticker, data['name'])
            data.update({"ver_status": v_status, "log": f"{data['source']} {v_msg}"})
            wiki_url = self.fetch_wiki_url_from_google(base_ticker, is_korea)
            data.update(self.fetch_wikipedia_details(wiki_url) if wiki_url else {"wiki_industry": "정보 없음", "wiki_service": "정보 없음"})
        return data

def process_page_job(page: Dict[str, Any], crawler: StockAutomationCrawler, notion: Client):
    try:
        page_id, props = page["id"], page["properties"]
        ticker_rich = props.get("티커", {}).get("title", [])
        if not ticker_rich: return
        
        raw_ticker = ticker_rich[0].get("plain_text", "").strip()
        data = crawler.get_stock_data(raw_ticker)
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        
        if data:
            status = {"PASS": "✅ 검증완료", "SKIP": "⏳ 검증대기", "FAIL": "⚠️ 확인필요"}.get(data['ver_status'], "⚠️ 확인필요")
            upd_props = {
                "데이터 상태": {"select": {"name": status}},
                "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                "산업 분류": {"rich_text": [{"text": {"content": data['industry']}}]},
                "업데이트 일자": {"date": {"start": now_iso}},
                "검증로그": {"rich_text": [{"text": {"content": data['log']}}]}
            }
            if "산업 분야" in props: upd_props["산업 분야"] = {"rich_text": [{"text": {"content": data['wiki_industry']}}]}
            if "서비스" in props: upd_props["서비스"] = {"rich_text": [{"text": {"content": data['wiki_service']}}]}
        else:
            upd_props = {"데이터 상태": {"select": {"name": "⚠️ 확인필요"}}, "업데이트 일자": {"date": {"start": now_iso}}}
        
        notion.pages.update(page_id=page_id, properties=upd_props)
        logger.info(f"DONE: {raw_ticker}")
    except Exception as e: logger.error(f"FAIL: {e}")

def main():
    logger.info("Starting Notion Stock Automation Professional v1.1 (Fixed)")
    notion, crawler = Client(auth=NOTION_TOKEN), StockAutomationCrawler()
    
    start_cursor = None
    total_processed = 0

    # [교정] 100개 제한 해결을 위한 무한 루프 (Pagination)
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if start_cursor: query_params["start_cursor"] = start_cursor
        
        response = notion.databases.query(**query_params)
        pages = response.get("results", [])
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(process_page_job, page, crawler, notion)
                time.sleep(0.4)
        
        total_processed += len(pages)
        if not response.get("has_more"): break
        start_cursor = response.get("next_cursor")

    logger.info(f"All jobs completed. Total: {total_processed}")

if __name__ == "__main__":
    main()
