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
        """데이터 부재 시 공백 반환 최적화"""
        if not text: return ""
        cleaned = re.sub(r'\[.*?\]', '', text).strip()
        return "" if cleaned in ["정보 없음", "정보없음"] else cleaned

    def verify_ticker_with_google(self, ticker: str) -> Tuple[str, str]:
        """[교정] 3단계: 구글 API 할당량 초과 여부 정밀 감지"""
        if not GOOGLE_API_KEY or not GOOGLE_CX:
            return "SKIP", "(API 키 없음)"
        try:
            query = f"{ticker} stock"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 3}
            res = self.session.get(url, params=params, timeout=5)
            
            # 할당량 초과 발생 시 (HTTP 429 또는 403 에러 처리)
            if res.status_code == 429:
                return "SKIP", "(API 사용량 초과: 429)"
            if res.status_code == 403:
                # 403 중에서도 할당량 초과 관련 메시지 확인
                if "quotaExceeded" in res.text:
                    return "SKIP", "(API 사용량 초과: 할당량 부족)"
                return "SKIP", f"(API 에러: {res.status_code})"

            items = res.json().get('items', [])
            is_valid = any(ticker.lower() in item.get('title', '').lower() for item in items)
            return ("PASS", "+ 구글검증됨") if is_valid else ("FAIL", "(검증 실패)")
        except Exception as e:
            return "SKIP", f"(검증 중 오류)"

    def fetch_wiki_url_direct(self, ticker: str, is_korea: bool) -> Optional[str]:
        """4단계-1: 구글 파이낸스 직통 링크 사냥"""
        exchange = "KRX" if is_korea else "NASDAQ"
        url = f"https://www.google.com/finance/quote/{ticker}:{exchange}?hl=ko"
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'), string=re.compile(r'Wikipedia'))
            return wiki_link.get('href') if wiki_link else None
        except: return None

    def fetch_wiki_infobox(self, wiki_url: str) -> Dict[str, str]:
        """4단계-2: 위키백과 데이터 추출 (공백 초기화)"""
        data = {"wiki_industry": "", "wiki_service": ""}
        try:
            res = self.session.get(wiki_url, timeout=10)
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

    def get_integrated_data(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        """통합 수집 파이프라인"""
        ticker = raw_ticker.strip().upper()
        base_ticker = ticker.split('.')[0]
        is_korea = (len(base_ticker) == 6) or ticker.endswith(('.KS', '.KQ'))
        
        data = {}
        try:
            if is_korea:
                url = f"https://finance.naver.com/item/main.naver?code={base_ticker}"
                res = self.session.get(url, timeout=10); res.encoding = res.apparent_encoding
                soup = BeautifulSoup(res.text, 'html.parser')
                data['name'] = soup.select_one('.wrap_company h2 a').get_text(strip=True)
                data['industry'] = soup.select_one('div.section.trade_compare h4 em a').get_text(strip=True) if soup.select_one('div.section.trade_compare h4 em a') else "ETF"
                data['source_tag'] = "네이버"
            else:
                y_ticker = ticker.replace('.', '-')
                stock = yf.Ticker(y_ticker)
                data['name'] = stock.info.get('longName') or stock.info.get('shortName') or base_ticker
                data['industry'] = YAHOO_SECTOR_MAP.get(stock.info.get('sector', ''), stock.info.get('sector', ''))
                data['source_tag'] = "야후"
            
            # 3단계: 구글 검증 (할당량 감지 포함)
            v_status, v_msg = self.verify_ticker_with_google(base_ticker)
            data.update({"ver_status": v_status, "ver_log": f"{data['source_tag']} {v_msg}"})
            
            # 4단계: 위키백과 정보 보강 (공백 원칙 적용)
            wiki_url = self.fetch_wiki_url_direct(base_ticker, is_korea)
            wiki_data = self.fetch_wiki_infobox(wiki_url) if wiki_url else {"wiki_industry": "", "wiki_service": ""}
            data.update(wiki_data)
            
            return data
        except Exception as e:
            logger.error(f"Data collection failed for {ticker}: {e}")
            return None

def process_notion_page(page: Dict[str, Any], crawler: StockCrawlerOptimizer, notion: Client):
    try:
        page_id, props = page["id"], page["properties"]
        ticker_rich = props.get("티커", {}).get("title", [])
        if not ticker_rich: return
        raw_ticker = ticker_rich[0].get("plain_text", "").strip()
        
        data = crawler.get_integrated_data(raw_ticker)
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        
        if data:
            status_map = {"PASS": "✅ 검증완료", "SKIP": "⏳ 검증대기", "FAIL": "⚠️ 확인필요"}
            upd_props = {
                "데이터 상태": {"select": {"name": status_map.get(data['ver_status'], "⚠️ 확인필요")}},
                "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                "산업 분류": {"rich_text": [{"text": {"content": data['industry']}}]},
                "업데이트 일자": {"date": {"start": now_iso}},
                "검증로그": {"rich_text": [{"text": {"content": data['ver_log']}}]}
            }
            if "산업 분야" in props: upd_props["산업 분야"] = {"rich_text": [{"text": {"content": data['wiki_industry']}}]}
            if "서비스" in props: upd_props["서비스"] = {"rich_text": [{"text": {"content": data['wiki_service']}}]}
        else:
            upd_props = {"데이터 상태": {"select": {"name": "⚠️ 확인필요"}}, "업데이트 일자": {"date": {"start": now_iso}}}
            
        notion.pages.update(page_id=page_id, properties=upd_props)
        logger.info(f"Updated: {raw_ticker}")
    except Exception as e:
        logger.error(f"Update failed: {e}")

def main():
    logger.info("Starting Notion Stock Automation Optimizer v1.3 (Quota Monitoring)")
    notion, crawler = Client(auth=NOTION_TOKEN), StockCrawlerOptimizer()
    
    start_cursor = None
    processed_total = 0

    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if start_cursor: query_params["start_cursor"] = start_cursor
        
        response = notion.databases.query(**query_params)
        pages = response.get("results", [])
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(process_notion_page, page, crawler, notion)
                time.sleep(0.4)
        
        processed_total += len(pages)
        if not response.get("has_more"): break
        start_cursor = response.get("next_cursor")

    logger.info(f"Automation Job Finished. Total items: {processed_total}")

if __name__ == "__main__":
    main()
