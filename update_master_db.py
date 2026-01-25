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
# 1. 전역 설정 및 로깅
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

# 제어 변수 (True: 전체 업데이트 / False: 검증완료 항목 제외)
IS_FULL_UPDATE = False 
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

    def verify_ticker_hybrid(self, ticker: str) -> Tuple[str, str]:
        """[하이브리드 검증] DDG 우선 시도 후 실패 시 구글 API 백업"""
        query = f"{ticker} stock"
        
        # 1단계: DuckDuckGo 검증 (무제한)
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=3))
                if results:
                    is_valid = any(ticker.lower() in r.get('title', '').lower() for r in results)
                    if is_valid:
                        return "PASS", "(DDG) +검증됨"
                    # DDG 결과는 있지만 티커가 안 보일 경우 다음 구글 백업으로 넘김
        except Exception as e:
            logger.debug(f"DDG Search failed for {ticker}: {e}")

        # 2단계: 구글 API 백업 (제한적)
        if not GOOGLE_API_KEY or not GOOGLE_CX:
            return "SKIP", "(DDG) 검증지연"

        try:
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 3}
            res = self.session.get(url, params=params, timeout=5)
            
            if res.status_code in [429, 403]:
                return "SKIP", "(GOO) 사용량초과"

            items = res.json().get('items', [])
            is_valid = any(ticker.lower() in item.get('title', '').lower() for item in items)
            return ("PASS", "(GOO) +검증됨") if is_valid else ("FAIL", "(GOO) 검증실패")
        except:
            return "SKIP", "(검증 서버 오류)"

    def fetch_wiki_url_from_google(self, ticker: str, is_korea: bool) -> Optional[str]:
        """구글 파이낸스 직통 Wikipedia 링크 추출"""
        exchange = "KRX" if is_korea else "NASDAQ"
        url = f"https://www.google.com/finance/quote/{ticker}:{exchange}?hl=ko"
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'), string=re.compile(r'Wikipedia'))
            return wiki_link.get('href') if wiki_link else None
        except: return None

    def fetch_wiki_infobox(self, wiki_url: str) -> Dict[str, str]:
        """위키백과 데이터 추출 (데이터 부재 시 공백 유지)"""
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
        ticker_upper = raw_ticker.strip().upper()
        base_ticker = ticker_upper.split('.')[0]
        # 6자리 판별 (영문 혼합 포함)
        is_korea = (len(base_ticker) == 6) or ticker_upper.endswith(('.KS', '.KQ'))
        
        data = {}
        try:
            # 1. 산업 분류 정보 수집 (네이버/야후)
            if is_korea:
                url = f"https://finance.naver.com/item/main.naver?code={base_ticker}"
                res = self.session.get(url, timeout=10); res.encoding = res.apparent_encoding
                soup = BeautifulSoup(res.text, 'html.parser')
                data['name'] = soup.select_one('.wrap_company h2 a').get_text(strip=True)
                data['industry'] = soup.select_one('div.section.trade_compare h4 em a').get_text(strip=True) if soup.select_one('div.section.trade_compare h4 em a') else "ETF"
                data['tag'] = "네이버"
            else:
                y_ticker = ticker_upper.replace('.', '-')
                stock = yf.Ticker(y_ticker)
                data['name'] = stock.info.get('longName') or stock.info.get('shortName') or base_ticker
                data['industry'] = YAHOO_SECTOR_MAP.get(stock.info.get('sector', ''), stock.info.get('sector', ''))
                data['tag'] = "야후"
            
            # 2. 하이브리드 검증 실행
            v_status, v_msg = self.verify_ticker_hybrid(base_ticker)
            data.update({"ver_status": v_status, "ver_log": f"{data['tag']}{v_msg}"})
            
            # 3. 위키백과 정보 보강
            wiki_url = self.fetch_wiki_url_from_google(base_ticker, is_korea)
            wiki_data = self.fetch_wiki_infobox(wiki_url) if wiki_url else {"wiki_industry": "", "wiki_service": ""}
            data.update(wiki_data)
            
            return data
        except Exception as e:
            logger.error(f"Execution Error for {raw_ticker}: {e}")
            return None

def process_page_job(page: Dict[str, Any], crawler: StockCrawlerOptimizer, notion: Client):
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
        logger.info(f"UPDATED: {raw_ticker}")
    except Exception as e:
        logger.error(f"NOTION UPDATE FAIL: {e}")

def main():
    logger.info(f"Stock Automation Hybrid v1.5 [Full Update: {IS_FULL_UPDATE}]")
    notion, crawler = Client(auth=NOTION_TOKEN), StockCrawlerOptimizer()
    
    start_cursor = None
    processed_total = 0

    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if start_cursor: query_params["start_cursor"] = start_cursor
        
        # 선택적 업데이트 필터 적용
        if not IS_FULL_UPDATE:
            query_params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
        
        response = notion.databases.query(**query_params)
        pages = response.get("results", [])
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(process_page_job, page, crawler, notion)
                time.sleep(0.4) # Rate Limit 안전장치
        
        processed_total += len(pages)
        if not response.get("has_more"): break
        start_cursor = response.get("next_cursor")

    logger.info(f"Job Completed. Total Processed: {processed_total}")

if __name__ == "__main__":
    main()
