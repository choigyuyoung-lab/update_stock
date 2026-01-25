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

# 최적화 포인트: 스레드 수 2개 유지 및 세션 재사용
MAX_WORKERS = 2
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

YAHOO_SECTOR_MAP = {
    "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재", "Communication Services": "통신 서비스",
    "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
    "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
}

class StockAutomationCrawler:
    """10년차 이상의 아키텍처: 세션 관리 및 에러 처리가 강화된 크롤러"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})

    def _clean_text(self, text: str) -> str:
        """위키백과 주석 및 불필요한 공백 제거 최적화"""
        if not text: return "정보 없음"
        text = re.sub(r'\[.*?\]', '', text) # 주석 제거
        return text.strip()

    def fetch_wiki_link_from_google(self, ticker: str, is_korea: bool) -> Optional[str]:
        """구글 파이낸스 직통 위키 링크 사냥"""
        exchange = "KRX" if is_korea else "NASDAQ"
        url = f"https://www.google.com/finance/quote/{ticker}:{exchange}?hl=ko"
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            # 텍스트가 'Wikipedia'이고 도메인이 wikipedia인 링크 매칭
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'), string=re.compile(r'Wikipedia'))
            return wiki_link.get('href') if wiki_link else None
        except Exception as e:
            logger.debug(f"Google Finance link fetch failed for {ticker}: {e}")
            return None

    def fetch_wikipedia_details(self, wiki_url: str) -> Dict[str, str]:
        """위키백과 정보 상자에서 산업 분야와 서비스를 정밀 추출"""
        data = {"wiki_industry": "정보 없음", "wiki_service": "정보 없음"}
        try:
            res = self.session.get(wiki_url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            infobox = soup.select_one('table.vcard, table.infobox')
            if infobox:
                for row in infobox.find_all('tr'):
                    th, td = row.find('th'), row.find('td')
                    if th and td:
                        label = th.get_text(strip=True)
                        value = self._clean_text(td.get_text(separator=' ', strip=True))
                        if '산업 분야' in label: data["wiki_industry"] = value
                        elif '서비스' in label: data["wiki_service"] = value
        except Exception as e:
            logger.error(f"Wikipedia data extraction failed: {e}")
        return data

    def verify_with_google_search(self, ticker: str, name: str) -> Tuple[str, str]:
        """3단계: 구글 검색 검증 (API 할당량 관리)"""
        if not GOOGLE_API_KEY or not GOOGLE_CX:
            return "SKIP", "(API Key Missing)"
        
        try:
            query = f"{ticker} 주식" if re.search(r'\d', ticker) else f"{ticker} stock"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 2}
            res = self.session.get(url, params=params, timeout=5)
            
            if res.status_code != 200: return "SKIP", f"(API Error {res.status_code})"
            
            items = res.json().get('items', [])
            core_name = name.split()[0].replace(',', '').lower()
            match = any(core_name in item.get('title', '').lower() for item in items)
            return ("PASS", "+ 구글검증됨") if match else ("FAIL", "(검증 실패)")
        except: return "SKIP", "(Verification Error)"

    def get_stock_data(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        """2단계: 티커 판별 및 데이터 수집 통합 프로세스"""
        ticker = raw_ticker.strip().upper()
        base_ticker = ticker.split('.')[0]
        
        # 최적화: 6자리(알파벳 혼합 포함) 또는 한국 접미사는 KRX로 판별
        is_korea = (len(base_ticker) == 6) or ticker.endswith(('.KS', '.KQ'))
        
        data = None
        try:
            if is_korea:
                # 네이버 금융 섹터 수집
                url = f"https://finance.naver.com/item/main.naver?code={base_ticker}"
                res = self.session.get(url, timeout=10)
                res.encoding = res.apparent_encoding
                soup = BeautifulSoup(res.text, 'html.parser')
                name = soup.select_one('.wrap_company h2 a').get_text(strip=True)
                industry = soup.select_one('div.section.trade_compare h4 em a').get_text(strip=True) if soup.select_one('div.section.trade_compare h4 em a') else "ETF"
                data = {"name": name, "industry": industry, "source": "네이버"}
            else:
                # 야후 파이낸스 섹터 수집
                yahoo_id = ticker.replace('.', '-')
                stock = yf.Ticker(yahoo_id)
                info = stock.info
                name = info.get('longName') or info.get('shortName') or base_ticker
                industry = YAHOO_SECTOR_MAP.get(info.get('sector', ''), info.get('sector', '분류없음'))
                data = {"name": name, "industry": industry, "source": "야후"}
        except Exception as e:
            logger.error(f"Basic data fetch failed for {ticker}: {e}")
            return None

        if data:
            # 3단계: 구글 검증
            v_status, v_msg = self.verify_with_google_search(base_ticker, data['name'])
            data.update({"ver_status": v_status, "log": f"{data['source']} {v_msg}"})
            
            # 4단계: 구글 파이낸스 직통 링크 기반 위키백과 수집
            wiki_url = self.fetch_wiki_url_from_google(base_ticker, is_korea)
            if wiki_url:
                data.update(self.fetch_wikipedia_details(wiki_url))
            else:
                data.update({"wiki_industry": "정보 없음", "wiki_service": "정보 없음"})
        
        return data

def process_page_job(page: Dict[str, Any], crawler: StockAutomationCrawler, notion: Client):
    """멀티스레딩 단위 작업: 개별 페이지 업데이트"""
    try:
        page_id = page["id"]
        props = page["properties"]
        ticker_rich = props.get("티커", {}).get("title", [])
        if not ticker_rich: return
        
        raw_ticker = ticker_rich[0].get("plain_text", "").strip()
        data = crawler.get_stock_data(raw_ticker)
        
        # 5단계: ISO 8601 시간 기록 (Notion Date 형식 최적화)
        now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        
        if data:
            status_map = {"PASS": "✅ 검증완료", "SKIP": "⏳ 검증대기", "FAIL": "⚠️ 확인필요"}
            status = status_map.get(data['ver_status'], "⚠️ 확인필요")
            
            # 열 매핑 최적화
            upd_props = {
                "데이터 상태": {"select": {"name": status}},
                "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                "산업 분류": {"rich_text": [{"text": {"content": data['industry']}}]}, # 네이버/야후
                "업데이트 일자": {"date": {"start": now_iso}},
                "검증로그": {"rich_text": [{"text": {"content": data['log']}}]}
            }
            if "산업 분야" in props:
                upd_props["산업 분야"] = {"rich_text": [{"text": {"content": data['wiki_industry']}}]}
            if "서비스" in props:
                upd_props["서비스"] = {"rich_text": [{"text": {"content": data['wiki_service']}}]}
        else:
            upd_props = {
                "데이터 상태": {"select": {"name": "⚠️ 확인필요"}},
                "업데이트 일자": {"date": {"start": now_iso}},
                "검증로그": {"rich_text": [{"text": {"content": "수집 실패"}}] }
            }
        
        notion.pages.update(page_id=page_id, properties=upd_props)
        logger.info(f"SUCCESS: {raw_ticker}")
    except Exception as e:
        logger.error(f"PAGE UPDATE ERROR: {e}")

def main():
    logger.info("Starting Notion Stock Automation Optimizer (Professional v1.0)")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        crawler = StockAutomationCrawler()
    except Exception as e:
        logger.error(f"Initialization failed: {e}"); return

    # 1단계: 티커 검색 및 쿼리
    query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
    # 미검증 항목만 필터링 (선택 사항)
    # query_params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
    
    try:
        response = notion.databases.query(**query_params)
        pages = response.get("results", [])
    except Exception as e:
        logger.error(f"Notion Query Error: {e}"); return

    # 멀티스레딩 최적화: 2개 스레드 + 안정적인 간격
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for page in pages:
            executor.submit(process_page_job, page, crawler, notion)
            time.sleep(0.5) # API Rate Limit 안전장치

    logger.info(f"Automation Job Completed. Total processed: {len(pages)}")

if __name__ == "__main__":
    main()
