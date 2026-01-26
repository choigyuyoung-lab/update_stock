import os
import re
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any

import requests
import pandas as pd
import FinanceDataReader as fdr
from bs4 import BeautifulSoup
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 설정 및 로깅 (Python 3.10+ 최적화)
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"
MAX_WORKERS = 4 

class StockAutomationEngine:
    def __init__(self):
        logger.info("📡 마스터 데이터 캐싱 및 정제 로직 준비...")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 시장 데이터 로드 (기존 성공 프로젝트 맥락 반영)
        self.df_kr_desc = fdr.StockListing('KRX-DESC')
        self.df_etf_kr = fdr.StockListing('ETF/KR')
        self.df_nasdaq = fdr.StockListing('NASDAQ')
        self.df_nyse = fdr.StockListing('NYSE')
        try:
            self.df_etf_us = fdr.StockListing('ETF/US')
        except:
            self.df_etf_us = pd.DataFrame()
        
        self.df_us_all = pd.concat([self.df_nasdaq, self.df_nyse], ignore_index=True)

    def clean_ticker_logic(self, raw_ticker: str) -> str:
        """기존 코드의 정제 규칙 완벽 이식: 접미어 제거 및 한국 숫자 추출"""
        ticker = raw_ticker.strip().upper()
        
        # 1. 한국 종목 특화: 숫자 6자리가 포함된 경우 숫자만 추출 (예: A060310 -> 060310)
        kr_match = re.search(r'(\d{6})', ticker)
        if kr_match:
            return kr_match.group(1)
            
        # 2. 접미어(. , -) 제거 규칙 (기존 로직 이식)
        ticker_base = re.split(r'[-.]', ticker)[0]
        
        # 3. 6자리 미만 숫자 보정
        if ticker_base.isdigit() and len(ticker_base) < 6:
            return ticker_base.zfill(6)
            
        return ticker_base

    def fetch_wiki_data(self, google_ticker: str) -> Dict[str, str]:
        """구글 파이낸스를 경유하여 통합 '산업분야'와 '서비스' 수집"""
        res_data = {"ind": "", "svc": ""}
        url = f"https://www.google.com/finance/quote/{google_ticker}?hl=ko"
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'))
            if wiki_link:
                w_res = self.session.get(wiki_link.get('href'), timeout=10)
                w_soup = BeautifulSoup(w_res.text, 'html.parser')
                infobox = w_soup.select_one('table.infobox')
                if infobox:
                    for row in infobox.find_all('tr'):
                        th, td = row.find('th'), row.find('td')
                        if th and td:
                            lbl, val = th.get_text(strip=True), td.get_text(separator=' ', strip=True)
                            if '산업' in lbl: res_data["ind"] = val
                            elif any(x in lbl for x in ['서비스', '제품', '분야']): res_data["svc"] = val
        except: pass
        return res_data

    def _search_lists(self, ticker: str) -> Optional[Dict[str, Any]]:
        """메모리에 로드된 리스트에서 티커 매칭 (한국 -> 미국 순)"""
        # 한국 주식
        t_col = 'Symbol' if 'Symbol' in self.df_kr_desc.columns else 'Code'
        match = self.df_kr_desc[self.df_kr_desc[t_col].astype(str) == ticker]
        if not match.empty:
            row = match.iloc[0]
            wiki = self.fetch_wiki_data(f"{ticker}:KRX")
            return {"origin": "KR", "name": row['Name'], "market": row.get('Market', 'KRX'),
                    "sector": row.get('Industry', row.get('Sector', '주식')),
                    "industry": row.get('Industry', ''), "wiki": wiki}
        
        # 한국 ETF
        match = self.df_etf_kr[self.df_etf_kr['Symbol'].astype(str) == ticker] if 'Symbol' in self.df_etf_kr.columns else pd.DataFrame()
        if not match.empty:
            row = match.iloc[0]
            return {"origin": "KR", "name": row['Name'], "market": "ETF/KR",
                    "sector": "ETF", "industry": row.get('Category', '국내ETF'), "wiki": {"ind": "", "svc": ""}}

        # 미국 주식
        match = self.df_us_all[self.df_us_all['Symbol'].astype(str) == ticker]
        if not match.empty:
            row = match.iloc[0]
            wiki = self.fetch_wiki_data(ticker)
            mkt = "NASDAQ" if ticker in self.df_nasdaq['Symbol'].values else "NYSE"
            return {"origin": "US", "name": row['Name'], "market": mkt,
                    "sector": row.get('Industry', '주식'), "industry": row.get('Industry', ''), "wiki": wiki}
        
        # 미국 ETF
        if not self.df_etf_us.empty:
            match = self.df_etf_us[self.df_etf_us['Symbol'].astype(str) == ticker]
            if not match.empty:
                row = match.iloc[0]
                return {"origin": "US", "name": row['Name'], "market": "ETF/US",
                        "sector": "ETF", "industry": "미국ETF", "wiki": {"ind": "", "svc": ""}}
        return None

    def find_info(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        # 1. 원형 검색 (BRK.B 등 대응)
        result = self._search_lists(raw_ticker.strip().upper())
        if result: return result
        
        # 2. 실패 시 정제 규칙 적용 검색 (기존 로직)
        clean = self.clean_ticker_logic(raw_ticker)
        if clean != raw_ticker.strip().upper():
            return self._search_lists(clean)
        return None

def process_page(notion, engine, page):
    pid = page["id"]
    raw_ticker = page["properties"]["티커"]["title"][0]["plain_text"].strip()
    
    try:
        data = engine.find_info(raw_ticker)
        if not data:
            notion.pages.update(page_id=pid, properties={"검증로그": {"rich_text": [{"text": {"content": "FDR 정보없음"}}]}})
            return

        # 공통 업데이트 (종목명, Market, 산업분야, 서비스 통합)
        upd_props = {
            "종목명": {"rich_text": [{"text": {"content": data["name"]}}]},
            "Market": {"select": {"name": data["market"]}}, 
            "산업분야": {"rich_text": [{"text": {"content": data["wiki"]["ind"]}}]},
            "서비스": {"rich_text": [{"text": {"content": data["wiki"]["svc"]}}]},
            "검증로그": {"rich_text": [{"text": {"content": "FDR확인됨"}}]},
            "데이터 상태": {"select": {"name": "✅ 검증완료"}},
            "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
        }

        # 시장별 상세 정보 분리 기록
        if data["origin"] == "KR":
            upd_props.update({
                "KR_산업": {"rich_text": [{"text": {"content": data["industry"]}}]},
                "KR_섹터": {"rich_text": [{"text": {"content": data["sector"]}}]},
                "US_섹터": {"rich_text": []}, "US_업종": {"rich_text": []}
            })
        else:
            upd_props.update({
                "US_섹터": {"rich_text": [{"text": {"content": data["sector"]}}]},
                "US_업종": {"rich_text": [{"text": {"content": data["industry"]}}]},
                "KR_산업": {"rich_text": []}, "KR_섹터": {"rich_text": []}
            })

        notion.pages.update(page_id=pid, properties=upd_props)
        logger.info(f"SUCCESS: {raw_ticker}")
    except Exception as e:
        logger.error(f"ERROR {raw_ticker}: {e}")

def main():
    notion = Client(auth=NOTION_TOKEN)
    engine = StockAutomationEngine()
    cursor = None
    while True:
        params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: params["start_cursor"] = cursor
        if not IS_FULL_UPDATE:
            params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
        
        response = notion.databases.query(**params)
        pages = response.get("results", [])
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages: executor.submit(process_page, notion, engine, page)
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
