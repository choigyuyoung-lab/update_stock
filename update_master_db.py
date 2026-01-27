import os
import re
import time
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
# 1. 환경 설정 및 로깅
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"
MAX_WORKERS = 2  # 기존 코드 기준 유지

class StockAutomationEngine:
    def __init__(self):
        logger.info("📡 마스터 데이터 캐싱 시작 (FDR 리스팅)...")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 시장 데이터 로드 (KRX-DESC: 산업분류 포함)
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
        """기존 코드의 정제 규칙: 접미어 제거 및 한국 숫자 6자리 추출"""
        ticker = raw_ticker.strip().upper()
        # 1. 한국 종목: 숫자 6자리 포함 시 숫자만 추출
        kr_match = re.search(r'(\d{6})', ticker)
        if kr_match: return kr_match.group(1)
        # 2. 접미어 제거 및 보정
        ticker_base = re.split(r'[-.]', ticker)[0]
        if ticker_base.isdigit() and len(ticker_base) < 6:
            return ticker_base.zfill(6)
        return ticker_base

    def fetch_wiki_data(self, google_ticker: str) -> Dict[str, str]:
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
        # 한국
        kr_col = 'Symbol' if 'Symbol' in self.df_kr_desc.columns else 'Code'
        match = self.df_kr_desc[self.df_kr_desc[kr_col].astype(str) == ticker]
        if not match.empty:
            row = match.iloc[0]
            wiki = self.fetch_wiki_data(f"{ticker}:KRX")
            return {"origin": "KR", "name": row['Name'], "market": row.get('Market', 'KRX'),
                    "sector": row.get('Industry', row.get('Sector', '주식')),
                    "industry": row.get('Industry', ''), "wiki": wiki}
        # 미국 주식
        match = self.df_us_all[self.df_us_all['Symbol'].astype(str) == ticker]
        if not match.empty:
            row = match.iloc[0]
            wiki = self.fetch_wiki_data(ticker)
            mkt = "NASDAQ" if ticker in self.df_nasdaq['Symbol'].values else "NYSE"
            return {"origin": "US", "name": row['Name'], "market": mkt,
                    "sector": row.get('Industry', '주식'), "industry": row.get('Industry', ''), "wiki": wiki}
        return None

    def find_info(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        # 1. 원형 검색 -> 2. 정제 검색
        res = self._search_lists(raw_ticker.strip().upper())
        if not res:
            res = self._search_lists(self.clean_ticker_logic(raw_ticker))
        return res

def process_page(page, engine, notion):
    pid = page["id"]
    props = page["properties"]
    ticker_title = props.get("티커", {}).get("title", [])
    if not ticker_title: return
    raw_ticker = ticker_title[0]["plain_text"].strip()
    
    try:
        data = engine.find_info(raw_ticker)
        if not data:
            notion.pages.update(page_id=pid, properties={"검증로그": {"rich_text": [{"text": {"content": "FDR 정보없음"}}]}})
            return

        now = datetime.now().isoformat()
        # str() 변환으로 int64 에러 방지
        upd = {
            "종목명": {"rich_text": [{"text": {"content": str(data["name"])}}]},
            "검증로그": {"rich_text": [{"text": {"content": "FDR확인됨"}}]},
            "데이터 상태": {"select": {"name": "✅ 검증완료"}},
            "업데이트 일자": {"date": {"start": now}}
        }

        # 열 존재 여부 체크 및 통합 필드 업데이트
        if "Market" in props: upd["Market"] = {"select": {"name": str(data["market"])}}
        if "산업분야" in props: upd["산업분야"] = {"rich_text": [{"text": {"content": str(data["wiki"]["ind"])}}]}
        if "서비스" in props: upd["서비스"] = {"rich_text": [{"text": {"content": str(data["wiki"]["svc"])}}]}

        # 시장별 상세 필드
        if data["origin"] == "KR":
            if "KR_산업" in props: upd["KR_산업"] = {"rich_text": [{"text": {"content": str(data["industry"])}}]}
            if "KR_섹터" in props: upd["KR_섹터"] = {"rich_text": [{"text": {"content": str(data["sector"])}}]}
        else:
            if "US_섹터" in props: upd["US_섹터"] = {"rich_text": [{"text": {"content": str(data["sector"])}}]}
            if "US_업종" in props: upd["US_업종"] = {"rich_text": [{"text": {"content": str(data["industry"])}}]}

        notion.pages.update(page_id=pid, properties=upd)
        logger.info(f"DONE: {raw_ticker}")
    except Exception as e:
        logger.error(f"FAIL {raw_ticker}: {e}")

def main():
    logger.info(f"Automation Start [Full Update: {IS_FULL_UPDATE}]")
    notion, engine = Client(auth=NOTION_TOKEN), StockAutomationEngine()
    cursor = None
    while True:
        params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: params["start_cursor"] = cursor
        
        # 기존의 전체 업데이트 분기 로직 완벽 이식
        if not IS_FULL_UPDATE:
            params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
        
        response = notion.databases.query(**params)
        pages = response.get("results", [])
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(process_page, page, engine, notion)
                time.sleep(0.4) # 기존의 안정적인 슬립 타임 유지
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")
    logger.info("All Jobs Done.")

if __name__ == "__main__":
    main()
