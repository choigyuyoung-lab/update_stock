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

# 1. 환경 설정 및 로깅
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
# YML에서 넘겨준 실행 주체에 따른 설정값
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"
MAX_WORKERS = 4 

class StockAutomationEngine:
    def __init__(self):
        logger.info("📡 마스터 데이터 캐싱 중...")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        self.df_kr_desc = fdr.StockListing('KRX-DESC')
        self.df_etf_kr = fdr.StockListing('ETF/KR')
        self.df_nasdaq = fdr.StockListing('NASDAQ')
        self.df_nyse = fdr.StockListing('NYSE')
        try: self.df_etf_us = fdr.StockListing('ETF/US')
        except: self.df_etf_us = pd.DataFrame()
        self.df_us_all = pd.concat([self.df_nasdaq, self.df_nyse], ignore_index=True)

    def clean_ticker(self, raw_ticker: str) -> str:
        """기존 프로젝트 정제 로직 반영: 한국 6자리 숫자 우선 추출"""
        t = raw_ticker.strip().upper()
        kr_match = re.search(r'(\d{6})', t)
        if kr_match: return kr_match.group(1)
        # 미국/글로벌 접미어 처리
        base = re.split(r'[-.]', t)[0]
        if base.isdigit() and len(base) < 6: return base.zfill(6)
        return base

    def fetch_wiki(self, ticker: str) -> Dict[str, str]:
        res_data = {"ind": "", "svc": ""}
        url = f"https://www.google.com/finance/quote/{ticker}?hl=ko"
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

    def _search(self, t: str) -> Optional[Dict[str, Any]]:
        # 한국 매칭
        kr_col = 'Symbol' if 'Symbol' in self.df_kr_desc.columns else 'Code'
        match = self.df_kr_desc[self.df_kr_desc[kr_col].astype(str) == t]
        if not match.empty:
            row = match.iloc[0]
            wiki = self.fetch_wiki(f"{t}:KRX")
            return {"origin": "KR", "name": row['Name'], "market": row.get('Market', 'KRX'),
                    "sector": row.get('Industry', row.get('Sector', '주식')),
                    "industry": row.get('Industry', ''), "wiki": wiki}
        # 미국 매칭
        match = self.df_us_all[self.df_us_all['Symbol'].astype(str) == t]
        if not match.empty:
            row = match.iloc[0]
            wiki = self.fetch_wiki(t)
            mkt = "NASDAQ" if t in self.df_nasdaq['Symbol'].values else "NYSE"
            return {"origin": "US", "name": row['Name'], "market": mkt,
                    "sector": row.get('Industry', '주식'), "industry": row.get('Industry', ''), "wiki": wiki}
        return None

    def find_info(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        res = self._search(raw_ticker.strip().upper())
        if not res: res = self._search(self.clean_ticker(raw_ticker))
        return res

def process_page(page, engine, notion):
    pid, props = page["id"], page["properties"]
    ticker = props["티커"]["title"][0]["plain_text"].strip()
    
    try:
        data = engine.find_info(ticker)
        if not data:
            notion.pages.update(page_id=pid, properties={"검증로그": {"rich_text": [{"text": {"content": "FDR 정보없음"}}]}})
            return

        # 모든 데이터를 str()로 감싸 int64 에러 원천 차단
        upd = {
            "종목명": {"rich_text": [{"text": {"content": str(data["name"])}}]},
            "검증로그": {"rich_text": [{"text": {"content": "FDR확인됨"}}]},
            "데이터 상태": {"select": {"name": "✅ 검증완료"}},
            "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
        }

        # 열 존재 여부 체크 후 업데이트
        if "Market" in props: upd["Market"] = {"select": {"name": str(data["market"])}}
        if "산업분야" in props: upd["산업분야"] = {"rich_text": [{"text": {"content": str(data["wiki"]["ind"])}}]}
        if "서비스" in props: upd["서비스"] = {"rich_text": [{"text": {"content": str(data["wiki"]["svc"])}}]}

        if data["origin"] == "KR":
            if "KR_산업" in props: upd["KR_산업"] = {"rich_text": [{"text": {"content": str(data["industry"])}}]}
            if "KR_섹터" in props: upd["KR_섹터"] = {"rich_text": [{"text": {"content": str(data["sector"])}}]}
        else:
            if "US_섹터" in props: upd["US_섹터"] = {"rich_text": [{"text": {"content": str(data["sector"])}}]}
            if "US_업종" in props: upd["US_업종"] = {"rich_text": [{"text": {"content": str(data["industry"])}}]}

        notion.pages.update(page_id=pid, properties=upd)
        logger.info(f"✅ {ticker} 업데이트 완료")
    except Exception as e: logger.error(f"❌ {ticker} 에러: {e}")

def main():
    logger.info(f"🚀 실행 모드: {'[전체 업데이트]' if IS_FULL_UPDATE else '[부분 업데이트]'}")
    notion, engine = Client(auth=NOTION_TOKEN), StockAutomationEngine()
    cursor = None
    while True:
        params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: params["start_cursor"] = cursor
        
        # 수동 실행이 아닐 때만 '검증완료' 필터를 겁니다.
        if not IS_FULL_UPDATE:
            params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
        
        res = notion.databases.query(**params)
        pages = res.get("results", [])
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(process_page, page, engine, notion)
                time.sleep(0.4)
        
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

if __name__ == "__main__": main()
