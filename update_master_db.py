import os, re, time, logging, io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client
from bs4 import BeautifulSoup

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class StockAutomationEngine:
    def __init__(self):
        logger.info("📡 주식 및 ETF 통합 데이터 로드 중...")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 1. 일반 주식 리스트 로드
        self.df_krx = fdr.StockListing('KRX') 
        self.df_nasdaq = fdr.StockListing('NASDAQ')
        self.df_nyse = fdr.StockListing('NYSE')
        
        # 2. [추가] ETF 리스트 로드 (국내/미국 통합) 
        try:
            self.df_etf_kr = fdr.StockListing('ETF/KR')
            self.df_etf_us = fdr.StockListing('ETF/US')
            logger.info(f"✅ ETF 데이터 로드 완료 (국내: {len(self.df_etf_kr)}, 미국: {len(self.df_etf_us)})")
        except Exception as e:
            logger.error(f"❌ ETF 데이터 로드 실패: {e}")
            self.df_etf_kr = pd.DataFrame()
            self.df_etf_us = pd.DataFrame()

        self.df_us_all = pd.concat([self.df_nasdaq, self.df_nyse], ignore_index=True)
        
        # 3. 4대 우량주 맵 구축
        self.blue_chip_map = {
            "S&P 500": self._get_sp500(),
            "NASDAQ 100": self._get_nas100(),
            "KOSPI 200": self._get_ks200(),
            "KOSDAQ GLOBAL": self._get_kglobal()
        }

    def _get_sp500(self) -> List[str]:
        try: return fdr.StockListing('S&P500')['Symbol'].tolist()
        except: return []

    def _get_nas100(self) -> List[str]:
        try:
            url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            res = self.session.get(url, timeout=10)
            df = pd.read_html(io.StringIO(res.text))[4]
            col = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
            return df[col].tolist()
        except: return []

    def _get_ks200(self) -> List[str]:
        for i in range(10):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            res = stock.get_index_portfolio_deposit_file("1028", date)
            if len(res) > 0: return res
        return []

    def _get_kglobal(self) -> List[str]:
        target = self.df_krx[self.df_krx['Market'].str.contains('KOSDAQ GLOBAL', case=False, na=False)]
        col = 'Code' if 'Code' in target.columns else 'Symbol'
        return target[col].tolist()

    def fetch_wiki_info(self, ticker: str, origin: str) -> Dict[str, str]:
        res_data = {"ind": "", "svc": ""}
        search_ticker = f"{ticker}:KRX" if origin == "KR" else ticker
        url = f"https://www.google.com/finance/quote/{search_ticker}?hl=ko"
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

    def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
        """주식 및 ETF 통합 검색 로직"""
        # 1. 한국 시장 검색 (주식 -> ETF 순)
        kr_match = self.df_krx[self.df_krx['Code'] == clean_t]
        if not kr_match.empty:
            row = kr_match.iloc[0]
            return {"name": row['Name'], "market": str(row['Market']), "origin": "KR", "wiki": self.fetch_wiki_info(clean_t, "KR")}
        
        # [추가] 한국 ETF 검색
        if not self.df_etf_kr.empty:
            etf_kr_match = self.df_etf_kr[self.df_etf_kr['Symbol'] == clean_t]
            if not etf_kr_match.empty:
                row = etf_kr_match.iloc[0]
                return {"name": row['Name'], "market": "ETF(KR)", "origin": "KR", "wiki": {"ind": "ETF", "svc": "국내 상장지수펀드"}}

        # 2. 미국 시장 검색 (주식 -> ETF 순)
        us_match = self.df_us_all[self.df_us_all['Symbol'] == clean_t]
        if not us_match.empty:
            row = us_match.iloc[0]
            mkt = "NASDAQ" if clean_t in self.df_nasdaq['Symbol'].values else "NYSE"
            return {"name": row['Name'], "market": mkt, "origin": "US", "wiki": self.fetch_wiki_info(clean_t, "US")}
        
        # [추가] 미국 ETF 검색
        if not self.df_etf_us.empty:
            etf_us_match = self.df_etf_us[self.df_etf_us['Symbol'] == clean_t]
            if not etf_us_match.empty:
                row = etf_us_match.iloc[0]
                return {"name": row['Name'], "market": "ETF(US)", "origin": "US", "wiki": {"ind": "ETF", "svc": "미국 상장지수펀드"}}

        return {"name": "", "market": "기타", "origin": "", "wiki": {"ind": "", "svc": ""}}

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

def process_page(page, engine, client):
    pid, props = page["id"], page["properties"]
    ticker_rich = props.get("티커", {}).get("title", [])
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip()
    clean_t = engine.clean_ticker(raw_ticker)

    info = engine.get_stock_detail(clean_t)
    bc_tags = [{"name": label} for label, lst in engine.blue_chip_map.items() if clean_t in lst]

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": info["market"]}},
        "산업분야": {"rich_text": [{"text": {"content": info["wiki"]["ind"]}}]},
        "서비스": {"rich_text": [{"text": {"content": info["wiki"]["svc"]}}]},
        "데이터 상태": {"select": {"name": "✅ 검증완료"}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if "우량주" in props:
        update_props["우량주"] = {"multi_select": bc_tags}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {raw_ticker} ({info['name']}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ {raw_ticker} 업데이트 실패: {e}")

def main():
    client = Client(auth=NOTION_TOKEN) 
    engine = StockAutomationEngine()
    
    cursor = None
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        if not IS_FULL_UPDATE:
            query_params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
        
        response = client.databases.query(**query_params) 
        pages = response.get("results", [])
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in pages:
                executor.submit(process_page, page, engine, client)
                time.sleep(0.3)
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
