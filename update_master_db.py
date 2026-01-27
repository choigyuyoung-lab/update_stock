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
        logger.info("📡 데이터 로드 및 우량주 리스트 구축 중...")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 1. 주식/ETF 데이터 로드
        self.df_krx = fdr.StockListing('KRX') 
        self.df_nasdaq = fdr.StockListing('NASDAQ')
        self.df_nyse = fdr.StockListing('NYSE')
        self.df_etf_kr = fdr.StockListing('ETF/KR')
        self.df_etf_us = fdr.StockListing('ETF/US')
        self.df_us_all = pd.concat([self.df_nasdaq, self.df_nyse], ignore_index=True)
        
        # 2. 우량주 맵 (태그 표기용)
        self.blue_chip_map = {
            "S&P 500": self._get_sp500(),
            "NASDAQ 100": self._get_nas100(),
            "KOSPI 200": self._get_ks200(),
            "KOSDAQ GLOBAL": self._get_kglobal() 
        }

    # ... (기존 _get_sp500, _get_nas100, _get_ks200 생략) ...

    def _get_kglobal(self) -> List[str]:
        """KOSDAQ GLOBAL 종목 리스트 추출 (태그용)"""
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
        """Market 명칭 통일 로직 포함"""
        # 국내 주식
        kr_match = self.df_krx[self.df_krx['Code'] == clean_t]
        if not kr_match.empty:
            row = kr_match.iloc[0]
            # [수정] Market은 KOSDAQ으로 통일
            mkt = "KOSDAQ" if "KOSDAQ" in str(row['Market']) else str(row['Market'])
            return {"name": row['Name'], "market": mkt, "origin": "KR", "wiki": self.fetch_wiki_info(clean_t, "KR")}
        
        # 국내 ETF
        etf_kr_match = self.df_etf_kr[self.df_etf_kr['Symbol'] == clean_t]
        if not etf_kr_match.empty:
            return {"name": etf_kr_match.iloc[0]['Name'], "market": "ETF(KR)", "origin": "KR", "wiki": {"ind": "ETF", "svc": "국내 상장지수펀드"}}

        # 미국 주식
        us_match = self.df_us_all[self.df_us_all['Symbol'] == clean_t]
        if not us_match.empty:
            mkt = "NASDAQ" if clean_t in self.df_nasdaq['Symbol'].values else "NYSE"
            return {"name": us_match.iloc[0]['Name'], "market": mkt, "origin": "US", "wiki": self.fetch_wiki_info(clean_t, "US")}
        
        # 미국 ETF
        etf_us_match = self.df_etf_us[self.df_etf_us['Symbol'] == clean_t]
        if not etf_us_match.empty:
            return {"name": etf_us_match.iloc[0]['Name'], "market": "ETF(US)", "origin": "US", "wiki": {"ind": "ETF", "svc": "미국 상장지수펀드"}}

        return {"name": "", "market": "기타", "origin": "", "wiki": {"ind": "", "svc": ""}}

    def clean_ticker(self, raw_ticker: str) -> str:
        """티커 정제 규칙 준수 (Python 3.10+)"""
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
    # [유지] 우량주 태그에는 'KOSDAQ GLOBAL'이 명확히 들어감
    bc_tags = [{"name": label} for label, lst in engine.blue_chip_map.items() if clean_t in lst]

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": info["market"]}}, # 'KOSDAQ'으로 입력됨
        "산업분야": {"rich_text": [{"text": {"content": info["wiki"]["ind"]}}]},
        "서비스": {"rich_text": [{"text": {"content": info["wiki"]["svc"]}}]},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if "우량주" in props:
        update_props["우량주"] = {"multi_select": bc_tags} # 'KOSDAQ GLOBAL' 태그 포함됨

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {raw_ticker} ({info['name']}) 업데이트 성공 | Market: {info['market']}")
    except Exception as e:
        logger.error(f"❌ {raw_ticker} 실패: {e}")

# ... (이하 main 생략) ...
