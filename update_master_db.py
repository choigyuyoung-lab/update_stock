import os, re, time, logging, io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client
from bs4 import BeautifulSoup # 크롤링을 위해 추가

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class StockAutomationEngine:
    def __init__(self):
        logger.info("📡 시장 데이터 및 4대 우량주 리스트 로드 중...")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 1. 기초 데이터 로드 (종목명, 마켓 정보 추출용)
        self.df_krx = fdr.StockListing('KRX') 
        self.df_nasdaq = fdr.StockListing('NASDAQ')
        self.df_nyse = fdr.StockListing('NYSE')
        self.df_us_all = pd.concat([self.df_nasdaq, self.df_nyse], ignore_index=True)
        
        # 2. 4대 우량주 맵 구축
        self.blue_chip_map = {
            "S&P 500": self._get_sp500(),
            "NASDAQ 100": self._get_nas100(),
            "KOSPI 200": self._get_ks200(),
            "KOSDAQ GLOBAL": self._get_kglobal()
        }

    # ... (기존 _get_sp500, _get_nas100, _get_ks200, _get_kglobal, clean_ticker 생략) ...

    def fetch_wiki_info(self, ticker: str, origin: str) -> Dict[str, str]:
        """구글 파이낸스를 거쳐 위키백과에서 산업/서비스 정보 크롤링"""
        res_data = {"ind": "", "svc": ""}
        # 한국 종목은 티커 뒤에 :KRX를 붙여야 구글 파이낸스 검색이 정확함
        search_ticker = f"{ticker}:KRX" if origin == "KR" else ticker
        url = f"https://www.google.com/finance/quote/{search_ticker}?hl=ko"
        
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            # 위키백과 링크 찾기
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'))
            
            if wiki_link:
                w_res = self.session.get(wiki_link.get('href'), timeout=10)
                w_soup = BeautifulSoup(w_res.text, 'html.parser')
                infobox = w_soup.select_one('table.infobox')
                if infobox:
                    for row in infobox.find_all('tr'):
                        th = row.find('th')
                        td = row.find('td')
                        if th and td:
                            lbl = th.get_text(strip=True)
                            val = td.get_text(separator=' ', strip=True)
                            if '산업' in lbl: res_data["ind"] = val
                            elif any(x in lbl for x in ['서비스', '제품', '분야']): res_data["svc"] = val
        except Exception as e:
            logger.warning(f"⚠️ {ticker} 위키 정보 수집 실패: {e}")
        return res_data

    def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
        """종목명, 마켓, 위키 정보 통합 조회"""
        # 1. 한국 시장
        kr_match = self.df_krx[self.df_krx['Code'] == clean_t]
        if not kr_match.empty:
            row = kr_match.iloc[0]
            mkt = "KOSDAQ" if "KOSDAQ" in str(row['Market']) else str(row['Market'])
            return {
                "name": row['Name'], "market": mkt, "origin": "KR",
                "wiki": self.fetch_wiki_info(clean_t, "KR")
            }
        
        # 2. 미국 시장
        us_match = self.df_us_all[self.df_us_all['Symbol'] == clean_t]
        if not us_match.empty:
            row = us_match.iloc[0]
            mkt = "NASDAQ" if clean_t in self.df_nasdaq['Symbol'].values else "NYSE"
            return {
                "name": row['Name'], "market": mkt, "origin": "US",
                "wiki": self.fetch_wiki_info(clean_t, "US")
            }
        return {"name": "", "market": "기타", "origin": "", "wiki": {"ind": "", "svc": ""}}

def process_page(page, engine, notion):
    pid = page["id"]
    props = page["properties"]
    
    ticker_rich = props.get("티커", {}).get("title", [])
    if not ticker_rich: return
    raw_ticker = ticker_rich[0]["plain_text"].strip()
    clean_t = engine.clean_ticker(raw_ticker)

    # 통합 데이터 조회 (위키백과 포함)
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
        notion.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {raw_ticker} 처리 완료")
    except Exception as e:
        logger.error(f"❌ {raw_ticker} 업데이트 실패: {e}")

# ... (이하 main 함수 동일) ...
