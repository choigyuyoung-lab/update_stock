import os, re, time, logging, io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    "US_SECTOR": ['Sector', 'GICS Sector'],
    "US_INDUSTRY": ['Industry', 'GICS Sub-Industry']
}

class StockAutomationEngineUS:
    def __init__(self):
        logger.info(f"📡 해외 주식 엔진 시작 (수동 모드: {IS_FULL_UPDATE})")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        logger.info("⏳ 미국 주식/ETF 데이터셋 로딩 중...")
        # FinanceDataReader 데이터셋 (미국)
        self.df_us_etf = fdr.StockListing('ETF/US')    
        self.df_sp500 = fdr.StockListing('S&P500')     
        self.df_nasdaq = fdr.StockListing('NASDAQ')    
        self.df_nyse = fdr.StockListing('NYSE')        
        self.df_amex = fdr.StockListing('AMEX')        
        logger.info("✅ 데이터셋 로딩 완료")
        
        self.blue_chip_map = {
            "S&P 500": self.df_sp500['Symbol'].tolist(),
            "NASDAQ 100": self._get_nas100()
        }

    def _get_nas100(self) -> List[str]:
        """위키피디아에서 나스닥 100 종목 리스트 추출"""
        try:
            url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            res = self.session.get(url, timeout=10)
            # lxml 엔진을 명시적으로 사용하여 속도와 안정성 확보
            dfs = pd.read_html(io.StringIO(res.text), flavor='lxml')
            # 보통 4번째 혹은 5번째 테이블에 종목 정보가 있음
            for df in dfs:
                if 'Ticker' in df.columns or 'Symbol' in df.columns:
                    col = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
                    return df[col].tolist()
            return []
        except Exception as e:
            logger.warning(f"⚠️ 나스닥 100 리스트 수집 실패: {e}")
            return []

    def _get_val_from_headers(self, row, candidates: List[str]) -> Optional[str]:
        for col in candidates:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return None

    def get_stock_detail(self, clean_t: str, raw_ticker: str) -> Dict[str, Any]:
        """티커 기반 국가별 상세 정보 조회"""
        res = {"name": "", "market": "기타", "us_sector": None, "us_ind": None}

        # 1. 미국 ETF 검색
        us_etf_match = self.df_us_etf[self.df_us_etf['Symbol'] == clean_t]
        if not us_etf_match.empty:
            row = us_etf_match.iloc[0]
            cat = row['Category'] if 'Category' in row.index else "US_ETF"
            res.update({"name": row['Name'], "market": "ETF(US)", "us_sector": cat, "us_ind": "ETF"})
            return res

        # 2. 미국 주식 검색 (S&P500 -> NASDAQ -> NYSE -> AMEX)
        search_targets = [
            (self.df_sp500, "S&P500"), (self.df_nasdaq, "NASDAQ"),
            (self.df_nyse, "NYSE"), (self.df_amex, "AMEX")
        ]
        
        for df, mkt_label in search_targets:
            match = df[df['Symbol'] == clean_t]
            if not match.empty:
                row = match.iloc[0]
                final_mkt = mkt_label
                if mkt_label == "S&P500":
                    final_mkt = "NASDAQ" if clean_t in self.df_nasdaq['Symbol'].values else "NYSE"
                if final_mkt == "NYSE" and clean_t in self.df_amex['Symbol'].values:
                    final_mkt = "AMEX"

                res.update({
                    "name": row['Name'], "market": final_mkt,
                    "us_sector": self._get_val_from_headers(row, HEADERS['US_SECTOR']),
                    "us_ind": self._get_val_from_headers(row, HEADERS['US_INDUSTRY'])
                })
                return res

        # 3. 글로벌/기타 (일본, 대만 등) - Yahoo Finance 활용
        try:
            # 세션을 사용하여 차단 방지 및 속도 향상
            stock = yf.Ticker(raw_ticker, session=self.session)
            stock_info = stock.info
            if stock_info:
                name = stock_info.get('longName') or stock_info.get('shortName') or stock_info.get('name')
                if name:
                    res.update({
                        "name": name,
                        "market": "기타",
                        "us_sector": stock_info.get("sector"),
                        "us_ind": stock_info.get("industry")
                    })
        except Exception as e:
            logger.debug(f"Yahoo Finance 검색 실패 ({raw_ticker}): {e}")

        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        """FinanceDataReader 조회용 티커 정제"""
        t = str(raw_ticker).strip().upper()
        # .T, .TW 등 접미사 제거
        return re.split(r'[-.]', t)[0]

def process_page_us(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    target_prop = props.get("티커", {})
    ticker_rich = target_prop.get("title") or target_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    
    # 🌟 한국 주식 판별 (건너뛰기)
    is_kr = (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())) and not raw_ticker.endswith(('.T', '.TA', '.TW'))
    if is_kr: return

    clean_t = engine.clean_ticker(raw_ticker)
    info = engine.get_stock_detail(clean_t, raw_ticker)
    
    if not info["name"]: return

    # 우량주 태그 (S&P 500, 나스닥 100)
    bc_tags = [{"name": label} for label, lst in engine.blue_chip_map.items() if clean_t in lst]

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": info["market"]}},
        "US_섹터": {"rich_text": [{"text": {"content": info["us_sector"]}}]} if info["us_sector"] else {"rich_text": []},
        "US_업종": {"rich_text": [{"text": {"content": info["us_ind"]}}]} if info["us_ind"] else {"rich_text": []},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    if "우량주" in props: 
        update_props["우량주"] = {"multi_select": bc_tags}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [Global] {raw_ticker} ({info['name']}) 업데이트 완료 [{info['market']}]")
    except Exception as e:
        logger.error(f"   ❌ [Global] {raw_ticker} 실패: {e}")

def main():
    client = Client(auth=NOTION_TOKEN) 
    engine = StockAutomationEngineUS()
    
    cursor = None
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        
        # '종목명'이 비어있는 것만 업데이트하거나(자동), 전체 업데이트(수동)
        if not IS_FULL_UPDATE:
            query_params["filter"] = {"property": "종목명", "rich_text": {"is_empty": True}}
        
        response = client.databases.query(**query_params) 
        pages = response.get("results", [])
        if not pages: break

        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in pages:
                executor.submit(process_page_us, page, engine, client)
                time.sleep(0.1) # 노션 API 속도 제한 준수
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
