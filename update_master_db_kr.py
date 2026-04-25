import os, re, time, logging, io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    "KR_SECTOR": ['Sector', 'WICS 업종명', '업종'],
    "KR_INDUSTRY": ['Industry', '주요제품', 'WICS 제품']
}

class StockAutomationEngineKR:
    def __init__(self):
        logger.info(f"📡 한국 주식 엔진 시작 (수동 모드: {IS_FULL_UPDATE})")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        logger.info("⏳ 한국 주식/ETF 데이터셋 로딩 중...")
        self.df_kr_desc = fdr.StockListing('KRX-DESC') 
        self.df_kr_etf = fdr.StockListing('ETF/KR')    
        logger.info("✅ 데이터셋 로딩 완료")
        
        self.blue_chip_map = {
            "KOSPI 200": self._get_ks200(),
            "KOSDAQ GLOBAL": self._get_kglobal() 
        }

    def _get_ks200(self) -> List[str]:
        for i in range(10):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            res = stock.get_index_portfolio_deposit_file("1028", date)
            if len(res) > 0: return res
        return []

    def _get_kglobal(self) -> List[str]:
        target = self.df_kr_desc[self.df_kr_desc['Market'].str.contains('KOSDAQ GLOBAL', case=False, na=False)]
        col = 'Code' if 'Code' in target.columns else 'Symbol'
        return target[col].tolist()

    def _get_val_from_headers(self, row, candidates: List[str]) -> Optional[str]:
        for col in candidates:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return None

    def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None}

        kr_match = self.df_kr_desc[self.df_kr_desc['Code'] == clean_t]
        if not kr_match.empty:
            row = kr_match.iloc[0]
            mkt = "KOSDAQ" if "KOSDAQ" in str(row['Market']) else str(row['Market'])
            res.update({
                "name": row['Name'], "market": mkt,
                "kr_sector": self._get_val_from_headers(row, HEADERS['KR_SECTOR']),
                "kr_ind": self._get_val_from_headers(row, HEADERS['KR_INDUSTRY'])
            })

        etf_match = self.df_kr_etf[self.df_kr_etf['Symbol'] == clean_t]
        if not etf_match.empty:
            row = etf_match.iloc[0]
            cat = str(row['Category']) if 'Category' in row.index else "ETF"
            res.update({"name": row['Name'], "market": "ETF(KR)", "kr_sector": cat, "kr_ind": "ETF"})
            
        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    target_prop = props.get("티커", {})
    ticker_rich = target_prop.get("title") or target_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    
    # 🌟 한국 주식이 아니면 바로 통과
    is_kr = (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())) and not raw_ticker.endswith(('.T', '.TA', '.TW'))
    if not is_kr: return

    clean_t = engine.clean_ticker(raw_ticker)
    info = engine.get_stock_detail(clean_t)
    
    if not info["name"]: return

    bc_tags = [{"name": label} for label, lst in engine.blue_chip_map.items() if clean_t in lst]

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": info["market"]}},
        "KR_섹터": {"rich_text": [{"text": {"content": info["kr_sector"]}}]} if info["kr_sector"] else {"rich_text": []},
        "KR_산업": {"rich_text": [{"text": {"content": info["kr_ind"]}}]} if info["kr_ind"] else {"rich_text": []},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    if "우량주" in props: update_props["우량주"] = {"multi_select": bc_tags}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {raw_ticker} ({info['name']}) 업데이트 완료")
    except Exception as e:
        logger.error(f"   ❌ [KR] {raw_ticker} 실패: {e}")

def main():
    client = Client(auth=NOTION_TOKEN) 
    engine = StockAutomationEngineKR()
    cursor = None
    
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        if not IS_FULL_UPDATE:
            query_params["filter"] = {"property": "종목명", "rich_text": {"is_empty": True}}
        
        response = client.databases.query(**query_params) 
        pages = response.get("results", [])
        if not pages: break

        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in pages:
                executor.submit(process_page_kr, page, engine, client)
                time.sleep(0.1)
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
