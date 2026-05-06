import os, re, time, logging
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx  # 🌟 타임아웃 제어를 위해 추가
import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")

FORCE_UPDATE = True 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 지표 DB 분석 (KR시장 / KR산업 구분 체계)
# ---------------------------------------------------------
def get_dynamic_config(client):
    logger.info("🔍 지표지수 DB 분석 중 (KR/US 확장 체계 적용)...")
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    
    try:
        pages = client.databases.query(database_id=BENCHMARK_DATABASE_ID).get("results", [])
        for page in pages:
            props = page["properties"]
            ticker_list = props.get("이름", {}).get("title", [])
            if not ticker_list: continue
            ticker = ticker_list[0]["plain_text"].strip()
            
            category = props.get("구분", {}).get("select", {}).get("name", "")
            config["ticker_to_id"][ticker] = page["id"]
            
            if category == "KR산업":
                config["kr_industry_tickers"].append(ticker)
                
        logger.info(f"✅ 분석 완료: 총 {len(config['ticker_to_id'])}개 지표 로드됨")
    except Exception as e:
        logger.error(f"❌ 지표 DB 분석 실패: {e}")
    return config

# ---------------------------------------------------------
# 3. 데이터 엔진
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 데이터 엔진 가동...")
        self.kr_listing = fdr.StockListing('KRX').set_index('Code').to_dict('index')
        self.kr_etf = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        
        self.k200_list = self._get_index_list("1028")
        self.kd150_list = self._get_index_list("2203")
        self.kr_industry_lookup = self._build_industry_lookup(kr_industry_tickers)

    def _get_index_list(self, code):
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(code, date)
                if res and len(res) > 50: return res
            except: continue
        return []

    def _build_industry_lookup(self, tickers):
        lookup = {}
        for etf_t in tickers:
            try:
                pdf = stock.get_etf_portfolio_deposit_file(etf_t)
                if pdf is not None and not pdf.empty:
                    for stock_t in pdf.index:
                        lookup[stock_t] = etf_t
            except: continue
        return lookup

# ---------------------------------------------------------
# 4. 페이지 처리
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val

    if clean_t in engine.kr_listing or clean_t in engine.kr_etf:
        item = engine.kr_listing.get(clean_t, {})
        m_raw = str(item.get('Market', '')).upper()
        market = "KOSDAQ" if "KOSDAQ" in m_raw else "KOSPI"
        
        target_m_t = None
        if clean_t in engine.k200_list: target_m_t = "069500"
        elif clean_t in engine.kd150_list: target_m_t = "229200"
        elif clean_t in engine.kr_etf: target_m_t = "292190"
        elif market == "KOSPI": target_m_t = "226490"

        target_ind_t = engine.kr_industry_lookup.get(clean_t)

        update_props = {"업데이트 일자": {"date": {"start": datetime.now().isoformat()}}}
        
        if target_m_t and target_m_t != clean_t:
            m_id = config["ticker_to_id"].get(target_m_t)
            if m_id: update_props["시장BM"] = {"relation": [{"id": m_id}]}
            
        if target_ind_t and target_ind_t != clean_t:
            ind_id = config["ticker_to_id"].get(target_ind_t)
            if ind_id: update_props["산업BM"] = {"relation": [{"id": ind_id}]}

        # 타임아웃 대비 3회 재시도 로직
        for attempt in range(3):
            try:
                client.pages.update(page_id=pid, properties=update_props)
                logger.info(f"✅ {clean_t} (KR) 처리 완료")
                break
            except Exception as e:
                if "timed out" in str(e).lower() and attempt < 2:
                    logger.warning(f"⏳ {clean_t} 타임아웃, 재시도 중... ({attempt+1}/3)")
                    time.sleep(2)
                    continue
                logger.error(f"❌ {clean_t} 업데이트 오류: {e}")
                break

# ---------------------------------------------------------
# 5. 메인 함수
# ---------------------------------------------------------
def main():
    # 🌟 httpx 클라이언트를 생성하여 타임아웃을 60초로 설정하고 Notion Client에 주입합니다.
    custom_client = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=custom_client)
    
    config = get_dynamic_config(client)
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])
    
    all_pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    logger.info(f"🔎 총 {len(all_pages)}개의 페이지를 처리합니다.")

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, config)
                time.sleep(0.05)
    
    logger.info("✨ 상장주식 DB 업데이트 완료")

if __name__ == "__main__":
    main()
