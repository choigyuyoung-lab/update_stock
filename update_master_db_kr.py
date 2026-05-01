import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

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
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 한국 주식 데이터 엔진
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 한국 주식 엔진 시작 (심플 로직 버전)")
        df_desc = fdr.StockListing('KRX-DESC')
        self.desc_map = df_desc.set_index('Code').to_dict('index')
        
        df_etf = fdr.StockListing('ETF/KR')
        self.etf_map = df_etf.set_index('Symbol').to_dict('index')
        
        # 최신 KOSPI 200, KOSDAQ 150 명단 확보
        self.kospi_200_list = self._get_index_by_code("코스피 200", "1028")
        self.kosdaq_150_list = self._get_index_by_code("코스닥 150", "2203")

    def _get_index_by_code(self, index_name: str, target_code: str) -> list:
        for i in range(10):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)
                if res and len(res) > 100: return res
            except:
                continue
        return []

    def get_stock_detail(self, clean_t: str) -> dict:
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None}
        if clean_t in self.desc_map:
            item = self.desc_map[clean_t]
            market_raw = str(item.get('Market', '')).upper()
            res["market"] = "KOSDAQ" if "KOSDAQ" in market_raw else ("KOSPI" if "KOSPI" in market_raw else market_raw)
            res["name"] = item.get('Name', '')
            res["kr_sector"] = item.get('Sector') or item.get('WICS 업종명') or item.get('업종')
            res["kr_ind"] = item.get('Industry') or item.get('주요제품') or item.get('WICS 제품')
        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

# ---------------------------------------------------------
# 3. 페이지 처리 로직 (가장 직관적인 IF-THEN)
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    ticker_prop = props.get("티커", {}) or props.get("Ticker", {})
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    if not (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())): return

    clean_t = engine.clean_ticker(raw_ticker)
    info = engine.get_stock_detail(clean_t)
    if not info["name"]: return

    # 🌟 [가장 단순한 우량주 & 벤치마크 판별 로직]
    target_tag = None
    target_benchmark_id = None

    if clean_t in engine.kospi_200_list and info["market"] == "KOSPI":
        target_tag = "KOSPI 200"
        target_benchmark_id = BENCHMARK_IDS["KOSPI 200"]
    
    elif clean_t in engine.kosdaq_150_list and info["market"] == "KOSDAQ":
        target_tag = "KOSDAQ 150"
        target_benchmark_id = BENCHMARK_IDS["KOSDAQ 150"]
    
    elif info["market"] == "KOSPI":
        # 우량주가 아닌 일반 KOSPI 종목
        target_benchmark_id = BENCHMARK_IDS["KOSPI_TOTAL"]

    # 업데이트 속성 구성
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]}, 
        "Market": {"select": {"name": str(info["market"])}}, 
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if info["kr_sector"] and str(info["kr_sector"]).strip() != "None": 
        update_props["KR_섹터"] = {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]}
    if info["kr_ind"] and str(info["kr_ind"]).strip() != "None": 
        update_props["KR_산업"] = {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]}
    
    # 태그와 벤치마크 세팅 (조건에 맞으면 넣고, 아니면 비움)
    if "우량주" in props: 
        update_props["우량주"] = {"multi_select": [{"name": target_tag}] if target_tag else []}
        
    if "시장 벤치마크" in props:
        update_props["시장 벤치마크"] = {"relation": [{"id": target_benchmark_id}] if target_benchmark_id else []}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {raw_ticker}({info['name']}) -> 태그: {target_tag or '없음'}, 벤치마크: {target_benchmark_id or '없음'}")
    except Exception as e:
        logger.error(f"   ❌ [KR] {raw_ticker} 실패: {e}")

# ---------------------------------------------------------
# 4. 메인 실행부
# ---------------------------------------------------------
def main():
    client = Client(auth=NOTION_TOKEN) 
    engine = StockAutomationEngineKR()
    cursor = None
    
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        
        # 필터 로직: 종목명이 비었거나, 시장 벤치마크가 비어있는 항목 무조건 업데이트
        if not IS_FULL_UPDATE:
            query_params["filter"] = {
                "or": [
                    {"property": "종목명", "rich_text": {"is_empty": True}},
                    {"property": "시장 벤치마크", "relation": {"is_empty": True}}
                ]
            }
        
        response = client.databases.query(**query_params) 
        pages = response.get("results", [])
        if not pages: break

        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in pages:
                executor.submit(process_page_kr, page, engine, client)
                time.sleep(0.05) 
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

    logger.info("✨ 완료되었습니다.")

if __name__ == "__main__":
    main()
