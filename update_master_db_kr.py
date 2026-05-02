import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정 (노션 API 정보)
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

# [시장 벤치마크 ID]
BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759"
}

# 🌟 [산업 벤치마크 ID] 사용자님이 제공하신 노션 페이지 ID 매핑
INDUSTRY_ETF_MAP = {
    "102970": "2f8f59dbdb5b8001a863e3b0d6c9f5e3",  # KODEX 증권
    "466920": "313f59dbdb5b80c688f2daed09ab727b",  # SOL 조선TOP3플러스
    "455850": "324f59dbdb5b809f9791f696ad2bc7d9",  # SOL AI반도체소부장
    "396500": "354f59dbdb5b80afb3cfc82a7f037603",  # TIGER 반도체TOP10
    "487240": "2f0f59dbdb5b8188b60dd5784982ec23",  # KODEX AI전력핵심설비
    "0091P0": "334f59dbdb5b804d8216df3dce96aac0"   # TIGER 코리아원자력
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 한국 주식 데이터 엔진 (비중 기반 산업 매핑 기능 포함)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 한국 주식 엔진 시작 (산업 벤치마크 역추적 모드)")
        # 기초 데이터 로드 (WICS 및 섹터 정보 포함)[cite: 1]
        df_desc = fdr.StockListing('KRX-DESC')
        self.desc_map = df_desc.set_index('Code').to_dict('index')
        
        # 지수 구성 종목 로드 (KOSPI 200, KOSDAQ 150)
        self.kospi_200_list = self._get_index_by_code("1028")
        self.kosdaq_150_list = self._get_index_by_code("2203")
        
        # 🌟 산업 ETF 구성 종목 및 비중(Weight) 분석[cite: 1]
        self.industry_lookup = {}
        self._build_industry_lookup()

    def _get_index_by_code(self, target_code: str) -> list:
        for i in range(10):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)
                if res and len(res) > 100: return res
            except: continue
        return []

    def _build_industry_lookup(self):
        """ETF PDF를 분석하여 종목별로 비중이 가장 높은 산업군을 결정합니다.[cite: 1]"""
        logger.info("🔍 산업별 대표 ETF 구성 종목(PDF) 및 비중 분석 중...")
        temp_mapping = {} # {티커: (노션ID, 비중)}

        for etf_ticker, notion_id in INDUSTRY_ETF_MAP.items():
            try:
                pdf = stock.get_etf_portfolio_deposit_file(etf_ticker)
                if not pdf.empty:
                    # 비중 컬럼 추출 (보통 '비중' 또는 첫 번째 숫자 컬럼)
                    weight_col = '비중' if '비중' in pdf.columns else pdf.columns[0]
                    
                    for target_ticker, row in pdf.iterrows():
                        weight = row[weight_col]
                        # 중복 종목의 경우 비중이 더 큰 산업으로 할당
                        if target_ticker not in temp_mapping or weight > temp_mapping[target_ticker][1]:
                            temp_mapping[target_ticker] = (notion_id, weight)
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"🚨 ETF {etf_ticker} 분석 실패: {e}")

        self.industry_lookup = {ticker: data[0] for ticker, data in temp_mapping.items()}
        logger.info(f"✅ 산업 매핑 완료 (총 {len(self.industry_lookup)}개 종목 매핑됨)")

    def get_stock_detail(self, clean_t: str) -> dict:
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None}
        if clean_t in self.desc_map:
            item = self.desc_map[clean_t]
            mkt = str(item.get('Market', '')).upper()
            res["market"] = "KOSDAQ" if "KOSDAQ" in mkt else ("KOSPI" if "KOSPI" in mkt else mkt)
            res["name"] = item.get('Name', '')
            res["kr_sector"] = item.get('Sector') or item.get('WICS 업종명') #[cite: 1]
            res["kr_ind"] = item.get('Industry') or item.get('주요제품') or item.get('WICS 제품') #[cite: 1]
        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

# ---------------------------------------------------------
# 3. 페이지 처리 로직 (시장 & 산업 벤치마크 동시 업데이트)
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    # 티커 확인 로직[cite: 1]
    ticker_prop = props.get("티커", {}) or props.get("Ticker", {})
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    clean_t = engine.clean_ticker(raw_ticker)
    info = engine.get_stock_detail(clean_t)
    if not info["name"]: return

    # 1. 시장 벤치마크 및 우량주 태그 결정[cite: 1]
    target_tag = None
    target_market_id = None

    if clean_t in engine.kospi_200_list and info["market"] == "KOSPI":
        target_tag, target_market_id = "KOSPI 200", BENCHMARK_IDS["KOSPI 200"]
    elif clean_t in engine.kosdaq_150_list and info["market"] == "KOSDAQ":
        target_tag, target_market_id = "KOSDAQ 150", BENCHMARK_IDS["KOSDAQ 150"]
    elif info["market"] == "KOSPI":
        target_market_id = BENCHMARK_IDS["KOSPI_TOTAL"]

    # 2. 🌟 산업 벤치마크 결정 (비중 기반 역추적 결과 활용)[cite: 1]
    target_industry_id = engine.industry_lookup.get(clean_t)

    # 업데이트 데이터 구성[cite: 1]
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]}, 
        "Market": {"select": {"name": str(info["market"])}}, 
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    # 섹터 및 산업 정보 (WICS 기반) 업데이트[cite: 1]
    if info["kr_sector"]: update_props["KR_섹터"] = {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]}
    if info["kr_ind"]: update_props["KR_산업"] = {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]}
    
    # 우량주 태그 및 시장 벤치마크 관계형 업데이트[cite: 1]
    if "우량주" in props: update_props["우량주"] = {"multi_select": [{"name": target_tag}] if target_tag else []}
    if "시장 벤치마크" in props: update_props["시장 벤치마크"] = {"relation": [{"id": target_market_id}] if target_market_id else []}
    
    # 🌟 산업 벤치마크 관계형 업데이트[cite: 1]
    if "산업 벤치마크" in props:
        update_props["산업 벤치마크"] = {"relation": [{"id": target_industry_id}] if target_industry_id else []}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {info['name']}({clean_t}) -> 시장: {target_tag or 'KOSPI'}, 산업매핑: {'O' if target_industry_id else 'X'}")
    except Exception as e:
        logger.error(f"   ❌ [KR] {clean_t} 실패: {e}")

# ---------------------------------------------------------
# 4. 메인 실행부 (선 수집 후 처리 방식)[cite: 1]
# ---------------------------------------------------------
def main():
    client = Client(auth=NOTION_TOKEN) 
    engine = StockAutomationEngineKR()
    
    all_pages = []
    cursor = None
    
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        
        # 필터: 종목명이 없거나 산업 벤치마크가 비어있는 항목 위주 수집[cite: 1]
        if not IS_FULL_UPDATE:
            query_params["filter"] = {
                "or": [
                    {"property": "종목명", "rich_text": {"is_empty": True}},
                    {"property": "산업 벤치마크", "relation": {"is_empty": True}}
                ]
            }
        
        response = client.databases.query(**query_params) 
        all_pages.extend(response.get("results", []))
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

    logger.info(f"🎯 총 {len(all_pages)}개의 종목 처리를 시작합니다.")

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client)
                time.sleep(0.05) 
    
    logger.info("✨ 모든 벤치마크 업데이트가 완료되었습니다.")

if __name__ == "__main__":
    main()
