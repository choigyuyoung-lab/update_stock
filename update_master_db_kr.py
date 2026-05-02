import os, re, time, logging
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# [시장 벤치마크 ID]
BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759"
}

# 🌟 [산업 벤치마크 ID] ETF 갯수가 늘어나도 안전하게 처리됩니다.[cite: 2]
INDUSTRY_ETF_MAP = {
    "102970": "2f8f59dbdb5b8001a863e3b0d6c9f5e3",  # KODEX 증권
    "466920": "313f59dbdb5b80c688f2daed09ab727b",  # SOL 조선TOP3플러스
    "455850": "324f59dbdb5b809f9791f696ad2bc7d9",  # SOL AI반도체소부장
    "396500": "354f59dbdb5b80afb3cfc82a7f037603",  # TIGER 반도체TOP10
    "487240": "2f0f59dbdb5b8188b60dd5784982ec23",  # KODEX AI전력핵심설비
    "0091P0": "334f59dbdb5b804d8216df3dce96aac0",  # TIGER 코리아원자력
    "305720": "353f59dbdb5b8021aba5e9a6eeb6af6e",  # KODEX 2차전지산업
    "244580": "354f59dbdb5b8015b207d14edc1118b7",  # KODEX 바이오
    "091170": "353f59dbdb5b80eda374ced58bdbc1b8",  # KODEX 은행
    "117700": "354f59dbdb5b8069bdc7e38f4cd66cb6",  # KODEX 건설
    "385510": "354f59dbdb5b80c1afd4f70f8f471215"   # KODEX 신재생에너지액티브
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 한국 주식 데이터 엔진 (하이브리드 정보 수집 및 분할 처리)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 주식 엔진 가동 (하이브리드 수집 및 분할 처리 모드)")
        
        # 1. KRX 전체 목록 (ETF 이름 확보용)
        df_all = fdr.StockListing('KRX')
        self.all_map = df_all.set_index('Code').to_dict('index')
        
        # 2. KRX-DESC 목록 (일반 기업의 섹터/산업 정보 확보용)[cite: 2]
        df_desc = fdr.StockListing('KRX-DESC')
        self.desc_map = df_desc.set_index('Code').to_dict('index')
        
        self.kospi_200_list = self._get_index_by_code("1028")
        self.kosdaq_150_list = self._get_index_by_code("2203")
        
        # ⚡ 10개 이상의 ETF를 안전하게 분할 수집
        self.industry_lookup = self._build_industry_lookup_chunked()

    def _get_index_by_code(self, target_code: str) -> list:
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)
                if res and len(res) > 50: return res
            except: continue
        return []

    def _fetch_etf_data(self, ticker, n_id):
        """개별 ETF의 비중 데이터를 가져옵니다.[cite: 2]"""
        try:
            pdf = stock.get_etf_portfolio_deposit_file(ticker)
            if pdf is not None and not pdf.empty:
                w_col = '비중' if '비중' in pdf.columns else pdf.columns[0]
                return [(t, (n_id, r[w_col])) for t, r in pdf.iterrows()]
        except Exception:
            return []
        return []

    def _build_industry_lookup_chunked(self):
        """🌟 ETF 리스트를 5개씩 쪼개어 분할 수집합니다 (Connection Pool 경고 방지)"""
        logger.info(f"⚡ 총 {len(INDUSTRY_ETF_MAP)}개 산업군 데이터 수집을 시작합니다 (5개씩 분할 진행)...")
        temp_mapping = {}
        etf_items = list(INDUSTRY_ETF_MAP.items())
        chunk_size = 5  # 통신망 과부하 방지를 위한 묶음 크기

        for i in range(0, len(etf_items), chunk_size):
            chunk = etf_items[i:i + chunk_size]
            logger.info(f"   ⏳ {i+1} ~ {min(i+chunk_size, len(etf_items))}번째 ETF 그룹 수집 중...")
            
            # 각 청크(묶음) 내에서만 병렬 처리 수행[cite: 2]
            with ThreadPoolExecutor(max_workers=len(chunk)) as executor:
                futures = {executor.submit(self._fetch_etf_data, t, i_id): t for t, i_id in chunk}
                for future in as_completed(futures):
                    results = future.result()
                    for ticker, (n_id, weight) in results:
                        if ticker not in temp_mapping or weight > temp_mapping[ticker][1]:
                            temp_mapping[ticker] = (n_id, weight)
            
            # 네트워크 휴식 부여 (연결 풀 리셋)
            time.sleep(1.0) 
        
        return {t: d[0] for t, d in temp_mapping.items()}

    def get_stock_detail(self, clean_t: str) -> dict:
        """KRX(전체)와 KRX-DESC(상세) 데이터를 교차 검증하여 완벽한 정보를 구성합니다."""
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None}
        
        # 1단계: ETF를 포함한 모든 종목에서 정확한 이름과 시장(Market) 정보 확인
        if clean_t in self.all_map:
            item = self.all_map[clean_t]
            m_raw = str(item.get('Market', '')).upper()
            res["market"] = "KOSDAQ" if "KOSDAQ" in m_raw else ("KOSPI" if "KOSPI" in m_raw else m_raw)
            res["name"] = item.get('Name', '')

        # 2단계: 기업 상세 정보에서 WICS 등 산업 분류 정보 덧붙이기[cite: 2]
        if clean_t in self.desc_map:
            desc_item = self.desc_map[clean_t]
            if not res["name"]:  # (예외 처리) 혹시 1단계에서 이름을 못 찾은 경우
                res["name"] = desc_item.get('Name', '')
                m_raw = str(desc_item.get('Market', '')).upper()
                res["market"] = "KOSDAQ" if "KOSDAQ" in m_raw else ("KOSPI" if "KOSPI" in m_raw else m_raw)
            res["kr_sector"] = desc_item.get('Sector') or desc_item.get('WICS 업종명')
            res["kr_ind"] = desc_item.get('Industry') or desc_item.get('WICS 제품')
            
        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

# ---------------------------------------------------------
# 3. 노션 업데이트 처리 로직
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    ticker_prop = props.get("티커", {}) or props.get("Ticker", {})
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_t = ticker_rich[0]["plain_text"].strip().upper()
    clean_t = engine.clean_ticker(raw_t)
    info = engine.get_stock_detail(clean_t)
    
    if not info["name"]: 
        return

    # 시장 벤치마크 및 산업 벤치마크 결정[cite: 2]
    tag, m_id = None, None
    if clean_t in engine.kospi_200_list and info["market"] == "KOSPI":
        tag, m_id = "KOSPI 200", BENCHMARK_IDS["KOSPI 200"]
    elif clean_t in engine.kosdaq_150_list and info["market"] == "KOSDAQ":
        tag, m_id = "KOSDAQ 150", BENCHMARK_IDS["KOSDAQ 150"]
    elif info["market"] == "KOSPI":
        m_id = BENCHMARK_IDS["KOSPI_TOTAL"]

    ind_id = engine.industry_lookup.get(clean_t)

    # 업데이트 데이터 패키징[cite: 2]
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]}, 
        "Market": {"select": {"name": str(info["market"])}}, 
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if info["kr_sector"]: update_props["KR_섹터"] = {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]}
    if info["kr_ind"]: update_props["KR_산업"] = {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]}
    if "우량주" in props: update_props["우량주"] = {"multi_select": [{"name": tag}] if tag else []}
    if "시장 벤치마크" in props: update_props["시장 벤치마크"] = {"relation": [{"id": m_id}] if m_id else []}
    if "산업 벤치마크" in props: update_props["산업 벤치마크"] = {"relation": [{"id": ind_id}] if ind_id else []}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [UPDATE] {info['name']}({clean_t})")
    except Exception as e:
        logger.error(f"   ❌ [FAIL] {clean_t}: {e}")

# ---------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------
def main():
    client = Client(auth=NOTION_TOKEN) 
    engine = StockAutomationEngineKR()
    
    all_pages = []
    cursor = None
    
    while True:
        query = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query["start_cursor"] = cursor
        
        # 종목명이나 벤치마크가 없는 행 수집[cite: 2]
        if not IS_FULL_UPDATE:
            query["filter"] = {
                "or": [
                    {"property": "종목명", "rich_text": {"is_empty": True}},
                    {"property": "산업 벤치마크", "relation": {"is_empty": True}}
                ]
            }
        
        response = client.databases.query(**query) 
        all_pages.extend(response.get("results", []))
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

    logger.info(f"🎯 총 {len(all_pages)}개 종목 분석 및 업데이트를 시작합니다.")

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client)
                time.sleep(0.05) 
    
    logger.info("✨ 모든 주도주 벤치마크 데이터 업데이트가 완료되었습니다.")

if __name__ == "__main__":
    main()
