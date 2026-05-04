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
IS_FULL_UPDATE = True 

BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759",
    "KODEX_300": "355f59dbdb5b80879573c5dce4d1e291"
}

INDUSTRY_ETF_MAP = {
    "102970": "2f8f59dbdb5b8001a863e3b0d6c9f5e3", "466920": "313f59dbdb5b80c688f2daed09ab727b",
    "455850": "324f59dbdb5b809f9791f696ad2bc7d9", "396500": "354f59dbdb5b80afb3cfc82a7f037603",
    "487240": "2f0f59dbdb5b8188b60dd5784982ec23", "0091P0": "334f59dbdb5b804d8216df3dce96aac0",
    "305720": "353f59dbdb5b8021aba5e9a6eeb6af6e", "244580": "354f59dbdb5b8015b207d14edc1118b7",
    "091170": "353f59dbdb5b80eda374ced58bdbc1b8", "117700": "354f59dbdb5b8069bdc7e38f4cd66cb6",
    "385510": "354f59dbdb5b80c1afd4f70f8f471215", "449450": "313f59dbdb5b80b49b3ae15f74d0c264",
    "091180": "353f59dbdb5b801c9161c510d2c33986", "139260": "354f59dbdb5b80f8a75ae3942eb6c502"
}

REV_INDUSTRY = {v: k for k, v in INDUSTRY_ETF_MAP.items()}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 주식 데이터 엔진 (하이브리드 수집)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 주식 엔진 가동 (순차 분석 모드)")
        df_all = fdr.StockListing('KRX')[cite: 3]
        self.all_map = df_all.set_index('Code').to_dict('index')[cite: 3]
        
        df_desc = fdr.StockListing('KRX-DESC')[cite: 3]
        self.desc_map = df_desc.set_index('Code').to_dict('index')[cite: 3]
        
        df_etf = fdr.StockListing('ETF/KR')[cite: 3]
        self.etf_map = df_etf.set_index('Symbol').to_dict('index')[cite: 3]
        
        self.kospi_200_list = self._get_index_by_code("1028")[cite: 3]
        self.kosdaq_150_list = self._get_index_by_code("2203")[cite: 3]
        self.industry_lookup = self._build_industry_lookup_chunked()[cite: 3]

    def _get_index_by_code(self, target_code: str) -> list:
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")[cite: 3]
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)[cite: 3]
                if res and len(res) > 50: return res[cite: 3]
            except: continue
        return []

    def _fetch_etf_data(self, ticker, n_id):
        try:
            pdf = stock.get_etf_portfolio_deposit_file(ticker)[cite: 3]
            if pdf is not None and not pdf.empty:
                w_col = '비중' if '비중' in pdf.columns else pdf.columns[0][cite: 3]
                return [(t, (n_id, r[w_col])) for t, r in pdf.iterrows()][cite: 3]
        except: return []

    def _build_industry_lookup_chunked(self):
        temp_mapping = {}[cite: 3]
        etf_items = list(INDUSTRY_ETF_MAP.items())[cite: 3]
        for i in range(0, len(etf_items), 5):
            chunk = etf_items[i:i + 5][cite: 3]
            with ThreadPoolExecutor(max_workers=len(chunk)) as executor:
                futures = {executor.submit(self._fetch_etf_data, t, i_id): t for t, i_id in chunk}[cite: 3]
                for future in as_completed(futures):
                    for ticker, (n_id, weight) in future.result():
                        if ticker not in temp_mapping or weight > temp_mapping[ticker][1]:
                            temp_mapping[ticker] = (n_id, weight)[cite: 3]
            time.sleep(1.0)
        return {t: d[0] for t, d in temp_mapping.items()}[cite: 3]

    def get_stock_detail(self, clean_t: str) -> dict:
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None, "is_etf": False}[cite: 3]
        
        # 🌟 ETF 여부 판별 (최우선)
        if clean_t in self.etf_map:
            etf_item = self.etf_map[clean_t][cite: 3]
            res.update({"market": "ETF(KR)", "name": str(etf_item.get('Name', '')), "kr_sector": str(etf_item.get('Category', 'ETF')), "kr_ind": "ETF", "is_etf": True})[cite: 3]
            return res

        if clean_t in self.all_map:
            item = self.all_map[clean_t][cite: 3]
            m_raw = str(item.get('Market', '')).upper()[cite: 3]
            res["market"] = "KOSDAQ" if "KOSDAQ" in m_raw else ("KOSPI" if "KOSPI" in m_raw else m_raw)[cite: 3]
            res["name"] = str(item.get('Name', ''))[cite: 3]

        if clean_t in self.desc_map:
            desc_item = self.desc_map[clean_t][cite: 3]
            if not res["name"]: res["name"] = str(desc_item.get('Name', ''))[cite: 3]
            res["kr_sector"] = str(desc_item.get('Sector') or desc_item.get('WICS 업종명') or "")[cite: 3]
            res["kr_ind"] = str(desc_item.get('Industry') or desc_item.get('WICS 제품') or "")[cite: 3]
            
        return res[cite: 3]

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()[cite: 3]
        if match := re.search(r'(\d{6})', t): return match.group(1)[cite: 3]
        return re.split(r'[-.]', t)[0][cite: 3]

# ---------------------------------------------------------
# 3. 페이지 업데이트 로직 (순차 분리 판별)
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"][cite: 3]
    ticker_prop = props.get("티커") or props.get("Ticker")[cite: 3]
    if not ticker_prop: return
    
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")[cite: 3]
    if not ticker_rich: return

    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()[cite: 3]
    is_kr = (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())) and not raw_ticker.endswith(('.T', '.TA', '.TW'))[cite: 3]
    if not is_kr: return

    clean_t = engine.clean_ticker(raw_ticker)[cite: 3]
    info = engine.get_stock_detail(clean_t)[cite: 3]
    if not info["name"]: return

    tag, m_id, m_reason = None, None, "유지 (해당 조건 없음)"[cite: 3]
    
    # 🌟 [1단계] 지수 종목(KOSPI 200, KOSDAQ 150) 판별
    if clean_t in engine.kospi_200_list and info["market"] == "KOSPI":
        tag, m_id, m_reason = "KOSPI 200", BENCHMARK_IDS["KOSPI 200"], "1차: KOSPI 200 기록"[cite: 3]
    elif clean_t in engine.kosdaq_150_list and info["market"] == "KOSDAQ":
        tag, m_id, m_reason = "KOSDAQ 150", BENCHMARK_IDS["KOSDAQ 150"], "1차: KOSDAQ 150 기록"[cite: 3]
    
    # 🌟 [2단계] 1단계 실패 시 ETF 종목 판별 (별도 진행 로직)[cite: 3]
    if not m_id:
        if info["is_etf"] or info["market"] == "ETF(KR)":
            m_id, m_reason = BENCHMARK_IDS["KODEX_300"], "2차: ETF(KR) -> KODEX 300 기록"[cite: 3]

    # [기타 주석 로직 유지][cite: 3]
    # elif info["market"] == "KOSPI":
    #     m_id, m_reason = BENCHMARK_IDS["KOSPI_TOTAL"], "기타 KOSPI"

    ind_id = engine.industry_lookup.get(clean_t)[cite: 3]
    ind_reason = f"산업 ETF({REV_INDUSTRY.get(ind_id)}) 포함" if ind_id else "산업 미포함 (유지)"[cite: 3]

    def format_notion_id(uid):
        if not uid: return None[cite: 3]
        u = str(uid).replace("-", "")[cite: 3]
        if len(u) == 32: return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"[cite: 3]
        return uid

    safe_m_id = format_notion_id(m_id)[cite: 3]
    safe_ind_id = format_notion_id(ind_id)[cite: 3]

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]},[cite: 3]
        "Market": {"select": {"name": str(info["market"])}},[cite: 3]
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}[cite: 3]
    }

    if info["kr_sector"]: update_props["KR_섹터"] = {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]}[cite: 3]
    if info["kr_ind"]: update_props["KR_산업"] = {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]}[cite: 3]
    
    # 🌟 방어적 업데이트: 값이 있을 때만 전송 (삭제 방지 핵심)[cite: 3]
    if "우량주" in props and tag: 
        update_props["우량주"] = {"multi_select": [{"name": tag}]}[cite: 3]
    if "시장BM" in props and safe_m_id: 
        update_props["시장BM"] = {"relation": [{"id": safe_m_id}]}[cite: 3]
    if "산업BM" in props and safe_ind_id: 
        update_props["산업BM"] = {"relation": [{"id": safe_ind_id}]}[cite: 3]

    try:
        client.pages.update(page_id=pid, properties=update_props)[cite: 3]
        logger.info(f"✅ {info['name']}({clean_t}) | {m_reason} | {ind_reason}")[cite: 3]
    except Exception as e:
        logger.error(f"❌ {info['name']}({clean_t}): {e}")[cite: 3]

def main():
    client = Client(auth=NOTION_TOKEN)[cite: 3]
    engine = StockAutomationEngineKR()[cite: 3]
    all_pages = [][cite: 3]
    cursor = None[cite: 3]
    while True:
        query = {"database_id": MASTER_DATABASE_ID, "page_size": 100}[cite: 3]
        if cursor: query["start_cursor"] = cursor[cite: 3]
        response = client.databases.query(**query)[cite: 3]
        all_pages.extend(response.get("results", []))[cite: 3]
        if not response.get("has_more"): break[cite: 3]
        cursor = response.get("next_cursor")[cite: 3]

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:[cite: 3]
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client)[cite: 3]
                time.sleep(0.05)[cite: 3]
    logger.info("✨ 모든 주식 및 ETF 순차 업데이트 완료")[cite: 3]

if __name__ == "__main__":
    main()[cite: 3]
