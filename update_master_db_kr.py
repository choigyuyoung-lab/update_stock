import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

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

HEADERS = {
    "KR_SECTOR": ['Sector', 'WICS 업종명', '업종'],
    "KR_INDUSTRY": ['Industry', '주요제품', 'WICS 제품']
}

# ---------------------------------------------------------
# 2. 한국 주식 데이터 엔진
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info(f"📡 한국 주식 엔진 시작 (수동 모드: {IS_FULL_UPDATE})")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        logger.info("⏳ 데이터셋 로딩 및 인덱싱 중...")
        df_desc = fdr.StockListing('KRX-DESC')
        self.desc_map = df_desc.set_index('Code').to_dict('index')
        
        df_etf = fdr.StockListing('ETF/KR')
        self.etf_map = df_etf.set_index('Symbol').to_dict('index')
        
        logger.info("✅ 로딩 완료")
        
        self.blue_chip_map = {
            "KOSPI 200": self._get_index_by_code("코스피 200", "1028"),
            "KOSDAQ 150": self._get_index_by_code("코스닥 150", "2203")
        }

    def _get_index_by_code(self, index_name: str, target_code: str) -> List[str]:
        for i in range(10):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)
                if res and len(res) > 100: 
                    return res
            except:
                continue
        logger.error(f"🚨 {index_name} 추출 실패.")
        return []

    def _get_val(self, data_dict: dict, candidates: List[str]) -> Optional[str]:
        for col in candidates:
            val = data_dict.get(col)
            if pd.notna(val) and str(val).strip() != "":
                return str(val).strip()
        return None

    def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None}
        if clean_t in self.desc_map:
            item = self.desc_map[clean_t]
            # 시장(Market) 분류를 엄격하게 파싱
            raw_market = str(item.get('Market', '')).strip().upper()
            if "KOSDAQ" in raw_market: mkt = "KOSDAQ"
            elif "KOSPI" in raw_market: mkt = "KOSPI"
            else: mkt = raw_market
            
            res.update({
                "name": item.get('Name', ''), "market": mkt,
                "kr_sector": self._get_val(item, HEADERS['KR_SECTOR']),
                "kr_ind": self._get_val(item, HEADERS['KR_INDUSTRY'])
            })
        if clean_t in self.etf_map:
            item = self.etf_map[clean_t]
            res.update({
                "name": str(item.get('Name', '')), "market": "ETF(KR)",
                "kr_sector": str(item.get('Category', 'ETF')), "kr_ind": "ETF"
            })
        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

# ---------------------------------------------------------
# 3. 페이지 처리 로직
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    ticker_prop = props.get("티커", {}) or props.get("Ticker", {})
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    is_kr = (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())) and not raw_ticker.endswith(('.T', '.TA', '.TW'))
    if not is_kr: return

    clean_t = engine.clean_ticker(raw_ticker)
    info = engine.get_stock_detail(clean_t)
    market_str = str(info["market"]).upper()

    # 1. 노션 수동 태그 + KRX 최신 태그 병합 (싱크로율 100%)
    notion_tags = [t["name"] for t in props.get("우량주", {}).get("multi_select", [])]
    krx_tags = [label for label, lst in engine.blue_chip_map.items() if clean_t in lst]
    final_tags = list(set(notion_tags + krx_tags))

    bc_tags = []
    target_benchmark_id = None

    # 2. 우량주 판별 및 벤치마크 할당
    if "KOSPI 200" in final_tags and "KOSPI" in market_str:
        target_benchmark_id = BENCHMARK_IDS.get("KOSPI 200")
        bc_tags.append({"name": "KOSPI 200"})
    elif "KOSDAQ 150" in final_tags and "KOSDAQ" in market_str:
        target_benchmark_id = BENCHMARK_IDS.get("KOSDAQ 150")
        bc_tags.append({"name": "KOSDAQ 150"})

    # 3. 🌟 KOSPI 일반 종목을 위한 안전망 로직 (위에서 할당 안 된 KOSPI 종목 전부)
    if not target_benchmark_id and "KOSPI" == market_str:
        target_benchmark_id = BENCHMARK_IDS.get("KOSPI_TOTAL")

    # 4. 업데이트 프로퍼티 강제 구성
    update_props = {
        "Market": {"select": {"name": market_str}}, 
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    # 종목명이 비어있을 때만 새로 채우기 (기존 데이터 보존)
    if info["name"] and not props.get("종목명", {}).get("rich_text", []):
        update_props["종목명"] = {"rich_text": [{"text": {"content": str(info["name"])}}]}
        
    if info["kr_sector"]: update_props["KR_섹터"] = {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]}
    if info["kr_ind"]: update_props["KR_산업"] = {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]}
    
    if "우량주" in props: update_props["우량주"] = {"multi_select": bc_tags}
    
    # 벤치마크를 무조건 덮어씌움
    if "시장 벤치마크" in props:
        update_props["시장 벤치마크"] = {"relation": [{"id": target_benchmark_id}] if target_benchmark_id else []}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {raw_ticker} -> 벤치마크 연결됨 ({target_benchmark_id if target_benchmark_id else '없음/KOSDAQ일반'})")
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
        
        # 🌟 핵심 수정: '종목명'이 비었거나 OR '시장 벤치마크'가 비어있는 모든 항목을 사냥합니다.
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

    logger.info("✨ 모든 업데이트가 완료되었습니다.")

if __name__ == "__main__":
    main()
