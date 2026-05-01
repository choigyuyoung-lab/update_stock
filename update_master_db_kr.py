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
        
        logger.info(f"✅ 로딩 완료")
        
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
            mkt = "KOSDAQ" if "KOSDAQ" in str(item.get('Market', '')) else str(item.get('Market', '기타'))
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
# 3. 페이지 처리 로직 (100% 동기화 로직 적용)
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
    
    # 만약 파이썬 API 문제로 종목명을 못 가져왔더라도, 기존 노션 이름을 살려 업데이트를 강행합니다.
    if not info["name"]:
        existing_name = props.get("종목명", {}).get("rich_text", [])
        if existing_name:
            info["name"] = existing_name[0]["plain_text"]
        else:
            return

    # 🌟 1. 사용자님 아이디어 적용: 노션에 이미 적힌 '우량주' 태그를 강제로 가져옴
    notion_tags = []
    if "우량주" in props:
        notion_tags = [t["name"] for t in props["우량주"].get("multi_select", [])]

    # 🌟 2. 파이썬(pykrx)에서 새로 가져온 태그 확인
    krx_tags = []
    for label, lst in engine.blue_chip_map.items():
        if clean_t in lst:
            krx_tags.append(label)

    # 🌟 3. 노션 태그와 최신 태그를 합침 (둘 중 하나라도 'KOSDAQ 150'이 있으면 인정)
    final_tag_names = list(set(notion_tags + krx_tags))

    bc_tags = []
    target_benchmark_id = None

    for tag in final_tag_names:
        # 시장 교차 검증 (안전장치 유지)
        if "KOSDAQ" in tag and info["market"] != "KOSDAQ": continue
        if "KOSPI" in tag and info["market"] != "KOSPI": continue
        
        bc_tags.append({"name": tag})
        
        # 🌟 4. [핵심] 확정된 태그를 바탕으로 벤치마크 무조건 할당 (오류 원천 차단)
        if tag == "KOSPI 200":
            target_benchmark_id = BENCHMARK_IDS.get("KOSPI 200")
        elif tag == "KOSDAQ 150":
            target_benchmark_id = BENCHMARK_IDS.get("KOSDAQ 150")

    # 🌟 5. 우량주가 아닌(없음) 코스피 종목 처리
    if not target_benchmark_id and info["market"] == "KOSPI":
        target_benchmark_id = BENCHMARK_IDS.get("KOSPI_TOTAL")

    # 업데이트 프로퍼티 강제 구성 (이름이 조금이라도 틀리면 튕기도록 하여 조용히 누락되는 것 방지)
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]}, 
        "Market": {"select": {"name": str(info["market"])}}, 
        "우량주": {"multi_select": bc_tags},
        "시장 벤치마크": {"relation": [{"id": target_benchmark_id}] if target_benchmark_id else []},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if info["kr_sector"]: update_props["KR_섹터"] = {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]}
    if info["kr_ind"]: update_props["KR_산업"] = {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {raw_ticker} ({info['name']}) -> 벤치마크 연결 완료")
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
        
        if not IS_FULL_UPDATE:
            query_params["filter"] = {"property": "종목명", "rich_text": {"is_empty": True}}
        
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
