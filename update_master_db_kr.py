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

# 🌟 사용자님이 확보하신 벤치마크 페이지 ID 적용 완료[cite: 2]
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
        
        logger.info(f"✅ 로딩 완료 (주식: {len(self.desc_map)}건, ETF: {len(self.etf_map)}건)")
        
        # 최적화: 고정된 코드로 즉시 데이터 로드[cite: 2]
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
                    logger.info(f"✅ {index_name} 로드 성공 (종목수: {len(res)})")
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
# 3. 페이지 처리 로직 (관계형 자동화 포함)
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    # 🌟 티커가 페이지 제목(Title)인 경우 최우선 처리[cite: 2]
    ticker_prop = props.get("티커", {}) or props.get("Ticker", {})
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    
    # 한국 주식 판별
    is_kr = (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())) and not raw_ticker.endswith(('.T', '.TA', '.TW'))
    if not is_kr: return

    clean_t = engine.clean_ticker(raw_ticker)
    info = engine.get_stock_detail(clean_t)
    if not info["name"]: return

    # 1. 🌟 [핵심] 우량주 태그와 벤치마크 ID를 단일 로직으로 동시 판단[cite: 2]
    bc_tags = []
    target_benchmark_id = None
    
    for label, lst in engine.blue_chip_map.items():
        if clean_t in lst:
            # 시장 교차 검증 (안전장치)
            if "KOSDAQ" in label and info["market"] != "KOSDAQ": continue
            if "KOSPI" in label and info["market"] != "KOSPI": continue
            
            # 우량주 태그와 벤치마크 ID 동시 설정
            bc_tags.append({"name": label})
            target_benchmark_id = BENCHMARK_IDS.get(label)
            
            # 🔍 예외 처리: 태그는 찾았으나 ID 매핑에 실패한 경우
            if not target_benchmark_id:
                logger.warning(f"   ⚠️ [{raw_ticker}] '{label}' 소속은 확인되었으나, BENCHMARK_IDS에서 ID를 찾을 수 없습니다.")
            break

    # 2. 우량주가 아닌 KOSPI 종목은 'KOSPI 전체'로 매칭 (KOSDAQ 전체는 제외)[cite: 2]
    if not target_benchmark_id and info["market"] == "KOSPI":
        target_benchmark_id = BENCHMARK_IDS.get("KOSPI_TOTAL")

    # 3. 업데이트 프로퍼티 구성[cite: 2]
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]}, 
        "Market": {"select": {"name": str(info["market"])}}, 
        "KR_섹터": {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]} if info["kr_sector"] else {"rich_text": []}, 
        "KR_산업": {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]} if info["kr_ind"] else {"rich_text": []}, 
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    # 4. 구성된 데이터를 최종적으로 딕셔너리에 삽입[cite: 2]
    # 우량주 열이 노션에 존재한다면, 새로운 태그를 덮어씌움 (편출입 반영)
    if "우량주" in props: 
        update_props["우량주"] = {"multi_select": bc_tags}
    
    # 시장 벤치마크 열이 노션에 존재한다면, 관계형 덮어씌움
    if "시장 벤치마크" in props:
        update_props["시장 벤치마크"] = {
            "relation": [{"id": target_benchmark_id}] if target_benchmark_id else []
        }

    # 5. 노션 서버에 업데이트 실행[cite: 2]
    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {raw_ticker} ({info['name']}) 업데이트 완료")
        
        # 디버깅용 로그 (필요 시 주석 처리 가능)[cite: 2]
        if target_benchmark_id:
            logger.info(f"      🔗 벤치마크 연결됨: {target_benchmark_id}")
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
        
        # '종목명'이 비어있는 것만 업데이트(자동)하거나 전체 업데이트(수동)[cite: 2]
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
