import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# 1. 환경 변수 및 설정
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
        
        logger.info("⏳ 데이터셋 로딩 및 인덱싱 중...")
        df_desc = fdr.StockListing('KRX-DESC')
        self.desc_map = df_desc.set_index('Code').to_dict('index')
        
        df_etf = fdr.StockListing('ETF/KR')
        self.etf_map = df_etf.set_index('Symbol').to_dict('index')
        
        logger.info(f"✅ 로딩 완료 (주식: {len(self.desc_map)}건, ETF: {len(self.etf_map)}건)")
        
        # 🌟 지수 데이터 동적 로드 (KOSPI 200, KOSDAQ 150)
        self.blue_chip_map = {
            "KOSPI 200": self._get_dynamic_index("KOSPI", "코스피 200"),
            "KOSDAQ 150": self._get_dynamic_index("KOSDAQ", "코스닥 150")
        }

def _get_dynamic_index(self, market_name: str, index_name: str) -> List[str]:
        """고유 코드를 우선 시도하고, 실패 시 시장에서 동적으로 찾는 무적 로직"""
        
        # 1. 🌟 가장 확실한 고유 코드로 먼저 데이터 직접 가져오기 (에러 원천 차단)
        target_codes = []
        if "코스피 200" in index_name:
            target_codes = ["1028"]
        elif "코스닥 150" in index_name:
            target_codes = ["2035", "1035"] # 코스닥 150의 실제 코드 후보들

        # 하드코딩된 코드로 찌르기
        for code in target_codes:
            for i in range(10): # 최근 10일 이내 영업일 찾기
                date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                try:
                    res = stock.get_index_portfolio_deposit_file(code, date)
                    # 50개짜리 엉뚱한 하위 지수(예: 코스피 50)를 걸러내기 위해 종목 수 검증
                    if res and len(res) > 100: 
                        logger.info(f"✅ {index_name} 직접 로드 성공 (코드: {code}, 종목수: {len(res)})")
                        return res
                except:
                    continue

        # 2. 거래소가 코드를 바꿨을 경우에만 이름으로 시장 전체 검색 (최후의 보루)
        logger.warning(f"🚨 {index_name} 기본 코드가 작동하지 않아 이름 검색을 시도합니다.")
        try:
            indices = stock.get_index_ticker_list(market_name)
            search_target = index_name.replace(" ", "").upper()
            search_target_en = search_target.replace("코스닥", "KOSDAQ").replace("코스피", "KOSPI")
            
            for code in indices:
                name = stock.get_index_ticker_name(code)
                name_clean = name.replace(" ", "").upper()
                
                # 레버리지, 인버스 등이 아닌 본래 지수 찾기
                is_kq150 = "150" in name_clean and ("코스닥" in name_clean or "KOSDAQ" in name_clean)
                is_ks200 = "200" in name_clean and ("코스피" in name_clean or "KOSPI" in name_clean)
                
                if (is_kq150 or is_ks200) and not any(x in name_clean for x in ["선물", "인버스", "레버리지", "TR", "PR"]):
                    for i in range(10):
                        date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                        res = stock.get_index_portfolio_deposit_file(code, date)
                        if res and len(res) > 100: 
                            logger.info(f"✅ {index_name} 검색 로드 성공 (코드: {code}, 종목수: {len(res)})")
                            return res
        except Exception as e:
            logger.error(f"🚨 {index_name} 검색 중 오류: {e}")
            
        logger.error(f"🚨 {index_name} 추출 최종 실패. 데이터를 가져오지 못했습니다.")
        return []

    def _get_val(self, data_dict: dict, candidates: List[str]) -> Optional[str]:
        for col in candidates:
            val = data_dict.get(col)
            if pd.notna(val) and str(val).strip() != "":
                return str(val).strip()
        return None

    def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
        """필터링 없이 딕셔너리 키로 즉시 조회 (O(1))"""
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None}

        if clean_t in self.desc_map:
            item = self.desc_map[clean_t]
            mkt = "KOSDAQ" if "KOSDAQ" in str(item.get('Market', '')) else str(item.get('Market', '기타'))
            res.update({
                "name": item.get('Name', ''),
                "market": mkt,
                "kr_sector": self._get_val(item, HEADERS['KR_SECTOR']),
                "kr_ind": self._get_val(item, HEADERS['KR_INDUSTRY'])
            })

        if clean_t in self.etf_map:
            item = self.etf_map[clean_t]
            res.update({
                "name": str(item.get('Name', '')),
                "market": "ETF(KR)",
                "kr_sector": str(item.get('Category', 'ETF')), 
                "kr_ind": "ETF"
            })
            
        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커", {})
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    
    is_kr = (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())) and not raw_ticker.endswith(('.T', '.TA', '.TW'))
    if not is_kr: return

    clean_t = engine.clean_ticker(raw_ticker)
    info = engine.get_stock_detail(clean_t)
    
    if not info["name"]: return

    # 🌟 물리적 방어 로직 (시장 교차 검증)
    bc_tags = []
    for label, lst in engine.blue_chip_map.items():
        if clean_t in lst:
            if "KOSDAQ" in label and info["market"] != "KOSDAQ":
                continue
            if "KOSPI" in label and info["market"] != "KOSPI":
                continue
            bc_tags.append({"name": label})

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]}, 
        "Market": {"select": {"name": str(info["market"])}}, 
        "KR_섹터": {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]} if info["kr_sector"] else {"rich_text": []}, 
        "KR_산업": {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]} if info["kr_ind"] else {"rich_text": []}, 
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    if "우량주" in props: update_props["우량주"] = {"multi_select": bc_tags}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {raw_ticker} 업데이트 완료")
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
                time.sleep(0.05) 
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
