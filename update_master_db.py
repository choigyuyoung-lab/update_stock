import os, re, time, logging, io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class StockAutomationEngine:
    def __init__(self):
        logger.info("📡 4대 우량주 리스트 및 시장 데이터 로드 중...")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 1. 기초 데이터 로드
        self.df_krx = fdr.StockListing('KRX') # Market 컬럼 포함
        
        # 2. 4대 우량주 맵 구축 (성공 조합)
        self.blue_chip_map = {
            "S&P 500": self._get_sp500(),
            "NASDAQ 100": self._get_nas100(),
            "KOSPI 200": self._get_ks200(),
            "KOSDAQ GLOBAL": self._get_kglobal()
        }
        
        for k, v in self.blue_chip_map.items():
            logger.info(f"✅ {k}: {len(v)}개 로드 완료")

    def _get_sp500(self) -> List[str]:
        try: return fdr.StockListing('S&P500')['Symbol'].tolist()
        except: return []

    def _get_nas100(self) -> List[str]:
        try:
            url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            res = self.session.get(url, timeout=10)
            df = pd.read_html(io.StringIO(res.text))[4]
            col = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
            return df[col].tolist()
        except: return []

    def _get_ks200(self) -> List[str]:
        for i in range(10): # 최근 10일 탐색 (0개 로드 방지)
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            res = stock.get_index_portfolio_deposit_file("1028", date)
            if len(res) > 0: return res
        return []

    def _get_kglobal(self) -> List[str]:
        # Market 컬럼에서 'KOSDAQ GLOBAL' 필터링 (사용자 발견 로직)
        target = self.df_krx[self.df_krx['Market'].str.contains('KOSDAQ GLOBAL', case=False, na=False)]
        col = 'Code' if 'Code' in target.columns else 'Symbol'
        return target[col].tolist()

    def clean_ticker(self, raw_ticker: str) -> str:
        """티커 정제 로직 (Python 3.10+)"""
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t):
            return match.group(1)
        return re.split(r'[-.]', t)[0]

def process_page(page, engine, notion):
    pid = page["id"]
    props = page["properties"]
    
    # 티커 가져오기
    ticker_rich = props.get("티커", {}).get("title", [])
    if not ticker_rich: return
    raw_ticker = ticker_rich[0]["plain_text"].strip()
    clean_t = engine.clean_ticker(raw_ticker)

    # 우량주 체크 (4개 리스트 대조) 
    bc_tags = []
    for label, ticker_list in engine.blue_chip_map.items():
        if clean_t in ticker_list:
            bc_tags.append({"name": label})

    # 업데이트 속성 구성
    update_props = {
        "데이터 상태": {"select": {"name": "✅ 검증완료"}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    # 우량주 열이 있을 경우에만 태그 삽입
    if "우량주" in props:
        update_props["우량주"] = {"multi_select": bc_tags}

    try:
        notion.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {raw_ticker} ({clean_t}) 업데이트 성공 | 태그: {[t['name'] for t in bc_tags]}")
    except Exception as e:
        logger.error(f"❌ {raw_ticker} 업데이트 실패: {e}")

def main():
    notion = Client(auth=NOTION_TOKEN)
    engine = StockAutomationEngine()
    
    cursor = None
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        
        # 수동 실행(Full)이 아니면 검증완료가 아닌 것만 필터링
        if not IS_FULL_UPDATE:
            query_params["filter"] = {
                "property": "데이터 상태",
                "select": {"does_not_equal": "✅ 검증완료"}
            }
        
        response = notion.databases.query(**query_params)
        pages = response.get("results", [])
        
        # 병렬 처리로 속도 향상
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in pages:
                executor.submit(process_page, page, engine, notion)
                time.sleep(0.3) # API 속도 제한 방지
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
