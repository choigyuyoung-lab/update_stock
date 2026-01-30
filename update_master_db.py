import os, re, time, logging, io
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# [최적화] 데이터셋별 우선순위 헤더 정의
HEADERS = {
    "KR_SECTOR": ['Sector', 'WICS 업종명', '업종'],      # 한국 대분류
    "KR_INDUSTRY": ['Industry', '주요제품', 'WICS 제품'], # 한국 세부설명
    "US_SECTOR": ['Sector', 'GICS Sector'],              # 미국 대분류
    "US_INDUSTRY": ['Industry', 'GICS Sub-Industry']     # 미국 세부설명
}

class StockAutomationEngine:
    def __init__(self):
        logger.info(f"📡 엔진 시작 (수동 전체 업데이트 모드: {IS_FULL_UPDATE})")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 1. 데이터 로드 (KRX-DESC, S&P500, NASDAQ, NYSE)
        logger.info("⏳ 주식 데이터셋 로딩 중...")
        self.df_kr_desc = fdr.StockListing('KRX-DESC') # 한국 상세 (Sector/Industry 포함)
        self.df_sp500 = fdr.StockListing('S&P500')     # 미국 우량 (정보 정확도 높음)
        self.df_nasdaq = fdr.StockListing('NASDAQ')    # 미국 전체 1
        self.df_nyse = fdr.StockListing('NYSE')        # 미국 전체 2
        logger.info("✅ 데이터셋 로딩 완료")
        
        # 2. 우량주 맵 구축
        self.blue_chip_map = {
            "S&P 500": self.df_sp500['Symbol'].tolist(),
            "NASDAQ 100": self._get_nas100(),
            "KOSPI 200": self._get_ks200(),
            "KOSDAQ GLOBAL": self._get_kglobal() 
        }

    def _get_nas100(self) -> List[str]:
        try:
            url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            res = self.session.get(url, timeout=10)
            df = pd.read_html(io.StringIO(res.text))[4]
            col = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
            return df[col].tolist()
        except: return []

    def _get_ks200(self) -> List[str]:
        for i in range(10):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            res = stock.get_index_portfolio_deposit_file("1028", date)
            if len(res) > 0: return res
        return []

    def _get_kglobal(self) -> List[str]:
        """KOSDAQ GLOBAL 태그용 리스트"""
        # KRX-DESC에도 Market 컬럼이 존재함
        target = self.df_kr_desc[self.df_kr_desc['Market'].str.contains('KOSDAQ GLOBAL', case=False, na=False)]
        col = 'Code' if 'Code' in target.columns else 'Symbol'
        return target[col].tolist()

    def _get_val_from_headers(self, row, candidates: List[str]) -> str:
        """여러 헤더 후보 중 값이 있는 것을 찾아 반환"""
        for col in candidates:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return "-"

    def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
        """
        티커를 기반으로 한국/미국 주식 정보를 조회하여 
        국가별 섹터/산업 정보를 반환 (ETF 로직 제거, 주식 중심 최적화)
        """
        res = {
            "name": "", "market": "기타", "origin": "",
            "kr_sector": "-", "kr_ind": "-",
            "us_sector": "-", "us_ind": "-"
        }

        # 1. 한국 주식 검색 (KRX-DESC)
        kr_match = self.df_kr_desc[self.df_kr_desc['Code'] == clean_t]
        if not kr_match.empty:
            row = kr_match.iloc[0]
            mkt = "KOSDAQ" if "KOSDAQ" in str(row['Market']) else str(row['Market'])
            
            res.update({
                "name": row['Name'],
                "market": mkt,
                "origin": "KR",
                "kr_sector": self._get_val_from_headers(row, HEADERS['KR_SECTOR']),
                "kr_ind": self._get_val_from_headers(row, HEADERS['KR_INDUSTRY'])
            })
            return res

        # 2. 미국 주식 검색 (S&P500 -> NASDAQ -> NYSE 순차 검색)
        # 검색 대상 리스트 (정확도/우선순위 순)
        search_targets = [self.df_sp500, self.df_nasdaq, self.df_nyse]
        
        for df in search_targets:
            match = df[df['Symbol'] == clean_t]
            if not match.empty:
                row = match.iloc[0]
                
                # Market 결정 (S&P500 리스트에는 Market 컬럼이 없을 수 있으므로 확인)
                if clean_t in self.df_nasdaq['Symbol'].values:
                    mkt = "NASDAQ"
                elif clean_t in self.df_nyse['Symbol'].values:
                    mkt = "NYSE"
                else:
                    mkt = "NYSE" # 기본값

                res.update({
                    "name": row['Name'],
                    "market": mkt,
                    "origin": "US",
                    "us_sector": self._get_val_from_headers(row, HEADERS['US_SECTOR']),
                    "us_ind": self._get_val_from_headers(row, HEADERS['US_INDUSTRY'])
                })
                return res

        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        """티커 정제"""
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

def process_page(page, engine, client):
    pid, props = page["id"], page["properties"]
    ticker_rich = props.get("티커", {}).get("title", [])
    if not ticker_rich: return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip()
    clean_t = engine.clean_ticker(raw_ticker)

    # 상세 정보 조회 (최적화 로직 적용)
    info = engine.get_stock_detail(clean_t)
    
    # 우량주 태그 계산
    bc_tags = [{"name": label} for label, lst in engine.blue_chip_map.items() if clean_t in lst]

    # 노션 업데이트 프로퍼티 구성 (국가별 섹터/산업 분리)
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": info["market"]}},
        
        # 한국 데이터
        "KR_섹터": {"rich_text": [{"text": {"content": info["kr_sector"]}}]},
        "KR_산업": {"rich_text": [{"text": {"content": info["kr_ind"]}}]},
        
        # 미국 데이터
        "US_섹터": {"rich_text": [{"text": {"content": info["us_sector"]}}]},
        "US_업종": {"rich_text": [{"text": {"content": info["us_ind"]}}]},
        
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if "우량주" in props:
        update_props["우량주"] = {"multi_select": bc_tags}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {raw_ticker} ({info['name']}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ {raw_ticker} 업데이트 실패: {e}")

def main():
    client = Client(auth=NOTION_TOKEN) 
    engine = StockAutomationEngine()
    
    cursor = None
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        
        if IS_FULL_UPDATE:
            logger.info("🚀 수동 모드: 데이터베이스 전체 종목을 갱신합니다.")
        else:
            logger.info("⏳ 자동 모드: '종목명'이 비어 있는 신규 종목만 처리합니다.")
            query_params["filter"] = {
                "property": "종목명",
                "rich_text": {"is_empty": True}
            }
        
        response = client.databases.query(**query_params) 
        pages = response.get("results", [])
        
        if not pages:
            logger.info("📢 처리할 종목이 없습니다.")
            break

        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in pages:
                executor.submit(process_page, page, engine, client)
                time.sleep(0.1) # 대기 시간 단축
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
