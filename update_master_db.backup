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
        
        # 1. 데이터 로드
        logger.info("⏳ 주식/ETF 데이터셋 로딩 중...")
        self.df_kr_desc = fdr.StockListing('KRX-DESC') # 한국 주식 상세
        self.df_kr_etf = fdr.StockListing('ETF/KR')    # 한국 ETF
        
        # [수정] 미국 ETF 및 AMEX 데이터 추가 로드
        self.df_us_etf = fdr.StockListing('ETF/US')    # 미국 ETF (자산군 정보용)
        self.df_sp500 = fdr.StockListing('S&P500')     # 미국 우량
        self.df_nasdaq = fdr.StockListing('NASDAQ')    # 미국 전체 1
        self.df_nyse = fdr.StockListing('NYSE')        # 미국 전체 2
        self.df_amex = fdr.StockListing('AMEX')        # [추가] 미국 전체 3 (AMEX)
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
        target = self.df_kr_desc[self.df_kr_desc['Market'].str.contains('KOSDAQ GLOBAL', case=False, na=False)]
        col = 'Code' if 'Code' in target.columns else 'Symbol'
        return target[col].tolist()

    def _get_val_from_headers(self, row, candidates: List[str]) -> Optional[str]:
        """값이 있으면 문자열 반환, 없으면 None 반환"""
        for col in candidates:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return None

    def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
        """티커 기반 국가별 상세 정보 조회 (ETF/AMEX 분류 로직 개선)"""
        res = {
            "name": "", "market": "기타", "origin": "",
            "kr_sector": None, "kr_ind": None,
            "us_sector": None, "us_ind": None
        }

        # ---------------------------------------------------------
        # 1. 한국 주식/ETF 검색
        # ---------------------------------------------------------
        # 먼저 주식 리스트(KRX-DESC)에서 기본 정보를 찾습니다.
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
            # 여기서 바로 리턴하지 않고, ETF 리스트에 있는지도 확인합니다.

        # 한국 ETF 리스트 확인 (있으면 덮어쓰기)
        etf_match = self.df_kr_etf[self.df_kr_etf['Symbol'] == clean_t]
        if not etf_match.empty:
            row = etf_match.iloc[0]
            # 카테고리가 있으면 쓰고, 없으면 'ETF'
            cat = str(row['Category']) if 'Category' in row.index else "ETF"
            
            res.update({
                "name": row['Name'],
                "market": "ETF(KR)",   # [변경] 명확한 구분
                "origin": "KR",
                "kr_sector": cat,      # 카테고리를 섹터로 활용
                "kr_ind": "ETF"
            })
            return res

        # 한국 주식으로 판명났으면 리턴 (ETF 아님)
        if res["origin"] == "KR":
            return res

        # ---------------------------------------------------------
        # 2. 미국 ETF 검색 (주식보다 우선 검색)
        # ---------------------------------------------------------
        us_etf_match = self.df_us_etf[self.df_us_etf['Symbol'] == clean_t]
        if not us_etf_match.empty:
            row = us_etf_match.iloc[0]
            # 미국 ETF는 Category(Equity, Bond 등) 정보가 있음
            cat = row['Category'] if 'Category' in row.index else "US_ETF"
            
            res.update({
                "name": row['Name'],
                "market": "ETF(US)",  # [변경] 명확한 구분
                "origin": "US",
                "us_sector": cat,     # 자산군 정보 활용
                "us_ind": "ETF"
            })
            return res

        # ---------------------------------------------------------
        # 3. 미국 주식 검색 (S&P500 -> NASDAQ -> NYSE -> AMEX)
        # ---------------------------------------------------------
        search_targets = [
            (self.df_sp500, "S&P500"),
            (self.df_nasdaq, "NASDAQ"),
            (self.df_nyse, "NYSE"),
            (self.df_amex, "AMEX")      # [추가] AMEX 검색
        ]
        
        for df, mkt_label in search_targets:
            match = df[df['Symbol'] == clean_t]
            if not match.empty:
                row = match.iloc[0]
                
                final_mkt = mkt_label
                # S&P500은 실제 시장(NASDAQ/NYSE) 확인
                if mkt_label == "S&P500":
                    if clean_t in self.df_nasdaq['Symbol'].values: final_mkt = "NASDAQ"
                    else: final_mkt = "NYSE"
                
                # NYSE 데이터에 AMEX가 섞여있을 수 있으므로 AMEX 리스트로 교차 검증
                if final_mkt == "NYSE" and clean_t in self.df_amex['Symbol'].values:
                    final_mkt = "AMEX"

                res.update({
                    "name": row['Name'],
                    "market": final_mkt,
                    "origin": "US",
                    "us_sector": self._get_val_from_headers(row, HEADERS['US_SECTOR']),
                    "us_ind": self._get_val_from_headers(row, HEADERS['US_INDUSTRY'])
                })
                return res

        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

def process_page(page, engine, client):
    pid, props = page["id"], page["properties"]
    
    # 티커 읽기 (Title, RichText 모두 호환)
    target_prop = props.get("티커", {})
    ticker_rich = target_prop.get("title") or target_prop.get("rich_text")
    
    if not ticker_rich: 
        return
    
    raw_ticker = ticker_rich[0]["plain_text"].strip()
    clean_t = engine.clean_ticker(raw_ticker)

    # 정보 조회
    info = engine.get_stock_detail(clean_t)
    
    # 우량주 태그
    bc_tags = [{"name": label} for label, lst in engine.blue_chip_map.items() if clean_t in lst]

    def make_rich_text(text_val):
        if text_val:
            return {"rich_text": [{"text": {"content": text_val}}]}
        return {"rich_text": []} 

    update_props = {
        "종목명": make_rich_text(info["name"]),
        "Market": {"select": {"name": info["market"]}},
        
        "KR_섹터": make_rich_text(info["kr_sector"]),
        "KR_산업": make_rich_text(info["kr_ind"]),
        
        "US_섹터": make_rich_text(info["us_sector"]),
        "US_업종": make_rich_text(info["us_ind"]),
        
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if "우량주" in props:
        update_props["우량주"] = {"multi_select": bc_tags}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {raw_ticker} ({info['name']}) 업데이트 완료 [{info['market']}]")
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
                time.sleep(0.1)
        
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
