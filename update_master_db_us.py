import os, re, time, logging, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import httpx
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
from notion_client import Client

# 1. 환경 변수 및 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 2. 지표 DB 분석 (벤치마크 ID 맵 생성)
def get_id_map(client):
    logger.info("🔍 지표지수 DB 분석 중...")
    id_map = {}
    try:
        pages = client.databases.query(database_id=BENCHMARK_DATABASE_ID).get("results", [])
        for page in pages:
            ticker_list = page["properties"].get("이름", {}).get("title", [])
            if ticker_list:
                ticker = ticker_list[0]["plain_text"].strip().upper()
                id_map[ticker] = page["id"]
        logger.info(f"✅ 총 {len(id_map)}개의 지표 로드 완료")
    except Exception as e:
        logger.error(f"❌ 지표 로드 실패: {e}")
    return id_map

# 3. 기존 검색 엔진 (S&P500 -> NASDAQ -> NYSE -> AMEX 순차 검색 보존)
class StockAutomationEngineUS:
    def __init__(self):
        logger.info("📡 미국 주식 엔진 시작 (기존 검색 체계 유지)")
        self.df_us_etf = fdr.StockListing('ETF/US')    
        self.df_sp500 = fdr.StockListing('S&P500')     
        self.df_nasdaq = fdr.StockListing('NASDAQ')    
        self.df_nyse = fdr.StockListing('NYSE')        
        self.df_amex = fdr.StockListing('AMEX')        
        
        # 나스닥 100 리스트 확보 (시장 지표 결정용)
        self.nasdaq_100 = self.df_nasdaq[self.df_nasdaq['Symbol'].isin(self.df_sp500['Symbol'])]['Symbol'].tolist()

    def get_market_info(self, clean_t):
        """기존의 상세 마켓 분류 로직"""
        # 1. 미국 ETF
        if not self.df_us_etf[self.df_us_etf['Symbol'] == clean_t].empty:
            return "ETF(US)"
        # 2. 거래소 검색
        if clean_t in self.df_nasdaq['Symbol'].values: return "NASDAQ"
        if clean_t in self.df_nyse['Symbol'].values: return "NYSE"
        if clean_t in self.df_amex['Symbol'].values: return "AMEX"
        return "기타"

# 4. 페이지 처리 (시장/산업 비교 최적화 반영)
def process_page_us(page, engine, client, id_map):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    raw_t = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    if re.match(r'^\d{6}', raw_t): return # 한국 종목 스킵

    # [기존 로직] 상세 마켓 판별[cite: 2]
    market_label = engine.get_market_info(raw_t)
    
    # [최적화 로직] 섹터 데이터 및 지표 결정[cite: 2]
    target_m_t, target_ind_t = None, None
    try:
        stock_yf = yf.Ticker(raw_t)
        info = stock_yf.info
        name = info.get("longName") or info.get("shortName") or raw_t
        sec = info.get("sector", "")
        ind = info.get("industry", "")

        # 1. 시장BM 결정 (나스닥 100 포함 시 QQQ, 그 외 SPY)
        target_m_t = "QQQ" if raw_t in engine.nasdaq_100 else "SPY"
        
        # 2. 산업BM 결정 (섹터/산업 키워드 기반 자동 매핑)[cite: 2]
        if sec == "Technology":
            target_ind_t = "SOXX" if "Semiconductors" in ind else "XLK"
        elif sec == "Industrials":
            target_ind_t = "XAR" if any(x in ind for x in ["Aerospace", "Defense"]) else "XLI"
        elif sec == "Healthcare": target_ind_t = "XLV"
        elif sec == "Financial Services": target_ind_t = "XLF"
        elif sec == "Communication Services": target_ind_t = "XLC"
        elif sec == "Consumer Cyclical": target_ind_t = "XLY"
        elif sec == "Basic Materials": target_ind_t = "GDX"
    except:
        return

    # 업데이트 속성 구성
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": name}}]},
        "Market": {"select": {"name": market_label}}, # 기존 상세 마켓 복구[cite: 2]
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    # 지표 관계형 연결 (자기참조 방지 및 유효성 확인)[cite: 2]
    if target_m_t and target_m_t != raw_t:
        if m_id := id_map.get(target_m_t):
            update_props["시장BM"] = {"relation": [{"id": m_id}]}
            
    if target_ind_t and target_ind_t != raw_t:
        if ind_id := id_map.get(target_ind_t):
            update_props["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ [US] {raw_t} ({market_label}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ [US] {raw_t} 오류: {e}")

# 5. 메인 함수
def main():
    # 타임아웃 방지를 위한 httpx 클라이언트 적용[cite: 2]
    notion_httpx = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=notion_httpx)
    
    id_map = get_id_map(client)
    engine = StockAutomationEngineUS()
    
    # 상장주식 DB 쿼리
    res = client.databases.query(database_id=MASTER_DATABASE_ID)
    pages = res.get("results", [])

    if pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in pages:
                executor.submit(process_page_us, page, engine, client, id_map)
                time.sleep(0.1)

if __name__ == "__main__":
    main()
