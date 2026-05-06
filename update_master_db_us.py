import os, re, time, logging, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import httpx
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 지표 DB 분석 (벤치마크 ID 맵 생성)
# ---------------------------------------------------------
def get_id_map(client):
    """지표지수 DB에서 티커별 Notion 페이지 ID를 수집"""
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

# ---------------------------------------------------------
# 3. 데이터 엔진 (기존 상세 마켓 분류 체계 유지)
# ---------------------------------------------------------
class StockAutomationEngineUS:
    def __init__(self):
        logger.info("📡 미국 주식 엔진 시작 (상세 마켓 및 지수 데이터 로딩)")
        # FinanceDataReader를 통한 기초 데이터셋 확보[cite: 2]
        self.df_us_etf = fdr.StockListing('ETF/US')    
        self.df_sp500 = fdr.StockListing('S&P500')     
        self.df_nasdaq = fdr.StockListing('NASDAQ')    
        self.df_nyse = fdr.StockListing('NYSE')        
        self.df_amex = fdr.StockListing('AMEX')        
        
        # 나스닥 100 리스트 확보 (QQQ/SPY 판별용)[cite: 2]
        self.nasdaq_100 = self.df_nasdaq[self.df_nasdaq['Symbol'].isin(self.df_sp500['Symbol'])]['Symbol'].tolist()

    def get_market_label(self, clean_t):
        """기존에 에러가 없던 상세 마켓 분류 로직을 그대로 사용[cite: 2]"""
        # 1. 미국 ETF 우선 판별
        if not self.df_us_etf[self.df_us_etf['Symbol'] == clean_t].empty:
            return "ETF(US)"
        # 2. 개별 거래소 순차 검색
        if clean_t in self.df_nasdaq['Symbol'].values: return "NASDAQ"
        if clean_t in self.df_nyse['Symbol'].values: return "NYSE"
        if clean_t in self.df_amex['Symbol'].values: return "AMEX"
        return "기타"

# ---------------------------------------------------------
# 4. 페이지 처리 (시장/산업 지표 자동 매핑 최적화)[cite: 2]
# ---------------------------------------------------------
def process_page_us(page, engine, client, id_map):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    raw_t = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    # 한국 주식(6자리 숫자) 제외 로직[cite: 2]
    if re.match(r'^\d{6}', raw_t): return 

    # [마켓 분류] 기존 로직 유지[cite: 2]
    market_label = engine.get_market_label(raw_t)
    
    # [데이터 수집] yfinance를 통한 상세 섹터 분석[cite: 2]
    target_m_t, target_ind_t = None, None
    try:
        stock_yf = yf.Ticker(raw_t)
        info = stock_yf.info
        name = info.get("longName") or info.get("shortName") or raw_t
        sec = info.get("sector", "")
        ind = info.get("industry", "")

        # 1. 시장BM 결정 (나스닥 100 포함 시 QQQ, 그 외 SPY)[cite: 2]
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
        "Market": {"select": {"name": market_label}}, 
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    # 지표 관계형 연결 (자기참조 방지)[cite: 2]
    if target_m_t and target_m_t != raw_t:
        if m_id := id_map.get(target_m_t):
            update_props["시장BM"] = {"relation": [{"id": m_id}]}
            
    if target_ind_t and target_ind_t != raw_t:
        if ind_id := id_map.get(target_ind_t):
            update_props["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [US] {raw_t} 완료 ({market_label})")
    except Exception as e:
        logger.error(f"   ❌ [US] {raw_t} 실패: {e}")

# ---------------------------------------------------------
# 5. 메인 함수 (페이지네이션 및 병렬 처리)[cite: 2]
# ---------------------------------------------------------
def main():
    # 타임아웃 방지를 위해 httpx 클라이언트 주입 (60초)[cite: 2]
    notion_httpx = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=notion_httpx)
    
    id_map = get_id_map(client)
    engine = StockAutomationEngineUS()
    
    # [페이지네이션] 100개 이상의 모든 종목을 가져오기 위한 루프[cite: 2]
    all_pages = []
    cursor = None
    logger.info("📡 노션 DB에서 전체 종목 리스트 수집 중 (페이지네이션 적용)...")
    
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor:
            query_params["start_cursor"] = cursor
            
        res = client.databases.query(**query_params)
        all_pages.extend(res.get("results", []))
        
        if not res.get("has_more"):
            break
        cursor = res.get("next_cursor")
        time.sleep(0.1)

    logger.info(f"🔎 총 {len(all_pages)}개 종목 분석 시작")

    # [병렬 처리] 속도 최적화[cite: 2]
    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_us, page, engine, client, id_map)
                time.sleep(0.2) # API Rate Limit 방지[cite: 2]

    logger.info("✨ 모든 US 종목 업데이트 프로세스 완료")

if __name__ == "__main__":
    main()
