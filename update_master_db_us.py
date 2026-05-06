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

# 2. 지표 DB 로드 (ID 맵 생성)
def get_us_benchmark_ids(client):
    """지표 DB에서 티커별 Notion ID만 추출"""
    logger.info("🔍 US 지표 ID 로드 중...")
    ticker_to_id = {}
    try:
        pages = client.databases.query(database_id=BENCHMARK_DATABASE_ID).get("results", [])
        for page in pages:
            ticker_list = page["properties"].get("이름", {}).get("title", [])
            if ticker_list:
                ticker = ticker_list[0]["plain_text"].strip().upper()
                ticker_to_id[ticker] = page["id"]
        logger.info(f"✅ 총 {len(ticker_to_id)}개의 지표 ID 확보")
    except Exception as e:
        logger.error(f"❌ 지표 ID 로드 실패: {e}")
    return ticker_to_id

# 3. 미국 데이터 엔진
class USAstockEngine:
    def __init__(self):
        logger.info("📡 US 데이터 세트 로딩 중...")
        # S&P 500 및 나스닥 100 리스트 확보
        self.sp500_list = fdr.StockListing('S&P500')['Symbol'].tolist()
        self.nasdaq_list = fdr.StockListing('NASDAQ')['Symbol'].tolist()
        self.session = httpx.Client(timeout=30.0)

    def get_info(self, ticker):
        """yfinance를 통한 상세 정보 수집"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            return {
                "name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market": info.get("exchange")
            }
        except: return None

# 4. 페이지 처리 (중복 방지 매핑 로직)
def process_page_us(page, engine, client, id_map):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    raw_t = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    if re.match(r'^\d{6}', raw_t): return # 한국 종목 제외

    info = engine.get_info(raw_t)
    if not info or not info["name"]: return

    # --- 🌟 자동 매핑 로직 (중복 해결) ---
    # 1. 시장BM (Market Benchmark)
    target_m_t = "QQQ" if raw_t in engine.nasdaq_list else "SPY"
    
    # 2. 산업BM (Industry Benchmark) - yfinance 데이터 기준 자동 분류
    target_ind_t = None
    sec, ind = info["sector"], info["industry"]

    if sec == "Technology":
        target_ind_t = "SOXX" if "Semiconductors" in ind else "XLK"
    elif sec == "Industrials":
        target_ind_t = "XAR" if any(x in ind for x in ["Aerospace", "Defense"]) else "XLI"
    elif sec == "Healthcare": target_ind_t = "XLV"
    elif sec == "Financial Services": target_ind_t = "XLF"
    elif sec == "Communication Services": target_ind_t = "XLC"
    elif sec == "Consumer Cyclical": target_ind_t = "XLY"
    elif sec == "Basic Materials": target_ind_t = "GDX"

    # --- 데이터 업데이트 ---
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": "ETF(US)" if "ETF" in info["name"] else "US"}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    # 지표 연결 (자기참조 방지)
    if target_m_t and target_m_t != raw_t:
        if m_id := id_map.get(target_m_t):
            update_props["시장BM"] = {"relation": [{"id": m_id}]}
            
    if target_ind_t and target_ind_t != raw_t:
        if ind_id := id_map.get(target_ind_t):
            update_props["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ [US] {raw_t} 완료 (BM: {target_m_t}, {target_ind_t})")
    except Exception as e:
        logger.error(f"❌ [US] {raw_t} 실패: {e}")

# 5. 메인 함수
def main():
    notion_httpx = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=notion_httpx)
    
    id_map = get_us_benchmark_ids(client)
    engine = USAstockEngine()
    
    res = client.databases.query(database_id=MASTER_DATABASE_ID)
    pages = res.get("results", [])

    with ThreadPoolExecutor(max_workers=5) as executor:
        for page in pages:
            executor.submit(process_page_us, page, engine, client, id_map)
            time.sleep(0.1)

if __name__ == "__main__":
    main()
