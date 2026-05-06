import os, re, time, logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import httpx
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
from notion_client import Client

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 2. 지표 DB 로드 (티커별 Notion ID 매핑)
def get_us_id_map(client):
    logger.info("🔍 지표 DB에서 US 벤치마크 ID 로드 중...")
    id_map = {}
    try:
        pages = client.databases.query(database_id=BENCHMARK_DATABASE_ID).get("results", [])
        for page in pages:
            t_list = page["properties"].get("이름", {}).get("title", [])
            if t_list:
                id_map[t_list[0]["plain_text"].strip().upper()] = page["id"]
        logger.info(f"✅ 총 {len(id_map)}개의 지표 확보")
    except Exception as e:
        logger.error(f"❌ 지표 로드 실패: {e}")
    return id_map

# 3. 상세 정보 수집 엔진
class USDetailedEngine:
    def __init__(self):
        logger.info("📡 상세 마켓 분석 엔진 가동...")
        # 거래소별 리스트 확보
        self.nasdaq_list = fdr.StockListing('NASDAQ')['Symbol'].tolist()
        self.sp500_list = fdr.StockListing('S&P500')['Symbol'].tolist()
        self.httpx_client = httpx.Client(timeout=30.0)

    def get_info(self, ticker):
        """상세 거래소 및 산업군 정보 추출"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            # 거래소 이름 정규화 (NYQ -> NYSE, NMS -> NASDAQ 등)
            raw_ex = info.get("exchange", "기타")
            market = "NASDAQ" if any(x in raw_ex for x in ["NMS", "NAS"]) else "NYSE" if "NYQ" in raw_ex else raw_ex
            
            return {
                "name": info.get("longName"),
                "market": market,
                "sector": info.get("sector"),
                "industry": info.get("industry")
            }
        except: return None

# 4. 페이지 처리 (상세 마켓 및 벤치마크 연동)
def process_page_us(page, engine, client, id_map):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    raw_t = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    if re.match(r'^\d{6}', raw_t): return # 한국 종목 제외

    info = engine.get_info(raw_t)
    if not info or not info["name"]: return

    # 🌟 시장 판별: 이름에 ETF가 있으면 ETF(US), 아니면 실제 거래소 명칭 사용
    final_market = "ETF(US)" if "ETF" in info["name"].upper() else info["market"]

    # 🌟 벤치마크 자동 결정 (한국 주식 방식 응용)[cite: 2]
    target_m_t = "QQQ" if raw_t in engine.nasdaq_list else "SPY"
    target_ind_t = None
    sec, ind = info["sector"], info.get("industry", "")

    # 산업별 상세 분류[cite: 2]
    if sec == "Technology":
        target_ind_t = "SOXX" if "Semiconductors" in ind else "XLK"
    elif sec == "Industrials":
        target_ind_t = "XAR" if any(x in ind for x in ["Aerospace", "Defense"]) else "XLI"
    elif sec == "Healthcare": target_ind_t = "XLV"
    elif sec == "Financial Services": target_ind_t = "XLF"
    elif sec == "Communication Services": target_ind_t = "XLC"
    elif sec == "Consumer Cyclical": target_ind_t = "XLY"
    elif sec == "Basic Materials": target_ind_t = "GDX"

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": final_market}}, # 상세 마켓 복구[cite: 2]
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
        logger.info(f"✅ [US] {raw_t} -> {final_market} 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ [US] {raw_t} 오류: {e}")

# 5. 메인 함수
def main():
    client = Client(auth=NOTION_TOKEN, client=httpx.Client(timeout=60.0))
    id_map = get_us_id_map(client)
    engine = USDetailedEngine()
    
    res = client.databases.query(database_id=MASTER_DATABASE_ID)
    pages = res.get("results", [])

    with ThreadPoolExecutor(max_workers=5) as executor:
        for page in pages:
            executor.submit(process_page_us, page, engine, client, id_map)
            time.sleep(0.1)

if __name__ == "__main__":
    main()
