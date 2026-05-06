import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# 🌟 중요: 지표 DB의 각 페이지 링크에서 추출한 실제 ID로 모두 교체하세요!
BENCHMARK_IDS = {
    "KOSPI 200": "실제_지표DB_KOSPI200_페이지ID",
    "KOSDAQ 150": "실제_지표DB_KOSDAQ150_페이지ID",
    "KOSPI_TOTAL": "실제_지표DB_코스피전체_페이지ID",
    "KRX 300": "실제_지표DB_KRX300_페이지ID" # 🌟 ETF가 연결될 곳
}

# 지표 그 자체인 종목은 상장주식 DB에서 업데이트 생략
EXCLUDE_TICKERS = {"069500", "233740", "226490", "292190"}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# (StockAutomationEngineKR 클래스 로직은 기존과 동일하므로 생략)

# ---------------------------------------------------------
# 3. 페이지 처리 로직 (속성명 '지표지수' 반영)[cite: 2]
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    raw_ticker = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    clean_t = engine.clean_ticker(raw_ticker)

    # 지표 종목 제외[cite: 2]
    if clean_t in EXCLUDE_TICKERS: return

    info = engine.get_stock_detail(clean_t)
    if not info["name"]: return

    m_id = None
    
    # 🌟 시장 지표 매칭 로직[cite: 2]
    if clean_t in engine.kospi_200_list and info["market"] == "KOSPI":
        m_id = BENCHMARK_IDS["KOSPI 200"]
    elif clean_t in engine.kosdaq_150_list and info["market"] == "KOSDAQ":
        m_id = BENCHMARK_IDS["KOSDAQ 150"]
    elif info["market"] == "ETF(KR)":
        m_id = BENCHMARK_IDS["KRX 300"] # 🌟 국내 ETF -> KRX 300 연결[cite: 2]
    elif info["market"] == "KOSPI":
        m_id = BENCHMARK_IDS["KOSPI_TOTAL"]

    # Notion ID 포맷팅 함수
    def format_notion_id(uid):
        if not uid: return None
        u = str(uid).replace("-", "")
        return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}" if len(u) == 32 else uid

    safe_m_id = format_notion_id(m_id)

    # 🌟 업데이트 데이터 구성
    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]},
        "Market": {"select": {"name": str(info["market"])}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    # 🌟 속성 이름이 '지표지수'인지 '시장BM'인지 확인 필수[cite: 2]
    # 스크린샷에 맞춰 '지표지수'로 설정함
    target_prop_name = "지표지수" 
    
    if target_prop_name in props and safe_m_id:
        update_props[target_prop_name] = {"relation": [{"id": safe_m_id}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {info['name']}({clean_t}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ {info['name']}({clean_t}) 업데이트 실패: {e}")

# (main 함수 로직 동일)
