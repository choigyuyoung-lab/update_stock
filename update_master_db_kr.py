import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client, errors

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# 지표 티커 제외 (자기 자신 업데이트 방지)
EXCLUDE_TICKERS = {"069500", "233740", "291680", "226490"}

BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759", # KODEX 코스피
    "KODEX_300": "355f59dbdb5b80879573c5dce4d1e291"
}

# (산업 매핑 데이터 생략 - 기존과 동일하게 유지)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 데이터 엔진 (Python 3.10+ 기준)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 데이터 수집 및 캐싱 중...")
        self.all_map = fdr.StockListing('KRX').set_index('Code').to_dict('index')
        self.etf_map = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        self.kospi_200_list = self._get_index_by_code("1028")
        self.kosdaq_150_list = self._get_index_by_code("2203")
        self.industry_lookup = self._build_industry_lookup_chunked()

    def _get_index_by_code(self, target_code: str) -> list:
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)
                if res and len(res) > 50: return res
            except: continue
        return []

    def _build_industry_lookup_chunked(self):
        # 산업 매핑 빌드 로직 (기존과 동일)
        return {}

    def get_stock_identity(self, ticker: str) -> dict:
        """한 번의 호출로 종목의 모든 신분 정보를 확정함"""
        if ticker in self.etf_map:
            return {"market": "ETF(KR)", "name": self.etf_map[ticker]['Name'], "type": "ETF"}
        
        if ticker in self.all_map:
            m_raw = str(self.all_map[ticker]['Market']).upper()
            market = "KOSPI" if any(x in m_raw for x in ["KOSPI", "STK"]) else ("KOSDAQ" if "KOSDAQ" in m_raw else m_raw)
            return {"market": market, "name": self.all_map[ticker]['Name'], "type": "STOCK"}
        
        return None

# ---------------------------------------------------------
# 3. 핵심 업데이트 로직 (Single Pass)
# ---------------------------------------------------------
def process_single_page(page, engine, client):
    props = page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    rich_text = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not rich_text: return
    
    raw = rich_text[0]["plain_text"].strip().upper()
    ticker = re.search(r'(\d{6})', raw).group(1) if re.search(r'(\d{6})', raw) else raw[:6]
    
    if ticker in EXCLUDE_TICKERS: return

    # 🌟 1. 종목 신분 확인 (딱 한 번)
    identity = engine.get_stock_identity(ticker)
    if not identity: return

    # 🌟 2. 업데이트 딕셔너리 동적 구성
    # 값이 확실한 공통 필드만 먼저 넣음
    update_data = {
        "종목명": {"rich_text": [{"text": {"content": identity["name"]}}]},
        "Market": {"select": {"name": identity["market"]}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    m_id, tag = None, None

    # 🌟 3. 신분에 따른 지표 할당 (함수 내부에서 엄격하게 분기)
    if identity["type"] == "ETF":
        m_id = BENCHMARK_IDS["KODEX_300"]
    elif identity["market"] == "KOSPI":
        if ticker in engine.kospi_200_list:
            tag, m_id = "KOSPI 200", BENCHMARK_IDS["KOSPI 200"]
        else:
            m_id = BENCHMARK_IDS["KOSPI_TOTAL"]
    elif identity["market"] == "KOSDAQ":
        if ticker in engine.kosdaq_150_list:
            tag, m_id = "KOSDAQ 150", BENCHMARK_IDS["KOSDAQ 150"]

    # 🌟 4. 유효한 결과가 있을 때만 필드 추가 (삭제 방지 핵심)
    if tag:
        update_data["우량주"] = {"multi_select": [{"name": tag}]}
    
    if m_id:
        u = str(m_id).replace("-", "")
        formatted_id = f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"
        update_data["시장BM"] = {"relation": [{"id": formatted_id}]}
    
    ind_id = engine.industry_lookup.get(ticker)
    if ind_id:
        update_data["산업BM"] = {"relation": [{"id": ind_id}]}

    # 🌟 5. 최종 한 번만 API 호출
    try:
        client.pages.update(page_id=page["id"], properties=update_data)
        logger.info(f"✅ [{identity['market']}] {identity['name']} ({ticker}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ {ticker} 업데이트 중 오류: {e}")

# ---------------------------------------------------------
# 4. 메인 실행
# ---------------------------------------------------------
def main():
    # 타임아웃 연장 설정
    client = Client(auth=NOTION_TOKEN, timeout_ms=60000)
    engine = StockAutomationEngineKR()
    
    logger.info("📦 페이지 목록 수집 중...")
    all_pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res["results"])
        if not res["has_more"]: break
        cursor = res["next_cursor"]

    # 🌟 병렬 처리로 시간 단축 (ThreadPool 사용)
    with ThreadPoolExecutor(max_workers=5) as executor:
        for page in all_pages:
            executor.submit(process_single_page, page, engine, client)
            time.sleep(0.1) # 노션 API 안정성을 위한 미세 지연

    logger.info("✨ 모든 업데이트 작업 완료")

if __name__ == "__main__":
    main()
