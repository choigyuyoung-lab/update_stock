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

# 지표 티커 제외 (자기 자신을 업데이트하여 데이터가 꼬이는 것을 방지)
EXCLUDE_TICKERS = {"069500", "233740", "291680", "226490"}

BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759", 
    "KODEX_300": "355f59dbdb5b80879573c5dce4d1e291"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 데이터 엔진 (엄격한 신분 분류)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 시장 데이터 로드 중...")
        self.all_map = fdr.StockListing('KRX').set_index('Code').to_dict('index')
        self.etf_map = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        self.kospi_200_list = self._get_index_by_code("1028")
        self.kosdaq_150_list = self._get_index_by_code("2203")

    def _get_index_by_code(self, target_code: str) -> list:
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)
                if res and len(res) > 50: return res
            except: continue
        return []

    def get_identity(self, ticker: str) -> dict:
        """종목의 신분을 ETF > KOSPI > KOSDAQ 순으로 단 하나만 확정"""
        if ticker in self.etf_map:
            return {"type": "ETF", "name": self.etf_map[ticker]['Name'], "market": "ETF(KR)"}
        
        if ticker in self.all_map:
            m_raw = str(self.all_map[ticker]['Market']).upper()
            market = "KOSPI" if any(x in m_raw for x in ["KOSPI", "STK"]) else ("KOSDAQ" if "KOSDAQ" in m_raw else m_raw)
            return {"type": "STOCK", "name": self.all_map[ticker]['Name'], "market": market}
        
        return None

# ---------------------------------------------------------
# 3. 핵심 업데이트 로직 (이중 기록 방지)
# ---------------------------------------------------------
def process_ticker(page, engine, client):
    props = page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    rich_text = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not rich_text: return
    
    raw = rich_text[0]["plain_text"].strip().upper()
    ticker = re.search(r'(\d{6})', raw).group(1) if re.search(r'(\d{6})', raw) else raw[:6]
    
    if ticker in EXCLUDE_TICKERS: return

    # 🌟 신분 확인 (여기서 ETF와 STOCK이 완전히 갈라짐)
    identity = engine.get_identity(ticker)
    if not identity: return

    # 기본 업데이트 데이터 조립
    update_data = {
        "종목명": {"rich_text": [{"text": {"content": identity["name"]}}]},
        "Market": {"select": {"name": identity["market"]}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    m_id, tag = None, None

    # 🌟 상호 배타적 지표 할당
    if identity["type"] == "ETF":
        # 1. ETF인 경우 무조건 KODEX 300만 할당
        m_id = BENCHMARK_IDS["KODEX_300"]
    elif identity["market"] == "KOSPI":
        # 2. 코스피인 경우
        if ticker in engine.kospi_200_list:
            tag, m_id = "KOSPI 200", BENCHMARK_IDS["KOSPI 200"]
        else:
            m_id = BENCHMARK_IDS["KOSPI_TOTAL"]
    elif identity["market"] == "KOSDAQ":
        # 3. 코스닥인 경우 (코스닥 150이 아니면 시장BM을 아예 건드리지 않음)
        if ticker in engine.kosdaq_150_list:
            tag, m_id = "KOSDAQ 150", BENCHMARK_IDS["KOSDAQ 150"]

    # 🌟 데이터가 존재할 때만 딕셔너리에 추가 (삭제 방지 핵심)
    if tag:
        update_data["우량주"] = {"multi_select": [{"name": tag}]}
    
    if m_id:
        u = str(m_id).replace("-", "")
        formatted_id = f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"
        update_data["시장BM"] = {"relation": [{"id": formatted_id}]}

    # 딱 한 번의 업데이트 요청
    try:
        client.pages.update(page_id=page["id"], properties=update_data)
        logger.info(f"✅ [{identity['market']}] {identity['name']} ({ticker}) 기록 완료")
    except Exception as e:
        logger.error(f"❌ {ticker} 오류: {e}")

def main():
    client = Client(auth=NOTION_TOKEN, timeout_ms=60000)
    engine = StockAutomationEngineKR()
    
    all_pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res["results"])
        if not res["has_more"]: break
        cursor = res["next_cursor"]

    # 병렬 처리로 속도 향상
    with ThreadPoolExecutor(max_workers=5) as executor:
        for page in all_pages:
            executor.submit(process_ticker, page, engine, client)
            time.sleep(0.1) # 속도 제한 방지

if __name__ == "__main__":
    main()
