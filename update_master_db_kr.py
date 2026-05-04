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

# 업데이트 제외 지표 티커 (KODEX 200, 코스닥150, KODEX 300, 코스피 지수)
EXCLUDE_TICKERS = {"069500", "233740", "291680", "226490"}

BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759",
    "KODEX_300": "355f59dbdb5b80879573c5dce4d1e291"
}

INDUSTRY_ETF_MAP = {
    "102970": "2f8f59dbdb5b8001a863e3b0d6c9f5e3", "466920": "313f59dbdb5b80c688f2daed09ab727b",
    "455850": "324f59dbdb5b809f9791f696ad2bc7d9", "396500": "354f59dbdb5b80afb3cfc82a7f037603",
    "487240": "2f0f59dbdb5b8188b60dd5784982ec23", "0091P0": "334f59dbdb5b804d8216df3dce96aac0",
    "305720": "353f59dbdb5b8021aba5e9a6eeb6af6e", "244580": "354f59dbdb5b8015b207d14edc1118b7",
    "091170": "353f59dbdb5b80eda374ced58bdbc1b8", "117700": "354f59dbdb5b8069bdc7e38f4cd66cb6",
    "385510": "354f59dbdb5b80c1afd4f70f8f471215", "449450": "313f59dbdb5b80b49b3ae15f74d0c264",
    "091180": "353f59dbdb5b801c9161c510d2c33986", "139260": "354f59dbdb5b80f8a75ae3942eb6c502"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 데이터 엔진 (Python 3.10+ 기준)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 주식 엔진 초기화 (2단계 순차 로직)")
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

    def _fetch_etf_data(self, ticker, n_id):
        try:
            pdf = stock.get_etf_portfolio_deposit_file(ticker)
            if pdf is not None and not pdf.empty:
                w_col = '비중' if '비중' in pdf.columns else pdf.columns[0]
                return [(t, (n_id, r[w_col])) for t, r in pdf.iterrows()]
        except: return []

    def _build_industry_lookup_chunked(self):
        temp_mapping = {}
        etf_items = list(INDUSTRY_ETF_MAP.items())
        for i in range(0, len(etf_items), 5):
            chunk = etf_items[i:i + 5]
            with ThreadPoolExecutor(max_workers=len(chunk)) as executor:
                futures = {executor.submit(self._fetch_etf_data, t, i_id): t for t, i_id in chunk}
                for future in as_completed(futures):
                    for ticker, (n_id, weight) in future.result():
                        if ticker not in temp_mapping or weight > temp_mapping[ticker][1]:
                            temp_mapping[ticker] = (n_id, weight)
            time.sleep(1.0)
        return {t: d[0] for t, d in temp_mapping.items()}

    def get_info(self, clean_t: str) -> dict:
        if clean_t in self.etf_map:
            return {"market": "ETF(KR)", "name": str(self.etf_map[clean_t]['Name']), "is_etf": True}
        if clean_t in self.all_map:
            m_raw = str(self.all_map[clean_t]['Market']).upper()
            market = "KOSPI" if any(x in m_raw for x in ["KOSPI", "STK"]) else ("KOSDAQ" if "KOSDAQ" in m_raw else m_raw)
            return {"market": market, "name": str(self.all_map[clean_t]['Name']), "is_etf": False}
        return None

# ---------------------------------------------------------
# 3. 유틸리티 (타임아웃 및 Syntax 교정)
# ---------------------------------------------------------
def safe_update(client, page_id, props, retries=3):
    for i in range(retries):
        try:
            client.pages.update(page_id=page_id, properties=props)
            return True
        except (errors.RequestTimeoutError, errors.HTTPResponseError):
            if i < retries - 1:
                time.sleep((i + 1) * 2)
            else: return False

def format_notion_id(uid):
    if not uid: return None
    u = str(uid).replace("-", "")
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}" if len(u) == 32 else uid

def get_base_update(info):
    # Syntax Error 교정: 불필요한 닫는 괄호 삭제
    return {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": info["market"]}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

def extract_ticker(page):
    props = page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return None
    rich_text = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not rich_text: return None
    raw = rich_text[0]["plain_text"].strip().upper()
    match = re.search(r'(\d{6})', raw)
    return match.group(1) if match else raw[:6]

# ---------------------------------------------------------
# 4. 단계별 실행 로직 (KOSPI+KOSDAQ -> ETF 순서)
# ---------------------------------------------------------

def run_stock_phase(pages, engine, client):
    logger.info("🚀 Phase 1: 일반 주식(KOSPI+KOSDAQ) 업데이트 시작")
    for page in pages:
        ticker = extract_ticker(page)
        if not ticker or ticker in EXCLUDE_TICKERS: continue
        
        info = engine.get_info(ticker)
        if not info or info["is_etf"]: continue 

        update_props = get_base_update(info)
        
        if info["market"] == "KOSPI":
            if ticker in engine.kospi_200_list:
                update_props["우량주"] = {"multi_select": [{"name": "KOSPI 200"}]}
                update_props["시장BM"] = {"relation": [{"id": format_notion_id(BENCHMARK_IDS["KOSPI 200"])}]}
            else:
                update_props["시장BM"] = {"relation": [{"id": format_notion_id(BENCHMARK_IDS["KOSPI_TOTAL"])}]}
        
        elif info["market"] == "KOSDAQ":
            if ticker in engine.kosdaq_150_list:
                update_props["우량주"] = {"multi_select": [{"name": "KOSDAQ 150"}]}
                update_props["시장BM"] = {"relation": [{"id": format_notion_id(BENCHMARK_IDS["KOSDAQ 150"])}]}
        
        ind_id = engine.industry_lookup.get(ticker)
        if ind_id: update_props["산업BM"] = {"relation": [{"id": format_notion_id(ind_id)}]}

        if safe_update(client, page["id"], update_props):
            logger.info(f"   ✅ [STOCK] {info['name']} ({ticker})")
            time.sleep(0.3)

def run_etf_phase(pages, engine, client):
    logger.info("🚀 Phase 2: ETF 업데이트 시작")
    for page in pages:
        ticker = extract_ticker(page)
        if not ticker or ticker in EXCLUDE_TICKERS: continue
        
        info = engine.get_info(ticker)
        if not info or not info["is_etf"]: continue

        update_props = get_base_update(info)
        update_props["시장BM"] = {"relation": [{"id": format_notion_id(BENCHMARK_IDS["KODEX_300"])}]}
        
        ind_id = engine.industry_lookup.get(ticker)
        if ind_id: update_props["산업BM"] = {"relation": [{"id": format_notion_id(ind_id)}]}
        
        if safe_update(client, page["id"], update_props):
            logger.info(f"   ✅ [ETF] {info['name']} ({ticker})")
            time.sleep(0.3)

# ---------------------------------------------------------
# 5. 메인 실행
# ---------------------------------------------------------
def main():
    # 타임아웃 연장 및 클라이언트 초기화
    client = Client(auth=NOTION_TOKEN, timeout_ms=60000)
    engine = StockAutomationEngineKR()
    
    logger.info("📦 노션 페이지 수집 중...")
    all_pages = []
    cursor = None
    while True:
        response = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(response["results"])
        if not response["has_more"]: break
        cursor = response["next_cursor"]

    # 🌟 2단계 순차 실행
    run_stock_phase(all_pages, engine, client)
    run_etf_phase(all_pages, engine, client)
    
    logger.info("✨ 모든 업데이트 작업 완료")

if __name__ == "__main__":
    main()
