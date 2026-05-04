import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = True 

# 🌟 업데이트 제외 지표 티커 4개 (KODEX 200, 코스닥150, KODEX 300, 코스피 지수)
EXCLUDE_TICKERS = {"069500", "233740", "291680", "226490"}

BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759", # KODEX 코스피
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
# 2. 데이터 엔진 (Python 3.10+ 및 StringIO 방식)[cite: 1]
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 주식 엔진 가동 (로직 완전 분리 모드)")
        self.all_map = fdr.StockListing('KRX').set_index('Code').to_dict('index')
        self.desc_map = fdr.StockListing('KRX-DESC').set_index('Code').to_dict('index')
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

    def get_stock_detail(self, clean_t: str) -> dict:
        res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None, "is_etf": False}
        if clean_t in self.etf_map:
            etf_item = self.etf_map[clean_t]
            res.update({"market": "ETF(KR)", "name": str(etf_item.get('Name', '')), "is_etf": True})
            return res
        if clean_t in self.all_map:
            item = self.all_map[clean_t]
            m_raw = str(item.get('Market', '')).upper()
            # KOSPI 판별 강화 (STK 등 포함)
            res["market"] = "KOSPI" if any(x in m_raw for x in ["KOSPI", "STK"]) else ("KOSDAQ" if "KOSDAQ" in m_raw else m_raw)
            res["name"] = str(item.get('Name', ''))
        if clean_t in self.desc_map:
            desc_item = self.desc_map[clean_t]
            if not res["name"]: res["name"] = str(desc_item.get('Name', ''))
            res["kr_sector"] = str(desc_item.get('Sector') or desc_item.get('WICS 업종명') or "")
            res["kr_ind"] = str(desc_item.get('Industry') or desc_item.get('WICS 제품') or "")
        return res

# ---------------------------------------------------------
# 3. 로직 분기 핸들러
# ---------------------------------------------------------
def format_notion_id(uid):
    if not uid: return None
    u = str(uid).replace("-", "")
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}" if len(u) == 32 else uid

def get_base_props(info):
    props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]},
        "Market": {"select": {"name": str(info["market"])}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    if info["kr_sector"]: props["KR_섹터"] = {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]}
    if info["kr_ind"]: props["KR_산업"] = {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]}
    return props

# 🌟 [핸들러 A] 일반 주식 로직 (지수 및 일반 코스피)
def handle_stock_update(page, info, engine, client, clean_t, props):
    tag, m_id, m_reason = None, None, "유지"
    
    # 1. KOSPI 200 판별
    if clean_t in engine.kospi_200_list and info["market"] == "KOSPI":
        tag, m_id, m_reason = "KOSPI 200", BENCHMARK_IDS["KOSPI 200"], "KOSPI 200 기록"
    # 2. KOSDAQ 150 판별
    elif clean_t in engine.kosdaq_150_list and info["market"] == "KOSDAQ":
        tag, m_id, m_reason = "KOSDAQ 150", BENCHMARK_IDS["KOSDAQ 150"], "KOSDAQ 150 기록"
    # 3. 일반 KOSPI 판별 (KOSPI 200 제외 모든 코스피)
    elif info["market"] == "KOSPI":
        m_id, m_reason = BENCHMARK_IDS["KOSPI_TOTAL"], "일반 KOSPI -> KODEX 코스피 기록"

    update_data = get_base_props(info)
    # 🌟 값이 존재할 때만 딕셔너리에 추가 (삭제 방지)
    if tag: update_data["우량주"] = {"multi_select": [{"name": tag}]}
    if m_id: update_data["시장BM"] = {"relation": [{"id": format_notion_id(m_id)}]}
    
    ind_id = engine.industry_lookup.get(clean_t)
    if ind_id: update_data["산업BM"] = {"relation": [{"id": format_notion_id(ind_id)}]}

    client.pages.update(page_id=page["id"], properties=update_data)
    logger.info(f"✅ [STOCK] {info['name']}({clean_t}) | {m_reason}")

# 🌟 [핸들러 B] ETF 전용 로직
def handle_etf_update(page, info, engine, client, clean_t, props):
    m_id = BENCHMARK_IDS["KODEX_300"]
    update_data = get_base_props(info)
    update_data["시장BM"] = {"relation": [{"id": format_notion_id(m_id)}]}
    
    ind_id = engine.industry_lookup.get(clean_t)
    if ind_id: update_data["산업BM"] = {"relation": [{"id": format_notion_id(ind_id)}]}

    client.pages.update(page_id=page["id"], properties=update_data)
    logger.info(f"✅ [ETF] {info['name']}({clean_t}) | KODEX 300 기록")

# ---------------------------------------------------------
# 4. 페이지 처리 (분배 및 제외)
# ---------------------------------------------------------
def process_page_kr(page, engine, client):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
    if not ticker_rich: return

    raw_ticker = ticker_rich[0]["plain_text"].strip().upper()
    clean_t = str(re.search(r'(\d{6})', raw_ticker).group(1)) if re.search(r'(\d{6})', raw_ticker) else raw_ticker[:6]
    
    # 🌟 지표 티커 제외 (업데이트 근처에도 못 오게 차단)
    if clean_t in EXCLUDE_TICKERS:
        logger.info(f"⏭️ [SKIP] 지표 제외: {clean_t}")
        return

    info = engine.get_stock_detail(clean_t)
    if not info["name"]: return

    try:
        # 🌟 입구에서 ETF 여부에 따라 핸들러를 완전히 분리하여 중복 실행 차단
        if info["is_etf"] or info["market"] == "ETF(KR)":
            handle_etf_update(page, info, engine, client, clean_t, props)
        else:
            handle_stock_update(page, info, engine, client, clean_t, props)
    except Exception as e:
        logger.error(f"❌ {info['name']}({clean_t}) 오류: {e}")

# ---------------------------------------------------------
# 5. 메인 실행
# ---------------------------------------------------------
def main():
    client = Client(auth=NOTION_TOKEN)
    engine = StockAutomationEngineKR()
    all_pages = []
    cursor = None
    while True:
        query = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query["start_cursor"] = cursor
        response = client.databases.query(**query)
        all_pages.extend(response.get("results", []))
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client)
                time.sleep(0.05)
    logger.info("✨ 로직 분리 및 일반 코스피 업데이트 완료")

if __name__ == "__main__":
    main()
