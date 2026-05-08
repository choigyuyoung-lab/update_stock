import os, re, time, logging, json, requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import httpx
from notion_client import Client

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

# 모의투자 서버 주소 유지
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# [ETF 테마 규칙 동일]
ETF_THEME_RULES = {
    "S&P500": {"tag": "S&P 500", "bm": "SPY"},
    "나스닥100": {"tag": "NASDAQ 100", "bm": "QQQ"},
    "AI전력": {"tag": "US AI Power", "bm": "XLU"},
    "AI광통신": {"tag": "US AI Optical Network", "bm": "IGN"},
    "미국빅테크": {"tag": "US Big Tech", "bm": "XLK"},
    "HBM": {"tag": "Global AI Memory", "bm": "SOXX"},
    "팔란티어밸류": {"tag": "Palantir Focused", "bm": "QQQ"}
}

# ---------------------------------------------------------
# 2. 안전한 KIS 데이터 엔진 (모의투자 에러 방어)
# ---------------------------------------------------------
class KISVtsEngine:
    def __init__(self):
        self.token = self._get_token()
        self.headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P"
        }

    def _get_token(self):
        url = f"{URL_BASE}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
        try:
            res = requests.post(url, json=body)
            return res.json().get('access_token')
        except: return None

    def safe_get_json(self, url, params, tr_id):
        """🌟 JSONDecodeError를 방지하는 안전한 호출 함수"""
        headers = {**self.headers, "tr_id": tr_id}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=10)
            if res.status_code != 200 or not res.text.strip():
                return {}
            return res.json() # 여기서 JSON 파싱 에러가 나면 아래 except로 이동
        except:
            return {}

    def get_index_members(self, index_code):
        """지수 구성 종목 (모의투자는 종종 빈 값을 줌)"""
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-index-category-constituent-stock"
        data = self.safe_get_json(url, {"fid_cond_mrkt_div_code": "U", "fid_input_iscd": index_code}, "FHPST01200000")
        return [item['mksc_shrn_iscd'] for item in data.get('output2', [])]

    def get_etf_pdf(self, etf_ticker):
        """ETF PDF (모의투자는 종종 빈 값을 줌)"""
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-etf-constituent-stocks"
        data = self.safe_get_json(url, {"fid_input_iscd": etf_ticker}, "FHPST02410000")
        return data.get('output2', [])

# ---------------------------------------------------------
# 3. 메인 프로세스
# ---------------------------------------------------------
def process_page(page, engine, client, config, k200, kd150, industry_lookup):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val

    # 1. KIS 시세/정보 조회 (가장 안정적인 TR 사용)
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/search-stock-info"
    data = engine.safe_get_json(url, {"PRDT_TYPE_CD": "300", "PDNO": clean_t}, "CTAC1503R")
    item = data.get('output', {})
    if not item: return

    stock_name = item.get('prdt_abrv_name', '')
    m_raw = item.get('mkt_id_nm', '')
    is_etf = "ETF" in m_raw.upper() or "ETN" in m_raw.upper()
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "유가증권" in m_raw else "KOSDAQ")
    
    us_tracking_tag, target_m_t = None, None
    if is_etf:
        name_clean = stock_name.replace(" ", "").upper()
        for kw, rule in ETF_THEME_RULES.items():
            if kw.upper() in name_clean:
                us_tracking_tag, target_m_t = rule["tag"], rule["bm"]
                break
    else:
        if clean_t in k200: us_tracking_tag, target_m_t = "KOSPI 200", "069500"
        elif clean_t in kd150: us_tracking_tag, target_m_t = "KOSDAQ 150", "229200"

    if not target_m_t:
        target_m_t = "292190" if is_etf else ("226490" if market_label == "KOSPI" else None)

    target_ind_t = industry_lookup.get(clean_t)

    # 노션 업데이트
    def make_rt(v): return {"rich_text": [{"text": {"content": str(v)}}]}
    upd = {
        "종목명": make_rt(stock_name),
        "Market": {"select": {"name": market_label}},
        "KR_섹터": make_rt(item.get('idx_bztp_lcls_nm', 'ETF')),
        "KR_산업": make_rt(item.get('idx_bztp_mcls_nm', 'ETF')),
        "업데이트 일자": {"date": {"start": datetime.now(timezone(timedelta(hours=9))).isoformat()}}
    }
    if us_tracking_tag: upd["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
    if target_m_t and (m_id := config["ticker_to_id"].get(target_m_t)):
        upd["시장BM"] = {"relation": [{"id": m_id}]}
    if target_ind_t and (ind_id := config["ticker_to_id"].get(target_ind_t)):
        upd["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=upd)
        logger.info(f"✅ {clean_t} 업데이트 완료")
    except: pass

def main():
    engine = KISVtsEngine()
    if not engine.token: return
    
    client = Client(auth=NOTION_TOKEN)
    
    # 지수 멤버십 (모의투자 서버 상태에 따라 빈 리스트일 수 있음)
    k200 = engine.get_index_members("0001")
    kd150 = engine.get_index_members("1001")
    
    # 지표지수 DB 로드
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    cursor = None
    while True:
        res = client.databases.query(database_id=BENCHMARK_DATABASE_ID, start_cursor=cursor)
        for p in res.get("results", []):
            t_list = p["properties"].get("이름", {}).get("title") or p["properties"].get("티커", {}).get("rich_text", [])
            if t_list:
                ticker = t_list[0]["plain_text"].strip().upper()
                config["ticker_to_id"][ticker] = p["id"]
                if p["properties"].get("구분", {}).get("select", {}).get("name") == "KR산업":
                    config["kr_industry_tickers"].append(ticker)
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    # 🌟 비중 기반 산업BM 테이블 구축
    industry_lookup = {}
    for etf_t in config["kr_industry_tickers"]:
        pdf = engine.get_etf_pdf(etf_t)
        for item in pdf:
            stk, w = item.get('stck_shrn_iscd'), float(item.get('etf_cnst_itms_rt') or 0)
            if stk and (stk not in industry_lookup or w > industry_lookup[stk][1]):
                industry_lookup[stk] = (etf_t, w)
        time.sleep(0.1)
    industry_lookup = {k: v[0] for k, v in industry_lookup.items()}

    # 🌟 수동 업데이트를 위해 모든 페이지 로드
    all_pages, cursor = [], None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    logger.info(f"🚀 총 {len(all_pages)}개 종목 업데이트 시작")
    with ThreadPoolExecutor(max_workers=3) as executor:
        for page in all_pages:
            executor.submit(process_page, page, engine, client, config, k200, kd150, industry_lookup)
            time.sleep(0.1)

if __name__ == "__main__":
    main()
