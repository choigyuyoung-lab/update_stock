import os, re, time, logging, json, requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import httpx
from notion_client import Client

# 1. 환경 변수 및 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

# 실전투자 주소: https://openapi.koreainvestment.com:9443
# 모의투자 주소: https://openapivts.koreainvestment.com:29443
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ETF 테마 규칙 (기존 유지)
ETF_THEME_RULES = {
    "S&P500": {"tag": "S&P 500", "bm": "SPY"},
    "나스닥100": {"tag": "NASDAQ 100", "bm": "QQQ"},
    "미국배당": {"tag": "US Dividend", "bm": "SCHD"},
    "AI전력": {"tag": "US AI Power", "bm": "XLU"},
    "AI광통신": {"tag": "US AI Optical Network", "bm": "IGN"},
    "미국빅테크": {"tag": "US Big Tech", "bm": "XLK"},
    "HBM": {"tag": "Global AI Memory", "bm": "SOXX"},
    "팔란티어밸류": {"tag": "Palantir Focused", "bm": "QQQ"}
}

# ---------------------------------------------------------
# 2. 한투 API 전용 엔진 (기존 엔진의 KIS 버전)
# ---------------------------------------------------------
class KISEngine:
    def __init__(self):
        self.token = self._get_token()
        self.headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "custtype": "P"
        }

    def _get_token(self):
        url = f"{URL_BASE}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
        res = requests.post(url, json=body).json()
        return res.get('access_token')

    def get_index_members(self, index_code):
        """K200: '0001', KD150: '1001' 구성 종목 조회 (TR: FHPST01200000)"""
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-index-category-constituent-stock"
        params = {"fid_cond_mrkt_div_code": "U", "fid_input_iscd": index_code}
        headers = {**self.headers, "tr_id": "FHPST01200000"}
        res = requests.get(url, headers=headers, params=params).json()
        return [item['mksc_shrn_iscd'] for item in res.get('output2', [])]

    def get_etf_pdf(self, etf_ticker):
        """ETF PDF(구성종목/비중) 조회 (TR: FHPST02410000)"""
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-etf-constituent-stocks"
        params = {"fid_input_iscd": etf_ticker}
        headers = {**self.headers, "tr_id": "FHPST02410000"}
        res = requests.get(url, headers=headers, params=params).json()
        return res.get('output2', [])

    def build_industry_lookup(self, industry_etfs):
        """🌟 지표지수 DB를 분석하여 비중에 따른 산업BM 매핑 테이블 생성"""
        lookup = {}
        logger.info(f"📊 {len(industry_etfs)}개 산업 ETF PDF 분석 중...")
        for etf_t in industry_etfs:
            pdf = self.get_etf_pdf(etf_t)
            for item in pdf:
                stock_t = item['stck_shrn_iscd']
                weight = float(item['etf_cnst_itms_rt'] or 0)
                if stock_t not in lookup or weight > lookup[stock_t][1]:
                    lookup[stock_t] = (etf_t, weight)
            time.sleep(0.05) # TPS 제한 준수
        return {k: v[0] for k, v in lookup.items()}

# ---------------------------------------------------------
# 3. 페이지 처리 로직
# ---------------------------------------------------------
def process_page(page, engine, client, config, k200, kd150, industry_lookup):
    pid = page["id"]
    props = page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val

    # 1. KIS API 종목 상세 조회 (종목명, 마켓, 섹터, 산업)
    headers = {**engine.headers, "tr_id": "CTAC1503R"}
    res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/search-stock-info", 
                       headers=headers, params={"PRDT_TYPE_CD": "300", "PDNO": clean_t}).json()
    item = res.get('output', {})
    if not item: return

    stock_name = item['prdt_abrv_name']
    m_raw = item['mkt_id_nm']
    is_etf = "ETF" in m_raw.upper() or "ETN" in m_raw.upper()
    
    # 🌟 Market 판별 (사용자 규칙)
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "유가증권" in m_raw else "KOSDAQ")
    
    # 🌟 우량주 및 벤치마크 판별
    us_tracking_tag = None
    target_m_t = None
    
    if is_etf:
        name_clean = stock_name.replace(" ", "").upper()
        for kw, rule in ETF_THEME_RULES.items():
            if kw.upper() in name_clean:
                us_tracking_tag, target_m_t = rule["tag"], rule["bm"]
                break
    else:
        if clean_t in k200: us_tracking_tag, target_m_t = "KOSPI 200", "069500"
        elif clean_t in kd150: us_tracking_tag, target_m_t = "KOSDAQ 150", "229200"

    # 기본 시장BM 설정
    if not target_m_t:
        if is_etf: target_m_t = "292190"
        elif market_label == "KOSPI": target_m_t = "226490"

    # 🌟 산업BM 설정 (비중 분석 결과 사용)
    target_ind_t = industry_lookup.get(clean_t)

    # 노션 데이터 업데이트 구성
    def make_rt(v): return {"rich_text": [{"text": {"content": str(v)}}]}
    upd = {
        "종목명": make_rt(stock_name),
        "Market": {"select": {"name": market_label}},
        "KR_섹터": make_rt(item.get('idx_bztp_lcls_nm', 'ETF')),
        "KR_산업": make_rt(item.get('idx_bztp_mcls_nm', 'ETF')),
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    if us_tracking_tag: upd["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
    
    # 관계형 BM 연결
    if target_m_t and target_m_t != clean_t:
        if m_id := config["ticker_to_id"].get(target_m_t):
            upd["시장BM"] = {"relation": [{"id": m_id}]}
    if target_ind_t and target_ind_t != clean_t:
        if ind_id := config["ticker_to_id"].get(target_ind_t):
            upd["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=upd)
        logger.info(f"✅ {clean_t} ({stock_name}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ {clean_t} 실패: {e}")

# ---------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------
def main():
    engine = KISEngine()
    client = Client(auth=NOTION_TOKEN)

    # A. 지수 데이터 및 지표 DB 로드
    k200 = engine.get_index_members("0001")
    kd150 = engine.get_index_members("1001")
    
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    cursor = None
    while True:
        res = client.databases.query(database_id=BENCHMARK_DATABASE_ID, start_cursor=cursor)
        for p in res.get("results", []):
            props = p["properties"]
            t_list = props.get("이름", {}).get("title", []) or props.get("티커", {}).get("rich_text", [])
            if t_list:
                ticker = t_list[0]["plain_text"].strip().upper()
                config["ticker_to_id"][ticker] = p["id"]
                if props.get("구분", {}).get("select", {}).get("name") == "KR산업":
                    config["kr_industry_tickers"].append(ticker)
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    # B. 🌟 산업BM 룩업 테이블 구축 (KIS PDF 분석)
    industry_lookup = engine.build_industry_lookup(config["kr_industry_tickers"])

    # C. 🌟 수동 전체 업데이트를 위해 모든 페이지 로드
    all_pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
    
    logger.info(f"🚀 총 {len(all_pages)}개 종목 전체 업데이트 시작...")
    
    # D. 병렬 처리 (TPS 제한 고려)
    with ThreadPoolExecutor(max_workers=3) as executor:
        for page in all_pages:
            executor.submit(process_page, engine, client, config, k200, kd150, industry_lookup)
            time.sleep(0.06)

if __name__ == "__main__":
    main()
