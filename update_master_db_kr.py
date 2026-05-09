import os, re, time, logging, json, requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from notion_client import Client

# 1. 환경 변수 및 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

# 모의투자 서버 주소
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ETF 테마 분류 규칙
ETF_THEME_RULES = {
    "S&P500": {"tag": "S&P 500", "bm": "SPY"},
    "나스닥100": {"tag": "NASDAQ 100", "bm": "QQQ"},
    "미국배당": {"tag": "US Dividend", "bm": "SCHD"},
    "AI전력": {"tag": "US AI Power", "bm": "XLU"},
    "HBM": {"tag": "Global AI Memory", "bm": "SOXX"}
}

# ---------------------------------------------------------
# 2. KIS 모의투자 전용 엔진 (안정성 강화 버전)
# ---------------------------------------------------------
class KISVtsEngine:
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
        try:
            res = requests.post(url, json=body)
            return res.json().get('access_token')
        except Exception as e:
            logger.error(f"❌ KIS 토큰 발급 실패: {e}")
            return None

    def safe_api_call(self, url, params, tr_id):
        """JSONDecodeError 및 빈 응답 방지를 위한 안전 호출"""
        headers = {**self.headers, "tr_id": tr_id}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=12)
            if res.status_code != 200 or not res.text.strip():
                return {}
            return res.json()
        except:
            return {}

    def get_stock_info(self, ticker):
        """🌟 최대 3회 재시도 로직 추가 (모의투자 서버 튕김 방지)"""
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": ticker}
        
        for attempt in range(3): # 최대 3번 시도
            data = self.safe_api_call(url, params, "FHKST01010100")
            item = data.get('output', {})
            
            if item and item.get('bstp_kor_isnm'):
                return item # 데이터 확보 성공 시 즉시 반환
            
            # 실패 시 대기 시간 조절 (재시도할수록 더 길게 대기)
            wait_time = 0.5 * (attempt + 1)
            logger.debug(f"🔄 {ticker} 재시도 중... ({attempt + 1}/3) - {wait_time}s 대기")
            time.sleep(wait_time)
            
        return {}

    def get_index_members(self, index_code):
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-index-category-constituent-stock"
        res = self.safe_api_call(url, {"fid_cond_mrkt_div_code": "U", "fid_input_iscd": index_code}, "FHPST01200000")
        return [item['mksc_shrn_iscd'] for item in res.get('output2', [])]

    def get_etf_pdf(self, etf_ticker):
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-etf-constituent-stocks"
        res = self.safe_api_call(url, {"fid_input_iscd": etf_ticker}, "FHPST02410000")
        return res.get('output2', [])

# ---------------------------------------------------------
# 3. 페이지 처리 로직
# ---------------------------------------------------------
def process_page(page, engine, client, config, k200, kd150, industry_lookup):
    pid = page["id"]
    props = page["properties"]
    
    ticker = ""
    for name in ["티커", "Ticker"]:
        if name in props:
            content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
            if content:
                ticker = content[0].get("plain_text", "").strip().upper().split('.')[0]
                break
    
    if not (ticker and len(ticker) == 6 and ticker.isdigit()): return

    # 1. KIS API 데이터 수집 (재시도 로직 포함된 함수 호출)
    item = engine.get_stock_info(ticker)
    
    if not item or not item.get('bstp_kor_isnm'):
        logger.warning(f"⏩ {ticker}: 3회 재시도 후에도 응답 없음 (Skip)")
        return

    # 종목명 확보
    stock_name_raw = props.get("종목정보", {}).get("rich_text") or props.get("종목명", {}).get("rich_text")
    stock_name = stock_name_raw[0].get("plain_text", "") if stock_name_raw else ticker

    # 마켓 및 업종명 추출
    m_raw = item.get('rprs_mrkt_kor_name', '')
    is_etf = "ETF" in m_raw.upper()
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "KOSPI" in m_raw.upper() else "KOSDAQ")
    industry_name = item.get('bstp_kor_isnm', '기타')

    # 2. 우량주 및 시장BM 판별
    us_tracking_tag, target_m_t = None, None
    if is_etf:
        name_clean = stock_name.replace(" ", "").upper()
        for kw, rule in ETF_THEME_RULES.items():
            if kw.upper() in name_clean:
                us_tracking_tag, target_m_t = rule["tag"], rule["bm"]
                break
    else:
        if ticker in k200: us_tracking_tag, target_m_t = "KOSPI 200", "069500"
        elif ticker in kd150: us_tracking_tag, target_m_t = "KOSDAQ 150", "229200"

    if not target_m_t:
        target_m_t = "292190" if is_etf else ("226490" if market_label == "KOSPI" else None)

    # 3. 산업BM (룩업 테이블 활용)
    target_ind_t = industry_lookup.get(ticker)

    # 4. 노션 업데이트
    def make_rt(v): return {"rich_text": [{"text": {"content": str(v)}}]}
    upd = {
        "Market": {"select": {"name": market_label}},
        "KR_섹터": make_rt(industry_name if not is_etf else "ETF"),
        "KR_산업": make_rt(industry_name if not is_etf else "ETF"),
        "업데이트 일자": {"date": {"start": datetime.now(timezone(timedelta(hours=9))).isoformat()}}
    }
    if us_tracking_tag: upd["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
    
    if target_m_t and (m_id := config["ticker_to_id"].get(target_m_t)):
        upd["시장BM"] = {"relation": [{"id": m_id}]}
    if target_ind_t and (ind_id := config["ticker_to_id"].get(target_ind_t)):
        upd["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=upd)
        logger.info(f"✅ {ticker} ({industry_name}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ {ticker} 에러: {e}")

# ---------------------------------------------------------
# 4. 메인 실행 제어
# ---------------------------------------------------------
def main():
    engine = KISVtsEngine()
    if not engine.token: return
    
    client = Client(auth=NOTION_TOKEN)
    
    # A. 지수 데이터 확보
    k200 = engine.get_index_members("0001")
    kd150 = engine.get_index_members("1001")
    
    # B. 지표지수 DB 로드
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    cursor = None
    while True:
        res = client.databases.query(database_id=BENCHMARK_DATABASE_ID, start_cursor=cursor)
        for p in res.get("results", []):
            props = p["properties"]
            t_list = props.get("이름", {}).get("title") or props.get("티커", {}).get("rich_text", [])
            if t_list:
                ticker = t_list[0]["plain_text"].strip().upper().split('.')[0]
                config["ticker_to_id"][ticker] = p["id"]
                if props.get("구분", {}).get("select", {}).get("name") == "KR산업":
                    config["kr_industry_tickers"].append(ticker)
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    # C. 산업BM 룩업 구축 (PDF 분석)
    industry_lookup = {}
    logger.info(f"📊 {len(config['kr_industry_tickers'])}개 산업 ETF 분석 중 (매우 천천히 진행)...")
    for etf_t in config["kr_industry_tickers"]:
        pdf = engine.get_etf_pdf(etf_t)
        for item in pdf:
            stk = item.get('stck_shrn_iscd')
            w = float(item.get('etf_cnst_itms_rt') or 0)
            if stk and (stk not in industry_lookup or w > industry_lookup[stk][1]):
                industry_lookup[stk] = (etf_t, w)
        time.sleep(0.3) # 산업 ETF 분석 사이 간격 추가
    industry_lookup = {k: v[0] for k, v in industry_lookup.items()}

    # D. 마스터 DB 전체 로드
    all_pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    logger.info(f"🚀 총 {len(all_pages)}개 종목 업데이트 시작 (안전 모드)")
    
    # 🌟 워커 수를 2개로 줄이고 간격을 늘려 서버 부하 최소화
    with ThreadPoolExecutor(max_workers=2) as executor:
        for page in all_pages:
            executor.submit(process_page, page, engine, client, config, k200, kd150, industry_lookup)
            time.sleep(0.3) # 요청 간격을 0.3초로 상향

if __name__ == "__main__":
    main()
