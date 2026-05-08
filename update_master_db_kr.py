import os, re, time, logging, json, requests
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

URL_BASE = "https://openapivts.koreainvestment.com:29443" 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ETF_THEME_RULES = {
    "S&P500": {"tag": "S&P 500", "bm": "SPY"},
    "나스닥100": {"tag": "NASDAQ 100", "bm": "QQQ"},
    "미국배당": {"tag": "US Dividend", "bm": "SCHD"},
    "AI전력": {"tag": "US AI Power", "bm": "XLU"},
    "HBM": {"tag": "Global AI Memory", "bm": "SOXX"}
}

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
        except Exception as e:
            logger.error(f"❌ 토큰 발급 실패: {e}")
            return None

    def safe_get_json(self, url, params, tr_id):
        headers = {**self.headers, "tr_id": tr_id}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=10)
            if res.status_code != 200:
                logger.debug(f"⚠️ API 응답 코드 에러: {res.status_code}")
                return {}
            return res.json()
        except Exception as e:
            logger.debug(f"⚠️ API 호출 중 예외: {e}")
            return {}

def process_page(page, engine, client, config, k200, kd150, industry_lookup):
    pid = page["id"]
    props = page["properties"]
    
    # 티커 추출 (Ticker/티커 공용)
    ticker = ""
    for name in ["티커", "Ticker"]:
        if name in props:
            content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
            if content:
                ticker = content[0].get("plain_text", "").strip().upper()
                break
    
    if not ticker: return

    # 한국 주식 여부 확인 (사용자님 update_price 규칙 적용)
    clean_t = ticker.split('.')[0]
    is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(clean_t) == 6 and clean_t.isdigit()))
    if not is_kr: return

    # 1. KIS API 종목 상세 조회
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/search-stock-info"
    data = engine.safe_get_json(url, {"PRDT_TYPE_CD": "300", "PDNO": clean_t}, "CTAC1503R")
    item = data.get('output', {})
    
    if not item:
        # ⚠️ 데이터를 못 가져왔을 때 로그 남기기
        logger.warning(f"⏩ {clean_t}: KIS API에서 종목 정보를 찾을 수 없음 (Skip)")
        return

    stock_name = item.get('prdt_abrv_name', '')
    m_raw = item.get('mkt_id_nm', '')
    is_etf = "ETF" in m_raw.upper() or "ETN" in m_raw.upper()
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "유가증권" in m_raw else "KOSDAQ")
    
    # 2. 우량주/시장BM 판별
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

    # 3. 산업BM
    target_ind_t = industry_lookup.get(clean_t)

    # 4. 노션 업데이트
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
        logger.info(f"✅ {clean_t} ({stock_name}) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ {clean_t} 업데이트 실패: {e}")

def main():
    engine = KISVtsEngine()
    if not engine.token: return
    
    client = Client(auth=NOTION_TOKEN)
    
    # 지수 멤버십
    k200 = [] # 모의투자 서버 이슈 대비 초기화
    kd150 = []
    try:
        url_idx = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-index-category-constituent-stock"
        res_k200 = engine.safe_get_json(url_idx, {"fid_cond_mrkt_div_code": "U", "fid_input_iscd": "0001"}, "FHPST01200000")
        k200 = [i['mksc_shrn_iscd'] for i in res_k200.get('output2', [])]
        
        res_kd150 = engine.safe_get_json(url_idx, {"fid_cond_mrkt_div_code": "U", "fid_input_iscd": "1001"}, "FHPST01200000")
        kd150 = [i['mksc_shrn_iscd'] for i in res_kd150.get('output2', [])]
    except:
        logger.warning("⚠️ 지수 리스트를 가져오지 못했습니다. (지수 판별 스킵)")

    # 지표지수 DB 로드
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=BENCHMARK_DATABASE_ID, start_cursor=cursor)
        pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
    
    for p in pages:
        t_list = p["properties"].get("이름", {}).get("title") or p["properties"].get("티커", {}).get("rich_text", [])
        if t_list:
            ticker = t_list[0]["plain_text"].strip().upper().split('.')[0]
            config["ticker_to_id"][ticker] = p["id"]
            if p["properties"].get("구분", {}).get("select", {}).get("name") == "KR산업":
                config["kr_industry_tickers"].append(ticker)

    # 산업BM 룩업 (동기 처리로 안정성 확보)
    industry_lookup = {}
    for etf_t in config["kr_industry_tickers"]:
        try:
            url_pdf = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-etf-constituent-stocks"
            pdf_data = engine.safe_get_json(url_pdf, {"fid_input_iscd": etf_t}, "FHPST02410000")
            for item in pdf_data.get('output2', []):
                stk = item.get('stck_shrn_iscd')
                w = float(item.get('etf_cnst_itms_rt') or 0)
                if stk and (stk not in industry_lookup or w > industry_lookup[stk][1]):
                    industry_lookup[stk] = (etf_t, w)
        except: continue
        time.sleep(0.1)
    industry_lookup = {k: v[0] for k, v in industry_lookup.items()}

    # 마스터 DB 로드
    all_pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    logger.info(f"🚀 총 {len(all_pages)}개 종목 업데이트 시작")
    
    # 안정성을 위해 우선 동기 방식으로 처리 (0.1초 대기 포함)
    for page in all_pages:
        process_page(page, engine, client, config, k200, kd150, industry_lookup)
        time.sleep(0.1)

if __name__ == "__main__":
    main()
