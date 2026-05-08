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

# [ETF 테마 규칙 생략 - 기존과 동일]

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

    def get_stock_master_info(self, ticker):
        """🌟 모의투자에서 가장 안정적인 '현재가 시세' TR(FHKST01010100) 활용"""
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = {**self.headers, "tr_id": "FHKST01010100"} # 가격 업데이트 때 썼던 그 ID
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": ticker}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=10)
            return res.json().get('output', {})
        except:
            return {}

def process_page(page, engine, client, config, k200, kd150, industry_lookup):
    pid = page["id"]
    props = page["properties"]
    
    # 1. 티커 추출
    ticker = ""
    for name in ["티커", "Ticker"]:
        if name in props:
            content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
            if content:
                ticker = content[0].get("plain_text", "").strip().upper().split('.')[0]
                break
    
    if not (ticker and len(ticker) == 6 and ticker.isdigit()): return

    # 2. 🌟 KIS 시세 API로 정보 확보
    item = engine.get_stock_master_info(ticker)
    
    # 시세 API가 응답을 주지 않을 경우 (모의투자 대비)
    if not item or not item.get('bstp_kor_isnm'):
        logger.warning(f"⏩ {ticker}: 시세 API 응답 없음 (Skip)")
        return

    # 종목명은 노션에 있는 것을 우선 쓰고, API에 있으면 그것을 사용
    stock_name_raw = props.get("종목정보", {}).get("rich_text")
    stock_name = stock_name_raw[0].get("plain_text", "") if stock_name_raw else ticker

    # API에서 주는 마켓 및 업종 정보 활용
    m_raw = item.get('rprs_mrkt_kor_name', '') # 대표 시장명 (KOSPI/KOSDAQ)
    is_etf = "ETF" in m_raw.upper()
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "KOSPI" in m_raw.upper() else "KOSDAQ")
    
    # 업종 정보를 섹터/산업으로 활용
    industry_info = item.get('bstp_kor_isnm', '') # 업종 한글명

    # 3. 우량주/지수 판별
    us_tracking_tag, target_m_t = None, None
    if not is_etf:
        if ticker in k200: us_tracking_tag, target_m_t = "KOSPI 200", "069500"
        elif ticker in kd150: us_tracking_tag, target_m_t = "KOSDAQ 150", "229200"

    if not target_m_t:
        target_m_t = "292190" if is_etf else ("226490" if market_label == "KOSPI" else None)

    target_ind_t = industry_lookup.get(ticker)

    # 4. 노션 업데이트
    def make_rt(v): return {"rich_text": [{"text": {"content": str(v)}}]}
    upd = {
        "Market": {"select": {"name": market_label}},
        "KR_섹터": make_rt(industry_info if not is_etf else "ETF"),
        "KR_산업": make_rt(industry_info if not is_etf else "ETF"),
        "업데이트 일자": {"date": {"start": datetime.now(timezone(timedelta(hours=9))).isoformat()}}
    }
    if us_tracking_tag: upd["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
    
    if target_m_t and (m_id := config["ticker_to_id"].get(target_m_t)):
        upd["시장BM"] = {"relation": [{"id": m_id}]}
    if target_ind_t and (ind_id := config["ticker_to_id"].get(target_ind_t)):
        upd["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=upd)
        logger.info(f"✅ {ticker} ({industry_info}) 완료")
    except Exception as e:
        logger.error(f"❌ {ticker} 업데이트 에러: {e}")

# [main 함수 생략 - 이전과 동일하게 유지]
