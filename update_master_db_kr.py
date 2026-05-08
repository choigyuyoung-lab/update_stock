import os, re, time, logging, requests, json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import httpx
import pandas as pd
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")

# 🌟 한국투자증권 API 설정 추가
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
URL_BASE = "https://openapivts.koreainvestment.com:29443" # 실전투자 시 https://openapi.koreainvestment.com:9443

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 🌟 최종 완성된 테마 ETF 판별 규칙 (기존 유지)
ETF_THEME_RULES = {
    "S&P500": {"tag": "S&P 500", "bm": "SPY"},
    "나스닥100": {"tag": "NASDAQ 100", "bm": "QQQ"},
    "미국배당": {"tag": "US Dividend", "bm": "SCHD"},
    "AI전력": {"tag": "US AI Power", "bm": "XLU"},
    "AI광통신": {"tag": "US AI Optical Network", "bm": "IGN"},
    "미국빅테크": {"tag": "US Big Tech", "bm": "XLK"},
    "구글밸류": {"tag": "Google Focused", "bm": "QQQ"},
    "마이크로소프트밸류": {"tag": "MS Focused", "bm": "QQQ"},
    "엔비디아밸류": {"tag": "Nvidia Focused", "bm": "SOXX"},
    "우주테크&방산": {"tag": "Global Aerospace & Defense", "bm": "XAR"},
    "우주항공": {"tag": "US Aerospace & Defense", "bm": "XAR"},
    "AI&로봇": {"tag": "Global AI & Robot", "bm": "BOTZ"},
    "HBM": {"tag": "Global AI Memory", "bm": "SOXX"},
    "AI메모리": {"tag": "Global AI Memory", "bm": "SOXX"},
    "팔란티어밸류": {"tag": "Palantir Focused", "bm": "QQQ"}
}

# ---------------------------------------------------------
# 2. 한투 API 유틸리티
# ---------------------------------------------------------
def get_access_token():
    """한투 API 접근 토큰 발급"""
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers=headers, data=json.dumps(body))
    return res.json().get('access_token')

def get_kis_stock_info(ticker, token):
    """🌟 한투 API를 이용한 종목 상세 정보 조회 (TR: CTAC1503R)"""
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "CTAC1503R", # 종목정보 조회 TR
        "custtype": "P"
    }
    params = {"PRDT_TYPE_CD": "300", "PDNO": ticker} # 300: 주식/ETF/ETN
    try:
        res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/search-stock-info", headers=headers, params=params)
        return res.json().get('output', {})
    except:
        return {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (기존 pykrx 기반 인덱스 로직 유지)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 인덱스/PDF 데이터 엔진 가동...")
        # fdr.StockListing 제거 (한투 API로 대체됨)
        self.k200_list = self._get_index_list("1028")
        self.kd150_list = self._get_index_list("2203")
        self.kr_industry_lookup = self._build_industry_lookup(kr_industry_tickers)

    def _get_index_list(self, code):
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(code, date)
                if res and len(res) > 50: return res
            except: continue
        return []

    def _build_industry_lookup(self, tickers):
        lookup = {}
        for etf_t in tickers:
            try:
                pdf = stock.get_etf_portfolio_deposit_file(etf_t)
                if pdf is not None and not pdf.empty:
                    w_col = '비중' if '비중' in pdf.columns else pdf.columns[0]
                    for stock_t, row in pdf.iterrows():
                        weight = float(row[w_col])
                        if stock_t not in lookup or weight > lookup[stock_t][1]:
                            lookup[stock_t] = (etf_t, weight)
            except: continue
        return {k: v[0] for k, v in lookup.items()}

# ---------------------------------------------------------
# 4. 페이지 처리 (한투 API 통합)
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config, kis_token):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val

    # 🌟 1. 한투 API로부터 데이터 수집
    item = get_kis_stock_info(clean_t, kis_token)
    if not item: return

    # 🌟 2. 데이터 매핑 (한투 API 결과값 기준)
    stock_name = item.get('prdt_abrv_name', '') # 종목 약명
    m_raw = item.get('mkt_id_nm', '')           # 시장명 (유가증권, 코스닥 등)
    is_etf = "ETF" in m_raw or "ETN" in m_raw
    market_label = "ETF(KR)" if is_etf else ("KOSDAQ" if "코스닥" in m_raw else "KOSPI")
    
    # 산업/섹터 정보 (한투 표준 분류 사용)
    sec_val = item.get('idx_bztp_lcls_nm', '') # 업종 대분류명
    ind_val = item.get('idx_bztp_mcls_nm', '') # 업종 중분류명 (산업)

    # 테마 판별 및 시장BM 결정 (기존 로직 유지)
    us_tracking_tag = None
    target_m_t = None
    
    if is_etf:
        name_no_space = stock_name.replace(" ", "").upper()
        for keyword, rule in ETF_THEME_RULES.items():
            if keyword.upper() in name_no_space:
                us_tracking_tag = rule["tag"]
                target_m_t = rule["bm"]
                break

    if not target_m_t:
        if clean_t in engine.k200_list: target_m_t = "069500"
        elif clean_t in engine.kd150_list: target_m_t = "229200"
        elif is_etf: target_m_t = "292190"
        elif market_label == "KOSPI": target_m_t = "226490"

    target_ind_t = engine.kr_industry_lookup.get(clean_t)

    def make_rich_text(val):
        return {"rich_text": [{"text": {"content": str(val)}}]} if val else {"rich_text": []}

    # 🌟 3. 노션 업데이트 데이터 구성
    update_props = {
        "종목명": make_rich_text(stock_name),
        "Market": {"select": {"name": market_label}},
        "KR_섹터": make_rich_text(sec_val),
        "KR_산업": make_rich_text(ind_val),
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    if us_tracking_tag:
        update_props["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
    
    if target_m_t and target_m_t != clean_t:
        if m_id := config["ticker_to_id"].get(target_m_t):
            update_props["시장BM"] = {"relation": [{"id": m_id}]}
    if target_ind_t and target_ind_t != clean_t:
        if ind_id := config["ticker_to_id"].get(target_ind_t):
            update_props["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KIS] {clean_t} ({stock_name}) 업데이트 완료")
    except Exception as e:
        logger.error(f"   ❌ [KR] {clean_t} 실패: {e}")

# ---------------------------------------------------------
# 5. 메인 실행 함수
# ---------------------------------------------------------
def main():
    custom_client = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=custom_client)
    
    # 1단계: 지표 설정 로드
    # (get_dynamic_config 함수는 기존 코드와 동일하므로 상단에서 가져온다고 가정)
    config = get_dynamic_config(client)
    
    # 2단계: KIS 토큰 및 엔진 초기화
    kis_token = get_access_token()
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])
    
    # 3단계: 노션 DB 쿼리
    all_pages, cursor = [], None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
        time.sleep(0.1)

    # 4단계: 병렬 업데이트 실행
    if all_pages:
        # 한투 API 과부하 방지를 위해 max_workers를 3~5 정도로 유지 권장
        with ThreadPoolExecutor(max_workers=3) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, config, kis_token)
                time.sleep(0.05) # KIS API 초당 호출 제한(TPS) 준수용
    
    logger.info("✨ KIS API 기반 한국 주식 마스터 DB 업데이트 완료")

# get_dynamic_config 함수 (기존 코드 그대로 사용)
def get_dynamic_config(client):
    logger.info("🔍 지표지수 DB 동적 분석 시작...")
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    try:
        pages, cursor = [], None
        while True:
            query_params = {"database_id": BENCHMARK_DATABASE_ID, "page_size": 100}
            if cursor: query_params["start_cursor"] = cursor
            res = client.databases.query(**query_params)
            pages.extend(res.get("results", []))
            if not res.get("has_more"): break
            cursor = res.get("next_cursor")
        for page in pages:
            props = page["properties"]
            ticker_list = props.get("이름", {}).get("title", []) or props.get("티커", {}).get("rich_text", [])
            if not ticker_list: continue
            ticker = ticker_list[0]["plain_text"].strip().upper()
            select_obj = props.get("구분", {}).get("select")
            category = select_obj.get("name", "") if select_obj else ""
            config["ticker_to_id"][ticker] = page["id"]
            if category == "KR산업":
                config["kr_industry_tickers"].append(ticker)
        logger.info(f"✅ 지표 로드 완료 (총 {len(config['ticker_to_id'])}개)")
    except Exception as e:
        logger.error(f"❌ 지표 DB 로드 실패: {e}")
    return config

if __name__ == "__main__":
    main()
