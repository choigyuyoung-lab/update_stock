import os, re, time, logging, json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import httpx
import requests
import pandas as pd
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID") or os.environ.get("DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID") or os.environ.get("INDEX_DATABASE_ID")

KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
KRX_ID = os.environ.get("KRX_ID")
KRX_PW = os.environ.get("KRX_PW")

URL_BASE = "https://openapivts.koreainvestment.com:29443"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

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
# 2. 엔진 및 유틸리티
# ---------------------------------------------------------
def get_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        return res.json().get('access_token')
    except: return None

def get_dynamic_config(client):
    logger.info("🔍 지표지수 DB 로드 중...")
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    pages, cursor = [], None
    while True:
        res = client.databases.query(database_id=BENCHMARK_DATABASE_ID, start_cursor=cursor)
        pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
    for page in pages:
        props = page["properties"]
        t_list = props.get("티커", {}).get("title") or props.get("이름", {}).get("title", [])
        if t_list:
            ticker = t_list[0]["plain_text"].strip().upper()
            config["ticker_to_id"][ticker] = page["id"]
            if props.get("구분", {}).get("select", {}).get("name") == "KR산업":
                config["kr_industry_tickers"].append(ticker)
    return config

class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
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
# 3. 핵심 처리 로직
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config, kis_token):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    if not (ticker_val.endswith(('.KS', '.KQ')) or (len(ticker_val) >= 6 and ticker_val[0].isdigit())): return

    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val.split('.')[0]

    # KIS API 호출
    headers = {"Content-Type":"application/json", "authorization":f"Bearer {kis_token}", "appkey":KIS_APP_KEY, "appsecret":KIS_APP_SECRET, "tr_id":"CTAC1503R", "custtype":"P"}
    try:
        res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/search-stock-info", headers=headers, params={"PRDT_TYPE_CD": "300", "PDNO": clean_t})
        item = res.json().get('output', {})
    except: return

    if not item.get('prdt_abrv_name'): return

    stock_name = item['prdt_abrv_name']
    m_raw = item.get('mkt_id_nm', '')
    is_etf = "ETF" in m_raw.upper()
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "유가증권" in m_raw else "KOSDAQ")
    
    sec_val = item.get('idx_bztp_lcls_nm', '') if not is_etf else "ETF"
    ind_val = item.get('idx_bztp_mcls_nm', '') if not is_etf else "ETF"

    us_tracking_tag, target_m_t = None, None
    if is_etf:
        name_no_space = stock_name.replace(" ", "").upper()
        for keyword, rule in ETF_THEME_RULES.items():
            if keyword.upper() in name_no_space:
                us_tracking_tag, target_m_t = rule["tag"], rule["bm"]
                break

    if not target_m_t:
        if clean_t in engine.k200_list: target_m_t = "069500"
        elif clean_t in engine.kd150_list: target_m_t = "229200"
        elif is_etf: target_m_t = "292190"
        elif market_label == "KOSPI": target_m_t = "226490"

    target_ind_t = engine.kr_industry_lookup.get(clean_t)

    def make_rt(val): return {"rich_text": [{"text": {"content": str(val)}}]} if val else {"rich_text": []}

    upd = {
        "종목명": make_rt(stock_name),
        "Market": {"select": {"name": market_label}},
        "KR_섹터": make_rt(sec_val),
        "KR_산업": make_rt(ind_val),
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    if us_tracking_tag: upd["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
    if target_m_t and target_m_t != clean_t:
        if m_id := config["ticker_to_id"].get(target_m_t): upd["시장BM"] = {"relation": [{"id": m_id}]}
    if target_ind_t and target_ind_t != clean_t:
        if ind_id := config["ticker_to_id"].get(target_ind_t): upd["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=upd)
        logger.info(f"✅ {clean_t} ({stock_name}) 완료")
    except Exception as e: logger.error(f"❌ {clean_t} 실패: {e}")

# ---------------------------------------------------------
# 4. 메인 (복구: 모든 페이지 조회 방식)
# ---------------------------------------------------------
def main():
    custom_client = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=custom_client)
    
    config = get_dynamic_config(client)
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])
    kis_token = get_access_token()
    
    if not kis_token: return

    is_full = os.environ.get("IS_FULL_UPDATE", "false").lower() == "true"
    
    all_pages, cursor = [], None
    while True:
        # 🌟 필터를 제거하고 모든 페이지를 가져옵니다 (원본 방식 복구)
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    targets = []
    for p in all_pages:
        if is_full: targets.append(p)
        else:
            # 부분 업데이트 로직: 종목명이 없거나 업데이트 일자가 없는 경우만
            props = p["properties"]
            has_name = props.get("종목명", {}).get("rich_text")
            has_date = props.get("업데이트 일자", {}).get("date")
            if not has_name or not has_date: targets.append(p)

    if targets:
        logger.info(f"🚀 {len(targets)}개 종목 업데이트 시작...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            for page in targets:
                executor.submit(process_page_kr, page, engine, client, config, kis_token)
                time.sleep(0.05)
    else: logger.info("✅ 새로운 항목이 없습니다.")

if __name__ == "__main__":
    main()
