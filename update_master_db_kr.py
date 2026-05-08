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
MASTER_DATABASE_ID = os.environ.get("DATABASE_ID") # 노션 DB ID
BENCHMARK_DATABASE_ID = os.environ.get("INDEX_DATABASE_ID") # 지표지수 DB ID
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

URL_BASE = "https://openapivts.koreainvestment.com:29443"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 사용자님의 오리지널 테마 규칙
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
# 2. 지표 DB 및 KIS API 토큰
# ---------------------------------------------------------
def get_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers=headers, data=json.dumps(body))
    return res.json().get('access_token')

def get_dynamic_config(client):
    """원본: 지표지수 DB 로드 (그대로 유지)"""
    logger.info("🔍 지표지수 DB 동적 분석 시작...")
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    try:
        pages, cursor = [], None
        while True:
            res = client.databases.query(database_id=BENCHMARK_DATABASE_ID, page_size=100, start_cursor=cursor)
            pages.extend(res.get("results", []))
            if not res.get("has_more"): break
            cursor = res.get("next_cursor")

        for page in pages:
            props = page["properties"]
            ticker_list = props.get("티커", {}).get("title") or props.get("이름", {}).get("title", [])
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

# ---------------------------------------------------------
# 3. 데이터 엔진 (FDR 제거, pykrx 유지)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 인덱스 및 ETF PDF 엔진 가동 (pykrx)...")
        # FDR 대신 KIS API를 개별적으로 호출할 것이므로 FDR 제거
        self.k200_list = self._get_index_list("1028")
        self.kd150_list = self._get_index_list("2203")
        self.kr_industry_lookup = self._build_industry_lookup(kr_industry_tickers)

    def _get_index_list(self, code):
        """원본 유지: 코스피200 / 코스닥150 목록 조회"""
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(code, date)
                if res and len(res) > 50: return res
            except: continue
        return []

    def _build_industry_lookup(self, tickers):
        """원본 유지: 다이내믹 산업BM 역추적 (최고 비중 ETF 매핑)"""
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
# 4. 개별 페이지 처리 (원본 로직 + KIS 데이터)
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config, kis_token):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    
    # 한국 종목 필터링
    is_kr = ticker_val and (ticker_val.endswith(('.KS', '.KQ')) or (len(ticker_val) >= 6 and ticker_val[0].isdigit())) and not ticker_val.endswith(('.T', '.TA', '.TW'))
    if not is_kr: return

    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val.split('.')[0]

    # 🌟 FDR 대신 KIS API 호출 (더 정확한 이름/섹터/산업 정보)
    headers = {"Content-Type":"application/json", "authorization":f"Bearer {kis_token}", "appkey":KIS_APP_KEY, "appsecret":KIS_APP_SECRET, "tr_id":"CTAC1503R", "custtype":"P"}
    try:
        res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/search-stock-info", headers=headers, params={"PRDT_TYPE_CD": "300", "PDNO": clean_t})
        item = res.json().get('output', {})
    except:
        item = {}

    if not item.get('prdt_abrv_name'): return # KIS에 데이터가 없으면 패스

    stock_name = item['prdt_abrv_name']
    m_raw = item.get('mkt_id_nm', '')
    is_etf = "ETF" in m_raw.upper()
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "유가증권" in m_raw else "KOSDAQ")
    
    # KIS API 기반 깔끔한 섹터/산업 정보 (기존 HEADERS 하드코딩 대체)
    sec_val = item.get('idx_bztp_lcls_nm', '') if not is_etf else "ETF"
    ind_val = item.get('idx_bztp_mcls_nm', '') if not is_etf else "ETF"

    # --- 여기서부터 원본의 강력한 BM 판별 로직 그대로 유지 ---
    us_tracking_tag = None
    target_m_t = None
    
    if is_etf:
        name_no_space = stock_name.replace(" ", "").upper()
        for keyword, rule in ETF_THEME_RULES.items():
            if keyword.upper() in name_no_space:
                us_tracking_tag = rule["tag"]
                target_m_t = rule["bm"]
                break

    # 일반 시장BM 로직 (테마가 없는 경우)
    if not target_m_t:
        if clean_t in engine.k200_list: target_m_t = "069500"
        elif clean_t in engine.kd150_list: target_m_t = "229200"
        elif is_etf: target_m_t = "292190"
        elif market_label == "KOSPI": target_m_t = "226490"

    # 🌟 원본의 다이내믹 산업BM 추출
    target_ind_t = engine.kr_industry_lookup.get(clean_t)

    def make_rich_text(val):
        return {"rich_text": [{"text": {"content": str(val)}}]} if val else {"rich_text": []}

    update_props = {
        "종목명": make_rich_text(stock_name),
        "Market": {"select": {"name": market_label}},
        "KR_섹터": make_rich_text(sec_val),
        "KR_산업": make_rich_text(ind_val),
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }
    
    # 원본: '우량주' 열에 테마 태그 부여 (Multi-select)
    if us_tracking_tag:
        update_props["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
    
    # 원본: 시장/산업 BM 관계형 연결
    if target_m_t and target_m_t != clean_t:
        if m_id := config["ticker_to_id"].get(target_m_t):
            update_props["시장BM"] = {"relation": [{"id": m_id}]}
    if target_ind_t and target_ind_t != clean_t:
        if ind_id := config["ticker_to_id"].get(target_ind_t):
            update_props["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [KR] {clean_t} ({stock_name}) 처리 완료")
    except Exception as e:
        logger.error(f"   ❌ [KR] {clean_t} 노션 전송 실패: {e}")

# ---------------------------------------------------------
# 5. 메인 (원본의 병렬 처리 유지)
# ---------------------------------------------------------
def main():
    custom_client = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=custom_client)
    
    config = get_dynamic_config(client)
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])
    kis_token = get_access_token()
    
    if not kis_token:
        logger.error("❌ KIS 토큰 발급 실패. 종료합니다.")
        return

    all_pages, cursor = [], None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
        time.sleep(0.1)

    if all_pages:
        # KIS API는 초당 20건 제한이 있으므로 workers를 3 정도로 조정
        with ThreadPoolExecutor(max_workers=3) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, config, kis_token)
                time.sleep(0.05) # API 과부하 방지
    
    logger.info("✨ 하이브리드 업데이트 프로세스 완료")

if __name__ == "__main__":
    main()
