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
# yml 파일과 호환되도록 다중 변수명 지원
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID") or os.environ.get("DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID") or os.environ.get("INDEX_DATABASE_ID")

KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

# KRX 인증 환경 변수 (사용자 커스텀 모듈 대응)
KRX_ID = os.environ.get("KRX_ID")
KRX_PW = os.environ.get("KRX_PW")

URL_BASE = "https://openapivts.koreainvestment.com:29443"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 🌟 최종 완성된 테마 ETF 판별 규칙
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
    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        return res.json().get('access_token')
    except Exception as e:
        logger.error(f"❌ KIS 토큰 발급 에러: {e}")
        return None

def get_dynamic_config(client):
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
# 3. 데이터 엔진 (pykrx 기반 PDF/인덱스 분석)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 인덱스 및 ETF PDF 엔진 가동 (pykrx)...")
        # KRX 로그인 체크 (사용자 커스텀 환경 대응)
        if not KRX_ID or not KRX_PW:
            logger.warning("⚠️ KRX_ID 또는 KRX_PW가 설정되지 않았습니다. (일부 데이터 조회가 제한될 수 있습니다)")
            
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
# 4. 개별 페이지 처리 (KIS API + 테마/BM 로직)
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

    # KIS API로 공식 데이터 조회
    headers = {"Content-Type":"application/json", "authorization":f"Bearer {kis_token}", "appkey":KIS_APP_KEY, "appsecret":KIS_APP_SECRET, "tr_id":"CTAC1503R", "custtype":"P"}
    try:
        res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/search-stock-info", headers=headers, params={"PRDT_TYPE_CD": "300", "PDNO": clean_t})
        item = res.json().get('output', {})
    except:
        item = {}

    if not item.get('prdt_abrv_name'): return 

    stock_name = item['prdt_abrv_name']
    m_raw = item.get('mkt_id_nm', '')
    is_etf = "ETF" in m_raw.upper()
    market_label = "ETF(KR)" if is_etf else ("KOSPI" if "유가증권" in m_raw else "KOSDAQ")
    
    sec_val = item.get('idx_bztp_lcls_nm', '') if not is_etf else "ETF"
    ind_val = item.get('idx_bztp_mcls_nm', '') if not is_etf else "ETF"

    # 테마 및 BM 판별
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
        logger.info(f"   ✅ [KR] {clean_t} ({stock_name}) 처리 완료")
    except Exception as e:
        logger.error(f"   ❌ [KR] {clean_t} 노션 전송 실패: {e}")

# ---------------------------------------------------------
# 5. 메인 로직 (전체/부분 업데이트 분기)
# ---------------------------------------------------------
def main():
    custom_client = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=custom_client)
    
    config = get_dynamic_config(client)
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])
    kis_token = get_access_token()
    
    if not kis_token:
        logger.error("❌ KIS 토큰 발급 실패. 환경 변수를 확인하세요.")
        return

    # 🌟 수동(전체) vs 자동(부분) 판단 로직
    is_full_update = os.environ.get("IS_FULL_UPDATE", "false").lower() == "true"
    
    query_params = {"database_id": MASTER_DATABASE_ID}
    
    if not is_full_update:
        logger.info("⚡ [부분 업데이트 모드] 새로 추가된 종목만 탐색합니다.")
        query_params["filter"] = {
            "or": [
                {"property": "종목명", "rich_text": {"is_empty": True}},
                {"property": "업데이트 일자", "date": {"is_empty": True}}
            ]
        }
    else:
        logger.info("🔥 [전체 업데이트 모드] 수동 실행 - 모든 항목을 재검사합니다.")

    all_pages, cursor = [], None
    while True:
        if cursor: 
            query_params["start_cursor"] = cursor
            
        res = client.databases.query(**query_params)
        all_pages.extend(res.get("results", []))
        
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
        time.sleep(0.1)

    if all_pages:
        logger.info(f"🚀 총 {len(all_pages)}개의 항목을 업데이트합니다.")
        # API 과부하 방지를 위해 워커 수는 3으로 유지
        with ThreadPoolExecutor(max_workers=3) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, config, kis_token)
                time.sleep(0.05) 
    else:
        logger.info("✅ 업데이트할 새로운 항목이 없습니다.")
    
    logger.info("✨ 하이브리드 업데이트 프로세스 완료")

if __name__ == "__main__":
    main()
