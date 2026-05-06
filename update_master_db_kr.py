import os, re, time, logging, io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import httpx
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 기초 정보 추출용 우선순위 헤더
HEADERS = {
    "KR_SECTOR": ['Sector', 'WICS 업종명', '업종'],
    "KR_INDUSTRY": ['Industry', '주요제품', 'WICS 제품']
}

# 🌟 최종 완성된 테마 ETF 판별 규칙 (광통신 네트워크 추가)
ETF_THEME_RULES = {
    "S&P500": {"tag": "S&P 500", "bm": "SPY"},
    "나스닥100": {"tag": "NASDAQ 100", "bm": "QQQ"},
    "미국배당": {"tag": "US Dividend", "bm": "SCHD"},
    "AI전력": {"tag": "US AI Power", "bm": "XLU"},
    "AI광통신": {"tag": "US AI Optical Network", "bm": "IGN"}, # 👈 추가된 로직
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
# 2. 지표 DB 동적 분석 (전체 로드 및 에러 방지)
# ---------------------------------------------------------
def get_dynamic_config(client):
    logger.info("🔍 지표지수 DB 동적 분석 시작...")
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    try:
        pages = []
        cursor = None
        while True:
            # 100개 이상의 지표를 누락 없이 가져오기 위한 페이지네이션[cite: 3]
            query_params = {"database_id": BENCHMARK_DATABASE_ID, "page_size": 100}
            if cursor: query_params["start_cursor"] = cursor
            res = client.databases.query(**query_params)
            pages.extend(res.get("results", []))
            if not res.get("has_more"): break
            cursor = res.get("next_cursor")

        for page in pages:
            props = page["properties"]
            ticker_list = props.get("이름", {}).get("title", [])
            if not ticker_list: continue
            ticker = ticker_list[0]["plain_text"].strip().upper()
            
            # NoneType 에러 방지를 위한 안전한 속성 추출[cite: 3]
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
# 3. 데이터 엔진
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 데이터 엔진 가동...")
        # 상세 텍스트 정보 수집용[cite: 4]
        self.df_kr_desc = fdr.StockListing('KRX-DESC').set_index('Code')
        self.kr_etf = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        self.k200_list = self._get_index_list("1028")
        self.kd150_list = self._get_index_list("2203")
        # ETF PDF 기반 최고 비중 매핑[cite: 3]
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

    def _get_val_from_headers(self, row, candidates):
        for col in candidates:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return None

# ---------------------------------------------------------
# 4. 페이지 처리 (기초 정보 + 테마 판별 + 지표 연결)
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val

    item, is_etf = None, False
    if clean_t in engine.df_kr_desc.index:
        item = engine.df_kr_desc.loc[clean_t]
    elif clean_t in engine.kr_etf:
        item = engine.kr_etf[clean_t]
        is_etf = True

    if item is not None:
        stock_name = item['Name']
        m_raw = str(item.get('Market', '')).upper()
        market_label = "ETF(KR)" if is_etf else ("KOSDAQ" if "KOSDAQ" in m_raw else "KOSPI")
        
        # 기초 텍스트 정보 추출[cite: 4]
        sec_val = engine._get_val_from_headers(item, HEADERS['KR_SECTOR']) if not is_etf else item.get('Category')
        ind_val = engine._get_val_from_headers(item, HEADERS['KR_INDUSTRY']) if not is_etf else "ETF"

        # 테마 판별 및 시장BM 결정[cite: 3]
        us_tracking_tag = None
        target_m_t = None
        
        if is_etf:
            name_no_space = stock_name.replace(" ", "").upper()
            for keyword, rule in ETF_THEME_RULES.items():
                if keyword.upper() in name_no_space:
                    us_tracking_tag = rule["tag"]
                    target_m_t = rule["bm"]
                    break

        # 일반 시장BM 로직 (테마가 없는 경우)[cite: 3]
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
        
        # '우량주' 열에 테마 태그 부여[cite: 4]
        if us_tracking_tag:
            update_props["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
        
        # 시장/산업 BM 관계형 연결[cite: 3]
        if target_m_t and target_m_t != clean_t:
            if m_id := config["ticker_to_id"].get(target_m_t):
                update_props["시장BM"] = {"relation": [{"id": m_id}]}
        if target_ind_t and target_ind_t != clean_t:
            if ind_id := config["ticker_to_id"].get(target_ind_t):
                update_props["산업BM"] = {"relation": [{"id": ind_id}]}

        try:
            client.pages.update(page_id=pid, properties=update_props)
            logger.info(f"   ✅ [KR] {clean_t} ({stock_name}) 업데이트 완료")
        except Exception as e:
            logger.error(f"   ❌ [KR] {clean_t} 실패: {e}")

# ---------------------------------------------------------
# 5. 메인 실행 함수
# ---------------------------------------------------------
def main():
    custom_client = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=custom_client)
    config = get_dynamic_config(client)
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])
    
    all_pages, cursor = [], None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
        time.sleep(0.1)

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, config)
                time.sleep(0.1)
    
    logger.info("✨ 한국 주식 마스터 DB 통합 업데이트 프로세스 완료")

if __name__ == "__main__":
    main()
