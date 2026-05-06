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

# [기초 정보 추출용] 데이터셋별 우선순위 헤더 정의[cite: 4]
HEADERS = {
    "KR_SECTOR": ['Sector', 'WICS 업종명', '업종'],
    "KR_INDUSTRY": ['Industry', '주요제품', 'WICS 제품']
}

# ---------------------------------------------------------
# 2. 지표 DB 동적 분석
# ---------------------------------------------------------
def get_dynamic_config(client):
    logger.info("🔍 지표지수 DB 동적 분석 시작...")
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    
    try:
        pages = client.databases.query(database_id=BENCHMARK_DATABASE_ID).get("results", [])
        for page in pages:
            props = page["properties"]
            ticker_list = props.get("이름", {}).get("title", [])
            if not ticker_list: continue
            
            ticker = ticker_list[0]["plain_text"].strip()
            category = props.get("구분", {}).get("select", {}).get("name", "")
            
            config["ticker_to_id"][ticker] = page["id"]
            if category == "KR산업":
                config["kr_industry_tickers"].append(ticker)
                
        logger.info(f"✅ 지표 로드 완료 (총 {len(config['ticker_to_id'])}개, 산업 {len(config['kr_industry_tickers'])}개)")
    except Exception as e:
        logger.error(f"❌ 지표 DB 로드 실패: {e}")
    
    return config

# ---------------------------------------------------------
# 3. 데이터 엔진 (기초 정보 + 지표 매핑 결합)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 데이터 엔진 가동...")
        # 기초 정보 수집을 위해 KRX-DESC 사용[cite: 4]
        self.df_kr_desc = fdr.StockListing('KRX-DESC').set_index('Code')
        self.kr_etf = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        
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

    def _get_val_from_headers(self, row, candidates):
        """헤더 우선순위에 따라 텍스트 정보 추출[cite: 4]"""
        for col in candidates:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return None

# ---------------------------------------------------------
# 4. 페이지 처리 (텍스트 기입 + 관계형 연결 통합)
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val

    # 정보 조회를 위한 데이터 매칭[cite: 3, 4]
    item = None
    is_etf = False
    
    if clean_t in engine.df_kr_desc.index:
        item = engine.df_kr_desc.loc[clean_t]
    elif clean_t in engine.kr_etf:
        item = engine.kr_etf[clean_t]
        is_etf = True

    if item is not None:
        # 기초 정보 추출[cite: 4]
        stock_name = item['Name']
        m_raw = str(item.get('Market', '')).upper()
        market_label = "ETF(KR)" if is_etf else ("KOSDAQ" if "KOSDAQ" in m_raw else "KOSPI")
        
        sec_val = engine._get_val_from_headers(item, HEADERS['KR_SECTOR']) if not is_etf else item.get('Category')
        ind_val = engine._get_val_from_headers(item, HEADERS['KR_INDUSTRY']) if not is_etf else "ETF"

        # 지표 결정 로직[cite: 3]
        target_m_t = None
        if clean_t in engine.k200_list: target_m_t = "069500"
        elif clean_t in engine.kd150_list: target_m_t = "229200"
        elif is_etf: target_m_t = "292190"
        elif market_label == "KOSPI": target_m_t = "226490"

        target_ind_t = engine.kr_industry_lookup.get(clean_t)

        # 업데이트 속성 조립[cite: 3, 4]
        def make_rich_text(val):
            return {"rich_text": [{"text": {"content": str(val)}}]} if val else {"rich_text": []}

        update_props = {
            "종목명": make_rich_text(stock_name),
            "Market": {"select": {"name": market_label}},
            "KR_섹터": make_rich_text(sec_val),
            "KR_산업": make_rich_text(ind_val),
            "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
        }
        
        # 관계형 연결[cite: 3]
        if target_m_t and target_m_t != clean_t:
            if m_id := config["ticker_to_id"].get(target_m_t):
                update_props["시장BM"] = {"relation": [{"id": m_id}]}
            
        if target_ind_t and target_ind_t != clean_t:
            if ind_id := config["ticker_to_id"].get(target_ind_t):
                update_props["산업BM"] = {"relation": [{"id": ind_id}]}

        # Notion API 업데이트
        for attempt in range(3):
            try:
                client.pages.update(page_id=pid, properties=update_props)
                logger.info(f"✅ {clean_t} ({stock_name}) 업데이트 완료")
                break
            except Exception as e:
                if "timed out" in str(e).lower() and attempt < 2:
                    time.sleep(2)
                    continue
                logger.error(f"❌ {clean_t} 실패: {e}")
                break

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

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, config)
                time.sleep(0.05)
    
    logger.info("✨ 상장주식 DB 통합 업데이트 완료")

if __name__ == "__main__":
    main()
