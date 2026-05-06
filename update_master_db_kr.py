import os, re, time, logging
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

# ---------------------------------------------------------
# 2. 지표 DB 동적 분석 (최적화)
# ---------------------------------------------------------
def get_dynamic_config(client):
    """지표 DB에서 KR산업 리스트와 전체 티커:ID 맵을 생성"""
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
# 3. 데이터 엔진 (옵션 A: 최고 비중 매핑 적용)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 데이터 엔진 가동...")
        self.kr_listing = fdr.StockListing('KRX').set_index('Code').to_dict('index')
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
        """[옵션 A] 종목이 여러 ETF에 있을 경우 '비중이 가장 높은' ETF 1개만 매핑"""
        lookup = {}
        for etf_t in tickers:
            try:
                pdf = stock.get_etf_portfolio_deposit_file(etf_t)
                if pdf is not None and not pdf.empty:
                    # 비중 컬럼 동적 탐색 (보통 '비중' 또는 첫 번째 열)
                    w_col = '비중' if '비중' in pdf.columns else pdf.columns[0]
                    
                    for stock_t, row in pdf.iterrows():
                        weight = float(row[w_col])
                        # 처음 등장한 종목이거나, 기존에 등록된 ETF의 비중보다 클 때만 덮어쓰기
                        if stock_t not in lookup or weight > lookup[stock_t][1]:
                            lookup[stock_t] = (etf_t, weight)
            except: continue
            
        # 최종적으로 비중 값은 버리고 { "종목티커": "ETF티커" } 형태로 최적화하여 반환
        return {k: v[0] for k, v in lookup.items()}

# ---------------------------------------------------------
# 4. 페이지 처리 로직 (자기참조 방지 및 간소화)
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    clean_t = re.search(r'(\d{6})', ticker_val).group(1) if re.search(r'\d{6}', ticker_val) else ticker_val

    if clean_t in engine.kr_listing or clean_t in engine.kr_etf:
        item = engine.kr_listing.get(clean_t, {})
        m_raw = str(item.get('Market', '')).upper()
        market = "KOSDAQ" if "KOSDAQ" in m_raw else "KOSPI"
        
        # 1. 타겟 지표 결정
        target_m_t = None
        if clean_t in engine.k200_list: target_m_t = "069500"
        elif clean_t in engine.kd150_list: target_m_t = "229200"
        elif clean_t in engine.kr_etf: target_m_t = "292190"
        elif market == "KOSPI": target_m_t = "226490"

        target_ind_t = engine.kr_industry_lookup.get(clean_t)

        # 2. 업데이트 속성 조립 (자기참조 방지)
        update_props = {"업데이트 일자": {"date": {"start": datetime.now().isoformat()}}}
        
        if target_m_t and target_m_t != clean_t:
            if m_id := config["ticker_to_id"].get(target_m_t):
                update_props["시장BM"] = {"relation": [{"id": m_id}]}
            
        if target_ind_t and target_ind_t != clean_t:
            if ind_id := config["ticker_to_id"].get(target_ind_t):
                update_props["산업BM"] = {"relation": [{"id": ind_id}]}

        # 3. Notion API 업데이트 (타임아웃 3회 재시도)
        for attempt in range(3):
            try:
                client.pages.update(page_id=pid, properties=update_props)
                logger.info(f"✅ {clean_t} 업데이트 (시장: {target_m_t or '-'}, 산업: {target_ind_t or '-'})")
                break
            except Exception as e:
                if "timed out" in str(e).lower() and attempt < 2:
                    logger.warning(f"⏳ {clean_t} 타임아웃, 2초 후 재시도 ({attempt+1}/3)...")
                    time.sleep(2)
                    continue
                logger.error(f"❌ {clean_t} 업데이트 실패: {e}")
                break

# ---------------------------------------------------------
# 5. 메인 실행 함수
# ---------------------------------------------------------
def main():
    # httpx를 활용한 안정적인 60초 타임아웃 설정
    custom_client = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=custom_client)
    
    config = get_dynamic_config(client)
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])
    
    # 노션 페이지 전체 수집 (페이지네이션)
    all_pages, cursor = [], None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    logger.info(f"🔎 총 {len(all_pages)}개 종목 페이지 처리 시작")

    # 병렬 처리
    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, config)
                time.sleep(0.05)
    
    logger.info("✨ 전체 상장주식 DB 업데이트 프로세스 완료")

if __name__ == "__main__":
    main()
