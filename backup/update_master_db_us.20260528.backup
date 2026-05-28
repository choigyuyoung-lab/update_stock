import os, re, time, logging, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import httpx
import requests
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
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
# 2. 지표 DB 분석 (관계형 ID 매핑)
# ---------------------------------------------------------
def get_id_map(client):
    """지표지수 DB의 티커를 기준으로 노션 페이지 ID 수집"""
    id_map = {}
    try:
        pages = client.databases.query(database_id=BENCHMARK_DATABASE_ID).get("results", [])
        for page in pages:
            ticker_list = page["properties"].get("이름", {}).get("title", [])
            if ticker_list:
                ticker = ticker_list[0]["plain_text"].strip().upper()
                id_map[ticker] = page["id"]
        logger.info(f"✅ 총 {len(id_map)}개의 지수 데이터 로드 완료")
    except Exception as e:
        logger.error(f"❌ 지표 로드 실패: {e}")
    return id_map

# ---------------------------------------------------------
# 3. 데이터 엔진 (위키피디아 스크래핑 및 상세 마켓 분류)
# ---------------------------------------------------------
class StockAutomationEngineUS:
    def __init__(self):
        logger.info("📡 미국 주식 엔진 가동 (상세 마켓 및 실시간 우량주 리스트 수집)")
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 기초 데이터셋 로드
        self.df_us_etf = fdr.StockListing('ETF/US')    
        self.df_sp500 = fdr.StockListing('S&P500')     
        self.df_nasdaq = fdr.StockListing('NASDAQ')    
        self.df_nyse = fdr.StockListing('NYSE')        
        self.df_amex = fdr.StockListing('AMEX')        
        
        # 🌟 정밀한 지수 분류를 위한 나스닥 100 리스트 (사용자 권장 방식 적용)
        self.nasdaq_100 = self._get_nas100()

    def _get_nas100(self):
        """StringIO와 pandas를 활용한 나스닥 100 스크래핑"""
        try:
            url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
            res = self.session.get(url, timeout=15)
            # StringIO로 감싸서 pandas.read_html 호출 (사용자 설정 기준)
            dfs = pd.read_html(io.StringIO(res.text))
            for df in dfs:
                if 'Ticker' in df.columns or 'Symbol' in df.columns:
                    col = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
                    return df[col].tolist()
            return []
        except Exception as e:
            logger.warning(f"⚠️ 나스닥 100 수집 실패: {e}")
            return []

    def get_market_label(self, clean_t):
        """기존 백업 코드의 상세 마켓 판별 로직"""
        if not self.df_us_etf[self.df_us_etf['Symbol'] == clean_t].empty:
            return "ETF(US)"
        if clean_t in self.df_nasdaq['Symbol'].values: return "NASDAQ"
        if clean_t in self.df_nyse['Symbol'].values: return "NYSE"
        if clean_t in self.df_amex['Symbol'].values: return "AMEX"
        return "기타"

# ---------------------------------------------------------
# 4. 페이지 처리 (기본 정보 자동 기입 + 정교한 지표 매핑)
# ---------------------------------------------------------
def process_page_us(page, engine, client, id_map):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    raw_t = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    
    # 🌟 정교한 한국 주식 판별 및 글로벌 예외 처리 (원본 복구)
    is_kr = (raw_t.endswith(('.KS', '.KQ')) or (len(raw_t) >= 6 and raw_t[0].isdigit())) and not raw_t.endswith(('.T', '.TA', '.TW'))
    if is_kr: return 

    market_label = engine.get_market_label(raw_t)
    
    target_m_t, target_ind_t = None, None
    try:
        # 야후 파이낸스를 통한 실시간 정보 수집[cite: 2]
        stock_yf = yf.Ticker(raw_t)
        info = stock_yf.info
        name = info.get("longName") or info.get("shortName") or raw_t
        sec = info.get("sector", "")
        ind = info.get("industry", "")

        # 🌟 시장BM 4단계 우선순위 로직 (우량주 최우선 판별)[cite: 2]
        if market_label != "기타":
            if raw_t in engine.nasdaq_100:
                target_m_t = "QQQ"  # 1순위: 나스닥 100 대형주
            elif raw_t in engine.df_sp500['Symbol'].values:
                target_m_t = "SPY"  # 2순위: S&P 500 대형주
            elif market_label == "NASDAQ":
                target_m_t = "ONEQ" # 3순위: 그 외 나스닥 종합
            else:
                target_m_t = "VTI"  # 4순위: 그 외 미국 전체 (NYSE/AMEX)
            
            # 산업BM 자동 매핑 (US_섹터 정보 활용)[cite: 2]
            if sec == "Technology":
                target_ind_t = "SOXX" if "Semiconductors" in ind else "XLK"
            elif sec == "Industrials":
                target_ind_t = "XAR" if any(x in ind for x in ["Aerospace", "Defense"]) else "XLI"
            elif sec == "Healthcare": target_ind_t = "XLV"
            elif sec == "Financial Services": target_ind_t = "XLF"
            elif sec == "Communication Services": target_ind_t = "XLC"
            elif sec == "Consumer Cyclical": target_ind_t = "XLY"
            elif sec == "Basic Materials": target_ind_t = "GDX"
    except:
        return

    # 🌟 기초 정보(종목명, 섹터, 업종) 자동 입력 로직 (백업 코드 이식)[cite: 2]
    def make_rich_text(text_val):
        return {"rich_text": [{"text": {"content": str(text_val)}}]} if text_val else {"rich_text": []}

    update_props = {
        "종목명": make_rich_text(name),
        "Market": {"select": {"name": market_label}}, 
        "US_섹터": make_rich_text(sec),
        "US_업종": make_rich_text(ind),
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    # '기타' 마켓은 지수 연결 제외 및 초기화[cite: 2]
    if market_label == "기타":
        update_props["시장BM"] = {"relation": []}
        update_props["산업BM"] = {"relation": []}
    else:
        if target_m_t and target_m_t != raw_t:
            if m_id := id_map.get(target_m_t):
                update_props["시장BM"] = {"relation": [{"id": m_id}]}
        if target_ind_t and target_ind_t != raw_t:
            if ind_id := id_map.get(target_ind_t):
                update_props["산업BM"] = {"relation": [{"id": ind_id}]}

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"   ✅ [US] {raw_t} ({name}) 업데이트 완료")
    except Exception as e:
        logger.error(f"   ❌ [US] {raw_t} 실패: {e}")

# ---------------------------------------------------------
# 5. 메인 함수 (페이지네이션 적용)
# ---------------------------------------------------------
def main():
    notion_httpx = httpx.Client(timeout=60.0)
    client = Client(auth=NOTION_TOKEN, client=notion_httpx)
    
    id_map = get_id_map(client)
    engine = StockAutomationEngineUS()
    
    all_pages = []
    cursor = None
    logger.info("📡 노션 DB 수집 및 페이지네이션 처리 중...")
    
    while True:
        query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: query_params["start_cursor"] = cursor
        res = client.databases.query(**query_params)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")
        time.sleep(0.1)

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_us, page, engine, client, id_map)
                time.sleep(0.2)

    logger.info("✨ 모든 US 종목 업데이트 프로세스가 완료되었습니다.")

if __name__ == "__main__":
    main()
