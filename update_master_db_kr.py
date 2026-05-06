import os, re, time, logging
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID") # 🌟 지표 DB ID

FORCE_UPDATE = True 
# 지표 자체인 티커들은 상장주식 DB 업데이트 로직에서 제외
EXCLUDE_TICKERS = {"069500", "226490", "229200", "292190"}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 지표 DB 동적 로드 (이름 필드의 티커 기반)
# ---------------------------------------------------------
def fetch_dynamic_benchmarks(client):
    """지표 DB의 '이름' 필드에서 티커를 읽어 ID 맵 생성"""
    logger.info("🔍 지표지수 DB에서 동적 데이터 로드 중...")
    bench_map = {} # { "069500": "page_id", ... }
    try:
        results = client.databases.query(database_id=BENCHMARK_DATABASE_ID).get("results", [])
        for page in results:
            props = page["properties"]
            # 지표지수 DB의 '이름' 필드(Title)에서 티커 추출
            name_prop = props.get("이름")
            if name_prop and name_prop["title"]:
                ticker = name_prop["title"][0]["plain_text"].strip()
                bench_map[ticker] = page["id"]
        logger.info(f"✅ 지표 로드 완료: {list(bench_map.keys())}")
    except Exception as e:
        logger.error(f"❌ 지표 DB 로드 실패: {e}")
    return bench_map

# ---------------------------------------------------------
# 3. 데이터 엔진 (Python 3.10+ 기준)
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self):
        logger.info("📡 KRX 데이터 엔진 가동...")
        # requests + StringIO + pandas.read_html 방식 권장 사항 준수[cite: 1, 3]
        self.all_map = fdr.StockListing('KRX').set_index('Code').to_dict('index')
        self.desc_map = fdr.StockListing('KRX-DESC').set_index('Code').to_dict('index')
        self.etf_map = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        
        self.kospi_200_list = self._get_index_by_code("1028")
        self.kosdaq_150_list = self._get_index_by_code("2203")

    def _get_index_by_code(self, target_code: str) -> list:
        for i in range(5):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(target_code, date)
                if res and len(res) > 50: return res
            except: continue
        return []

    def get_stock_detail(self, clean_t: str) -> dict:
        res = {"name": "", "market": "기타"}
        if clean_t in self.all_map:
            item = self.all_map[clean_t]
            m_raw = str(item.get('Market', '')).upper()
            res["market"] = "KOSDAQ" if "KOSDAQ" in m_raw else ("KOSPI" if "KOSPI" in m_raw else m_raw)
            res["name"] = str(item.get('Name', ''))
        if clean_t in self.etf_map:
            res["market"] = "ETF(KR)"
            res["name"] = self.etf_map[clean_t].get('Name', '')
        return res

    def clean_ticker(self, raw_ticker: str) -> str:
        t = str(raw_ticker).strip().upper()
        if match := re.search(r'(\d{6})', t): return match.group(1)
        return re.split(r'[-.]', t)[0]

# ---------------------------------------------------------
# 4. 페이지 처리 로직 (동적 티커 매핑)
# ---------------------------------------------------------
def process_page_kr(page, engine, client, dynamic_benchmarks):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: return
    
    raw_ticker = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip().upper()
    clean_t = engine.clean_ticker(raw_ticker)

    # 1. 지표 종목 제외
    if clean_t in EXCLUDE_TICKERS: return

    # 2. 강제 업데이트 체크[cite: 3]
    if not FORCE_UPDATE:
        last_updated = props.get("업데이트 일자", {}).get("date")
        if last_updated and last_updated.get("start", "")[:10] == datetime.now().strftime("%Y-%m-%d"):
            return 

    info = engine.get_stock_detail(clean_t)
    if not info["name"]: return

    # 🌟 3. 목표 지표 티커 결정[cite: 3]
    target_bench_ticker = None
    tag = None

    if clean_t in engine.kospi_200_list and info["market"] == "KOSPI":
        tag, target_bench_ticker = "KOSPI 200", "069500"
    elif clean_t in engine.kosdaq_150_list and info["market"] == "KOSDAQ":
        tag, target_bench_ticker = "KOSDAQ 150", "229200"
    elif info["market"] == "ETF(KR)":
        target_bench_ticker = "292190" # 모든 국내 ETF -> KRX 300[cite: 3]
    elif info["market"] == "KOSPI":
        target_bench_ticker = "226490" # KOSPI_TOTAL (KODEX 코스피)[cite: 3]

    # 🌟 4. 동적 맵에서 지표 ID 추출 및 비교[cite: 3]
    target_bench_id = dynamic_benchmarks.get(target_bench_ticker)
    current_relation = props.get("시장BM", {}).get("relation", [])
    current_id = current_relation[0]["id"] if current_relation else None

    update_props = {
        "종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]},
        "Market": {"select": {"name": str(info["market"])}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

    if tag: update_props["우량주"] = {"multi_select": [{"name": tag}]}

    # 🌟 ID가 다를 때만 '시장BM' 업데이트 (동적 티커 기반 연결)[cite: 3]
    if target_bench_id and target_bench_id != current_id:
        update_props["시장BM"] = {"relation": [{"id": target_bench_id}]}
        msg = f"🔄 지표 연결됨 ({target_bench_ticker})"
    else:
        msg = "✅ 유지됨"

    try:
        client.pages.update(page_id=pid, properties=update_props)
        logger.info(f"✅ {info['name']}({clean_t}) | {msg}")
    except Exception as e:
        logger.error(f"❌ {clean_t} 오류: {e}")

def main():
    client = Client(auth=NOTION_TOKEN)
    # 🌟 지표지수 DB에서 실시간 티커 정보를 먼저 가져옴[cite: 3]
    dynamic_benchmarks = fetch_dynamic_benchmarks(client)
    
    engine = StockAutomationEngineKR()
    all_pages = []
    cursor = None
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    logger.info(f"🔎 총 {len(all_pages)}개의 종목 페이지를 처리합니다.")

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page in all_pages:
                executor.submit(process_page_kr, page, engine, client, dynamic_benchmarks)
                time.sleep(0.05)
    logger.info("✨ 상장주식 DB 업데이트 완료")

if __name__ == "__main__":
    main()
