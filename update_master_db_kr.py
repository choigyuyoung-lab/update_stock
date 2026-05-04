import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

# 1. 환경 변수 및 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
"KR_SECTOR": ['Sector', 'WICS 업종명', '업종'],
"KR_INDUSTRY": ['Industry', '주요제품', 'WICS 제품']
}

class StockAutomationEngineKR:
def __init__(self):
logger.info(f"📡 한국 주식 엔진 시작 (수동 모드: {IS_FULL_UPDATE})")
self.session = requests.Session()
self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

logger.info("⏳ 데이터셋 로딩 및 인덱싱 중...")
df_desc = fdr.StockListing('KRX-DESC')
self.desc_map = df_desc.set_index('Code').to_dict('index')

df_etf = fdr.StockListing('ETF/KR')
self.etf_map = df_etf.set_index('Symbol').to_dict('index')

logger.info(f"✅ 로딩 완료 (주식: {len(self.desc_map)}건, ETF: {len(self.etf_map)}건)")

# 최적화: 고정된 코드로 즉시 데이터 로드
self.blue_chip_map = {
"KOSPI 200": self._get_index_by_code("코스피 200", "1028"),
            "KOSDAQ 150": self._get_index_by_code("코스닥 150", "2154")
            "KOSDAQ 150": self._get_index_by_code("코스닥 150", "2203")
}

def _get_index_by_code(self, index_name: str, target_code: str) -> List[str]:
"""고유 코드를 사용하여 빠르고 정확하게 종목 리스트를 추출"""
for i in range(10):
date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
try:
res = stock.get_index_portfolio_deposit_file(target_code, date)
if res and len(res) > 100: 
logger.info(f"✅ {index_name} 로드 성공 (종목수: {len(res)})")
return res
except:
continue

logger.error(f"🚨 {index_name} 추출 실패. 데이터를 가져오지 못했습니다.")
return []

def _get_val(self, data_dict: dict, candidates: List[str]) -> Optional[str]:
for col in candidates:
val = data_dict.get(col)
if pd.notna(val) and str(val).strip() != "":
return str(val).strip()
return None

def get_stock_detail(self, clean_t: str) -> Dict[str, Any]:
"""필터링 없이 딕셔너리 키로 즉시 조회 (O(1))"""
res = {"name": "", "market": "기타", "kr_sector": None, "kr_ind": None}

if clean_t in self.desc_map:
item = self.desc_map[clean_t]
mkt = "KOSDAQ" if "KOSDAQ" in str(item.get('Market', '')) else str(item.get('Market', '기타'))
res.update({
"name": item.get('Name', ''),
"market": mkt,
"kr_sector": self._get_val(item, HEADERS['KR_SECTOR']),
"kr_ind": self._get_val(item, HEADERS['KR_INDUSTRY'])
})

if clean_t in self.etf_map:
item = self.etf_map[clean_t]
res.update({
"name": str(item.get('Name', '')),
"market": "ETF(KR)",
"kr_sector": str(item.get('Category', 'ETF')), 
"kr_ind": "ETF"
})

return res

def clean_ticker(self, raw_ticker: str) -> str:
t = str(raw_ticker).strip().upper()
if match := re.search(r'(\d{6})', t): return match.group(1)
return re.split(r'[-.]', t)[0]

def process_page_kr(page, engine, client):
pid, props = page["id"], page["properties"]
ticker_prop = props.get("티커", {})
ticker_rich = ticker_prop.get("title") or ticker_prop.get("rich_text")
if not ticker_rich: return

raw_ticker = ticker_rich[0]["plain_text"].strip().upper()

is_kr = (raw_ticker.endswith(('.KS', '.KQ')) or (len(raw_ticker) >= 6 and raw_ticker[0].isdigit())) and not raw_ticker.endswith(('.T', '.TA', '.TW'))
if not is_kr: return

clean_t = engine.clean_ticker(raw_ticker)
info = engine.get_stock_detail(clean_t)

if not info["name"]: return

# 시장 교차 검증 (안전장치 유지)
bc_tags = []
for label, lst in engine.blue_chip_map.items():
if clean_t in lst:
if "KOSDAQ" in label and info["market"] != "KOSDAQ":
continue
if "KOSPI" in label and info["market"] != "KOSPI":
continue
bc_tags.append({"name": label})

update_props = {
"종목명": {"rich_text": [{"text": {"content": str(info["name"])}}]}, 
"Market": {"select": {"name": str(info["market"])}}, 
"KR_섹터": {"rich_text": [{"text": {"content": str(info["kr_sector"])}}]} if info["kr_sector"] else {"rich_text": []}, 
"KR_산업": {"rich_text": [{"text": {"content": str(info["kr_ind"])}}]} if info["kr_ind"] else {"rich_text": []}, 
"업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
}
if "우량주" in props: update_props["우량주"] = {"multi_select": bc_tags}

try:
client.pages.update(page_id=pid, properties=update_props)
logger.info(f"   ✅ [KR] {raw_ticker} 업데이트 완료")
except Exception as e:
logger.error(f"   ❌ [KR] {raw_ticker} 실패: {e}")

def main():
client = Client(auth=NOTION_TOKEN) 
engine = StockAutomationEngineKR()
cursor = None

while True:
query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
if cursor: query_params["start_cursor"] = cursor
if not IS_FULL_UPDATE:
query_params["filter"] = {"property": "종목명", "rich_text": {"is_empty": True}}

response = client.databases.query(**query_params) 
pages = response.get("results", [])
if not pages: break

with ThreadPoolExecutor(max_workers=5) as executor:
for page in pages:
executor.submit(process_page_kr, page, engine, client)
time.sleep(0.05) 

if not response.get("has_more"): break
cursor = response.get("next_cursor")

if __name__ == "__main__":
main()
