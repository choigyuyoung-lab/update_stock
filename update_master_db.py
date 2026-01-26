import os
import re
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, Tuple

import requests
import yfinance as yf
from bs4 import BeautifulSoup
from notion_client import Client
from ddgs import DDGS

# ---------------------------------------------------------
# 1. 환경 설정 및 로깅
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")
IS_FULL_UPDATE = os.environ.get("IS_FULL_UPDATE", "False").lower() == "true"
MAX_WORKERS = 2 

YAHOO_SECTOR_MAP = {
    "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재", "Communication Services": "통신 서비스",
    "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
    "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
}

class StockAutomationEngine:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    def _clean_text(self, text: str) -> str:
        """데이터 부재 시 공백 처리 원칙"""
        if not text: return ""
        cleaned = re.sub(r'\[.*?\]', '', text).strip()
        return "" if cleaned in ["정보 없음", "정보없음"] else cleaned

    def define_ticker_logic(self, raw_ticker: str) -> Dict[str, Any]:
        """
        [시니어 최적화] 사용자 임의 표기(-, .) 완전 제거 및 플랫폼별 검색어 정의
        """
        original = raw_ticker.strip().upper()
        
        # 핵심: 첫 번째 마침표(.)나 하이픈(-)이 나오기 전까지만 순수 티커로 인정
        # 예: AAPL-K -> AAPL / 005930.KS -> 005930
        pure_ticker = re.split(r'[-.]', original)[0]
        
        # 한국 주식 판별: 정제된 티커가 6자리인 경우 (숫자/영문 혼합 포함)
        is_korea = (len(pure_ticker) == 6)
        
        # 네이버용 숫자 추출 (A060310 -> 060310)
        kr_code_match = re.search(r'(\d{6})', pure_ticker)
        naver_code = kr_code_match.group(1) if is_korea and kr_code_match else pure_ticker

        return {
            "is_korea": is_korea,
            "pure": pure_ticker,                   # 모든 검색의 기준
            "naver": naver_code,                  # 네이버 금융 코드
            "google_finance": f"{naver_code}:KRX" if is_korea else pure_ticker # 구글 파이낸스용
        }

    def verify_hybrid(self, t_info: Dict[str, Any], name: str) -> Tuple[str, str]:
        """순수 티커와 수집된 이름을 조합하여 하이브리드 검증"""
        suffix = "주식" if t_info['is_korea'] else "stock"
        query = f"{name} {t_info['pure']} {suffix}"
        
        # 1. DuckDuckGo (무제한)
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=5))
                if results and any(t_info['pure'].lower() in (r.get('title', '') + r.get('body', '')).lower() for r in results):
                    return "PASS", "(DDG) +검증됨"
        except: pass

        # 2. 구글 API (백업)
        if not GOOGLE_API_KEY: return "SKIP", "(DDG) 지연"
        try:
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 3}
            res = self.session.get(url, params=params, timeout=5)
            if res.status_code in [429, 403]: return "SKIP", "(GOO) 할당량초과"
            items = res.json().get('items', [])
            if any(t_info['pure'].lower() in item.get('title', '').lower() for item in items):
                return "PASS", "(GOO) +검증됨"
            return "FAIL", "(GOO) 검증실패"
        except: return "SKIP", "(검증에러)"

    def fetch_wiki_data(self, t_info: Dict[str, Any]) -> Dict[str, str]:
        """구글 파이낸스 직통 링크를 통해 위키백과 데이터 수집"""
        res_data = {"wiki_industry": "", "wiki_service": ""}
        url = f"https://www.google.com/finance/quote/{t_info['google_finance']}?hl=ko"
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            wiki_link = soup.find('a', href=re.compile(r'wikipedia\.org'), string=re.compile(r'Wikipedia'))
            if wiki_link:
                w_res = self.session.get(wiki_link.get('href'), timeout=10)
                w_soup = BeautifulSoup(w_res.text, 'html.parser')
                infobox = w_soup.select_one('table.vcard, table.infobox')
                if infobox:
                    for row in infobox.find_all('tr'):
                        th, td = row.find('th'), row.find('td')
                        if th and td:
                            lbl, val = th.get_text(strip=True), self._clean_text(td.get_text(separator=' ', strip=True))
                            if '산업 분야' in lbl: res_data["wiki_industry"] = val
                            elif '서비스' in lbl: res_data["wiki_service"] = val
        except: pass
        return res_data

    def get_integrated_data(self, raw_ticker: str) -> Optional[Dict[str, Any]]:
        t_info = self.define_ticker_logic(raw_ticker)
        try:
            if t_info['is_korea']:
                url = f"https://finance.naver.com/item/main.naver?code={t_info['naver']}"
                res = self.session.get(url, timeout=10); res.encoding = res.apparent_encoding
                soup = BeautifulSoup(res.text, 'html.parser')
                name = soup.select_one('.wrap_company h2 a').get_text(strip=True)
                industry = soup.select_one('div.section.trade_compare h4 em a').get_text(strip=True) if soup.select_one('div.section.trade_compare h4 em a') else "ETF"
                data = {"name": name, "industry": industry, "tag": "네이버"}
            else:
                stock = yf.Ticker(t_info['pure'])
                name = stock.info.get('longName') or t_info['pure']
                industry = YAHOO_SECTOR_MAP.get(stock.info.get('sector', ''), stock.info.get('sector', ''))
                data = {"name": name, "industry": industry, "tag": "야후"}

            v_stat, v_log = self.verify_hybrid(t_info, data['name'])
            data.update({"ver_status": v_stat, "ver_log": f"{data['tag']}{v_log}"})
            data.update(self.fetch_wiki_data(t_info))
            return data
        except: return None

def process_page(page, engine, notion):
    try:
        pid, props = page["id"], page["properties"]
        ticker_rich = props.get("티커", {}).get("title", [])
        if not ticker_rich: return
        raw_ticker = ticker_rich[0].get("plain_text", "").strip()
        
        data = engine.get_integrated_data(raw_ticker)
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
        
        if data:
            status_map = {"PASS": "✅ 검증완료", "SKIP": "⏳ 검증대기", "FAIL": "⚠️ 확인필요"}
            upd = {
                "데이터 상태": {"select": {"name": status_map.get(data['ver_status'], "⚠️ 확인필요")}},
                "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                "산업 분류": {"rich_text": [{"text": {"content": data['industry']}}]},
                "업데이트 일자": {"date": {"start": now}},
                "검증로그": {"rich_text": [{"text": {"content": data['ver_log']}}]}
            }
            if "산업 분야" in props: upd["산업 분야"] = {"rich_text": [{"text": {"content": data['wiki_industry']}}]}
            if "서비스" in props: upd["서비스"] = {"rich_text": [{"text": {"content": data['wiki_service']}}]}
            notion.pages.update(page_id=pid, properties=upd)
            logger.info(f"DONE: {raw_ticker}")
    except Exception as e: logger.error(f"FAIL {raw_ticker}: {e}")

def main():
    logger.info(f"Automation Start [Full Update: {IS_FULL_UPDATE}]")
    notion, engine = Client(auth=NOTION_TOKEN), StockAutomationEngine()
    cursor = None
    while True:
        params = {"database_id": MASTER_DATABASE_ID, "page_size": 100}
        if cursor: params["start_cursor"] = cursor
        if not IS_FULL_UPDATE:
            params["filter"] = {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}}
        
        response = notion.databases.query(**params)
        pages = response.get("results", [])
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in pages:
                executor.submit(process_page, page, engine, notion)
                time.sleep(0.4)
        if not response.get("has_more"): break
        cursor = response.get("next_cursor")
    logger.info("All Jobs Done.")

if __name__ == "__main__":
    main()
