import os
import time
import requests
import re
from bs4 import BeautifulSoup
from notion_client import Client
from datetime import datetime

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

TARGET_TICKERS = []
IS_FULL_UPDATE = True 

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

class StockCrawler:
    def __init__(self):
        self.headers = {'User-Agent': USER_AGENT}

    # ------------------------------------------------------------------
    # [기능] 구글 검색 검증 (기존 로직 유지)
    # ------------------------------------------------------------------
    def verify_with_google(self, ticker, fetched_name):
        if not GOOGLE_API_KEY or not GOOGLE_CX:
            return "SKIP", "(API키 없음/건너뜀)"
        try:
            query = f"{ticker} 주식" if re.search(r'\d', ticker) else f"{ticker} stock"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CX,
                'q': query,
                'num': 2
            }
            res = requests.get(url, params=params, timeout=5)
            if res.status_code in [429, 403]:
                return "SKIP", f"(일일할당량 초과/대기: {res.status_code})"
            if res.status_code != 200:
                return "SKIP", f"(구글 에러 {res.status_code})"

            items = res.json().get('items', [])
            if not items:
                return "FAIL", "(구글결과 없음)"

            core_name = fetched_name.split()[0].replace(',', '').lower()
            is_matched = False
            for item in items:
                title = item.get('title', '').lower()
                snippet = item.get('snippet', '').lower()
                if (core_name in title or core_name in snippet) or \
                   (ticker.lower().split('.')[0] in title):
                    is_matched = True
                    break
            
            if is_matched:
                return "PASS", "+ 구글검증됨"
            else:
                return "FAIL", "(구글검증 실패)"
        except Exception as e:
            return "SKIP", f"(검증 에러: {str(e)})"

    # ------------------------------------------------------------------
    # [1순위] 구글 파이낸스 크롤링 (원본 로직 반영)
    # ------------------------------------------------------------------
    def fetch_google_finance(self, ticker_with_exchange):
        url = f"https://www.google.com/finance/quote/{ticker_with_exchange}?hl=ko"
        try:
            res = requests.get(url, headers=self.headers, timeout=10)
            if res.status_code != 200: return None
            soup = BeautifulSoup(res.text, 'html.parser')

            # 종목명 및 산업분류 (구조 유지를 위해 추가)
            name_tag = soup.select_one('div.zz6uS') # 구글 파이낸스 종목명 클래스
            name = name_tag.text.strip() if name_tag else ticker_with_exchange.split(':')[0]
            
            # 회사 개요 (사용자 원본 로직)
            summary = ""
            summary_tag = soup.select_one('div.bNoYQe')
            if not summary_tag:
                summary_tag = soup.find('div', string=lambda t: t and len(t) > 50)
            
            if summary_tag:
                summary = summary_tag.text.strip()
            else:
                return None # 개요를 못 찾으면 다음 단계(네이버)로 넘어가기 위해 None 반환

            return {
                "name": name,
                "industry": "해외주식" if ":" in ticker_with_exchange else "기타",
                "summary": summary,
                "source": "구글 파이낸스"
            }
        except Exception: pass
        return None

    # ------------------------------------------------------------------
    # [2순위] 네이버 금융 크롤링 (Fallback)
    # ------------------------------------------------------------------
    def fetch_naver_crawling(self, ticker):
        try:
            url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            res = requests.get(url, headers=self.headers, timeout=10)
            res.encoding = res.apparent_encoding 
            if res.status_code != 200: return None
            
            soup = BeautifulSoup(res.text, 'html.parser')
            name_tag = soup.select_one('.wrap_company h2 a')
            if not name_tag: return None 
            name = name_tag.text.strip()

            industry = "한국증시"
            try:
                ind_tag = soup.select_one('div.section.trade_compare h4 em a')
                if ind_tag: industry = ind_tag.text.strip()
            except: pass

            summary = ""
            summary_div = soup.select_one('#summary_info p')
            if summary_div: summary = summary_div.text.strip()
            
            return {
                "name": name,
                "industry": industry,
                "summary": summary,
                "source": "네이버 정보"
            }
        except Exception: pass
        return None

    def get_data(self, ticker):
        raw_ticker = ticker.strip().upper()
        is_korea = (len(raw_ticker) == 6 and raw_ticker[0].isdigit()) or raw_ticker.endswith(('.KS', '.KQ'))
        search_code = raw_ticker.split('.')[0]

        # 구글 파이낸스용 티커 형식 생성
        google_ticker = f"{search_code}:KRX" if is_korea else f"{search_code}:NASDAQ"

        # 1. 구글 파이낸스 시도
        data = self.fetch_google_finance(google_ticker)

        # 2. 구글 실패 시 네이버 시도
        if not data and is_korea:
            data = self.fetch_naver_crawling(search_code)

        if data:
            v_status, msg = self.verify_with_google(search_code, data['name'])
            data['ver_status'] = v_status 
            data['source'] = f"{data['source']} {msg}"

        return data

def main():
    mode_msg = "전체 강제 업데이트" if IS_FULL_UPDATE else "미검증 항목만 업데이트"
    print(f"🚀 [Master DB] 시작: {mode_msg} (구글/네이버 전용)")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        crawler = StockCrawler()
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return

    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            query_params = {"database_id": MASTER_DATABASE_ID, "page_size": 50}
            if not IS_FULL_UPDATE:
                query_params["filter"] = {
                    "property": "데이터 상태", 
                    "select": {"does_not_equal": "✅ 검증완료"}
                }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            if not pages: break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS: continue

                print(f"🔍 업데이트 중: {raw_ticker} ...")
                data = crawler.get_data(raw_ticker)
                today_str = datetime.now().strftime("%Y-%m-%d")
                
                if data:
                    v_stat = data.get('ver_status', 'SKIP')
                    status = "✅ 검증완료" if v_stat == "PASS" else ("⏳ 검증대기" if v_stat == "SKIP" else "⚠️ 확인필요")
                    
                    safe_summary = data['summary'][:1900] + "..." if len(data['summary']) > 1900 else data['summary']
                    
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": data['source']}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]},
                        "업데이트 일자": {"date": {"start": today_str}}
                    }
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                    
                    print(f"   └ {status}: {data['name']} ({data['source']})")
                else:
                    upd_props = {
                        "데이터 상태": {"select": {"name": "⚠️ 확인필요"}},
                        "검증로그": {"rich_text": [{"text": {"content": "데이터 수집 실패"}}]},
                        "업데이트 일자": {"date": {"start": today_str}}
                    }
                    print(f"   └ 실패: 데이터 수집 불가")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.5) 

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 업데이트 완료: 총 {processed_count}건")

if __name__ == "__main__":
    main()
