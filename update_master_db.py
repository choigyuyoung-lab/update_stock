import os
import time
import requests
import re
import yfinance as yf
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# 구글 검증용 API 키
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

# [설정] 특정 티커만 테스트하고 싶을 때 사용 (비워두면 전체 실행)
TARGET_TICKERS = []

# ---------------------------------------------------------
# 2. 크롤러 클래스 (하이브리드 API + 구글 검증)
# ---------------------------------------------------------
class StockCrawler:
    def __init__(self):
        # [핵심] 모바일 아이폰으로 위장하여 네이버 보안을 통과합니다.
        self.mobile_headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        }

    # ------------------------------------------------------------------
    # [기능] 구글 검색 검증 (3단 상태 반환)
    # ------------------------------------------------------------------
    def verify_with_google(self, ticker, fetched_name):
        """
        반환값: (상태코드, 로그메시지)
        - PASS: 검증 성공 (✅ 검증완료)
        - SKIP: 할당량 초과 또는 API 키 없음 (⏳ 검증대기)
        - FAIL: 검증 실패 (⚠️ 확인필요)
        """
        if not GOOGLE_API_KEY or not GOOGLE_CX:
            return "SKIP", "(API키 없음/건너뜀)"

        try:
            query = f"{ticker} 주식" if re.search(r'\d', ticker) else f"{ticker} stock"
            url = "https://www.googleapis.com/customsearch/v1"
            params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CX, 'q': query, 'num': 2}
            
            res = requests.get(url, params=params, timeout=5)
            
            # [중요] 할당량 초과(429) 또는 권한 없음(403) -> 검증대기 상태로 전환
            if res.status_code in [429, 403]:
                return "SKIP", "(일일할당량 초과/대기)"
            
            if res.status_code != 200:
                return "SKIP", f"(구글 에러 {res.status_code})"

            items = res.json().get('items', [])
            if not items:
                return "FAIL", "(구글결과 없음)"

            # 이름 비교 로직 (핵심 단어 포함 여부)
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
                return "FAIL", "(이름 불일치)"

        except Exception as e:
            return "SKIP", f"(시스템 에러: {str(e)})"

    # ------------------------------------------------------------------
    # [1] 한국 주식 (모바일 API)
    # ------------------------------------------------------------------
    def fetch_korean_stock(self, ticker):
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        headers = self.mobile_headers.copy()
        headers['Referer'] = f"https://m.stock.naver.com/domestic/stock/{ticker}/total"
        headers['Origin'] = 'https://m.stock.naver.com'
        
        try:
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code != 200: return None

            data = res.json()
            name = data.get('stockName', ticker)
            
            # 산업분류
            industry = ""
            if 'stocks' in data and data['stocks']:
                 industry = data['stocks'][0].get('industryCodeName', '')
            if not industry and 'stockItem' in data:
                industry = data['stockItem'].get('industryName', '')
            if not industry: industry = "한국증시"

            # 개요
            total_infos = data.get('totalInfos', [])
            summary = ""
            for info in total_infos:
                if info.get('key') == 'summary_info':
                    summary = info.get('value', '')
                    break

            return {"name": name, "industry": industry, "summary": summary, "source": "네이버(국내)"}
        except Exception: return None

    # ------------------------------------------------------------------
    # [2] 미국 주식 (PC API + 모바일 헤더)
    # ------------------------------------------------------------------
    def fetch_us_stock(self, ticker):
        suffixes = ['.O', '', '.K', '.N'] # 나스닥, NYSE, 아멕스 순
        
        for suffix in suffixes:
            try:
                search_ticker = f"{ticker}{suffix}"
                url = f"https://api.stock.naver.com/stock/{search_ticker}/integration"
                
                headers = self.mobile_headers.copy()
                headers['Referer'] = f"https://m.stock.naver.com/worldstock/stock/{search_ticker}/total"
                headers['Origin'] = 'https://m.stock.naver.com'
                
                res = requests.get(url, headers=headers, timeout=5)
                if res.status_code != 200: continue

                data = res.json()
                if not data.get('symbolCode'): continue

                kor_name = data.get('stockName', '')
                eng_name = data.get('engStockName', '')
                final_name = kor_name if kor_name else (eng_name if eng_name else ticker)

                industry_map = data.get('industryCodeType', {})
                industry = industry_map.get('industryGroupKor', "미국주식")
                summary = data.get('corpSummary', "")

                if final_name:
                    return {"name": final_name, "industry": industry, "summary": summary, "source": "네이버(해외)"}
            except Exception: continue
        return None

    # ------------------------------------------------------------------
    # 데이터 수집 총괄
    # ------------------------------------------------------------------
    def get_data(self, ticker):
        raw_ticker = ticker.strip().upper()
        search_code = raw_ticker
        is_korea = False

        if (len(raw_ticker) == 6 and raw_ticker[0].isdigit()) or \
           raw_ticker.endswith('.KS') or raw_ticker.endswith('.KQ'):
            is_korea = True
            if '.' in raw_ticker: search_code = raw_ticker.split('.')[0]
        else:
            if '.' in raw_ticker: search_code = raw_ticker.split('.')[0]

        data = None
        if is_korea:
            data = self.fetch_korean_stock(search_code)
        else:
            data = self.fetch_us_stock(search_code)

        # 데이터가 있으면 구글 검증 진행
        if data:
            status, msg = self.verify_with_google(search_code, data['name'])
            data['ver_status'] = status # PASS, SKIP, FAIL
            data['source'] = f"{data['source']} {msg}"
        
        return data

# ---------------------------------------------------------
# 3. 메인 실행 함수
# ---------------------------------------------------------
def main():
    print(f"🚀 [Master DB] 전체 종목 업데이트 시작 (필터 없음)")
    
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
            # [수정됨] 필터 제거 -> 모든 데이터베이스 항목을 가져옵니다.
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "page_size": 50
            }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 데이터베이스가 비어있습니다.")
                break
            if not pages: break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS: continue

                print(f"🔍 조회 중: {raw_ticker} ...")
                
                data = crawler.get_data(raw_ticker)
                
                final_status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    # [상태 결정 로직]
                    v_stat = data.get('ver_status', 'SKIP')
                    
                    if v_stat == "PASS":
                        final_status = "✅ 검증완료"
                    elif v_stat == "SKIP":
                        final_status = "⏳ 검증대기" # 할당량 초과/에러 등
                    else:
                        final_status = "⚠️ 확인필요" # 구글 검색 실패
                    
                    log_msg = data['source']
                    summary_text = data['summary']
                    safe_summary = summary_text[:1900] + "..." if summary_text and len(summary_text) > 1900 else (summary_text or "")
                    
                    upd_props = {
                        "데이터 상태": {"select": {"name": final_status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                    
                    print(f"   └ {final_status}: {data['name']} ({log_msg})")
                else:
                    final_status = "⚠️ 확인필요"
                    log_msg = "데이터 없음(네이버/야후 실패)"
                    upd_props = {
                        "데이터 상태": {"select": {"name": final_status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ 실패: {log_msg}")

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
