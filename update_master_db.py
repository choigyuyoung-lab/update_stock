import os
import time
import requests
import re
import yfinance as yf
from notion_client import Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------
# 1. 환경 변수 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# [설정] 전체 업데이트 (특정 티커 테스트 시에만 채우고, 실사용 시 비워두세요)
TARGET_TICKERS = []

# 시스템 상수
MAX_RETRIES = 3
TIMEOUT = 10
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 야후 산업분류 한글 매핑
YAHOO_SECTOR_MAP = {
    "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
    "Consumer Cyclical": "경기소비재", "Communication Services": "통신 서비스",
    "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
    "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
}

class StockDataProvider:
    """
    네이버(1순위)와 야후(2순위)를 통합하여 데이터를 수집하는 클래스
    """
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[403, 404, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Referer': 'https://m.stock.naver.com/',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        })

    def fetch_naver(self, ticker):
        """[1순위] 네이버 통합 검색 (한글 데이터)"""
        if not ticker: return None
        
        # 접미어 제거 및 정제 (AAPL.O -> AAPL)
        clean_ticker = ticker.strip().upper()
        search_query = clean_ticker.split('.')[0] if '.' in clean_ticker else clean_ticker

        try:
            # 1. 검색
            search_url = f"https://m.stock.naver.com/api/search/all?query={search_query}"
            res = self.session.get(search_url, timeout=TIMEOUT)
            if res.status_code != 200: return None

            search_result = res.json().get("searchList", [])
            if not search_result: return None

            # 2. 코드 매칭
            target_code = None
            for item in search_result:
                r_code = item.get("reutersCode", "")
                s_id = item.get("stockId", "")
                # 검색어가 코드에 포함되면 채택
                if search_query in r_code or search_query in s_id:
                    target_code = r_code if r_code else s_id
                    break
            
            if not target_code:
                # 없으면 첫 번째 결과 사용 (유연성)
                first = search_result[0]
                target_code = first.get("reutersCode", "") or first.get("stockId", "")

            # 3. 상세 정보 수집
            detail_url = f"https://m.stock.naver.com/api/stock/{target_code}/integration"
            self.session.headers.update({'Referer': f'https://m.stock.naver.com/domestic/stock/{target_code}/total'})
            
            res_detail = self.session.get(detail_url, timeout=TIMEOUT)
            if res_detail.status_code == 200:
                data = res_detail.json()
                r = data.get("result", {})
                item = (r.get("stockItem") or r.get("etfItem") or 
                        r.get("etnItem") or r.get("reitItem"))
                
                if item:
                    # 데이터 추출
                    name = item.get("stockName") or item.get("itemname") or item.get("gname")
                    industry = item.get("industryName", "") or item.get("industryCodeName", "")
                    
                    # 회사개요 (한글 우선)
                    summary = (item.get("description") or item.get("gsummary") or 
                               item.get("corpSummary") or item.get("summary") or "")
                    
                    return {
                        "name": name,
                        "industry": industry,
                        "summary": summary,
                        "source_type": "NAVER"
                    }
        except Exception:
            pass
        return None

    def fetch_yahoo(self, ticker):
        """[2순위] 야후 파이낸스 (영문 데이터 + 한글 섹터)"""
        clean_ticker = ticker.strip().upper()
        # 야후 검색용 티커 (접미어 제거 시도)
        query_ticker = clean_ticker.split('.')[0] if '.' in clean_ticker else clean_ticker

        try:
            stock = yf.Ticker(query_ticker)
            info = stock.info
            
            # 데이터 유효성 검사
            if 'regularMarketPrice' not in info and 'symbol' not in info:
                # 실패 시 하이픈(-) 포맷으로 재시도 (예: BRK.B -> BRK-B)
                if 'B' in query_ticker and '-' not in query_ticker:
                     query_ticker = query_ticker.replace('B', '-B') # 단순 예시
                     stock = yf.Ticker(query_ticker)
                     info = stock.info
                
                if 'regularMarketPrice' not in info and 'symbol' not in info:
                    return None

            name = info.get('longName') or info.get('shortName') or query_ticker
            eng_sector = info.get('sector', '')
            industry = YAHOO_SECTOR_MAP.get(eng_sector, eng_sector) # 한글 매핑
            summary = info.get('longBusinessSummary', '')

            return {
                "name": name,
                "industry": industry,
                "summary": summary,
                "source_type": "YAHOO"
            }
        except Exception:
            pass
        return None

    def get_data(self, ticker):
        """통합 데이터 수집 (네이버 -> 야후)"""
        # 1. 네이버 시도
        data = self.fetch_naver(ticker)
        if data: return data
        
        # 2. 야후 시도
        data = self.fetch_yahoo(ticker)
        if data: return data
        
        return None

def main():
    print(f"🚀 [Master DB] 전체 종목 강제 업데이트 시작")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        provider = StockDataProvider()
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return

    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # [필터 제거] 모든 데이터 가져오기
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

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                # 타겟 필터링 (설정된 경우에만)
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS:
                    continue

                print(f"🔍 업데이트 중: {raw_ticker} ...")
                
                # 데이터 수집 실행
                data = provider.get_data(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    status = "✅ 검증완료"
                    # [요청하신 로그 포맷 적용]
                    if data['source_type'] == "NAVER":
                        log_msg = "네이버 크롤링 성공 -> 네이버 정보"
                    else:
                        log_msg = "야후(yfinance) 성공 -> 야후 정보"
                    
                    # 요약본 길이 제한
                    summary_text = data['summary']
                    safe_summary = summary_text[:1900] + "..." if summary_text and len(summary_text) > 1900 else (summary_text or "")
                    
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                        print(f"   └ [완료] {data['name']} ({data['source_type']})")
                    else:
                        print(f"   └ [완료] {data['name']} (개요 열 없음)")
                else:
                    status = "⚠️ 확인필요"
                    log_msg = "❌ 실패: 네이버/야후 모두 데이터 없음"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] 데이터 찾을 수 없음")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.2) # API 부하 조절

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 전체 업데이트 완료: 총 {processed_count}건")

if __name__ == "__main__":
    main()
