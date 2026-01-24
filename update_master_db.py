import os
import time
import requests
import re
import yfinance as yf
from notion_client import Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 1. 환경 변수 설정 (구글 키 불필요)
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# 2. 시스템 상수
MAX_RETRIES = 3
TIMEOUT = 10
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# 산업분류 매핑
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

class StockAPIClient:
    """데이터 수집 전담 클래스 (재시도 로직 포함)"""
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({'User-Agent': USER_AGENT})

    def fetch_korean_stock(self, ticker):
        """네이버 모바일 API (안정성 강화)"""
        try:
            # 1. 통합 API (개요 포함)
            self.session.headers.update({'Referer': f'https://m.stock.naver.com/domestic/stock/{ticker}/total'})
            url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
            res = self.session.get(url, timeout=TIMEOUT)
            
            if res.status_code == 200:
                data = res.json()
                item = data.get("result", {}).get("stockItem") or data.get("result", {}).get("etfItem")
                if item:
                    return {
                        "name": item.get("stockName") or item.get("itemname"),
                        "industry": item.get("industryName", ""),
                        "summary": item.get("description", ""),
                        "source": "NAVER"
                    }
            
            # 2. 기본 API (비상용)
            url_basic = f"https://m.stock.naver.com/api/stock/{ticker}/basic"
            res = self.session.get(url_basic, timeout=TIMEOUT)
            if res.status_code == 200:
                data = res.json()
                if "stockName" in data:
                    return {
                        "name": data.get("stockName"),
                        "industry": "",
                        "summary": "",
                        "source": "NAVER_BASIC"
                    }
        except Exception as e:
            print(f"      ⚠️ [KR] 통신 오류: {e}")
        return None

    def fetch_us_stock(self, ticker):
        """야후 파이낸스 API"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # 데이터 없음 -> 원본 티커 재시도
            if not info or ('longName' not in info and 'shortName' not in info):
                return None

            name = info.get("longName") or info.get("shortName")
            sector = info.get("sector", "")
            summary = info.get("longBusinessSummary", "")
            
            return {
                "name": name,
                "industry": INDUSTRY_MAP.get(sector, sector),
                "summary": summary,
                "source": "YAHOO"
            }
        except Exception as e:
            print(f"      ⚠️ [US] 통신 오류: {e}")
        return None

    def get_data(self, ticker):
        """티커 라우팅"""
        clean_ticker = ticker.split('.')[0].strip().upper()
        if len(clean_ticker) == 6 and clean_ticker.isdigit():
            return self.fetch_korean_stock(clean_ticker)
        else:
            # 미국 주식: 정제된 티커 우선 시도 -> 실패시 원본 시도
            result = self.fetch_us_stock(clean_ticker)
            if not result:
                result = self.fetch_us_stock(ticker)
            return result

def main():
    print(f"🚀 [Master DB] 티커 기준 동기화 시작 (Google API 미사용)")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        api = StockAPIClient()
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return

    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # 필터: '데이터 상태'가 '✅ 검증완료'가 아닌 것만 (속도 최적화)
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
                "page_size": 50 # 한 번에 많이 처리
            }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 모든 데이터가 최신입니다.")
                break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                # 1. 티커 확보
                ticker_obj = props.get("티커", {}).get("title", [])
                if not ticker_obj: continue
                raw_ticker = ticker_obj[0].get("plain_text", "").strip().upper()
                
                print(f"🔍 동기화: {raw_ticker} ...")
                
                # 2. API 데이터 수집 (티커만 믿음)
                data = api.get_data(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    # 성공: 공식 데이터로 덮어씌움
                    status = "✅ 검증완료"
                    log_msg = f"✅ 업데이트 완료 ({data['name']} / {data['source']})"
                    
                    # 요약문 길이 안전 처리
                    summary = data['summary']
                    safe_summary = summary[:1900] + "..." if summary and len(summary) > 1900 else (summary or "")

                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    
                    # '회사개요' 열이 있으면 채움
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                    
                    print(f"   └ [완료] {data['name']}")
                else:
                    # 실패: 티커가 잘못됨
                    status = "⚠️ 확인필요"
                    log_msg = f"❌ 티커 오류: 해당 코드({raw_ticker})의 데이터를 찾을 수 없음"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] 데이터 없음")

                # 3. 노션 반영
                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.2) # 노션 API 부하 조절

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 총 {processed_count}건 동기화 완료")

if __name__ == "__main__":
    main()
