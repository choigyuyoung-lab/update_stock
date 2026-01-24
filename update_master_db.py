import os
import time
import requests
import re
from notion_client import Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# 재시도 및 타임아웃 설정
MAX_RETRIES = 5
TIMEOUT = 15
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
# [중요] 검색 단계부터 차단을 막기 위한 필수 헤더 주소
REFERER_URL = 'https://m.stock.naver.com/'

class NaverStockClient:
    """
    네이버 통합 검색 로직을 사용하여 
    국내/해외 주식의 '한글 데이터'를 수집하는 전담 클래스
    """
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=MAX_RETRIES, backoff_factor=2, status_forcelist=[403, 404, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # [수정됨] 처음부터 헤더에 Referer를 심어서 404 차단을 방지합니다.
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Referer': REFERER_URL,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        })

    def search_and_fetch(self, ticker):
        """
        티커 -> 네이버 검색 -> 정확한 코드 식별 -> 한글 상세 데이터 반환
        """
        if not ticker:
            return None

        # 1. 검색어 정제
        clean_ticker = ticker.strip().upper()
        search_query = clean_ticker.split('.')[0]

        try:
            # -----------------------------------------------------
            # STEP A: 네이버 검색 API로 '실제 코드' 조회
            # -----------------------------------------------------
            search_url = f"https://m.stock.naver.com/api/search/all?query={search_query}"
            res = self.session.get(search_url, timeout=TIMEOUT)
            
            if res.status_code != 200:
                # 404가 뜨더라도 로그를 남기고 부드럽게 넘어가도록 처리
                print(f"      ⚠️ 검색 접속 실패 (상태코드: {res.status_code})")
                return None

            search_result = res.json().get("searchList", [])
            if not search_result:
                return None

            # 검색 결과 중 가장 적합한 코드 찾기
            target_code = None
            
            # 1순위: 검색어와 코드가 정확히 일치하거나 포함되는 경우
            for item in search_result:
                code = item.get("reutersCode", "") or item.get("stockId", "")
                if search_query == code or search_query in code:
                    target_code = item.get("reutersCode") or item.get("stockId")
                    break
            
            # 2순위: 없으면 가장 상단 결과 선택
            if not target_code:
                first_item = search_result[0]
                target_code = first_item.get("reutersCode", "") or first_item.get("stockId", "")

            # -----------------------------------------------------
            # STEP B: 상세 정보(Integration) 수집
            # -----------------------------------------------------
            detail_url = f"https://m.stock.naver.com/api/stock/{target_code}/integration"
            
            # 상세 페이지에 맞게 Referer 갱신
            self.session.headers.update({'Referer': f'https://m.stock.naver.com/domestic/stock/{target_code}/total'})
            
            res_detail = self.session.get(detail_url, timeout=TIMEOUT)
            if res_detail.status_code == 200:
                data = res_detail.json()
                
                r = data.get("result", {})
                item = (r.get("stockItem") or r.get("etfItem") or 
                        r.get("etnItem") or r.get("reitItem"))
                
                if item:
                    # 1. 종목명
                    korean_name = item.get("stockName") or item.get("itemname") or item.get("gname")
                    
                    # 2. 산업분류
                    industry = item.get("industryName", "") or item.get("industryCodeName", "") or item.get("categoryName", "")
                    
                    # 3. 회사개요 (가장 긴 텍스트 선택)
                    summary_candidates = [
                        item.get("description"),   # 국내
                        item.get("summary"),       # 해외1
                        item.get("gsummary"),      # 해외2 (한글)
                        item.get("corpSummary")    # ETF
                    ]
                    valid_summaries = [s for s in summary_candidates if s]
                    summary = max(valid_summaries, key=len) if valid_summaries else ""

                    return {
                        "name": korean_name,
                        "industry": industry,
                        "summary": summary,
                        "code": target_code
                    }

        except Exception as e:
            print(f"      ⚠️ API 처리 중 오류 ({ticker}): {e}")
        
        return None

def main():
    print(f"🚀 [Master DB] 한글 데이터 동기화 시작 (헤더 보강 버전)")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        naver = NaverStockClient()
    except Exception as e:
        print(f"❌ 시스템 초기화 실패: {e}")
        return

    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
                "page_size": 30
            }
            if next_cursor:
                query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 대상이 없습니다 (모두 최신 상태).")
                break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list:
                    continue
                
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                print(f"🔍 조회 중: {raw_ticker} ...")
                
                data = naver.search_and_fetch(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    status = "✅ 검증완료"
                    log_msg = f"✅ 수집 성공: {data['name']} ({data['code']})"
                    
                    summary_text = data['summary']
                    safe_summary = summary_text[:1900] + "..." if summary_text and len(summary_text) > 1900 else (summary_text or "")
                    
                    # 로그에 개요 길이 표시 (확인용)
                    summary_len = len(safe_summary)

                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                        print(f"   └ [완료] {data['name']} (개요: {summary_len}자 포함)")
                    else:
                        print(f"   └ [완료] {data['name']} (개요 열 없음)")
                
                else:
                    status = "⚠️ 확인필요"
                    log_msg = f"❌ 검색 실패 ({raw_ticker})"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] 데이터 없음")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.5) # 안전을 위해 대기 시간 약간 늘림

            if not response.get("has_more"):
                break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 실행 중 오류 발생: {e}")
            break
            
    print(f"🏁 작업 완료: 총 {processed_count}건 처리됨")

if __name__ == "__main__":
    main()
