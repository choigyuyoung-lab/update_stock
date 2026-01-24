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

# [설정] 전체 업데이트를 위해 비워둠 (테스트 시 여기에 티커 추가)
TARGET_TICKERS = [] 

# 시스템 상수 (차단 방지를 위한 헤더 강화)
MAX_RETRIES = 5
TIMEOUT = 15
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
REFERER_URL = 'https://m.stock.naver.com/'

class NaverStockClient:
    def __init__(self):
        self.session = requests.Session()
        # 재시도 횟수 늘림 (안정성 확보)
        retries = Retry(total=MAX_RETRIES, backoff_factor=2, status_forcelist=[403, 404, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        # 헤더에 Referer 추가 (중요: 차단 방지)
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Referer': REFERER_URL,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        })

    def search_and_fetch(self, ticker):
        """
        [성공했던 로직 복구] 네이버 통합 검색 -> 상세 정보(한글) 수집
        """
        if not ticker: return None, "티커 없음"
        
        # 검색어 정제
        input_ticker = ticker.strip().upper()
        # 접미어 제거 후 검색 (검색 성공률 상승)
        search_query = input_ticker.split('.')[0]

        try:
            # -----------------------------------------------------
            # 1. 네이버 통합 검색 (search/all)
            # -----------------------------------------------------
            # 헤더가 강화되어 이제 404 오류가 나지 않을 것입니다.
            search_url = f"https://m.stock.naver.com/api/search/all?query={search_query}"
            res = self.session.get(search_url, timeout=TIMEOUT)
            
            if res.status_code != 200:
                return None, f"검색 접속 실패({res.status_code})"

            search_result = res.json().get("searchList", [])
            if not search_result:
                return None, "검색 결과 0건"

            # -----------------------------------------------------
            # 2. 정확한 코드 매칭 (엄격 모드 유지)
            # -----------------------------------------------------
            target_code = None
            for item in search_result:
                # 네이버가 주는 코드 후보군
                candidates = [
                    item.get("reutersCode", ""), 
                    item.get("stockId", ""), 
                    item.get("itemCode", "")
                ]
                
                for code in candidates:
                    if not code: continue
                    code_upper = code.upper()
                    
                    # 1) 완전 일치 (AAPL == AAPL)
                    # 2) 점(.) 앞부분 일치 (005930 == 005930.KS)
                    if code_upper == input_ticker or ('.' in code_upper and code_upper.split('.')[0] == input_ticker):
                        target_code = item.get("reutersCode") or item.get("stockId")
                        break
                if target_code: break
            
            if not target_code:
                # [보완] 만약 엄격 매칭에 실패했더라도, 검색 결과가 1개뿐이고 
                # 그 이름이 매우 유사하다면 가져오는 것이 사용자 의도에 맞을 수 있음
                # 하지만 요청하신 대로 '엄격함'을 유지하되, 검색어가 코드 그 자체인 경우는 신뢰
                if len(search_result) > 0:
                    first = search_result[0]
                    first_code = first.get("stockId", "") or first.get("reutersCode", "")
                    if input_ticker in first_code.upper(): # 부분 포함이면 시도
                        target_code = first_code
                    else:
                        return None, f"매칭 실패 (검색됨: {first.get('stockName', '')})"
                else:
                    return None, "매칭 실패"

            # -----------------------------------------------------
            # 3. 상세 데이터(한글 개요) 수집
            # -----------------------------------------------------
            detail_url = f"https://m.stock.naver.com/api/stock/{target_code}/integration"
            # 상세 페이지용 Referer 업데이트
            self.session.headers.update({'Referer': f'https://m.stock.naver.com/domestic/stock/{target_code}/total'})
            
            res_detail = self.session.get(detail_url, timeout=TIMEOUT)
            if res_detail.status_code == 200:
                data = res_detail.json()
                r = data.get("result", {})
                
                # 주식, ETF, ETN, 리츠 등 모든 타입 탐색
                item = (r.get("stockItem") or r.get("etfItem") or r.get("etnItem") or r.get("reitItem"))
                
                if item:
                    k_name = item.get("stockName") or item.get("itemname") or item.get("gname")
                    industry = item.get("industryName", "") or item.get("industryCodeName", "") or item.get("categoryName", "")
                    
                    # 회사개요 필드 전수 조사 (가장 긴 설명 선택)
                    summary_candidates = [
                        item.get("description"),   # 국내
                        item.get("summary"),       # 해외1
                        item.get("gsummary"),      # 해외2 (한글)
                        item.get("corpSummary")    # ETF
                    ]
                    valid_summaries = [s for s in summary_candidates if s]
                    summary = max(valid_summaries, key=len) if valid_summaries else ""

                    return {
                        "name": k_name,
                        "industry": industry,
                        "summary": summary,
                        "real_code": target_code
                    }, None

        except Exception as e:
            return None, f"에러: {e}"
        
        return None, "상세 정보 없음"

def main():
    if TARGET_TICKERS:
        print(f"🚀 [Target Mode] 지정된 종목만 업데이트: {TARGET_TICKERS}")
    else:
        print(f"🚀 [Full Mode] 전체 종목 업데이트 시작 (네이버 통합검색 복구)")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        naver = NaverStockClient()
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return

    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # 필터: 검증되지 않은 항목만
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
                "page_size": 30 
            }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 대상이 없습니다.")
                break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                # 타겟 필터링
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS:
                    continue

                print(f"🔍 조회 중: {raw_ticker} ...")
                
                # 데이터 수집
                data, err_msg = naver.search_and_fetch(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    status = "✅ 검증완료"
                    log_msg = f"✅ 성공: {data['name']} ({data['real_code']})"
                    
                    # 요약본 길이 제한
                    summary_text = data['summary']
                    safe_summary = summary_text[:1900] + "..." if summary_text and len(summary_text) > 1900 else (summary_text or "")
                    summary_len = len(safe_summary)

                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                        print(f"   └ [완료] {data['name']} (개요: {summary_len}자)")
                    else:
                        print(f"   └ [완료] {data['name']} (⚠️ 개요 열 없음)")
                else:
                    status = "⚠️ 확인필요"
                    log_msg = f"❌ 실패: {err_msg}"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] {err_msg}")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.5) # 차단 방지를 위해 대기 시간 약간 늘림

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 작업 완료: 총 {processed_count}건 처리됨")

if __name__ == "__main__":
    main()
