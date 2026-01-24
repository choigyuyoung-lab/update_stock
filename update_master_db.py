import os
import time
import requests
import re
from notion_client import Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# 2. 시스템 상수
MAX_RETRIES = 3
TIMEOUT = 10
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

class NaverStockClient:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({'User-Agent': USER_AGENT})

    def search_and_fetch(self, ticker):
        """
        [엄격 모드] 입력된 티커와 정확히 일치하는 종목만 가져옵니다.
        """
        if not ticker: return None, "티커 없음"

        # 입력값 정제 (공백제거, 대문자)
        input_ticker = ticker.strip().upper()
        
        # 검색어: 한국 주식 등에서 접미어(.KS)가 붙어있으면 떼고 검색하는 것이 정확함
        # 예: 005930.KS -> 005930 검색 / LENB -> LENB 검색
        search_query = input_ticker.split('.')[0]

        try:
            # -----------------------------------------------------
            # 1. 네이버 검색 API 호출
            # -----------------------------------------------------
            search_url = f"https://m.stock.naver.com/api/search/all?query={search_query}"
            res = self.session.get(search_url, timeout=TIMEOUT)
            
            if res.status_code != 200:
                return None, f"네이버 접속 오류({res.status_code})"

            search_result = res.json().get("searchList", [])
            if not search_result:
                return None, "검색 결과 0건 (존재하지 않는 티커)"

            # -----------------------------------------------------
            # 2. 결과 중 '정확히 일치하는' 코드 찾기
            # -----------------------------------------------------
            target_code = None
            
            for item in search_result:
                # 네이버가 제공하는 다양한 코드 필드 확인
                # reutersCode: 005930.KS, AAPL.O
                # stockId: 005930 (국내), AAPL (해외)
                # itemCode: 005930 (일부)
                candidates = [
                    item.get("reutersCode", ""),
                    item.get("stockId", ""),
                    item.get("itemCode", "")
                ]
                
                # 후보 코드들 중 하나라도 입력 티커와 '정확히' 일치하는지 확인
                # 조건 1: 완전 일치 (AAPL == AAPL)
                # 조건 2: 접미어 제외 일치 (005930 == 005930.KS의 앞부분)
                for code in candidates:
                    if not code: continue
                    code_upper = code.upper()
                    
                    # 정확히 일치하거나 (AAPL)
                    if code_upper == input_ticker:
                        target_code = item.get("reutersCode") or item.get("stockId")
                        break
                    
                    # 입력값(005930)이 검색된 코드(005930.KS)의 앞부분과 정확히 일치하는지
                    if "." in code_upper:
                        base_code = code_upper.split('.')[0]
                        if base_code == input_ticker:
                            target_code = item.get("reutersCode") or item.get("stockId")
                            break
                
                if target_code: break
            
            # [중요] 일치하는 코드가 없으면 절대 가져오지 않음 (유사종목 차단)
            if not target_code:
                return None, f"검색 결과는 있으나 정확한 티커 매칭 실패 ({input_ticker})"

            # -----------------------------------------------------
            # 3. 상세 데이터 수집 (Integration)
            # -----------------------------------------------------
            detail_url = f"https://m.stock.naver.com/api/stock/{target_code}/integration"
            self.session.headers.update({'Referer': f'https://m.stock.naver.com/domestic/stock/{target_code}/total'})
            
            res_detail = self.session.get(detail_url, timeout=TIMEOUT)
            if res_detail.status_code == 200:
                data = res_detail.json()
                r = data.get("result", {})
                
                # 주식, ETF, ETN, 리츠 등 모든 타입 확인
                item = (r.get("stockItem") or r.get("etfItem") or 
                        r.get("etnItem") or r.get("reitItem"))
                
                if item:
                    k_name = item.get("stockName") or item.get("itemname") or item.get("gname")
                    industry = item.get("industryName", "") or item.get("industryCodeName", "") or item.get("categoryName", "")
                    
                    summary = (
                        item.get("description") or 
                        item.get("summary") or 
                        item.get("gsummary") or 
                        item.get("corpSummary") or
                        ""
                    )

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
    print(f"🚀 [Master DB] 엄격 모드 동기화 (유사종목 차단)")
    
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
            # 아직 검증되지 않은 항목만 필터링
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
                print(f"🔍 조회 중: {raw_ticker} ...")
                
                # 데이터 수집 요청
                data, err_msg = naver.search_and_fetch(raw_ticker)
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    status = "✅ 검증완료"
                    log_msg = f"✅ 매칭 성공: {data['name']} (코드: {data['real_code']})"
                    
                    safe_summary = data['summary'][:1900] + "..." if data['summary'] and len(data['summary']) > 1900 else (data['summary'] or "")

                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                        print(f"   └ [성공] {data['name']}")
                    else:
                        print(f"   └ [성공] {data['name']} (개요 열 없음)")
                
                else:
                    # 엄격 모드: 매칭 실패 시 '확인필요' 상태 유지 및 실패 로그 기록
                    status = "⚠️ 확인필요"
                    log_msg = f"❌ 매칭 실패: {err_msg}"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] {err_msg} (유사종목 연결 안 함)")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.3)

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 작업 완료: 총 {processed_count}건")

if __name__ == "__main__":
    main()
