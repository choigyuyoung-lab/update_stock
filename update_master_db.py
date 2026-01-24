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

# [설정] 전체 업데이트를 위해 리스트를 비워두었습니다.
TARGET_TICKERS = [] 

# 시스템 상수
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
        """[엄격 모드] 정확히 일치하는 종목만 수집하며, 회사개요를 반드시 포함"""
        if not ticker: return None, "티커 없음"
        
        input_ticker = ticker.strip().upper()
        # 검색 정확도를 위해 접미어 제거 후 검색 (LENB -> LENB, 005930.KS -> 005930)
        search_query = input_ticker.split('.')[0]

        try:
            # 1. 네이버 검색 API
            search_url = f"https://m.stock.naver.com/api/search/all?query={search_query}"
            res = self.session.get(search_url, timeout=TIMEOUT)
            
            if res.status_code != 200: return None, f"접속 오류({res.status_code})"

            search_result = res.json().get("searchList", [])
            if not search_result: return None, "검색 결과 0건"

            # 2. 코드 정밀 매칭 (엄격)
            target_code = None
            for item in search_result:
                candidates = [
                    item.get("reutersCode", ""), 
                    item.get("stockId", ""), 
                    item.get("itemCode", "")
                ]
                for code in candidates:
                    if not code: continue
                    code_upper = code.upper()
                    # 조건: 완전 일치 하거나, 점(.) 앞부분이 일치하는 경우 (LEN.B == LENB)
                    if code_upper == input_ticker or ('.' in code_upper and code_upper.split('.')[0] == input_ticker):
                        target_code = item.get("reutersCode") or item.get("stockId")
                        break
                if target_code: break
            
            if not target_code: return None, f"매칭 실패 (정확한 티커 불일치)"

            # 3. 상세 데이터(개요 포함) 수집
            detail_url = f"https://m.stock.naver.com/api/stock/{target_code}/integration"
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
                    
                    # [중요] 회사개요가 들어있는 모든 필드를 확인합니다.
                    summary_candidates = [
                        item.get("description"),   # 국내주식
                        item.get("summary"),       # 해외주식 (일부)
                        item.get("gsummary"),      # 해외주식 (메인)
                        item.get("corpSummary")    # ETF/ETN
                    ]
                    # 필드 중 내용이 있는 가장 긴 텍스트를 선택 (정보량 최대화)
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
        print(f"🚀 [Test Mode] 지정된 {len(TARGET_TICKERS)}개 종목만 업데이트합니다.")
    else:
        print(f"🚀 [Full Mode] 전체 종목 업데이트를 시작합니다.")
    
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
            # 전체 모드: 검증되지 않은 모든 항목 조회
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
                
                # 타겟 필터링 (리스트가 비어있으면 전체 실행)
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
                    # 실제 가져온 코드 표시
                    log_msg = f"✅ 성공: {data['name']} (코드: {data['real_code']})"
                    
                    # 회사개요 길이 제한 (1900자)
                    summary_text = data['summary']
                    safe_summary = summary_text[:1900] + "..." if summary_text and len(summary_text) > 1900 else (summary_text or "")
                    summary_len = len(safe_summary)

                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    
                    # [확인] 회사개요 열 업데이트
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                        print(f"   └ [완료] {data['name']} (개요: {summary_len}자)")
                    else:
                        print(f"   └ [완료] {data['name']} (⚠️ 개요 열 없음)")
                else:
                    status = "⚠️ 확인필요"
                    log_msg = f"❌ 매칭 실패: {err_msg}"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] {err_msg}")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.3)

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 작업 완료: 총 {processed_count}건 처리됨")

if __name__ == "__main__":
    main()
