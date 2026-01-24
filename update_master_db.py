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

    def search_ticker(self, ticker):
        """
        [핵심 수정] 404 오류가 나는 검색 API 대신,
        네이버 검색창이 사용하는 '자동완성(AC) API'를 사용하여 종목을 찾습니다.
        """
        # 검색어 정제
        query = ticker.strip().upper()
        
        # 1. 자동완성 API 호출 (한국/미국 통합)
        # 이 API는 'LENB'를 넣으면 'LEN.B'를, '005930'을 넣으면 '삼성전자'를 찾아줍니다.
        ac_url = "https://ac.finance.naver.com/ac"
        params = {
            "q": query,
            "q_enc": "euc-kr",
            "st": "111",
            "r_format": "json",
            "r_enc": "euc-kr",
            "r_unicode": "0",
            "t_koreng": "1",
            "r_lt": "111"
        }

        try:
            res = self.session.get(ac_url, params=params, timeout=TIMEOUT)
            if res.status_code != 200:
                return None, f"검색 접속 실패({res.status_code})"

            data = res.json()
            # items 구조: [[['종목코드', '종목명', ...], ...]]
            items = data.get("items", [])
            
            if not items or not items[0]:
                return None, "검색 결과 없음"

            # 2. 최적의 결과 매칭
            # 자동완성 결과 중 입력한 티커와 가장 비슷한 것을 찾습니다.
            best_match = None
            
            # items[0] 리스트를 순회
            for item in items[0]:
                # item[0]: 코드 (005930, AAPL 등)
                # item[1]: 종목명 (삼성전자, 애플 등)
                code = item[0]
                name = item[1]
                
                # 정제된 코드로 비교 (LEN.B -> LENB)
                clean_code = re.sub(r'[^a-zA-Z0-9]', '', code).upper()
                clean_query = re.sub(r'[^a-zA-Z0-9]', '', query).upper()

                # 정확히 일치하거나, 코드가 쿼리를 포함하는 경우
                if clean_query == clean_code or clean_query in clean_code:
                    best_match = {"code": code, "name": name}
                    break
            
            # 일치하는 게 없으면 첫 번째 결과 사용 (유연한 매칭)
            if not best_match:
                first = items[0][0]
                best_match = {"code": first[0], "name": first[1]}

            return best_match, None

        except Exception as e:
            return None, f"검색 에러: {e}"

    def get_details(self, target_code):
        """찾아낸 코드(target_code)로 상세 정보(개요 등) 수집"""
        try:
            # 통합 상세 정보 URL
            detail_url = f"https://m.stock.naver.com/api/stock/{target_code}/integration"
            self.session.headers.update({'Referer': f'https://m.stock.naver.com/domestic/stock/{target_code}/total'})
            
            res = self.session.get(detail_url, timeout=TIMEOUT)
            if res.status_code != 200:
                return None

            data = res.json()
            r = data.get("result", {})
            
            # 주식, ETF, ETN, 리츠 등 모든 타입 확인
            item = (r.get("stockItem") or r.get("etfItem") or 
                    r.get("etnItem") or r.get("reitItem"))
            
            if item:
                # 한글 데이터 추출
                k_name = item.get("stockName") or item.get("itemname") or item.get("gname")
                industry = item.get("industryName", "") or item.get("industryCodeName", "") or item.get("categoryName", "")
                
                # 개요 필드 전수 조사
                summary_candidates = [
                    item.get("description"), item.get("summary"), 
                    item.get("gsummary"), item.get("corpSummary")
                ]
                valid_summaries = [s for s in summary_candidates if s]
                summary = max(valid_summaries, key=len) if valid_summaries else ""

                return {
                    "name": k_name,
                    "industry": industry,
                    "summary": summary,
                    "real_code": target_code
                }
        except Exception:
            pass
        return None

def main():
    print(f"🚀 [Master DB] 검색 엔진 교체 (404 해결 버전)")
    
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
            # 필터링: 검증되지 않은 항목만
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
                
                # 1. 검색 (자동완성 API)
                search_result, err_msg = naver.search_ticker(raw_ticker)
                
                data = None
                if search_result:
                    # 2. 상세 정보 수집
                    data = naver.get_details(search_result['code'])
                
                status = ""
                log_msg = ""
                upd_props = {}
                
                if data:
                    status = "✅ 검증완료"
                    log_msg = f"✅ 성공: {data['name']} (코드: {data['real_code']})"
                    safe_summary = data['summary'][:1900] + "..." if data['summary'] and len(data['summary']) > 1900 else (data['summary'] or "")

                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                        print(f"   └ [완료] {data['name']} (개요 확보)")
                    else:
                        print(f"   └ [완료] {data['name']} (개요 열 없음)")
                else:
                    status = "⚠️ 확인필요"
                    fail_reason = err_msg if err_msg else "상세 정보 없음"
                    log_msg = f"❌ 실패: {fail_reason}"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] {fail_reason}")

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
