import os
import time
import re
import requests
import yfinance as yf
from notion_client import Client
from googleapiclient.discovery import build

# 1. 환경 변수 및 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID") # '상장주식 DB' 전용 ID
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")

# 노션 클라이언트 초기화 (v2.2.1 호환 방식)
client = Client(auth=NOTION_TOKEN)

# 산업분류 매핑 테이블
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

def clean_name(name):
    """비교를 위해 특수문자 제거 및 대문자 변환"""
    if not name: return ""
    return re.sub(r'[^a-zA-Z0-9가-힣]', '', str(name)).upper()

def google_search_verify(ticker, target_name):
    """구글 검색을 통한 2차 무결성 검증"""
    if not GOOGLE_API_KEY or not GOOGLE_CX: return False
    try:
        service = build("customsearch", "v1", developerKey=GOOGLE_API_KEY)
        query = f"{ticker} {target_name} 주식"
        res = service.cse().list(q=query, cx=GOOGLE_CX, num=3).execute()
        items = res.get("items", [])
        combined = "".join([i.get("title", "") + i.get("snippet", "") for i in items])
        return clean_name(target_name) in clean_name(combined)
    except Exception as e:
        print(f"      ⚠️ 구글 API 오류: {e}")
        return False

def main():
    print(f"🚀 [상장주식 DB 검증] 프로세스 시작")
    print(f"🔎 대상 DB ID: {MASTER_DATABASE_ID[:8]}***") # 보안상 일부만 출력
    
    google_count = 0
    next_cursor = None
    
    while True:
        try:
            # [필터 수정] '✅ 검증완료'가 아닌 모든 행을 가져옵니다.
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {
                    "property": "데이터 상태",
                    "select": {"does_not_equal": "✅ 검증완료"}
                },
                "page_size": 20 # 한 번에 가져올 양
            }
            if next_cursor:
                query_params["start_cursor"] = next_cursor
            
            response = client.databases.query(**query_params)
            pages = response.get("results", [])
            
            print(f"📊 이번 루프에서 발견된 미검증 종목: {len(pages)}개")
            
            if not pages:
                print("✅ 모든 종목의 검증이 완료되었거나 처리할 대상이 없습니다.")
                break

            for page in pages:
                # 구글 API 일일 한도(100건) 보호
                if google_count >= 90:
                    print("🛑 구글 API 일일 할당량(90건)에 도달하여 작업을 중단합니다.")
                    return

                page_id = page["id"]
                props = page["properties"]
                
                # 1. 티커 추출 및 접미어 제거 (.KS, .KQ, .O 등)
                raw_ticker = props.get("티커", {}).get("title", [{}])[0].get("plain_text", "").strip().upper()
                if not raw_ticker:
                    print("   ⏭️ 티커가 없는 행은 건너뜁니다.")
                    continue
                
                ticker = raw_ticker.split('.')[0] # [중요] 접미어 제거 로직
                
                # 2. 기준 이름 (종목명(기존)) 추출
                existing_name_list = props.get("종목명(기존)", {}).get("rich_text", [])
                existing_name = existing_name_list[0].get("plain_text", "").strip() if existing_name_list else ""
                
                print(f"▶️ 처리 중: {ticker} (기존명: {existing_name})")
                
                try:
                    # 3. 데이터 수집 (한국: 네이버, 해외: 야후)
                    if len(ticker) == 6 and ticker.isdigit():
                        res = requests.get(f"https://m.stock.naver.com/api/stock/{ticker}/integration", timeout=10).json()
                        item = res.get("result", {}).get("stockItem", {})
                        actual_name = item.get("stockName")
                        summary = item.get("description", "")
                        sector = item.get("industryName", "")
                    else:
                        info = yf.Ticker(ticker).info
                        actual_name = info.get("longName") or info.get("shortName")
                        summary = info.get("longBusinessSummary", "")
                        sector = info.get("sector", "")

                    # 4. 검증 및 보정 로직
                    verified = False
                    log = ""
                    
                    if not actual_name:
                        log = "❌ API 데이터 수집 실패(None)"
                    elif clean_name(existing_name) in clean_name(actual_name) or clean_name(actual_name) in clean_name(existing_name):
                        verified, log = True, "✅ 1차 대조 성공"
                    else:
                        # 이름이 다른 경우(예: 약어 등) 구글 검색으로 최종 판단
                        google_count += 1
                        if google_search_verify(ticker, existing_name):
                            verified, log = True, "✅ 2차 구글 검증 성공"
                        else:
                            log = f"❌ 이름 불일치 (API: {actual_name})"

                    # 5. 노션 업데이트
                    status_val = "✅ 검증완료" if verified else "⚠️ 확인필요"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status_val}},
                        "검증로그": {"rich_text": [{"text": {"content": log}}]}
                    }
                    
                    if verified:
                        upd_props.update({
                            "종목명(텍스트)": {"rich_text": [{"text": {"content": actual_name}}]},
                            "산업분류(원문)": {"rich_text": [{"text": {"content": sector}}]},
                            "산업분류(텍스트)": {"rich_text": [{"text": {"content": INDUSTRY_MAP.get(sector, sector)}}]},
                            "회사개요": {"rich_text": [{"text": {"content": summary[:1900] if summary else ""}}]}
                        })
                    
                    client.pages.update(page_id=page_id, properties=upd_props)
                    print(f"   └ 결과: {status_val} ({log})")
                    
                except Exception as e:
                    print(f"   ⚠️ {ticker} 상세 처리 중 오류 발생: {e}")
                    continue

                time.sleep(0.5) # API 부하 방지

            if not response.get("has_more"):
                break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 전체 프로세스 중단: {e}")
            break

if __name__ == "__main__":
    main()
