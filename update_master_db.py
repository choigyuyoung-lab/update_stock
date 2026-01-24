import os
import time
import requests
import yfinance as yf
from notion_client import Client

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

client = Client(auth=NOTION_TOKEN)

# 산업분류 영문 -> 한글 매핑
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

def get_naver_basic_info(ticker):
    """
    [안정화된 로직] 네이버 모바일 API를 통해 기본 정보(이름, 산업, 개요)만 가져옵니다.
    """
    try:
        # 네이버가 차단하지 않도록 브라우저인 척 헤더를 설정합니다.
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': f'https://m.stock.naver.com/domestic/stock/{ticker}/total'
        }
        
        # 통합 정보 API 호출
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(url, headers=headers, timeout=5)
        
        if res.status_code == 200:
            data = res.json()
            # 데이터 위치가 조금씩 다를 수 있어 안전하게 가져옵니다.
            item = data.get("result", {}).get("stockItem", {})
            
            if not item:
                return None, None, None, False, f"❌ 네이버 데이터 없음 ({ticker})"
                
            name = item.get("stockName")
            industry = item.get("industryName", "")
            summary = item.get("description", "")
            
            return name, industry, summary, True, "✅ 네이버 수집 성공"
        else:
            return None, None, None, False, f"❌ 네이버 접속 차단/오류 ({res.status_code})"
            
    except Exception as e:
        return None, None, None, False, f"❌ 네이버 에러: {e}"

def get_stock_data(ticker):
    """티커를 기반으로 [종목명, 산업분류, 회사개요]만 수집합니다."""
    # 접미어 제거 (005930.KS -> 005930)
    clean_ticker = ticker.split('.')[0].strip().upper()
    
    # ---------------------------
    # CASE A: 한국 주식 (숫자 6자리) -> 네이버 로직 적용
    # ---------------------------
    if len(clean_ticker) == 6 and clean_ticker.isdigit():
        return get_naver_basic_info(clean_ticker)

    # ---------------------------
    # CASE B: 미국/해외 주식 -> 야후 파이낸스
    # ---------------------------
    else:
        try:
            stock = yf.Ticker(clean_ticker)
            info = stock.info
            
            # 실패 시 원본 티커로 재시도
            if not info or ('longName' not in info and 'shortName' not in info):
                stock = yf.Ticker(ticker)
                info = stock.info
            
            if info and ('longName' in info or 'shortName' in info):
                name = info.get("longName") or info.get("shortName")
                sector = info.get("sector", "")
                summary = info.get("longBusinessSummary", "")
                
                korean_sector = INDUSTRY_MAP.get(sector, sector)
                return name, korean_sector, summary, True, "✅ 야후 수집 성공"
            else:
                return None, None, None, False, f"❌ 야후 데이터 없음 ({ticker})"
        except Exception as e:
            return None, None, None, False, f"❌ 야후 에러: {e}"

def main():
    print(f"🚀 [상장주식 DB 업데이트] 시작 (EPS/BPS 제외, 기본정보 집중)")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # '데이터 상태'가 '✅ 검증완료'가 아닌 항목 조회
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
                "page_size": 30
            }
            if next_cursor:
                query_params["start_cursor"] = next_cursor
            
            response = client.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 대상이 없습니다.")
                break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                # 티커 추출
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                print(f"🔍 {raw_ticker} 조회 중...")
                
                # 데이터 수집 (EPS/BPS 제외)
                name, industry, summary, success, log_msg = get_stock_data(raw_ticker)
                
                # 노션 업데이트 준비
                status = "✅ 검증완료" if success else "⚠️ 확인필요"
                upd_props = {
                    "데이터 상태": {"select": {"name": status}},
                    "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                }
                
                if success:
                    # 회사개요 길이 제한 (1900자)
                    safe_summary = summary[:1900] + "..." if summary and len(summary) > 1900 else (summary or "")
                    
                    upd_props.update({
                        "종목명": {"rich_text": [{"text": {"content": name}}]},
                        "산업분류": {"rich_text": [{"text": {"content": industry if industry else ""}}]},
                        "회사개요(텍스트)": {"rich_text": [{"text": {"content": safe_summary}}]}
                    })
                    print(f"   └ 성공: {name} ({industry})")
                else:
                    print(f"   └ 실패: {log_msg}")

                client.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.3)

            if not response.get("has_more"):
                break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            break
            
    print(f"🏁 총 {processed_count}개 종목 처리 완료")

if __name__ == "__main__":
    main()
