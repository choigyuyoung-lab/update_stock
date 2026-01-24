import os
import time
import requests
import yfinance as yf
from notion_client import Client

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

client = Client(auth=NOTION_TOKEN)

# 산업분류 매핑
INDUSTRY_MAP = {
    "Technology": "IT/기술", "Financial Services": "금융 서비스",
    "Healthcare": "헬스케어", "Consumer Cyclical": "경기 소비재",
    "Communication Services": "통신 서비스", "Industrials": "산업재",
    "Consumer Defensive": "필수 소비재", "Energy": "에너지",
    "Basic Materials": "기초 소재", "Real Estate": "부동산",
    "Utilities": "유틸리티"
}

def get_naver_data_robust(ticker):
    """
    [안정화 로직] 267250 등 일부 종목 수집 실패를 방지하는 2중 수집 함수
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': f'https://m.stock.naver.com/domestic/stock/{ticker}/total'
    }

    name, industry, summary = None, None, None

    # ---------------------------------------------------------
    # 1단계: 'integration' API 시도 (가장 상세한 정보 - 회사개요 포함)
    # ---------------------------------------------------------
    try:
        url_integ = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(url_integ, headers=headers, timeout=5)
        
        if res.status_code == 200:
            data = res.json()
            # 일반 종목(stockItem) 또는 ETF(etfItem) 구조 확인
            item = data.get("result", {}).get("stockItem") or data.get("result", {}).get("etfItem")
            
            if item:
                name = item.get("stockName") or item.get("itemname")
                industry = item.get("industryName", "")
                summary = item.get("description", "") # 기업개요
                
                if name: # 이름이 있으면 성공으로 간주
                    return name, industry, summary, True, "✅ 네이버(통합) 수집 성공"
    except Exception:
        pass # 1단계 실패 시 조용히 2단계로 넘어감

    # ---------------------------------------------------------
    # 2단계: 'basic' API 시도 (267250 등의 구조적 문제 해결용 - 가장 기본)
    # ---------------------------------------------------------
    try:
        url_basic = f"https://m.stock.naver.com/api/stock/{ticker}/basic"
        res = requests.get(url_basic, headers=headers, timeout=5)
        
        if res.status_code == 200:
            data = res.json()
            # 'basic'은 구조가 조금 다를 수 있어 바로 접근 시도
            if "stockName" in data:
                name = data.get("stockName")
                # basic에는 industryCode만 있고 industryName이 없는 경우가 많아 공란 처리 가능성 있음
                industry = industry if industry else "" 
                # basic에는 보통 description(개요)이 없습니다.
                summary = summary if summary else "" 
                
                return name, industry, summary, True, "✅ 네이버(기본) 수집 성공 (개요 없음)"
    except Exception as e:
        return None, None, None, False, f"❌ 네이버 2단계 실패: {e}"

    return None, None, None, False, f"❌ 데이터 없음 ({ticker})"

def get_stock_data(ticker):
    """티커를 기반으로 [종목명, 산업분류, 회사개요] 수집"""
    clean_ticker = ticker.split('.')[0].strip().upper()
    
    # CASE A: 한국 주식 (네이버)
    if len(clean_ticker) == 6 and clean_ticker.isdigit():
        return get_naver_data_robust(clean_ticker)

    # CASE B: 미국/해외 주식 (야후)
    else:
        try:
            stock = yf.Ticker(clean_ticker)
            info = stock.info
            
            # 1차 실패 시 원본 티커 재시도
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
    print(f"🚀 [Master DB 업데이트] 시작 (이중 안전장치 적용)")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # 필터: '데이터 상태'가 '✅ 검증완료'가 아닌 것
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
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                print(f"🔍 {raw_ticker} 조회 중...")
                
                name, industry, summary, success, log_msg = get_stock_data(raw_ticker)
                
                status = "✅ 검증완료" if success else "⚠️ 확인필요"
                upd_props = {
                    "데이터 상태": {"select": {"name": status}},
                    "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                }
                
                if success:
                    safe_summary = summary[:1900] + "..." if summary and len(summary) > 1900 else (summary or "")
                    
                    upd_props.update({
                        "종목명": {"rich_text": [{"text": {"content": name}}]},
                        "산업분류": {"rich_text": [{"text": {"content": industry if industry else ""}}]},
                        "회사개요(텍스트)": {"rich_text": [{"text": {"content": safe_summary}}]}
                    })
                    print(f"   └ 성공: {name}")
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
