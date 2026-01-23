import os, time, re, yfinance as yf
from notion_client import Client

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def clean_ticker(ticker):
    """
    사용자님 맞춤형 티커 판별 로직:
    1. .K, .KS 등 접미사 제거 및 대문자화
    2. 'A'로 시작하는 7자리인 경우 'A' 제거 (예: A005930 -> 005930)
    3. 최종 결과가 6글자이고 숫자가 포함되어 있다면 한국 종목(KR)으로 간주
    """
    raw_ticker = ticker.strip().upper().split('.')[0]
    
    # 'A'로 시작하는 7자리 처리
    if raw_ticker.startswith('A') and len(raw_ticker) == 7:
        raw_ticker = raw_ticker[1:]
    
    # 판별: 길이가 6자이고 숫자가 최소 하나 이상 포함되어 있는가?
    is_kr = len(raw_ticker) == 6 and any(char.isdigit() for char in raw_ticker)
    
    return raw_ticker, is_kr

def get_yahoo_info(ticker_with_suffix):
    """야후 파이낸스 API 호출 공통 함수"""
    try:
        stock = yf.Ticker(ticker_with_suffix)
        info = stock.info
        sector = info.get('sector')
        industry = info.get('industry')
        if sector:
            return f"{sector} | {industry}" if industry else sector
    except:
        pass
    return None

def get_industry_logic(ticker_val):
    """KOSPI/KOSDAQ 자동 판별 및 미국 종목 처리"""
    code, is_kr = clean_ticker(ticker_val)
    
    if is_kr:
        # 1. KOSPI(.KS) 먼저 시도
        print(f"      - [{code}.KS] 시도 중...")
        result = get_yahoo_info(f"{code}.KS")
        if result: return result
        
        # 2. 실패 시 KOSDAQ(.KQ) 시도
        print(f"      - [{code}.KQ] 재시도 중...")
        return get_yahoo_info(f"{code}.KQ")
    else:
        # 3. 미국 종목 (6자가 아니거나 숫자가 없는 경우)
        print(f"      - 미국 종목으로 조회 중...")
        return get_yahoo_info(code)

def main():
    print("🏗️ [마스터 DB] 야후 기반 산업분류 자동화 시작 (6자 숫자 포함 로직)...")
    next_cursor = None
    update_count = 0

    while True:
        query_params = {
            "database_id": MASTER_DB_ID,
            "start_cursor": next_cursor,
            "filter": {
                "property": "산업분류",
                "rich_text": {"is_empty": True}
            }
        }
        res = notion.databases.query(**query_params)
        pages = res.get("results", [])

        if not pages:
            print("💡 업데이트할 새로운 종목이 없습니다.")
            break

        for page in pages:
            props = page["properties"]
            # 티커 속성명 확인 (티커 또는 Ticker)
            ticker_val = ""
            for name in ["티커", "Ticker"]:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content: ticker_val = content[0].get("plain_text", "").strip(); break
            
            if not ticker_val: continue
            
            print(f"🔍 분석 대상: {ticker_val}")
            industry_info = get_industry_logic(ticker_val)

            if industry_info:
                try:
                    notion.pages.update(
                        page_id=page["id"],
                        properties={"산업분류": {"rich_text": [{"text": {"content": industry_info}}]}}
                    )
                    print(f"   ✅ 완료: {industry_info}")
                    update_count += 1
                except Exception as e:
                    print(f"   ❌ 노션 업데이트 실패: {e}")
            else:
                print(f"   ⚠️ 야후에서 정보를 찾을 수 없음")
            
            time.sleep(0.7) # API 속도 제한 고려

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"✨ 전체 작업 완료! 총 {update_count}개 종목 처리됨.")

if __name__ == "__main__":
    main()
