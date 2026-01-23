import os, time, re, yfinance as yf
from notion_client import Client

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def clean_ticker(ticker):
    """티커에서 불필요한 접미사 및 영문자 제거 후 순수 숫자 또는 심볼 반환"""
    ticker = ticker.strip().upper().split('.')[0] # .K, .KS 등 제거
    kr_code = re.sub(r'[^0-9]', '', ticker) # 숫자만 추출 (한국 종목용)
    
    if len(kr_code) == 6:
        return kr_code, True # 한국 종목
    return ticker, False # 미국 종목

def get_yahoo_info(ticker_with_suffix):
    """야후 파이낸스에서 섹터 및 산업 정보 추출 공통 함수"""
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

def get_industry_auto_logic(ticker_val):
    """KOSPI/KOSDAQ 자동 판별 및 미국 종목 처리 로직"""
    code, is_kr = clean_ticker(ticker_val)
    
    if is_kr:
        # 1. KOSPI(.KS) 시도
        print(f"      - KOSPI(.KS) 시도 중...")
        result = get_yahoo_info(f"{code}.KS")
        if result: return result
        
        # 2. 결과 없으면 KOSDAQ(.KQ) 시도
        print(f"      - KOSDAQ(.KQ) 재시도 중...")
        result = get_yahoo_info(f"{code}.KQ")
        return result
    else:
        # 3. 미국 종목 처리
        return get_yahoo_info(code)

def main():
    print("🏗️ [마스터 DB 산업분류] KOSPI/KOSDAQ 자동 판별 시스템 시작...")
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

        for page in pages:
            props = page["properties"]
            ticker_val = ""
            for name in ["티커", "Ticker"]:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content: ticker_val = content[0].get("plain_text", "").strip(); break
            
            if not ticker_val: continue
            
            print(f"🔍 분석 중: {ticker_val}")
            industry_info = get_industry_auto_logic(ticker_val)

            if industry_info:
                try:
                    notion.pages.update(
                        page_id=page["id"],
                        properties={"산업분류": {"rich_text": [{"text": {"content": industry_info}}]}}
                    )
                    print(f"   ✅ 완료: {industry_info}")
                    update_count += 1
                except Exception as e:
                    print(f"   ❌ 업데이트 실패: {e}")
            else:
                print(f"   ⚠️ 정보를 찾을 수 없습니다.")
            
            time.sleep(0.8) # API 부하 방지

        if not res.get("has
