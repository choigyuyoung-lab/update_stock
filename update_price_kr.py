import os
import time
import math
import requests
from datetime import datetime, timedelta, timezone
from notion_client import Client
from bs4 import BeautifulSoup

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")

notion = Client(auth=NOTION_TOKEN)

# ---------------------------------------------------------
# 2. 유틸리티 함수
# ---------------------------------------------------------
def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

# ---------------------------------------------------------
# 3. 데이터 수집 (현재가 + 전일 종가) - 세션 활용 최적화
# ---------------------------------------------------------
def get_kr_prices_optimized(ticker, session):
    """🌟 [최적화] Session과 lxml을 사용하여 속도 향상"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    prices = {"현재가": None, "전일 종가": None}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = session.get(url, headers=headers, timeout=5)
        # 🌟 속도가 빠른 lxml 파서 사용
        soup = BeautifulSoup(res.text, 'lxml')
        
        # 1. 현재가 추출
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area: 
            prices["현재가"] = float(today_area.text.replace(',', '').strip())
            
        # 2. 전일 종가 추출
        prev_close_area = soup.select_one('td.first em .blind')
        if prev_close_area:
            prices["전일 종가"] = float(prev_close_area.text.replace(',', '').strip())
            
    except Exception as e:
        print(f"   ⚠️ [Price Error] {ticker}: {e}")
        
    return prices

# ---------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    print(f"⚡ [최적화] 한국 주식 가격 업데이트 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    # 🌟 [최적화] 통신 연결을 유지하는 Session 객체 생성
    session = requests.Session()
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
            print(f"❌ 노션 연결 실패: {e}")
            break

        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = ""
            
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        # 한국 주식 판별 로직 (기존 유지)
                        is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
                        break
            
            if not ticker or not is_kr: continue
            
            try:
                upd = {}
                # 🌟 [최적화] 세션을 인자로 전달
                prices = get_kr_prices_optimized(ticker, session)
                
                if is_valid(prices["현재가"]): 
                    upd["현재가"] = {"number": prices["현재가"]}
                if is_valid(prices["전일 종가"]):
                    upd["전일 종가"] = {"number": prices["전일 종가"]}

                if upd:
                    if "마지막 업데이트" in props:
                        upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
                        
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [KR: {ticker}] 완료 - 현재가: {prices['현재가']} / 전일종가: {prices['전일 종가']}")
                
            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.4) # [최적화] 속도 향상을 위해 대기 시간 소폭 단축

        if not res.get("has_more"): 
            break
            
        next_cursor = res.get("next_cursor")
        print(f"--- {processed_count}건 완료. 다음 페이지로 이동 전 3초 휴식 ---")
        time.sleep(3)

    print(f"\n✨ 종료. 총 {processed_count}건 업데이트 완료.")
    
if __name__ == "__main__":
    main()
