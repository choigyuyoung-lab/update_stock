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
    """노션 API 전송 전 숫자 유효성 검사 (NaN, Inf 제외)"""
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

# ---------------------------------------------------------
# 3. 데이터 수집 (현재가 + 전일 종가)
# ---------------------------------------------------------
def get_kr_prices(ticker):
    """한국 주식 현재가와 전일 종가를 신속하게 추출"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    prices = {"현재가": None, "전일 종가": None}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. 현재가 추출
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area: 
            prices["현재가"] = float(today_area.text.replace(',', '').strip())
            
        # 2. 전일 종가 추출 (추가됨)
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
    print(f"⚡ [한국 주식 가격 업데이트] 실행 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # 한 번에 100개씩 호출
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
            print(f"❌ 노션 연결 실패: {e}")
            break

        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = ""
            is_kr = False
            
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        # 🌟 [수정된 로직] 기본적으로 한국 주식 조건을 만족하더라도, 
                        # 끝이 .T(일본), .TA 또는 .TW(대만)로 끝나면 한국 주식이 '아닌' 것으로 강력하게 제외!
                        is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
                        break
            
            if not ticker or not is_kr: continue
            
            try:
                upd = {}
                prices = get_kr_prices(ticker)
                
                # --- 현재가 업데이트 ---
                if is_valid(prices["현재가"]): 
                    upd["현재가"] = {"number": prices["현재가"]}
                    
                # --- 전일 종가 업데이트 (추가됨) ---
                if is_valid(prices["전일 종가"]):
                    upd["전일 종가"] = {"number": prices["전일 종가"]}

                # --- 공통 업데이트 ---
                if upd:
                    # 실시간 업데이트 시간 기록
                    if "마지막 업데이트" in props:
                        current_now_iso = datetime.now(kst).isoformat()
                        upd["마지막 업데이트"] = {"date": {"start": current_now_iso}}
                        
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [KR: {ticker}] 완료 - 현재가: {prices['현재가']} / 전일종가: {prices['전일 종가']}")
                
            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.6) # API 제한 고려 약간의 대기

        if not res.get("has_more"): 
            break
            
        next_cursor = res.get("next_cursor")
        print(f"--- {processed_count}건 완료. 다음 페이지(100건)로 이동 전 3초 휴식 ---")
        time.sleep(3) # 페이지 전환 시 3초 휴식

    print(f"\n✨ 종료. 총 {processed_count}건 업데이트 완료.")
    
if __name__ == "__main__":
    main()
