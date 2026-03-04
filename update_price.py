import os
import time
import math
import requests
import yfinance as yf
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
# 3. 데이터 수집 (현재가 전용으로 초경량화)
# ---------------------------------------------------------
def get_kr_current_price(ticker):
    """한국 주식 현재가만 신속하게 추출"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area: 
            return float(today_area.text.replace(',', '').strip())
    except Exception as e:
        print(f"   ⚠️ [Price Error] {ticker}: {e}")
    return None

# ---------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"⚡ [주가 업데이트: 초고속 현재가 모드] 실행 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
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
                        is_kr = ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())
                        break
            
            if not ticker: continue
            
            try:
                upd = {}
                
                # --- 1. 한국 주식 현재가 ---
                if is_kr:
                    price = get_kr_current_price(ticker)
                    if is_valid(price): upd["현재가"] = {"number": price}

                # --- 2. 미국 주식 현재가 ---
                else:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    last_price = info.get('currentPrice') or info.get('regularMarketPrice')
                    if is_valid(last_price): upd["현재가"] = {"number": last_price}

                # --- 3. 공통 업데이트 ---
                if upd:
                    upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    price_log = upd.get("현재가", {}).get("number", "N/A")
                    print(f"   ✅ [{ticker}] 완료 ({'KR' if is_kr else 'US'}) - 현재가: {price_log}")
                
            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.3) # API 제한 고려 약간의 대기

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"\n✨ 종료. 총 {processed_count}건 업데이트 완료.")

if __name__ == "__main__":
    main()
