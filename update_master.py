import os
import time
import requests
import pandas as pd
import yfinance as yf
import math
import io  # <--- 이 라이브러리가 추가되었습니다.
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def is_valid_number(val):
    """노션에 기록 가능한 유효한 숫자인지 확인 (NaN, Inf 방지)"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def clean_num(val):
    if val is None: return None
    s = str(val).replace(",", "").replace("원", "").strip()
    try: 
        num = float(s)
        return num if is_valid_number(num) else None
    except: return None

# --- [한국 주식 로직] ---
def get_kr_finance(ticker):
    eps, bps, msg = None, None, ""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # 1순위: 모바일 API
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(url, headers=headers, timeout=10).json()
        items = res.get("result", {}).get("totalInfos", [])
        for item in items:
            key = item.get("key", "").upper()
            if "EPS" in key: eps = clean_num(item.get("value"))
            if "BPS" in key: bps = clean_num(item.get("value"))
        
        # 2순위 안전장치: PC 웹페이지
        if eps is None or bps is None:
            pc_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            response = requests.get(pc_url, headers=headers)
            
            try:
                content = response.content.decode('cp949')
            except:
                content = response.content.decode('utf-8', errors='ignore')
            
            # [수정 포인트] 경고를 방지하기 위해 io.StringIO(content)를 사용합니다.
            tables = pd.read_html(io.StringIO(content))
            for table in tables:
                if any("주요재무정보" in str(col) for col in table.columns):
                    table.columns = table.columns.get_level_values(-1)
                    table = table.set_index(table.columns[0])
                    if eps is None and "EPS(원)" in table.index:
                        eps = clean_num(table.loc["EPS(원)"].iloc[3])
                    if bps is None and "BPS(원)" in table.index:
                        bps = clean_num(table.loc["BPS(원)"].iloc[3])
                    break
        msg = "✅ 성공" if (eps is not None or bps is not None) else "⚠️ 데이터없음"
    except Exception as e:
        msg = f"🚨 KR에러: {str(e)}"
    return eps, bps, msg

# --- [미국 주식 로직] ---
def get_us_finance(ticker):
    eps, bps, msg = None, None, ""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        eps = info.get("trailingEps") or info.get("forwardEps")
        bps = info.get("bookValue")
        msg = "✅ 성공" if (eps is not None or bps is not None) else "⚠️ 데이터없음"
    except Exception as e:
        msg = f"🚨 US에러: {str(e)}"
    return eps, bps, msg

# --- [공통 가격 로직] ---
def get_price_data(ticker, is_kr):
    symbol = ticker + (".KS" if is_kr else "")
    try:
        stock = yf.Ticker(symbol)
        d = stock.fast_info
        return d.get("last_price"), d.get("year_high"), d.get("year_low")
    except: return None, None, None

def extract_ticker_info(props):
    for name in ["티커", "Ticker"]:
        prop = props.get(name, {})
        content = prop.get("title") or prop.get("rich_text")
        if content:
            t = content[0].get("plain_text", "").strip().upper()
            # 6자리 숫자로 시작하면 한국 주식으로 간주
            is_kr = len(t) == 6 and t[0].isdigit()
            return t, is_kr
    return None, False

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"🚀 [통합 마스터] 시작 - {datetime.now(kst)}")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None 
    
    while True:
        response = notion.databases.query(
            database_id=DATABASE_ID, 
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker, is_kr = extract_ticker_info(props)
            if not ticker:
                skip += 1; continue

            # 1. 데이터 수집
            price, h52, l52 = get_price_data(ticker, is_kr)
            eps, bps, fin_msg = get_kr_finance(ticker) if is_kr else get_us_finance(ticker)

            # 2. 노션 기록 (유효성 검사 강화)
            try:
                upd = {}
                if is_valid_number(price): upd["현재가"] = {"number": price}
                if is_valid_number(h52): upd["52주 최고가"] = {"number": h52}
                if is_valid_number(l52): upd["52주 최저가"] = {"number": l52}
                if is_valid_number(eps): upd["EPS"] = {"number": eps}
                if is_valid_number(bps): upd["BPS"] = {"number": bps}
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                print(f"   [{ticker}] 가격:{price} | 재무:{fin_msg}")
                success += 1
            except Exception as e:
                print(f"   [{ticker}] 🚨 기록실패: {e}")
                fail += 1
            
            time.sleep(0.4)

        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 완료 | 성공: {success} | 실패: {fail} | 건너뜀: {skip}")

if __name__ == "__main__":
    main()
