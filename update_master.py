import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def clean_num(val):
    if val is None: return None
    s = str(val).replace(",", "").replace("원", "").strip()
    try: return float(s)
    except: return None

# --- [한국 주식 로직] ---
def get_kr_finance(ticker):
    eps, bps, msg = None, None, ""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # 1순위: 모바일 API (TTM 수치 위주)
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(url, headers=headers, timeout=10).json()
        items = res.get("result", {}).get("totalInfos", [])
        for item in items:
            key = item.get("key", "").upper()
            if "EPS" in key: eps = clean_num(item.get("value"))
            if "BPS" in key: bps = clean_num(item.get("value"))
        
        # 2순위 안전장치: PC 웹페이지 표 (최신 분기 또는 전년 결산)
        if eps is None or bps is None:
            pc_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            tables = pd.read_html(pc_url, encoding='cp949')
            for table in tables:
                if any("주요재무정보" in str(col) for col in table.columns):
                    table.columns = table.columns.get_level_values(-1)
                    table = table.set_index(table.columns[0])
                    if eps is None and "EPS(원)" in table.index:
                        eps = clean_num(table.loc["EPS(원)"].iloc[3]) # 최근분기
                        if eps is None: eps = clean_num(table.loc["EPS(원)"].iloc[0]) # 연간
                    if bps is None and "BPS(원)" in table.index:
                        bps = clean_num(table.loc["BPS(원)"].iloc[3])
                        if bps is None: bps = clean_num(table.loc["BPS(원)"].iloc[0])
                    break
        msg = "✅ 성공" if (eps and bps) else "⚠️ 일부누락"
    except Exception as e:
        msg = f"🚨 KR에러: {str(e)}"
    return eps, bps, msg

# --- [미국 주식 로직] ---
def get_us_finance(ticker):
    eps, bps, msg = None, None, ""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        eps = info.get("trailingEps") or info.get("forwardEps") # TTM -> FY 안전장치
        bps = info.get("bookValue")
        msg = "✅ 성공" if (eps and bps) else "⚠️ 일부누락"
    except Exception as e:
        msg = f"🚨 US에러: {str(e)}"
    return eps, bps, msg

# --- [공통 가격 로직] ---
def get_price_data(ticker, is_kr):
    symbol = ticker + (".KS" if len(ticker) == 6 else "") # 한국주식은 .KS 시도
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
            is_kr = len(t) == 6 and t[0].isdigit()
            return t, is_kr
    return None, False

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"🚀 [통합 업데이트 스케줄러] 시작 - {datetime.now(kst)}")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None
    
    while True:
        response = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker, is_kr = extract_ticker_info(props)
            if not ticker:
                skip += 1; continue

            # 1. 가격 업데이트 (yfinance 공통)
            price, h52, l52 = get_price_data(ticker, is_kr)
            
            # 2. 재무 업데이트 (분기/연간 안전장치)
            if is_kr:
                eps, bps, fin_msg = get_kr_finance(ticker)
            else:
                eps, bps, fin_msg = get_us_finance(ticker)

            # 3. 노션 기록
            try:
                upd = {
                    "현재가": {"number": price} if price else None,
                    "52주 최고가": {"number": h52} if h52 else None,
                    "52주 최저가": {"number": l52} if l52 else None,
                    "EPS": {"number": eps} if eps is not None else None,
                    "BPS": {"number": bps} if bps is not None else None,
                    "마지막 업데이트": {"date": {"start": now_iso}}
                }
                upd = {k: v for k, v in upd.items() if v is not None}
                notion.pages.update(page_id=page["id"], properties=upd)
                
                print(f"   [{ticker}] 가격: {price} | 재무: {fin_msg} (EPS:{eps}, BPS:{bps})")
                success += 1
            except Exception as e:
                print(f"   [{ticker}] 🚨 노션 기록 실패: {e}")
                fail += 1
            
            time.sleep(0.4)

        if not response.get("has_more"): break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 완료 | 성공: {success} | 실패: {fail} | 건너뜀: {skip}")

if __name__ == "__main__":
    main()
