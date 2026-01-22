import os
import warnings
warnings.filterwarnings("ignore")

import yfinance as yf
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 안전장치 (20분)
MAX_RUNTIME_SEC = 1200 

def fetch_yahoo_price(symbol):
    """가격 정보 가져오기 (실패 시 None 반환)"""
    try:
        stock = yf.Ticker(symbol)
        # .info 접근 시 발생하는 404 로그를 줄이기 위해 최대한 조심스럽게 접근
        d = stock.fast_info
        price = d.get("last_price")
        
        if price is None:
            d = stock.info
            price = d.get("currentPrice") or d.get("regularMarketPrice")

        if price is None: return None

        return {
            "price": price,
            "high52w": d.get("year_high") or d.get("fiftyTwoWeekHigh"), 
            "low52w": d.get("year_low") or d.get("fiftyTwoWeekLow")    
        }
    except:
        return None

def get_smart_stock_data(ticker, market_hint):
    """종목코드 및 시장 자동 감지 (한국 우선 검색 로직 적용)"""
    ticker = str(ticker).strip().upper()
    
    # 1. Market 힌트가 있을 때
    if market_hint:
        symbol = ticker
        if "KOSPI" in market_hint.upper(): 
            if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
        elif "KOSDAQ" in market_hint.upper(): 
            if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
        else:
            symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
        return fetch_yahoo_price(symbol), market_hint

    # 2. 힌트 없을 때 (6글자면 한국 주식으로 간주하여 우선 검색)
    else:
        # 한국 주식 특징 (6글자) - 0104P0 같은 케이스 대응
        if len(ticker) == 6:
            # KOSPI 먼저 시도
            data = fetch_yahoo_price(f"{ticker}.KS")
            if data: return data, "KOSPI(Auto)"
            
            # KOSDAQ 시도
            data = fetch_yahoo_price(f"{ticker}.KQ")
            if data: return data, "KOSDAQ(Auto)"
            
            # 둘 다 아니면 미국/기타 시도
            data = fetch_yahoo_price(ticker)
            if data: return data, "US(Auto)"

        # 그 외 (미국 주식 등)
        else:
            clean_ticker = ticker.replace(".KS", "").replace(".KQ", "").replace(".K", "")
            data = fetch_yahoo_price(clean_ticker)
            if data: return data, "US(Auto)"
            
            # 한국 ETF 등 재시도
            data = fetch_yahoo_price(f"{clean_ticker}.KS")
            if data: return data, "KOSPI(Auto-Retry)"

    return None, "Unknown"

def extract_value(prop):
    """속성값 안전 추출"""
    if not prop: return ""
    p_type = prop.get("type")
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        return extract_value(array[0]) if array else ""
    if p_type == "select": return prop.get("select", {}).get("name", "")
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        return text_list[0].get("plain_text", "") if text_list else ""
    if p_type == "formula":
        f = prop.get("formula", {})
        return str(f.get("number", "") if f.get("type")=="number" else f.get("string", ""))
    return ""

def main():
    start_time = time.time()
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat() 
    print(f"🚀 [가격 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    response = notion.databases.query(database_id=DATABASE_ID)
    pages = response.get("results", [])
    
    success, fail = 0, 0
    for page in pages:
        if time.time() - start_time > MAX_RUNTIME_SEC: break
        try:
            props = page["properties"]
            ticker = extract_value(props.get("티커"))
            market = extract_value(props.get("Market"))
            if not ticker: continue
            
            data, mkt = get_smart_stock_data(ticker, market)
            if data:
                upd = {
                    "현재가": {"number": data["price"]},
                    "마지막 업데이트": {"date": {"start": now_iso}},
                    "52주 최고가": {"number": data["high52w"]} if data["high52w"] else None,
                    "52주 최저가": {"number": data["low52w"]} if data["low52w"] else None
                }
                # None 값 제거
                upd = {k: v for k, v in upd.items() if v is not None}
                notion.pages.update(page_id=page["id"], properties=upd)
                success += 1
                print(f"   => ✅ [{mkt}] {ticker} : {data['price']:,.0f}")
            else:
                fail += 1
        except: fail += 1; continue

    print(f"\n✨ 완료: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
