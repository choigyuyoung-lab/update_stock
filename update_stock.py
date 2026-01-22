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

def fetch_yahoo_price(symbol):
    try:
        stock = yf.Ticker(symbol)
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
    except: return None

def get_smart_stock_data(ticker, market_hint):
    ticker = str(ticker).strip().upper()
    if market_hint:
        symbol = ticker
        if "KOSPI" in market_hint.upper(): 
            if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
        elif "KOSDAQ" in market_hint.upper(): 
            if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
        else:
            symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
        return fetch_yahoo_price(symbol), market_hint
    else:
        if len(ticker) == 6:
            data = fetch_yahoo_price(f"{ticker}.KS")
            if data: return data, "KOSPI(Auto)"
            data = fetch_yahoo_price(f"{ticker}.KQ")
            if data: return data, "KOSDAQ(Auto)"
            data = fetch_yahoo_price(ticker)
            if data: return data, "US(Auto)"
        else:
            clean_ticker = ticker.replace(".KS", "").replace(".KQ", "").replace(".K", "")
            data = fetch_yahoo_price(clean_ticker)
            if data: return data, "US(Auto)"
    return None, "Unknown"

def extract_value(prop):
    if not prop: return ""
    p_type = prop.get("type")
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        return extract_value(array[0]) if array else ""
    if p_type == "select": return prop.get("select", {}).get("name", "")
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        return text_list[0].get("plain_text", "") if text_list else ""
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat() 
    print(f"🚀 [가격 업데이트] 시작 (전체 데이터 모드)")
    
    success, fail = 0, 0
    next_cursor = None
    
    # [핵심] 100개 제한을 풀기 위한 무한 루프 페이지네이션
    while True:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
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
                    upd = {k: v for k, v in upd.items() if v is not None}
                    notion.pages.update(page_id=page["id"], properties=upd)
                    success += 1
                    print(f"   => ✅ [{mkt}] {ticker} 업데이트 완료")
                else: fail += 1
                time.sleep(0.3)
            except: fail += 1; continue

        # 다음 페이지가 없으면 루프 종료
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 가격 업데이트 완료: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
