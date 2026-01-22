import sys
import subprocess
import os
import time
from datetime import datetime, timedelta, timezone

# [자가 치유] 실행 시 라이브러리 강제 재설치 (환경 꼬임 방지)
try:
    import notion_client
    # 버전 확인 또는 특정 기능 테스트 시도
    from notion_client import Client
except (ImportError, AttributeError):
    print("🚑 라이브러리 긴급 복구 중...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--force-reinstall", "notion-client>=2.0.0"])
    import notion_client
    from notion_client import Client

import yfinance as yf

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
# 강제 재설치 후 클라이언트 초기화
notion = notion_client.Client(auth=NOTION_TOKEN)

# 안전장치
MAX_RUNTIME_SEC = 1200 

def fetch_yahoo_price(symbol):
    """가격 정보만 가져오기"""
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        price = d.get("currentPrice") or d.get("regularMarketPrice")
        if price is None: return None
        return {
            "price": price,
            "high52w": d.get("fiftyTwoWeekHigh"), 
            "low52w": d.get("fiftyTwoWeekLow")    
        }
    except:
        return None

def get_smart_stock_data(ticker, market_hint):
    """한/미 주식 자동 감지"""
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
        if ticker.isdigit() and len(ticker) == 6:
            data = fetch_yahoo_price(f"{ticker}.KS")
            if data: return data, "KOSPI(Auto)"
            data = fetch_yahoo_price(f"{ticker}.KQ")
            if data: return data, "KOSDAQ(Auto)"
        else:
            clean_ticker = ticker.replace(".KS", "").replace(".KQ", "").replace(".K", "")
            data = fetch_yahoo_price(clean_ticker)
            if data: return data, "US(Auto)"
            data = fetch_yahoo_price(f"{clean_ticker}.KS")
            if data: return data, "KOSPI(Auto-Retry)"
            data = fetch_yahoo_price(f"{clean_ticker}.KQ")
            if data: return data, "KOSDAQ(Auto-Retry)"
    return None, "Unknown"

def extract_value(prop):
    """속성값 안전 추출"""
    if not prop: return ""
    p_type = prop.get("type")
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        if array: return extract_value(array[0])
    if p_type == "select": return prop.get("select", {}).get("name", "")
    if p_type in ["rich_text", "title"]:
        return prop.get(p_type, [{}])[0].get("plain_text", "") if prop.get(p_type) else ""
    if p_type == "formula":
        f = prop.get("formula", {})
        return str(f.get("number") if f.get("type")=="number" else f.get("string", ""))
    return ""

def main():
    start_time = time.time()
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat() 
    print(f"🚀 [가격 업데이트(자가치유)] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        if time.time() - start_time > MAX_RUNTIME_SEC: break
        try:
            # 여기서 에러가 나면 notion-client 버전 문제임 -> 위에서 이미 해결함
            response = notion.databases.query(
                database_id=DATABASE_ID, 
                start_cursor=next_cursor
            )
            pages = response.get("results", [])
            if not pages: break

            for page in pages:
                if time.time() - start_time > MAX_RUNTIME_SEC: has_more=False; break 
                try:
                    props = page["properties"]
                    ticker = extract_value(props.get("티커"))
                    market = extract_value(props.get("Market"))
                    
                    if not ticker: continue
                    
                    data, mkt = get_smart_stock_data(ticker, market)
                    if data:
                        upd = {
                            "현재가": {"number": data["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        if data["high52w"]: upd["52주 최고가"] = {"number": data["high52w"]}
                        if data["low52w"]: upd["52주 최저가"] = {"number": data["low52w"]}
                        
                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        print(f"   => ✅ [{mkt}] {ticker} : {data['price']:,.0f}")
                    else:
                        fail += 1
                    time.sleep(0.5) 
                except: fail += 1; continue
            
            if not has_more: break
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 치명적 오류: {e}")
            # 디버깅을 위해 속성 출력
            try: print(f"DEBUG: notion.databases attributes: {dir(notion.databases)}")
            except: pass
            break

    print(f"\n✨ 완료: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
