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

# 안전장치: 30분 간격 실행이므로 20분이면 충분
MAX_RUNTIME_SEC = 1200 

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def extract_value_from_property(prop):
    """노션 속성값 추출 (롤업/선택/텍스트 호환)"""
    if not prop: return ""
    p_type = prop.get("type")
    
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        if not array: return ""
        return extract_value_from_property(array[0])

    if p_type == "select":
        return prop.get("select", {}).get("name", "")
    
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        if text_list:
            return text_list[0].get("plain_text", "")
        return ""

    if p_type == "formula":
        f_type = prop.get("formula", {}).get("type")
        if f_type == "string":
            return prop.get("formula", {}).get("string", "")
        elif f_type == "number":
            return str(prop.get("formula", {}).get("number", ""))

    return ""

def fetch_yahoo_price(symbol):
    """
    [변경] 재무정보(PER/EPS)는 빼고, 오직 '가격' 관련 정보만 가져옵니다.
    """
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        price = d.get("currentPrice") or d.get("regularMarketPrice")
        
        if price is None: return None

        return {
            "price": price,
            "high52w": d.get("fiftyTwoWeekHigh"), # 52주 신고가는 가격 정보라 유지
            "low52w": d.get("fiftyTwoWeekLow")    # 52주 신저가도 유지
        }
    except:
        return None

def get_smart_stock_data(ticker, market_hint):
    """한/미 주식 자동 감지 및 가격 조회"""
    ticker = str(ticker).strip().upper()
    
    # 1. Market 힌트가 있는 경우
    if market_hint:
        symbol = ticker
        if "KOSPI" in market_hint.upper(): 
            if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
        elif "KOSDAQ" in market_hint.upper(): 
            if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
        else:
            symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
        
        return fetch_yahoo_price(symbol), market_hint

    # 2. Market 힌트가 없는 경우 (자동 감지)
    else:
        # 한국 주식 (숫자 6자리)
        if ticker.isdigit() and len(ticker) == 6:
            data = fetch_yahoo_price(f"{ticker}.KS")
            if data: return data, "KOSPI(Auto)"
            
            data = fetch_yahoo_price(f"{ticker}.KQ")
            if data: return data, "KOSDAQ(Auto)"

        # 미국 주식 (알파벳 등)
        else:
            clean_ticker = ticker.replace(".KS", "").replace(".KQ", "").replace(".K", "")
            data = fetch_yahoo_price(clean_ticker)
            if data: return data, "US(Auto)"
            
            # 한국 ETF 예외처리 (알파벳 섞인 것)
            data = fetch_yahoo_price(f"{clean_ticker}.KS")
            if data: return data, "KOSPI(Auto-Retry)"
            
            data = fetch_yahoo_price(f"{clean_ticker}.KQ")
            if data: return data, "KOSDAQ(Auto-Retry)"

    return None, "Unknown"

def main():
    start_time = time.time()
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat() 
    print(f"🚀 [가격 전용 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        if time.time() - start_time > MAX_RUNTIME_SEC:
            break

        try:
            response = notion.databases.query(
                database_id=DATABASE_ID, 
                start_cursor=next_cursor
            )
            pages = response.get("results", [])
            
            if not pages: break

            for page in pages:
                if time.time() - start_time > MAX_RUNTIME_SEC:
                    has_more = False; break 

                try:
                    props = page["properties"]
                    market = extract_value_from_property(props.get("Market"))
                    ticker = extract_value_from_property(props.get("티커"))
                    
                    if not ticker: continue
                    
                    # 스마트 가격 조회
                    data, detected_market = get_smart_stock_data(ticker, market)

                    if data is not None:
                        # [변경] 업데이트할 항목이 줄어들었습니다.
                        upd = {
                            "현재가": {"number": data["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        # 52주 고가/저가는 가격 변동과 연관되므로 유지
                        if data["high52w"]: upd["52주 최고가"] = {"number": data["high52w"]}
                        if data["low52w"]: upd["52주 최저가"] = {"number": data["low52w"]}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        print(f"   => ✅ [{detected_market}] {ticker} : {data['price']:,.0f}")
                    else:
                        fail += 1
                    
                    time.sleep(0.5) 
                        
                except:
                    fail += 1
                    continue
            
            if not has_more: break
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 노션 연결 오류: {e}")
            break

    print(f"\n✨ 완료: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
