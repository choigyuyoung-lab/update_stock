import sys
import subprocess
import os
import time
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------
# 🧹 [시스템 초기화] 라이브러리 강제 재설치 (에러 방지용)
# ---------------------------------------------------------
print("🚑 [시스템 초기화] 라이브러리 정리 및 재설치 중...")
try:
    # 1. 기존 라이브러리 강제 제거 (충돌 방지)
    subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "notion-client", "notion"])
    # 2. 최신 정품 라이브러리 강제 설치
    subprocess.check_call([sys.executable, "-m", "pip", "install", "notion-client==2.2.1", "yfinance"])
    print("✅ 라이브러리 준비 완료!")
except Exception as e:
    print(f"⚠️ 설치 중 경고 (진행에는 문제 없음): {e}")

# 라이브러리 불러오기
import notion_client
from notion_client import Client
import yfinance as yf

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 안전장치 (20분)
MAX_RUNTIME_SEC = 1200 

def fetch_yahoo_price(symbol):
    """가격 정보 가져오기"""
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
    """종목코드 및 시장 자동 감지"""
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

    # 2. 힌트 없을 때 (자동 감지)
    else:
        # 한국 주식 (숫자 6자리)
        if ticker.isdigit() and len(ticker) == 6:
            data = fetch_yahoo_price(f"{ticker}.KS")
            if data: return data, "KOSPI(Auto)"
            
            data = fetch_yahoo_price(f"{ticker}.KQ")
            if data: return data, "KOSDAQ(Auto)"

        # 미국 주식 및 기타
        else:
            clean_ticker = ticker.replace(".KS", "").replace(".KQ", "").replace(".K", "")
            data = fetch_yahoo_price(clean_ticker)
            if data: return data, "US(Auto)"
            
            # 재시도 (한국 ETF 등)
            data = fetch_yahoo_price(f"{clean_ticker}.KS")
            if data: return data, "KOSPI(Auto-Retry)"
            
            data = fetch_yahoo_price(f"{clean_ticker}.KQ")
            if data: return data, "KOSDAQ(Auto-Retry)"

    return None, "Unknown"

def extract_value(prop):
    """노션 속성값 안전하게 추출하기"""
    if not prop: return ""
    p_type = prop.get("type")
    
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        if array: return extract_value(array[0])
        return ""
        
    if p_type == "select":
        return prop.get("select", {}).get("name", "")
        
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        if text_list:
            return text_list[0].get("plain_text", "")
        return ""
        
    if p_type == "formula":
        f = prop.get("formula", {})
        f_type = f.get("type")
        if f_type == "number":
            return str(f.get("number", ""))
        elif f_type == "string":
            return f.get("string", "")
            
    return ""

def main():
    start_time = time.time()
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat() 
    
    print(f"🚀 [가격 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        # 시간 제한 체크
        if time.time() - start_time > MAX_RUNTIME_SEC:
            break

        try:
            # 노션 데이터 가져오기 (여기가 핵심!)
            response = notion.databases.query(
                database_id=DATABASE_ID, 
                start_cursor=next_cursor
            )
            pages = response.get("results", [])
            
            if not pages: break

            for page in pages:
                # 개별 종목 시간 체크
                if time.time() - start_time > MAX_RUNTIME_SEC:
                    has_more = False; break 

                try:
                    props = page["properties"]
                    # 속성값 추출
                    ticker = extract_value(props.get("티커"))
                    market = extract_value(props.get("Market"))
                    
                    if not ticker: continue
                    
                    # 가격 조회
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
                    
                    # 너무 빠르면 차단되니 잠시 대기
                    time.sleep(0.5) 
                        
                except Exception as e:
                    fail += 1
                    continue
            
            if not has_more: break
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 오류 발생: {e}")
            break

    print(f"\n✨ 완료: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
