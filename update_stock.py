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

# [설정 변경] 5분(300초) -> 10분(600초)으로 연장
# 평균 6~7분이 걸리므로 넉넉하게 잡음
MAX_RUNTIME_SEC = 600 

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_stock_data_from_yahoo(ticker, market):
    symbol = str(ticker).strip().upper()
    
    # 한국 주식 티커 변환
    if market == "KOSPI":
        if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
    elif market == "KOSDAQ":
        if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
    
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        price = d.get("currentPrice") or d.get("regularMarketPrice")
        
        if price is None: return None

        return {
            "price": price,
            "per": d.get("trailingPE"),
            "pbr": d.get("priceToBook"),
            "eps": d.get("trailingEps"),
            "high52w": d.get("fiftyTwoWeekHigh"),
            "low52w": d.get("fiftyTwoWeekLow")
        }
    except:
        return None

def main():
    # [안전장치] 시작 시간 기록
    start_time = time.time()
    
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 [안전 모드] 업데이트 시작 (제한시간 10분) - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        # [안전장치] 전체 시간 체크
        elapsed_time = time.time() - start_time
        if elapsed_time > MAX_RUNTIME_SEC:
            print(f"\n⏰ [Time Over] 10분이 경과하여 강제 종료합니다. (성공: {success}건)")
            break

        try:
            print(f"\n📡 노션 페이지 조회 중... (Cursor: {next_cursor})")
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            
            if not pages and success == 0 and fail == 0:
                print("🚨 가져온 페이지가 0개입니다.")
                break

            for page in pages:
                # [안전장치] 개별 종목 처리 전 시간 체크
                if time.time() - start_time > MAX_RUNTIME_SEC:
                    print(f"⏰ [Time Over] 제한 시간이 되어 작업을 중단합니다.")
                    has_more = False 
                    break 

                try:
                    props = page["properties"]
                    
                    market_obj = props.get("Market", {}).get("select")
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    if not market or not ticker: continue
                    
                    data = get_stock_data_from_yahoo(ticker, market)

                    if data is not None:
                        upd = {
                            "현재가": {"number": data["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        fields = {
                            "PER": "per",
                            "PBR": "pbr",
                            "EPS": "eps",
                            "52주 최고가": "high52w",
                            "52주 최저가": "low52w"
                        }
                        
                        for n_key, d_key in fields.items():
                            val = safe_float(data[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        print(f"   => ✅ [{market}] {ticker} : {data['price']:,.0f}")
                    else:
                        print(f"   => ❌ [{market}] {ticker} : 야후 검색 실패")
                        fail += 1
                    
                    time.sleep(0.5) 
                        
                except Exception as e:
                    print(f"   => 🚨 에러: {e}")
                    fail += 1
                    continue
            
            if not has_more: break
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 노션 연결 오류: {e}")
            break

    print("\n---------------------------------------------------")
    print(f"✨ 최종 결과: 성공 {success}건 / 실패 {fail}건")
    print(f"⏱️ 총 소요 시간: {time.time() - start_time:.1f}초")

if __name__ == "__main__":
    main()
