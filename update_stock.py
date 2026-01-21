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

# [안전장치] 20분(1200초) 이상 돌면 자동 종료 (서버 멈춤 방지)
MAX_RUNTIME_SEC = 1200 

def safe_float(value):
    """지저분한 데이터를 숫자로 변환"""
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_stock_data_from_yahoo(ticker, market):
    """야후 파이낸스에서 데이터 조회 (오타 자동 보정 포함)"""
    symbol = str(ticker).strip().upper()
    
    # [핵심 기능] 티커/시장 불일치 자동 해결 로직
    if market == "KOSPI":
        # 코스피인데 .KS가 없으면 붙여줌
        if not symbol.endswith(".KS"): 
            symbol = f"{symbol}.KS"
    elif market == "KOSDAQ":
        # 코스닥인데 .KQ가 없으면 붙여줌
        if not symbol.endswith(".KQ"): 
            symbol = f"{symbol}.KQ"
    else:
        # 미국/해외 주식인데 실수로 한국 꼬리표(.KS, .K 등)를 붙였다면 제거
        symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
    
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        
        # 현재가 가져오기 (장중: currentPrice, 장마감: regularMarketPrice)
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
    start_time = time.time()
    
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 [24시간 모드] 주식 업데이트 시작 - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        # 전체 실행 시간 체크
        if time.time() - start_time > MAX_RUNTIME_SEC:
            print(f"\n⏰ 안전을 위해 10분이 경과하여 종료합니다. (성공: {success}건)")
            break

        try:
            # 노션 데이터 가져오기 (로그 간소화)
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            
            if not pages and success == 0 and fail == 0:
                print("🚨 노션에서 가져온 페이지가 없습니다.")
                break

            for page in pages:
                # 개별 종목 처리 전 시간 체크
                if time.time() - start_time > MAX_RUNTIME_SEC:
                    has_more = False 
                    break 

                try:
                    props = page["properties"]
                    
                    market_obj = props.get("Market", {}).get("select")
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    if not market or not ticker: continue
                    
                    # 데이터 조회 (오타 보정 적용됨)
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
                        print(f"   => ❌ [{market}] {ticker} : 검색 실패")
                        fail += 1
                    
                    # 서버 부하 방지 딜레이
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
