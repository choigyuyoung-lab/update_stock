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

# [안전장치] 20분(1200초) 설정 (종목이 늘어나도 넉넉함)
MAX_RUNTIME_SEC = 1200 

def safe_float(value):
    """지저분한 데이터를 숫자로 변환"""
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def extract_market_name(props):
    """
    [핵심 추가] Market 속성이 '선택'이든 '롤업'이든 상관없이 값을 추출하는 함수
    """
    market_prop = props.get("Market", {})
    prop_type = market_prop.get("type")
    
    market_name = ""

    # 1. 기존 방식 (선택/Select 인 경우)
    if prop_type == "select":
        market_name = market_prop.get("select", {}).get("name", "")
        
    # 2. 새로운 방식 (롤업/Rollup 인 경우) -> 이 부분이 추가되었습니다!
    elif prop_type == "rollup":
        # 롤업은 배열(Array) 형태입니다. 첫 번째 값을 가져옵니다.
        rollup_array = market_prop.get("rollup", {}).get("array", [])
        if rollup_array:
            # 롤업된 원본 속성이 'Select'라고 가정
            first_item = rollup_array[0]
            if first_item.get("type") == "select":
                market_name = first_item.get("select", {}).get("name", "")
            # 롤업된 원본이 '수식'이나 '텍스트'일 수도 있으므로 대비
            elif first_item.get("type") == "formula":
                market_name = first_item.get("formula", {}).get("string", "")
            elif first_item.get("type") == "rich_text":
                text_list = first_item.get("rich_text", [])
                if text_list:
                    market_name = text_list[0].get("plain_text", "")

    return market_name

def get_stock_data_from_yahoo(ticker, market):
    """야후 파이낸스에서 데이터 조회 (오타 자동 보정 포함)"""
    symbol = str(ticker).strip().upper()
    
    # [오타 보정]
    if market == "KOSPI":
        if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
    elif market == "KOSDAQ":
        if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
    else:
        # 미국/해외 주식은 꼬리표 제거
        symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
    
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
    start_time = time.time()
    
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 [롤업 호환 모드] 업데이트 시작 - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        if time.time() - start_time > MAX_RUNTIME_SEC:
            print(f"\n⏰ 20분이 경과하여 안전 종료합니다. (성공: {success}건)")
            break

        try:
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            
            if not pages and success == 0 and fail == 0:
                print("🚨 가져온 페이지가 0개입니다.")
                break

            for page in pages:
                if time.time() - start_time > MAX_RUNTIME_SEC:
                    has_more = False 
                    break 

                try:
                    props = page["properties"]
                    
                    # [수정됨] 이제 롤업이든 선택이든 다 읽을 수 있습니다.
                    market = extract_market_name(props)
                    
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    if not market or not ticker: continue
                    
                    # 데이터 조회
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
                    
                    time.sleep(0.5) 
                        
                except Exception as e:
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
