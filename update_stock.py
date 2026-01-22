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

# 안전장치: 20분
MAX_RUNTIME_SEC = 1200 

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def extract_value_from_property(prop):
    """노션 롤업/선택/텍스트 등 모든 속성에서 텍스트 추출"""
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

def fetch_yahoo_data(symbol):
    """실제 야후 파이낸스 접속 함수"""
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

def get_smart_stock_data(ticker, market_hint):
    """
    [업그레이드] 알파벳이 섞인 한국 ETF도 찾아내는 3단 콤보 로직
    """
    ticker = str(ticker).strip().upper()
    
    # 1. 사용자가 Market을 명확히 지정해둔 경우 (가장 우선)
    if market_hint:
        symbol = ticker
        if "KOSPI" in market_hint.upper(): 
            if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
        elif "KOSDAQ" in market_hint.upper(): 
            if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
        else:
            # 미국 등 해외는 꼬리표 제거
            symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
        
        # 지정된 시장에서 조회
        data = fetch_yahoo_data(symbol)
        return data, market_hint

    # 2. Market이 비어있는 경우 (자동 추리)
    else:
        # Case A: 숫자 6자리 -> 누가 봐도 한국 주식
        if ticker.isdigit() and len(ticker) == 6:
            data = fetch_yahoo_data(f"{ticker}.KS")
            if data: return data, "KOSPI(Auto)"
            
            data = fetch_yahoo_data(f"{ticker}.KQ")
            if data: return data, "KOSDAQ(Auto)"

        # Case B: 알파벳이 섞여있거나 길이가 다름 (미국 주식 OR 특수 한국 ETF)
        else:
            # 1단계: 미국 주식이라고 가정하고 검색 (원래 로직)
            clean_ticker = ticker.replace(".KS", "").replace(".KQ", "").replace(".K", "")
            data = fetch_yahoo_data(clean_ticker)
            if data: return data, "US(Auto)"
            
            # [추가된 로직] 2단계: 미국에 없으면 한국(.KS)에서 검색 시도
            data = fetch_yahoo_data(f"{clean_ticker}.KS")
            if data: return data, "KOSPI(Auto-Retry)"
            
            # [추가된 로직] 3단계: 코스닥(.KQ)에서도 검색 시도
            data = fetch_yahoo_data(f"{clean_ticker}.KQ")
            if data: return data, "KOSDAQ(Auto-Retry)"

    return None, "Unknown"

def main():
    start_time = time.time()
    
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 [집요한 검색 모드] 업데이트 시작 - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        if time.time() - start_time > MAX_RUNTIME_SEC:
            print(f"\n⏰ 20분 경과. 안전 종료.")
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
                    has_more = False; break 

                try:
                    props = page["properties"]
                    
                    # 롤업된 Market과 티커 추출
                    market = extract_value_from_property(props.get("Market"))
                    ticker = extract_value_from_property(props.get("티커"))
                    
                    if not ticker: continue
                    
                    # 스마트 조회 (3단 콤보 적용됨)
                    data, detected_market = get_smart_stock_data(ticker, market)

                    if data is not None:
                        upd = {
                            "현재가": {"number": data["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        fields = {"PER": "per", "PBR": "pbr", "EPS": "eps", "52주 최고가": "high52w", "52주 최저가": "low52w"}
                        for n_key, d_key in fields.items():
                            val = safe_float(data[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        print(f"   => ✅ [{detected_market}] {ticker} : {data['price']:,.0f}")
                    else:
                        print(f"   => ❌ [{market or 'Unknown'}] {ticker} : 검색 실패")
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
    print(f"✨ 결과: 성공 {success} / 실패 {fail}")
    print(f"⏱️ 총 소요 시간: {time.time() - start_time:.1f}초")

if __name__ == "__main__":
    main()
