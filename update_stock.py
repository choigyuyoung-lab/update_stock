import os
import yfinance as yf
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def safe_float(value):
    """지저분한 값을 안전하게 숫자로 변환"""
    try:
        if value is None or value in ["", "-", "N/A"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_stock_info_yahoo(ticker, market):
    """야후 파이낸스를 이용해 전 세계 종목 데이터 통합 추출"""
    # 1. 야후 파이낸스용 티커 변환
    # 네이버용 접미사(.K, .O 등)가 있다면 먼저 제거
    clean_ticker = ticker.split('.')[0]
    
    if market == "KOSPI":
        symbol = f"{clean_ticker}.KS"
    elif market == "KOSDAQ":
        symbol = f"{clean_ticker}.KQ"
    else:
        # 해외 주식(NYSE, NASDAQ 등)은 순수 티커만 사용
        symbol = clean_ticker

    info = {"price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None}
    
    try:
        stock = yf.Ticker(symbol)
        d = stock.info # 야후 파이낸스 데이터 뭉치 가져오기
        
        # 2. 데이터 매핑 (야후 표준 필드명 사용)
        info["price"] = d.get("currentPrice") or d.get("regularMarketPrice")
        info["per"] = d.get("trailingPE")
        info["pbr"] = d.get("priceToBook")
        info["eps"] = d.get("trailingEps")
        info["high52w"] = d.get("fiftyTwoWeekHigh")
        info["low52w"] = d.get("fiftyTwoWeekLow")
        
        return info
    except Exception as e:
        print(f"⚠️ {symbol} 야후 데이터 추출 실패: {e}")
        return None

def main():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 야후 파이낸스 통합 업데이트 시작 - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more, next_cursor, success, fail = True, None, 0, 0

    while has_more:
        try:
            response = notion.databases.query(
                **{
                    "database_id": DATABASE_ID,
                    "start_cursor": next_cursor
                }
            )
            pages = response.get("results", [])
            
            for page in pages:
                ticker = ""
                try:
                    props = page["properties"]
                    # 시장 및 티커 정보 추출
                    market_obj = props.get("Market", {}).get("select")
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    if not market or not ticker: continue

                    # 야후 파이낸스에서 정보 가져오기
                    stock = get_stock_info_yahoo(ticker, market)

                    if stock and stock["price"] is not None:
                        # 노션 업데이트용 딕셔너리 구성
                        upd = {
                            "현재가": {"number": stock["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        # 나머지 지표들 (값이 있을 때만 추가)
                        fields = {
                            "PER": "per", 
                            "PBR": "pbr", 
                            "EPS": "eps", 
                            "52주 최고가": "high52w", 
                            "52주 최저가": "low52w"
                        }
                        
                        for n_key, d_key in fields.items():
                            val = safe_float(stock[d_key])
                            if val is not None:
                                upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        if success % 10 == 0:
                            print(f"✅ {success}개 완료 (최근: {ticker})")
                    else:
                        fail += 1
                    
                    time.sleep(0.5) # 야후 파이낸스 속도 제한 준수
                except Exception as e:
                    print(f"❌ {ticker} 처리 중 오류: {e}")
                    fail += 1
                    continue
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"🚨 노션 쿼리 오류: {e}")
            break

    print(f"✨ 최종 결과: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
