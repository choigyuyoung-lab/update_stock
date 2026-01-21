import os
import requests
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
        if value is None or value in ["", "-", "N/A", "null"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_korean_stock_info(ticker):
    """국내 주식: 네이버 모바일 API (티커 원본 사용 + 보안 헤더 적용)"""
    
    # [요청 반영] 0을 채워주는 zfill 기능을 삭제했습니다.
    # 이제 노션에서 넘어온 ticker 값을 수정 없이 그대로 사용합니다.
    
    url = f"https://m.stock.naver.com/api/stock/{ticker}/basic"
    
    # 네이버 차단 방지용 헤더 (필수)
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
        'Referer': 'https://m.stock.naver.com/'
    }
    
    info = {"price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        info["price"] = safe_float(data.get('closePrice'))
        info["per"] = safe_float(data.get('per'))
        info["pbr"] = safe_float(data.get('pbr'))
        info["eps"] = safe_float(data.get('eps'))
        info["high52w"] = safe_float(data.get('high52wPrice'))
        info["low52w"] = safe_float(data.get('low52wPrice'))
        
        return info
    except Exception as e:
        # print(f"⚠️ 국내 종목({ticker}) 실패: {e}") 
        return None

def get_overseas_stock_info(ticker):
    """해외 주식: 야후 파이낸스"""
    symbol = ticker.split('.')[0]
    info = {"price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None}
    
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        info["price"] = d.get("currentPrice") or d.get("regularMarketPrice")
        info["per"] = d.get("trailingPE")
        info["pbr"] = d.get("priceToBook")
        info["eps"] = d.get("trailingEps")
        info["high52w"] = d.get("fiftyTwoWeekHigh")
        info["low52w"] = d.get("fiftyTwoWeekLow")
        return info
    except:
        return None

def main():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 업데이트 시작 (티커 원본 사용) - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more, next_cursor, success, fail = True, None, 0, 0

    while has_more:
        try:
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            
            for page in pages:
                ticker = ""
                try:
                    props = page["properties"]
                    market_obj = props.get("Market", {}).get("select")
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    if not market or not ticker: continue

                    # 시장 구분에 따른 분기
                    if market in ["KOSPI", "KOSDAQ"]:
                        stock = get_korean_stock_info(ticker)
                    else:
                        stock = get_overseas_stock_info(ticker)

                    if stock and stock["price"] is not None:
                        upd = {
                            "현재가": {"number": stock["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        fields = {"PER": "per", "PBR": "pbr", "EPS": "eps", "52주 최고가": "high52w", "52주 최저가": "low52w"}
                        for n_key, d_key in fields.items():
                            val = safe_float(stock[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        if success % 10 == 0: print(f"✅ {success}개 완료 (최근: {ticker})")
                    else:
                        fail += 1
                    
                    time.sleep(0.3) 
                except Exception as e:
                    print(f"❌ {ticker} 처리 중 오류: {e}")
                    fail += 1
                    continue
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"🚨 노션 쿼리 오류: {e}"); break

    print(f"✨ 최종 결과: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
