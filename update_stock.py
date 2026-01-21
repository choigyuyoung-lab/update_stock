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
    """어떤 형태의 값이든 안전하게 숫자로 변환"""
    try:
        if value is None or value in ["", "-", "N/A"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_korean_stock_info(ticker):
    """국내 주식: 네이버 통합 API (지표 탐색 강화 버전)"""
    url = f"https://api.stock.naver.com/stock/{ticker}/integration"
    headers = {'User-Agent': 'Mozilla/5.0'}
    info = {"price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        # 1. 가격 및 52주 정보 (여러 경로 탐색)
        total = data.get('total', {})
        info["price"] = safe_float(total.get('currentPrice') or data.get('closePrice'))
        info["high52w"] = safe_float(total.get('high52wPrice') or data.get('high52WeekPrice'))
        info["low52w"] = safe_float(total.get('low52wPrice') or data.get('low52WeekPrice'))
        
        # 2. 재무 지표 (stockFina 리스트 또는 total에서 탐색)
        fina_list = data.get('stockFina', [])
        fina = fina_list[0] if isinstance(fina_list, list) and len(fina_list) > 0 else {}
        
        # PER, PBR, EPS를 찾을 수 있는 모든 곳을 뒤집니다.
        info["per"] = safe_float(fina.get('per') or total.get('per'))
        info["pbr"] = safe_float(fina.get('pbr') or total.get('pbr'))
        info["eps"] = safe_float(fina.get('eps') or total.get('eps'))
        
        return info
    except:
        return None

def get_overseas_stock_info(ticker):
    """해외 주식: 야후 파이낸스 API"""
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
    print(f"🚀 최종 하이브리드 업데이트 시작 - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
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

                    # 시장에 따른 분기 처리
                    if market in ["KOSPI", "KOSDAQ"]:
                        stock = get_korean_stock_info(ticker)
                    else:
                        stock = get_overseas_stock_info(ticker)

                    if stock and stock["price"] is not None:
                        upd = {
                            "현재가": {"number": stock["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        # 지표 업데이트 (값이 있을 때만)
                        fields = {"PER": "per", "PBR": "pbr", "EPS": "eps", "52주 최고가": "high52w", "52주 최저가": "low52w"}
                        for n_key, d_key in fields.items():
                            val = safe_float(stock[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        if success % 10 == 0: print(f"✅ {success}개 완료 (최근: {ticker})")
                    else:
                        fail += 1
                    
                    time.sleep(0.4)
                except Exception as e:
                    print(f"❌ {ticker} 처리 오류: {e}")
                    fail += 1
                    continue
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"🚨 노션 쿼리 오류: {e}"); break

    print(f"✨ 최종 결과: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
