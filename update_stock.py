import os
import requests
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. Notion 및 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def safe_float(value):
    """문자열이나 혼합 타입을 안전하게 숫자로 변환"""
    try:
        if value is None: return None
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None

def get_stock_info(ticker, market):
    """국내/해외 주식의 데이터를 구조에 상관없이 안전하게 추출"""
    info = {"price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None}
    
    # 시장별 심볼 및 API URL 설정
    if market in ["KOSPI", "KOSDAQ"]:
        url = f"https://api.stock.naver.com/stock/{ticker}/integration"
        symbol = ticker
    else:
        symbol = ticker if "." in ticker else (f"{ticker}.K" if market == "NYSE" else f"{ticker}.O")
        url = f"https://api.stock.naver.com/stock/{symbol}/basic"

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        if market in ["KOSPI", "KOSDAQ"]:
            # 국내 주식: total 키가 없으면 root에서 시도
            total = data.get('total', {})
            info["price"] = safe_float(total.get('currentPrice') or data.get('closePrice'))
            info["high52w"] = safe_float(total.get('high52wPrice'))
            info["low52w"] = safe_float(total.get('low52wPrice'))
            
            # 재무 지표 (fina 항목 또는 total 항목에서 추출)
            fina = data.get('stockFina', [{}])[0] if data.get('stockFina') else {}
            info["per"] = safe_float(fina.get('per') or total.get('per'))
            info["pbr"] = safe_float(fina.get('pbr') or total.get('pbr'))
            info["eps"] = safe_float(fina.get('eps') or total.get('eps'))
        else:
            # 해외 주식: 구조가 다를 수 있으므로 get()으로 안전하게 접근
            info["price"] = safe_float(data.get('closePrice'))
            info["per"] = safe_float(data.get('per'))
            info["pbr"] = safe_float(data.get('pbr'))
            info["eps"] = safe_float(data.get('eps'))
            info["high52w"] = safe_float(data.get('high52wPrice'))
            info["low52w"] = safe_float(data.get('low52wPrice'))
            
        return info
    except Exception as e:
        print(f"⚠️ {symbol} 데이터 호출 중 에러 발생: {e}")
        return None

def main():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 업데이트 시작 (KST: {now.strftime('%Y-%m-%d %H:%M:%S')})")
    
    has_more, next_cursor, total_count = True, None, 0

    while has_more:
        try:
            response = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
            pages = response.get("results", [])
            
            for page in pages:
                props = page["properties"]
                market = props.get("Market", {}).get("select", {}).get("name", "")
                ticker_data = props.get("티커", {}).get("title", [])
                ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                
                if market and ticker:
                    stock = get_stock_info(ticker, market)
                    if stock and stock["price"]:
                        # 값이 있는 항목만 업데이트 딕셔너리에 추가
                        upd = {"현재가": {"number": stock["price"]}, "마지막 업데이트": {"date": {"start": now_iso}}}
                        if stock["per"]: upd["PER"] = {"number": stock["per"]}
                        if stock["pbr"]: upd["PBR"] = {"number": stock["pbr"]}
                        if stock["eps"]: upd["EPS"] = {"number": stock["eps"]}
                        if stock["high52w"]: upd["52주 최고가"] = {"number": stock["high52w"]}
                        if stock["low52w"]: upd["52주 최저가"] = {"number": stock["low52w"]}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        total_count += 1
                        if total_count % 10 == 0: print(f"진행 중... {total_count}개 완료")
                    
                    time.sleep(0.4) 
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"❌ 중단됨: {e}"); break

    print(f"✨ 총 {total_count}개 종목 업데이트 완료!")

if __name__ == "__main__":
    main()
