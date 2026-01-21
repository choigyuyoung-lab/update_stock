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
        if value is None or value == "" or value == "-": return None
        # 숫자인 경우 그대로 반환, 문자열인 경우 쉼표 제거 후 변환
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None

def get_stock_info(ticker, market):
    """국내/해외 주식의 데이터를 모든 경로에서 탐색"""
    info = {"price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None}
    
    if market in ["KOSPI", "KOSDAQ"]:
        url = f"https://api.stock.naver.com/stock/{ticker}/integration"
    else:
        symbol = ticker if "." in ticker else (f"{ticker}.K" if market == "NYSE" else f"{ticker}.O")
        url = f"https://api.stock.naver.com/stock/{symbol}/basic"

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        if market in ["KOSPI", "KOSDAQ"]:
            total = data.get('total', {})
            # 가격 및 52주 정보
            info["price"] = safe_float(total.get('currentPrice') or data.get('closePrice'))
            info["high52w"] = safe_float(total.get('high52wPrice') or data.get('high52WeekPrice'))
            info["low52w"] = safe_float(total.get('low52wPrice') or data.get('low52WeekPrice'))
            
            # 국내 재무 지표 탐색 (리스트 또는 딕셔너리 대응)
            fina_data = data.get('stockFina')
            fina = {}
            if isinstance(fina_data, list) and len(fina_data) > 0:
                fina = fina_data[0]
            elif isinstance(fina_data, dict):
                fina = fina_data
            
            info["per"] = safe_float(fina.get('per') or total.get('per'))
            info["pbr"] = safe_float(fina.get('pbr') or total.get('pbr'))
            info["eps"] = safe_float(fina.get('eps') or total.get('eps'))
        else:
            # 해외 주식
            info["price"] = safe_float(data.get('closePrice'))
            info["per"] = safe_float(data.get('per'))
            info["pbr"] = safe_float(data.get('pbr'))
            info["eps"] = safe_float(data.get('eps'))
            info["high52w"] = safe_float(data.get('high52wPrice') or data.get('high52WeekPrice'))
            info["low52w"] = safe_float(data.get('low52wPrice') or data.get('low52WeekPrice'))
            
        return info
    except Exception as e:
        print(f"⚠️ {ticker} 분석 중 오류: {e}")
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
                    if stock and stock["price"] is not None:
                        # 업데이트할 속성 딕셔너리 생성
                        upd = {
                            "현재가": {"number": stock["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        # 0이나 None이 아닐 때만 업데이트 목록에 추가 (is not None 체크가 핵심)
                        if stock["per"] is not None: upd["PER"] = {"number": stock["per"]}
                        if stock["pbr"] is not None: upd["PBR"] = {"number": stock["pbr"]}
                        if stock["eps"] is not None: upd["EPS"] = {"number": stock["eps"]}
                        if stock["high52w"] is not None: upd["52주 최고가"] = {"number": stock["high52w"]}
                        if stock["low52w"] is not None: upd["52주 최저가"] = {"number": stock["low52w"]}

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
