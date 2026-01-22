import os, time, math, yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    """노션 JSON 에러 방지를 위한 수치 유효성 검사"""
    return val is not None and not (math.isnan(val) or math.isinf(val))

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"💰 [주가 업데이트] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    while True:
        # 100개 제한 해제 (페이지네이션)
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = ""
            for name in ["티커", "Ticker"]:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content: ticker = content[0].get("plain_text", "").strip().upper(); break
            
            if not ticker: continue
            
            try:
                # 한국 종목(6자리 숫자) 판별
                is_kr = len(ticker) == 6 and ticker[0].isdigit()
                symbol = ticker + (".KS" if is_kr else "")
                stock = yf.Ticker(symbol)
                d = stock.fast_info
                
                upd = {}
                if is_valid(d.get("last_price")): upd["현재가"] = {"number": d.get("last_price")}
                if is_valid(d.get("year_high")): upd["52주 최고가"] = {"number": d.get("year_high")}
                if is_valid(d.get("year_low")): upd["52주 최저가"] = {"number": d.get("year_low")}
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                print(f"   [{ticker}] 가격 업데이트 완료")
            except: pass
            time.sleep(0.3)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

if __name__ == "__main__":
    main()
