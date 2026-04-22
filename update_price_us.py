import os
import time
import math
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"🇺🇸 [미국 주식 업데이트] 실행 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
            print(f"❌ 노션 연결 실패: {e}")
            break

        pages = res.get("results", [])
        for page in pages:
            props = page["properties"]
            ticker = ""
            is_kr = False
            
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        is_kr = ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())
                        break
            
            if not ticker: continue
            
            # [핵심] 한국 주식이면 업데이트하지 않고 다음으로 넘어감
            if is_kr: continue
            
            try:
                upd = {}
                stock = yf.Ticker(ticker)
                info = stock.info
                last_price = info.get('currentPrice') or info.get('regularMarketPrice')
                
                if is_valid(last_price): 
                    upd["현재가"] = {"number": last_price}
                    upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [US: {ticker}] 업데이트 완료 - 현재가: {last_price}")
                
            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.6)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")
        time.sleep(3)

    print(f"\n✨ 미국 주식 총 {processed_count}건 업데이트 완료.")
    
if __name__ == "__main__":
    main()
