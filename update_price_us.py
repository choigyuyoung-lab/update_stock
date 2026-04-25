import os
import time
import math
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")

notion = Client(auth=NOTION_TOKEN)

# ---------------------------------------------------------
# 2. 유틸리티 함수
# ---------------------------------------------------------
def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

# ---------------------------------------------------------
# 3. 메인 실행 함수
# ---------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    print(f"⚡ [최적화] 해외 주식 가격 업데이트 시작 - {datetime.now(kst)}")
    
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
            
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        # 한국 주식 제외 판별 로직
                        is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
                        break
            
            if not ticker or is_kr: continue
            
            try:
                upd = {}
                stock = yf.Ticker(ticker)
                
                # 🌟 [최적화] info 대신 압도적으로 빠른 fast_info 사용
                f_info = stock.fast_info
                
                # 1. 현재가 추출 (last_price)
                current_price = f_info.get('last_price')
                if is_valid(current_price): 
                    upd["현재가"] = {"number": current_price}
                    
                # 2. 전일 종가 추출 (previous_close)
                prev_close = f_info.get('previous_close')
                if is_valid(prev_close):
                    upd["전일 종가"] = {"number": prev_close}

                if upd:
                    if "마지막 업데이트" in props:
                        upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
                        
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [Global: {ticker}] 완료 - 현재가: {round(current_price, 2)} / 전일종가: {round(prev_close, 2)}")
                
            except Exception as e:
                # fast_info 실패 시 최후의 수단으로 history 사용 (상장폐지 등 특수 상황 대비)
                try:
                    hist = stock.history(period="1d")
                    if not hist.empty:
                        last_c = hist['Close'].iloc[-1]
                        notion.pages.update(page_id=page["id"], properties={"현재가": {"number": last_c}})
                        print(f"   ⚠️ [{ticker}] history 우회 성공")
                except:
                    print(f"   ❌ [{ticker}] 최종 실패: {e}")
            
            time.sleep(0.3) # [최적화] fast_info는 가벼우므로 대기 시간을 소폭 줄임

        if not res.get("has_more"): 
            break
            
        next_cursor = res.get("next_cursor")
        print(f"--- {processed_count}건 완료. 다음 페이지 이동 전 2초 휴식 ---")
        time.sleep(2)

    print(f"\n✨ 종료. 총 {processed_count}건 업데이트 완료.")
    
if __name__ == "__main__":
    main()
