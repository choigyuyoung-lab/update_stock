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
    """노션 API 전송 전 숫자 유효성 검사 (NaN, Inf 제외)"""
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
    print(f"⚡ [미국 주식 가격 업데이트] 실행 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # 한 번에 100개씩 호출
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
            
            if not ticker or is_kr: continue # 한국 주식이면 건너뜀 (미국 주식 전용 문지기)
            
            try:
                upd = {}
                
                # --- 야후 파이낸스에서 데이터 추출 ---
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # 1. 현재가 추출
                current_price = info.get('currentPrice') or info.get('regularMarketPrice')
                if is_valid(current_price): 
                    upd["현재가"] = {"number": current_price}
                    
                # 2. 전일 종가 추출 (추가됨)
                prev_close = info.get('previousClose')
                if is_valid(prev_close):
                    upd["전일 종가"] = {"number": prev_close}

                # --- 공통 업데이트 ---
                if upd:
                    # 실시간 업데이트 시간 기록
                    if "마지막 업데이트" in props:
                        current_now_iso = datetime.now(kst).isoformat()
                        upd["마지막 업데이트"] = {"date": {"start": current_now_iso}}
                        
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [US: {ticker}] 완료 - 현재가: {current_price} / 전일종가: {prev_close}")
                
            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.6) # API 제한 고려 약간의 대기

        if not res.get("has_more"): 
            break
            
        next_cursor = res.get("next_cursor")
        print(f"--- {processed_count}건 완료. 다음 페이지(100건)로 이동 전 3초 휴식 ---")
        time.sleep(3) # 페이지 전환 시 3초 휴식

    print(f"\n✨ 종료. 총 {processed_count}건 업데이트 완료.")
    
if __name__ == "__main__":
    main()
