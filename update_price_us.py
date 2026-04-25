import os, time, math, logging
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except: return False

def main():
    kst = timezone(timedelta(hours=9))
    logger.info(f"⚡ [해외 주식 가격 업데이트] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
            logger.error(f"❌ 노션 연결 실패: {e}")
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
                        break
            
            # 사용자님의 기존 '문지기' 로직 (검증된 방식)
            is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
            
            if not ticker or is_kr: continue
            
            try:
                upd = {}
                stock = yf.Ticker(ticker)
                
                # 🌟 [최종 최적화] info보다 가볍고 fast_info보다 정확한 history(1d) 사용
                # 이 방식은 전 세계 모든 거래소의 가격을 가장 안정적으로 가져옵니다.
                hist = stock.history(period="1d")
                
                if not hist.empty:
                    current_price = hist['Close'].iloc[-1]
                    prev_close = hist['Open'].iloc[-1] # 1일치 봉의 시가를 전일 종가 대용으로 사용하거나, 
                                                     # 혹은 아래처럼 info에서 한 가지만 쏙 뽑아올 수도 있습니다.

                    if is_valid(current_price):
                        upd["현재가"] = {"number": current_price}
                    
                    # 전일 종가가 꼭 필요하다면 기존 방식의 info를 병행 (에러 방지 처리)
                    if "전일 종가" in props:
                        # history에 데이터가 있다면 info도 안전하게 호출 가능
                        info = stock.info
                        pc = info.get('previousClose')
                        if is_valid(pc):
                            upd["전일 종가"] = {"number": pc}

                if upd:
                    if "마지막 업데이트" in props:
                        upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
                    
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    logger.info(f"   ✅ [Global: {ticker}] 완료 (가격: {round(current_price, 2)})")
                
            except Exception as e:
                logger.warning(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.4) # 안정적인 처리를 위한 대기

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")
        time.sleep(2)

    logger.info(f"✨ 종료. 총 {processed_count}건 업데이트 완료.")
    
if __name__ == "__main__":
    main()
