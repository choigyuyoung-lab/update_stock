import os, time, math, logging
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 로그 설정 (GitHub Actions 로그에서 상세 확인 가능)
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
    total_scanned = 0
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
            logger.error(f"❌ 노션 연결 실패: {e}")
            break

        pages = res.get("results", [])
        total_scanned += len(pages)
        
        for page in pages:
            props = page["properties"]
            ticker = ""
            
            # 1. 티커 추출
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        break
            
            # 2. 판별 로직 (한국 주식 제외)
            is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
            
            if not ticker: continue
            if is_kr:
                # logger.info(f"   ⏩ [Skip] {ticker} (한국 주식으로 판별됨)") # 너무 많이 찍히면 주석 처리 가능
                continue
            
            try:
                upd = {}
                stock = yf.Ticker(ticker)
                
                # 🌟 fast_info로 현재가/전일종가 한 번에 가져오기
                f_info = stock.fast_info
                curr = f_info.get('last_price')
                prev = f_info.get('previous_close')
                
                if is_valid(curr): upd["현재가"] = {"number": curr}
                if is_valid(prev): upd["전일 종가"] = {"number": prev}

                if upd:
                    if "마지막 업데이트" in props:
                        upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
                    
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    logger.info(f"   ✅ [Global: {ticker}] 완료 (현재가: {round(curr, 2)})")
                
            except Exception as e:
                # fast_info 에러 시 history로 최후의 시도
                try:
                    hist = stock.history(period="1d")
                    if not hist.empty:
                        last_c = hist['Close'].iloc[-1]
                        notion.pages.update(page_id=page["id"], properties={"현재가": {"number": last_c}})
                        processed_count += 1
                        logger.info(f"   ✅ [Global: {ticker}] 완료 (History 우회)")
                except:
                    logger.warning(f"   ❌ [{ticker}] 업데이트 실패: {e}")
            
            time.sleep(0.3)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")
        time.sleep(2)

    logger.info(f"\n✨ 종료. (스캔: {total_scanned}건 / 업데이트: {processed_count}건)")
    
if __name__ == "__main__":
    main()
