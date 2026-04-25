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
    logger.info(f"🧪 [진단 모드] 해외 주식 가격 업데이트 시작 - {datetime.now(kst)}")
    
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
            
            # 1. 티커 속성 찾기 진단
            ticker_found = False
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        ticker_found = True
                        break
            
            if not ticker_found:
                logger.info(f"   ⏩ SKIPPED: 티커 속성이 비어있음 (Page ID: {page['id']})")
                continue

            # 2. 판별 로직 진단
            # 한국 주식 조건: (.KS/.KQ로 끝남) OR (6자 이상 숫자 시작) / 예외: .T, .TA, .TW
            is_kr_pattern = ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())
            is_exception = ticker.endswith(('.T', '.TA', '.TW'))
            is_kr = is_kr_pattern and not is_exception
            
            if is_kr:
                logger.info(f"   ⏩ SKIPPED: {ticker} (한국 주식으로 판별되어 제외)")
                continue
            
            # 3. 업데이트 시도
            try:
                stock = yf.Ticker(ticker)
                f_info = stock.fast_info
                curr = f_info.get('last_price')
                
                if is_valid(curr):
                    notion.pages.update(page_id=page["id"], properties={"현재가": {"number": curr}})
                    processed_count += 1
                    logger.info(f"   ✅ UPDATE SUCCESS: {ticker} (현재가: {curr})")
                else:
                    logger.info(f"   ⚠️ DATA MISSING: {ticker} (야후 파이낸스에 가격 데이터 없음)")
                
            except Exception as e:
                logger.warning(f"   ❌ ERROR: {ticker} 업데이트 중 오류 발생 ({e})")
            
            time.sleep(0.3)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")
        time.sleep(2)

    logger.info(f"\n✨ 진단 종료. (스캔: {total_scanned}건 / 업데이트 성공: {processed_count}건)")
    
if __name__ == "__main__":
    main()
