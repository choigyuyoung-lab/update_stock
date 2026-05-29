import logging
import math
import time

import yfinance as yf
from datetime import timedelta, timezone

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    kst_isoformat,
    paginate_database,
    safe_page_update,
)

NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = get_env_var("DATABASE_ID")
notion = build_notion_client(NOTION_TOKEN)

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
    
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker"]).upper()
        if not ticker:
            continue

        is_kr = (ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith((".T", ".TA", ".TW"))
        if is_kr:
            continue

        try:
            upd = {}
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            current_price = None

            if not hist.empty:
                current_price = hist['Close'].iloc[-1]
                if is_valid(current_price):
                    upd["현재가"] = {"number": current_price}

                if "전일 종가" in props:
                    info = stock.info
                    pc = info.get('previousClose')
                    if is_valid(pc):
                        upd["전일 종가"] = {"number": pc}

            if upd:
                if "마지막 업데이트" in props:
                    upd["마지막 업데이트"] = {"date": {"start": kst_isoformat()}}

                if safe_page_update(notion, page["id"], upd):
                    processed_count += 1
                    if current_price is not None:
                        logger.info(f"   ✅ [Global: {ticker}] 완료 (가격: {round(current_price, 2)})")
                    else:
                        logger.info(f"   ✅ [Global: {ticker}] 완료")

        except Exception as e:
            logger.warning(f"   ❌ [{ticker}] 실패: {e}")

        time.sleep(0.4)

    logger.info(f"✨ 종료. 총 {processed_count}건 업데이트 완료.")
    
if __name__ == "__main__":
    main()
