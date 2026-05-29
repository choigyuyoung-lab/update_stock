import logging
import math
import time

import yfinance as yf
from datetime import datetime, timedelta, timezone

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def get_us_fin_optimized(ticker):
    """🌟 [최적화] fast_info와 info를 전략적으로 사용하여 속도 향상"""
    res = {
        "PER": None, "추정PER": None, "EPS": None, "추정EPS": None, 
        "PBR": None, "BPS": None, "배당수익률": None,
        "52주 최고가": None, "52주 최저가": None, "목표주가": None, "의견": None
    }
    try:
        stock = yf.Ticker(ticker)
        
        # 1. 속도가 빠른 fast_info에서 52주 가격 정보 먼저 추출
        f_info = stock.fast_info
        res["52주 최고가"] = f_info.get('year_high')
        res["52주 최저가"] = f_info.get('year_low')

        # 2. 나머지 재무 정보는 info에서 추출 (해외 주식의 경우 여기서 시간이 걸릴 수 있음)
        info = stock.info
        if info:
            res.update({
                "PER": info.get("trailingPE"),
                "추정PER": info.get("forwardPE"),
                "EPS": info.get("trailingEps"),
                "추정EPS": info.get("forwardEps"),
                "PBR": info.get("priceToBook"),
                "BPS": info.get("bookValue"),
                "목표주가": info.get('targetMeanPrice'),
                "52주 최고가": info.get("fiftyTwoWeekHigh"),
                "52주 최저가": info.get("fiftyTwoWeekLow")
            })
            if info.get("dividendYield"):
                res["배당수익률"] = info.get("dividendYield") * 100
                
            rec_key = str(info.get('recommendationKey', '')).lower()
            opinion_map = {"strong_buy": "적극매수", "buy": "매수", "hold": "중립", "underperform": "매도", "sell": "적극매도"}
            res['의견'] = opinion_map.get(rec_key)

        return res
    except Exception as e:
        logger.warning(f"   ⚠️ [{ticker}] 데이터 수집 실패: {e}")
        return res

def main():
    kst = timezone(timedelta(hours=9))
    logger.info("🌍 [해외 주식 재무 업데이트] 시작")
    
    next_cursor = None
    success_cnt = 0
    number_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률", "52주 최고가", "52주 최저가", "목표주가"]

    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker"]).upper()
        if not ticker:
            continue

        is_kr = (ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith((".T", ".TA", ".TW"))
        if is_kr:
            continue

        fin_data = get_us_fin_optimized(ticker)
        update_props = {
            key: {"number": fin_data[key]}
            for key in number_keys
            if is_valid(fin_data.get(key))
        }

        if fin_data.get("의견"):
            update_props["목표가 범위"] = {"select": {"name": fin_data["의견"]}}
        if "마지막 업데이트" in props:
            update_props["마지막 업데이트"] = {"date": {"start": kst_isoformat()}}

        if not update_props:
            logger.info(f"⚠️ [{ticker}] 업데이트할 유효 데이터 없음")
            continue

        if safe_page_update(notion, page["id"], update_props):
            logger.info(f"   ✅ [Global: {ticker}] 업데이트 완료")
            success_cnt += 1

        time.sleep(0.4)

    logger.info(f"✨ 종료. 총 {success_cnt}건 처리 완료.")

if __name__ == "__main__":
    main()
