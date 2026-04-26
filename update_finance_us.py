import os, time, math, logging
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

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

    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
            logger.error(f"❌ 노션 쿼리 실패: {e}")
            break

        pages = res.get("results", [])
        for page in pages:
            props = page["properties"]
            ticker = ""
            
            # 티커 추출 및 한국 주식 판별 (최적화 버전)
            for name in ["티커", "Ticker"]:
                if name in props:
                    content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
                        break
            
            if not ticker or is_kr: continue

            fin_data = get_us_fin_optimized(ticker)
            upd = {key: {"number": fin_data[key] if is_valid(fin_data[key]) else None} for key in number_keys}
            
            # 의견(Select) 및 마지막 업데이트(Date) 추가
            if fin_data.get("의견"):
                upd["목표가 범위"] = {"select": {"name": fin_data["의견"]}}
            
            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
            
            try:
                notion.pages.update(page_id=page["id"], properties=upd)
                logger.info(f"   ✅ [Global: {ticker}] 업데이트 완료")
                success_cnt += 1
            except Exception as e:
                logger.error(f"   ❌ [{ticker}] 전송 실패: {e}")
            
            time.sleep(0.4) # API 속도 제한 준수

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")
        time.sleep(2)

    logger.info(f"✨ 종료. 총 {success_cnt}건 처리 완료.")

if __name__ == "__main__":
    main()
