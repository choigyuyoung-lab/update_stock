import logging
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    kst_isoformat,
    paginate_database,
    safe_page_update,
    get_http_session,
    is_kr_ticker,
    is_valid_num,
    safe_float,
)

NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = get_env_var("DATABASE_ID")
notion = build_notion_client(NOTION_TOKEN)

SESSION = get_http_session()

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def get_us_fin_optimized(ticker: str, max_retries: int = 3, base_delay: float = 2.0) -> dict:
    """
    Yahoo Finance에서 해외 주식 재무 데이터를 조회합니다.
    네트워크 에러나 타임아웃이 발생하면 2~3초 대기 후 최대 3번까지 재시도합니다.
    """
    res = {
        "PER": None, "추정PER": None, "EPS": None, "추정EPS": None, 
        "PBR": None, "BPS": None, "배당수익률": None,
        "52주 최고가": None, "52주 최저가": None, "목표주가": None, "의견": None,
        "직전고점": None, "직전저점": None
    }
    
    attempt = 1
    while attempt <= max_retries:
        try:
            stock = yf.Ticker(ticker, session=SESSION)
            
            # 1. fast_info에서 52주 가격 정보 먼저 추출
            f_info = stock.fast_info
            res["52주 최고가"] = safe_float(f_info.get('year_high'))
            res["52주 최저가"] = safe_float(f_info.get('year_low'))

            # 2. 나머지 재무 정보는 info에서 추출
            info = stock.info
            if info:
                res.update({
                    "PER": safe_float(info.get("trailingPE")),
                    "추정PER": safe_float(info.get("forwardPE")),
                    "EPS": safe_float(info.get("trailingEps")),
                    "추정EPS": safe_float(info.get("forwardEps")),
                    "PBR": safe_float(info.get("priceToBook")),
                    "BPS": safe_float(info.get("bookValue")),
                    "목표주가": safe_float(info.get('targetMeanPrice')),
                    "52주 최고가": safe_float(info.get("fiftyTwoWeekHigh")) or res["52주 최고가"],
                    "52주 최저가": safe_float(info.get("fiftyTwoWeekLow")) or res["52주 최저가"]
                })
                if is_valid_num(info.get("dividendYield")):
                    res["배당수익률"] = float(info.get("dividendYield")) * 100
                    
                rec_key = str(info.get('recommendationKey', '')).lower()
                opinion_map = {"strong_buy": "적극매수", "buy": "매수", "hold": "중립", "underperform": "매도", "sell": "적극매도"}
                res['의견'] = opinion_map.get(rec_key)

            # 3. 20영업일 내 직전고점, 직전저점 계산
            hist = stock.history(period="40d")
            if not hist.empty:
                recent_20 = hist.tail(20)
                res["직전고점"] = safe_float(recent_20["High"].max())
                res["직전저점"] = safe_float(recent_20["Low"].min())

            return res
            
        except (ConnectionError, TimeoutError) as exc:
            if attempt < max_retries:
                delay = base_delay * attempt
                logger.info(f"   ⚠️ [{ticker}] 네트워크 에러 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            logger.warning(f"   ❌ [{ticker}] 네트워크 에러 (최대 재시도 초과): {exc}")
            return res
            
        except Exception as exc:
            if attempt < max_retries:
                delay = base_delay * attempt
                logger.info(f"   ⚠️ [{ticker}] 조회 실패 재시도 {attempt}/{max_retries}, {delay}초 대기: {exc}")
                time.sleep(delay)
                attempt += 1
                continue
            logger.warning(f"   ❌ [{ticker}] 데이터 수집 실패 (시도 {attempt}/{max_retries}): {exc}")
            return res
    
    logger.warning(f"   ❌ [{ticker}] 최대 재시도 횟수 초과")
    return res


def build_finance_update_for_page(page):
    """개별 해외 주식 페이지의 재무 데이터를 수집하고 업데이트 정보를 반환합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker or is_kr_ticker(ticker):
        return None

    number_keys = [
        "PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", 
        "배당수익률", "52주 최고가", "52주 최저가", "목표주가", 
        "직전고점", "직전저점"
    ]
    
    try:
        fin_data = get_us_fin_optimized(ticker)
        update_props = {
            key: {"number": fin_data[key]}
            for key in number_keys
            if is_valid_num(fin_data.get(key)) and key in props
        }

        if fin_data.get("의견"):
            update_props["목표가 범위"] = {"select": {"name": fin_data["의견"]}}
        
        if "마지막 업데이트" in props:
            update_props["마지막 업데이트"] = {"date": {"start": kst_isoformat()}}

        if not update_props:
            logger.info(f"⚠️ [{ticker}] 업데이트할 유효 데이터 없음")
            return None
        
        preview = ", ".join([f"{k}={v}" for k, v in list(fin_data.items())[:3]])
        return (page["id"], ticker, update_props, preview)
        
    except Exception as e:
        logger.warning(f"❌ [{ticker}] 데이터 수집 중 에러: {e}")
        return None


def batch_collect_us_finance_data(pages: list, max_workers: int = 5):
    """ 여러 페이지의 해외 주식 재무 데이터를 병렬로 수집합니다. """
    updates = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_finance_update_for_page, page): page for page in pages}
        
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    updates.append(result)
            except Exception as exc:
                page = futures[fut]
                ticker = get_page_text(page.get("properties", {}), ["티커", "Ticker"]).upper() or "UNKNOWN"
                logger.warning(f"❌ [{ticker}] 데이터 수집 중 에러: {exc}")
    
    return updates


def batch_update_us_finance_pages(notion_client, updates: list, batch_size: int = 10, delay_between_batches: float = 0.3):
    """ 배치 단위로 노션 해외 주식 재무 정보 페이지를 업데이트합니다. """
    if not updates:
        return
    
    logger.info(f"📦 [{len(updates)}개 항목] 해외 주식 재무 정보 배치 업데이트 시작 (배치 크기: {batch_size})")
    success_count = 0
    fail_count = 0
    
    for batch_idx, i in enumerate(range(0, len(updates), batch_size), 1):
        chunk = updates[i : i + batch_size]
        logger.info(f"   📤 배치 {batch_idx}/{(len(updates) + batch_size - 1) // batch_size} 처리 중 ({len(chunk)}개)...")
        
        with ThreadPoolExecutor(max_workers=min(len(chunk), 5)) as exe:
            futures = {}
            for pid, ticker, props, preview in chunk:
                fut = exe.submit(safe_page_update, notion_client, pid, props)
                futures[fut] = (pid, ticker, preview)
            
            for fut in as_completed(futures):
                pid, ticker, preview = futures[fut]
                try:
                    ok = fut.result()
                    if ok:
                        logger.info(f"      ✅ [Global: {ticker}] {preview}...")
                        success_count += 1
                    else:
                        logger.warning(f"      ❌ [Global: {ticker}] 업데이트 실패")
                        fail_count += 1
                except Exception as exc:
                    logger.warning(f"      ❌ [Global: {ticker}] 예외 발생: {exc}")
                    fail_count += 1
        
        if batch_idx < (len(updates) + batch_size - 1) // batch_size:
            time.sleep(delay_between_batches)
    
    logger.info(f"\n✨ 해외 주식 재무 정보 배치 업데이트 완료: 성공 {success_count}개, 실패 {fail_count}개")


def main():
    kst = timezone(timedelta(hours=9))
    logger.info(f"🌍 [해외 주식 재무 업데이트] 시작 - {datetime.now(kst)}")
    
    all_pages = []
    
    logger.info("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        all_pages.append(page)
    
    logger.info(f"📊 총 {len(all_pages)}개 항목 발견")
    
    batch_collect_size = 20
    updates = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        logger.info(f"\n🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_us_finance_data(batch, max_workers=4)
        updates.extend(batch_updates)
        
        if i + batch_collect_size < len(all_pages):
            time.sleep(1.5)
    
    if updates:
        logger.info(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_us_finance_pages(notion, updates, batch_size=10, delay_between_batches=0.3)
    else:
        logger.warning("⚠️ 업데이트할 항목이 없습니다.")


if __name__ == "__main__":
    main()


