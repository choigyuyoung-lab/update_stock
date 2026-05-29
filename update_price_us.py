import logging
import math
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
    except:
        return False


def get_stock_data(ticker: str, max_retries: int = 3, base_delay: float = 2.0) -> tuple:
    """
    Yahoo Finance에서 주식 데이터를 조회합니다.
    네트워크 에러나 타임아웃이 발생하면 2~3초 대기 후 최대 3번까지 재시도합니다.
    
    Returns: (current_price, previous_close) 튜플. 실패 시 (None, None) 반환
    """
    attempt = 1
    while attempt <= max_retries:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")
            
            current_price = None
            previous_close = None
            
            if not hist.empty:
                current_price = hist['Close'].iloc[-1]
            
            if current_price is not None:
                try:
                    info = stock.info
                    previous_close = info.get('previousClose')
                except Exception as e:
                    logger.debug(f"   ⚠️ [{ticker}] 전일 종가 조회 실패: {e}")
            
            return (current_price, previous_close)
            
        except (ConnectionError, TimeoutError) as exc:
            # 네트워크 관련 에러는 재시도
            if attempt < max_retries:
                delay = base_delay * attempt
                logger.info(f"   ⚠️ [{ticker}] 네트워크 에러 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            logger.warning(f"   ❌ [{ticker}] 네트워크 에러 (최대 재시도 초과): {exc}")
            return (None, None)
            
        except Exception as exc:
            # 기타 에러도 재시도 시도
            if attempt < max_retries:
                delay = base_delay * attempt
                logger.info(f"   ⚠️ [{ticker}] 조회 실패 재시도 {attempt}/{max_retries}, {delay}초 대기: {exc}")
                time.sleep(delay)
                attempt += 1
                continue
            logger.warning(f"   ❌ [{ticker}] 데이터 조회 실패 (시도 {attempt}/{max_retries}): {exc}")
            return (None, None)
    
    logger.warning(f"   ❌ [{ticker}] 최대 재시도 횟수 초과")
    return (None, None)


def build_price_update_for_page(page):
    """개별 해외 주식 페이지의 가격 데이터를 수집하고 업데이트 정보를 반환합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker:
        return None

    # 국내 주식은 제외
    is_kr = (ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith((".T", ".TA", ".TW"))
    if is_kr:
        return None

    try:
        current_price, previous_close = get_stock_data(ticker)
        
        upd = {}
        if is_valid(current_price):
            upd["현재가"] = {"number": current_price}
        
        if is_valid(previous_close) and "전일 종가" in props:
            upd["전일 종가"] = {"number": previous_close}
        
        if upd:
            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": kst_isoformat()}}
            
            price_str = f"{round(current_price, 2)}" if is_valid(current_price) else "N/A"
            return (page["id"], ticker, upd, price_str)
        else:
            logger.warning(f"⚠️ [{ticker}] 유효한 데이터 없음")
            return None
            
    except Exception as e:
        logger.warning(f"❌ [{ticker}] 예상치 못한 에러: {e}")
        return None


def batch_collect_us_price_data(pages: list, max_workers: int = 5):
    """
    여러 페이지의 해외 주식 가격 데이터를 병렬로 수집합니다.
    """
    updates = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_price_update_for_page, page): page for page in pages}
        
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


def batch_update_us_price_pages(notion_client, updates: list, batch_size: int = 10, delay_between_batches: float = 0.3):
    """
    배치 단위로 노션 해외 주식 가격 페이지를 업데이트합니다.
    """
    if not updates:
        return
    
    logger.info(f"📦 [{len(updates)}개 항목] 해외 주식 가격 배치 업데이트 시작 (배치 크기: {batch_size})")
    success_count = 0
    fail_count = 0
    
    for batch_idx, i in enumerate(range(0, len(updates), batch_size), 1):
        chunk = updates[i : i + batch_size]
        logger.info(f"   📤 배치 {batch_idx}/{(len(updates) + batch_size - 1) // batch_size} 처리 중 ({len(chunk)}개)...")
        
        with ThreadPoolExecutor(max_workers=min(len(chunk), 5)) as exe:
            futures = {}
            for pid, ticker, props, price_str in chunk:
                fut = exe.submit(safe_page_update, notion_client, pid, props)
                futures[fut] = (pid, ticker, price_str)
            
            for fut in as_completed(futures):
                pid, ticker, price_str = futures[fut]
                try:
                    ok = fut.result()
                    if ok:
                        logger.info(f"      ✅ [Global: {ticker}] 가격: {price_str}")
                        success_count += 1
                    else:
                        logger.warning(f"      ❌ [Global: {ticker}] 업데이트 실패")
                        fail_count += 1
                except Exception as exc:
                    logger.warning(f"      ❌ [Global: {ticker}] 예외 발생: {exc}")
                    fail_count += 1
        
        if batch_idx < (len(updates) + batch_size - 1) // batch_size:
            time.sleep(delay_between_batches)
    
    logger.info(f"\n✨ 해외 주식 가격 배치 업데이트 완료: 성공 {success_count}개, 실패 {fail_count}개")


def main():
    kst = timezone(timedelta(hours=9))
    logger.info(f"⚡ [해외 주식 가격 업데이트] 시작 - {datetime.now(kst)}")
    
    all_pages = []
    
    # 1단계: 모든 페이지 수집
    logger.info("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        all_pages.append(page)
    
    logger.info(f"📊 총 {len(all_pages)}개 항목 발견")
    
    # 2단계: 배치 크기로 그룹화하여 데이터 수집 (병렬화)
    batch_collect_size = 35
    updates = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        logger.info(f"\n🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_us_price_data(batch, max_workers=6)
        updates.extend(batch_updates)
        
        # 배치 간에 짧은 대기 (API 제한 준수)
        if i + batch_collect_size < len(all_pages):
            time.sleep(0.5)
    
    # 3단계: 수집된 데이터를 배치로 노션에 업데이트
    if updates:
        logger.info(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_us_price_pages(notion, updates, batch_size=12, delay_between_batches=0.3)
    else:
        logger.warning("⚠️ 업데이트할 항목이 없습니다.")
    
if __name__ == "__main__":
    main()

