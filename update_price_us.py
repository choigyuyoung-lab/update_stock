"""
update_price_us.py
===================
Yahoo Finance(yfinance)를 호출하여 미국/해외 상장 주식 및 ADR의 현재가 및 전일 종가를 수집하고
노션(Notion) 데이터베이스에 배치(Batch) 업데이트합니다.
- 데이터 소스: Yahoo Finance (yfinance API)
- 기능: 해외 주식 실시간 시세 수집, 전일 종가 매핑, 마지막 업데이트 일시(KST) 기록
- 안정성: 타임아웃/연결 실패 시 재시도, 배치 기반 노션 API 전송
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import logging
import warnings
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf

# yfinance 및 pandas 경고 숨기기
warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    kst_isoformat,
    set_page_date_property,
    paginate_database,
    safe_page_update,
    get_http_session,
    is_kr_ticker,
    is_valid_num,
    batch_update_pages,
    build_dirty_payload,
    is_market_holiday,
)


# ==============================================================================
# 1. 환경 변수 및 로거 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = (
    os.environ.get("DATABASE_ID")
    or os.environ.get("MASTER_DATABASE_ID")
    or os.environ.get("MASTER_DB_ID")
    or get_env_var("DATABASE_ID")
)

SESSION = get_http_session()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("PriceSyncUS")


# ==============================================================================
# 2. 야후 파이낸스 실시간 시세 수집부
# ==============================================================================
def get_stock_data(
    ticker: str,
    max_retries: int = 3,
    base_delay: float = 1.0
) -> Tuple[Optional[float], Optional[float]]:
    """yfinance를 호출하여 해외 주식 현재가 및 전일 종가를 추출합니다."""
    clean_ticker = ticker.strip().upper()
    
    for attempt in range(1, max_retries + 1):
        try:
            stock = yf.Ticker(clean_ticker, session=SESSION)
            
            # fast_info를 통한 초고속 추출
            current_price = None
            previous_close = None
            
            try:
                fast = stock.fast_info
                current_price = getattr(fast, "last_price", None)
                previous_close = getattr(fast, "previous_close", None)
            except Exception:
                pass
            
            # fast_info 누락 시 history 폴백
            if current_price is None or previous_close is None:
                hist = stock.history(period="5d")
                if not hist.empty and len(hist) >= 2:
                    current_price = float(hist["Close"].iloc[-1])
                    previous_close = float(hist["Close"].iloc[-2])
                elif not hist.empty and len(hist) == 1:
                    current_price = float(hist["Close"].iloc[-1])
                    previous_close = float(hist["Open"].iloc[0])
            
            if is_valid_num(current_price):
                return (current_price, previous_close)
                
        except (ConnectionError, TimeoutError) as exc:
            if attempt < max_retries:
                delay = base_delay * attempt
                logger.info(f"   ⚠️ [{ticker}] 통신 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                continue
            logger.warning(f"   ❌ [{ticker}] 네트워크 에러 (최대 재시도 초과): {exc}")
            return (None, None)
            
        except Exception as exc:
            if attempt < max_retries:
                delay = base_delay * attempt
                logger.info(f"   ⚠️ [{ticker}] 재시도 {attempt}/{max_retries}, {delay}초 대기: {exc}")
                time.sleep(delay)
                continue
            logger.warning(f"   ❌ [{ticker}] 시세 조회 실패: {exc}")
            return (None, None)
            
    return (None, None)


# ==============================================================================
# 3. 개별 페이지 가격 분석 및 페이로드 빌더
# ==============================================================================
def build_price_update_for_page(
    page: Dict[str, Any]
) -> Optional[Tuple[str, Dict[str, Any], str, str]]:
    """개별 해외 주식 페이지의 가격 데이터를 수집하고 변경된 경우에만 업데이트 정보를 반환합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    name = get_page_text(props, ["종목명", "Name"]) or ticker
    if not ticker or is_kr_ticker(ticker):
        return None

    try:
        current_price, previous_close = get_stock_data(ticker)
        
        cand_data: Dict[str, Any] = {}
        if is_valid_num(current_price):
            cand_data["현재가"] = current_price
        
        if is_valid_num(previous_close):
            cand_data["전일 종가"] = previous_close
        
        dirty_props = build_dirty_payload(
            existing_props=props,
            candidate_data=cand_data,
            num_fields=["현재가", "전일 종가"],
            select_fields=[],
        )

        if dirty_props:
            price_str = f"{round(current_price, 2)}" if current_price is not None and is_valid_num(current_price) else "N/A"
            return (page["id"], dirty_props, ticker, f"{name} (${price_str})")
        else:
            return None
            
    except Exception as e:
        logger.warning(f"❌ [{ticker}] 예상치 못한 에러: {e}")
        return None


# ==============================================================================
# 4. 배치 수집
# ==============================================================================
def batch_collect_us_price_data(
    pages: List[Dict[str, Any]],
    max_workers: int = 5
) -> List[Tuple[str, Dict[str, Any], str, str]]:
    """여러 페이지의 해외 주식 가격 데이터를 병렬로 수집합니다."""
    updates: List[Tuple[str, Dict[str, Any], str, str]] = []
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


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """해외 주식 현재가 일괄 업데이트 메인 파이프라인"""
    # 0. 미국 휴장일 감지 및 조기 종료 (리소스 및 액션스 사용량 절감)
    force_run = os.environ.get("FORCE_RUN", "").lower() in ("true", "1") or "--force" in sys.argv
    is_closed, reason = is_market_holiday("US")
    if is_closed and not force_run:
        logger.info(f"🛑 [미국 증시 휴장일 감지] 오늘은 {reason}입니다. 불필요한 API 호출 및 리소스를 절약하기 위해 작업을 즉시 종료합니다. (강제실행: FORCE_RUN=true 또는 --force)")
        return

    notion_client = build_notion_client(NOTION_TOKEN)
    kst = timezone(timedelta(hours=9))
    logger.info(f"⚡ [해외 주식 가격 업데이트] 시작 - {datetime.now(kst)}")
    
    all_pages = []
    
    logger.info("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion_client, DATABASE_ID, page_size=100, retry_delay=0.05):
        all_pages.append(page)
    
    logger.info(f"📊 총 {len(all_pages)}개 항목 발견")
    
    batch_collect_size = 35
    updates: List[Tuple[str, Dict[str, Any], str, str]] = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        logger.info(f"\n🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_us_price_data(batch, max_workers=6)
        updates.extend(batch_updates)
        
        if i + batch_collect_size < len(all_pages):
            time.sleep(0.5)
    
    if updates:
        batch_update_pages(notion_client, updates, max_workers=3, delay=0.05, logger=logger)
    else:
        logger.warning("⚠️ 업데이트할 항목이 없습니다.")
        
    logger.info("✨ 해외 주식 현재가 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()
