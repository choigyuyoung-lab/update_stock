"""
update_finance_us.py
=====================
Yahoo Finance(yfinance)를 호출하여 미국/해외 상장 주식 및 ADR의 밸류에이션 지표,
배당수익률, 52주 최고/최저가, 목표주가, 투자의견, 최근 20영업일 직전 고점/저점을 수집하여
노션(Notion) 데이터베이스에 배치 업데이트합니다.
- 데이터 소스: Yahoo Finance (yfinance API)
- 수집 지표: PER, 추정PER, EPS, 추정EPS, PBR, BPS, 배당수익률, 52주 최고/최저, 목표주가, 투자의견, 직전고점/저점
- 안정성: fast_info 우선 추출 및 타임아웃 지수 재시도, 배치 기반 노션 API 전송
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf
import numpy as np
import pandas as pd

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
    safe_float,
    is_valid_num,
    calculate_quant_indicators,
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

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("FinanceSyncUS")


# ==============================================================================
# 2. 해외 주식 재무 데이터 수집부 (Yahoo Finance)
# ==============================================================================
def get_stock_financials(
    ticker: str,
    max_retries: int = 3,
    base_delay: float = 2.0
) -> Dict[str, Any]:
    """
    Yahoo Finance에서 해외 주식 재무 데이터 및 5대 퀀트 지표를 조회합니다.
    네트워크 에러나 타임아웃이 발생하면 지수 백오프 후 최대 3번까지 재시도합니다.
    """
    res: Dict[str, Any] = {
        "PER": None, "추정PER": None, "EPS": None, "추정EPS": None, 
        "PBR": None, "BPS": None, "배당수익률": None,
        "52주 최고가": None, "52주 최저가": None,
        "직전고점": None, "직전저점": None,
        "200일선": None, "추세": None, "12M 모멘텀": None, "52주 낙폭": None, "60일 변동성": None,
    }
    
    attempt = 1
    while attempt <= max_retries:
        try:
            stock = yf.Ticker(ticker, session=SESSION)
            
            # 1. fast_info에서 52주 가격 정보 먼저 추출 (고속)
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
                    "52주 최고가": safe_float(info.get("fiftyTwoWeekHigh")) or res["52주 최고가"],
                    "52주 최저가": safe_float(info.get("fiftyTwoWeekLow")) or res["52주 최저가"],
                })
                div_yield = safe_float(info.get("dividendYield"))
                if div_yield is not None:
                    res["배당수익률"] = div_yield * 100

            # 3. 1년치 일봉 시계열로 직전고저점 및 5대 퀀트 지표 계산
            hist = stock.history(period="1y")
            if not hist.empty:
                quant = calculate_quant_indicators(hist, is_kr=False)
                res["직전고점"] = safe_float(quant["swing_high"])
                res["직전저점"] = safe_float(quant["swing_low"])
                res["50일선"] = safe_float(quant["ma_supply"])
                res["수급선"] = safe_float(quant["ma_supply"])
                res["200일선"] = safe_float(quant["ma200"])
                res["추세"] = quant["trend"]
                res["12M 모멘텀"] = safe_float(quant["mom_12m"])
                res["모멘텀 진단"] = quant["mom_diag"]
                res["52주 낙폭"] = safe_float(quant["drawdown_52w"])
                res["낙폭율"] = safe_float(quant["drawdown_52w"])
                res["60일 변동성"] = safe_float(quant["vol_60d"])
                res["위험도 등급"] = quant["risk_grade"]
                res["스마트 가이드"] = quant["smart_guide"]

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
    
    return res


# ==============================================================================
# 3. 개별 페이지 재무 분석 및 페이로드 빌더
# ==============================================================================
def get_diagnostic_color(text: str) -> str:
    """기호에 따른 노션 컬러값 반환 (▲: red, ━: green, ▼: blue)"""
    if not text:
        return "default"
    if "▲" in text:
        return "red"
    elif "━" in text:
        return "green"
    elif "▼" in text:
        return "blue"
    return "default"


def build_finance_update_for_page(
    page: Dict[str, Any]
) -> Optional[Tuple[str, str, Dict[str, Any], str]]:
    """개별 해외 주식 페이지의 재무 데이터를 수집하고 업데이트 정보를 반환합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker or is_kr_ticker(ticker):
        return None

    number_keys = [
        "PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률",
        "직전고점", "직전저점", "200일선", "50일선", "수급선", "12M 모멘텀", "52주 낙폭", "낙폭율", "60일 변동성"
    ]
    select_keys = ["추세", "스마트 가이드", "모멘텀 진단", "위험도 등급"]
    
    try:
        fin_data = get_stock_financials(ticker)
        update_props = {
            key: {"number": fin_data[key]}
            for key in number_keys
            if is_valid_num(fin_data.get(key)) and key in props
        }

        # 노션 속성 타입(Select vs Status vs Rich_text) 자동 감지 방어 매핑
        for s_key in select_keys:
            val = fin_data.get(s_key)
            if val and s_key in props:
                p_type = props[s_key].get("type", "select")
                if p_type == "status":
                    update_props[s_key] = {"status": {"name": val}}
                elif p_type == "rich_text":
                    color = get_diagnostic_color(val)
                    update_props[s_key] = {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": val},
                                "annotations": {"color": color, "bold": True}
                            }
                        ]
                    }
                else:
                    update_props[s_key] = {"select": {"name": val}}
        
        set_page_date_property(update_props, props)

        if not update_props:
            logger.info(f"⚠️ [{ticker}] 업데이트할 유효 데이터 없음")
            return None
        
        preview = ", ".join([f"{k}={v}" for k, v in list(fin_data.items())[:3]])
        return (page["id"], ticker, update_props, preview)
        
    except Exception as e:
        logger.warning(f"❌ [{ticker}] 데이터 수집 중 에러: {e}")
        return None


# ==============================================================================
# 4. 배치 수집 및 노션 다중 스레드 반영
# ==============================================================================
def batch_collect_us_finance_data(
    pages: List[Dict[str, Any]],
    max_workers: int = 5
) -> List[Tuple[str, str, Dict[str, Any], str]]:
    """여러 페이지의 해외 주식 재무 데이터를 병렬로 수집합니다."""
    updates: List[Tuple[str, str, Dict[str, Any], str]] = []
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


def batch_update_us_finance_pages(
    notion_client: Any,
    updates: List[Tuple[str, str, Dict[str, Any], str]],
    batch_size: int = 10,
    delay_between_batches: float = 0.3
) -> None:
    """배치 단위로 노션 해외 주식 재무 정보 페이지를 업데이트합니다."""
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


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """해외 주식 재무 정보 일괄 업데이트 메인 파이프라인"""
    notion_client = build_notion_client(NOTION_TOKEN)
    kst = timezone(timedelta(hours=9))
    logger.info(f"🌍 [해외 주식 재무 업데이트] 시작 - {datetime.now(kst)}")
    
    all_pages = []
    
    logger.info("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion_client, DATABASE_ID, page_size=100, retry_delay=0.3):
        all_pages.append(page)
    
    logger.info(f"📊 총 {len(all_pages)}개 항목 발견")
    
    batch_collect_size = 20
    updates: List[Tuple[str, str, Dict[str, Any], str]] = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        logger.info(f"\n🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_us_finance_data(batch, max_workers=4)
        updates.extend(batch_updates)
        
        if i + batch_collect_size < len(all_pages):
            time.sleep(1.5)
    
    if updates:
        logger.info(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_us_finance_pages(notion_client, updates, batch_size=10, delay_between_batches=0.3)
    else:
        logger.warning("⚠️ 업데이트할 항목이 없습니다.")

    logger.info("✨ 해외 주식 재무 정보 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()


