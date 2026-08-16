"""
update_price_kr.py
===================
한국투자증권(KIS) Open API를 호출하여 국내 상장 주식의 현재가 및 전일 종가를 수집하고
노션(Notion) 데이터베이스에 안전하게 배치(Batch) 업데이트합니다.
- 데이터 소스: 한국투자증권(KIS) Open API (FHKST01010100)
- 기능: 실시간 시세 수집, 전일 종가 매핑, 마지막 업데이트 일시(KST) 기록
- 안정성: 지수 백오프 기반 재시도, 멀티스레드 병렬 수집 및 청크 단위 쓰기
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

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
    paginate_database,
    safe_page_update,
    RETRY_STATUS_CODES,
    set_page_date_property,
    get_kis_auth_context,
    get_http_session,
    is_kr_ticker,
    is_valid_num,
)


# ==============================================================================
# 1. 환경 변수 및 공통 세션 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = (
    os.environ.get("DATABASE_ID")
    or os.environ.get("MASTER_DATABASE_ID")
    or os.environ.get("MASTER_DB_ID")
    or get_env_var("DATABASE_ID")
)

SESSION = get_http_session()


# ==============================================================================
# 2. 한국투자증권 시세 수집부
# ==============================================================================
def get_price_data(
    ticker: str,
    kis_ctx: Dict[str, Any],
    max_retries: int = 3,
    base_delay: float = 2.0
) -> Dict[str, Optional[float]]:
    """한투 API에서 국내 주식 가격 데이터를 조회합니다. 정밀 지수 백오프를 수행합니다."""
    if not kis_ctx or not isinstance(kis_ctx, dict) or not kis_ctx.get("token"):
        return {}

    clean_ticker = ticker.split(".")[0].strip()
    token = kis_ctx["token"]
    url_base = kis_ctx["url_base"]
    app_key = kis_ctx["app_key"]
    app_secret = kis_ctx["app_secret"]

    headers = {
        "authorization": f"Bearer {token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST01010100",
        "custtype": "P",
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker}
    
    attempt = 1
    while attempt <= max_retries:
        try:
            response = SESSION.get(
                url=f"{url_base}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=headers,
                params=params,
                timeout=10,
            )
            
            status = response.status_code
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ [{ticker}] KIS API 재시도 {attempt}/{max_retries} - status={status}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            
            response.raise_for_status()
            out = response.json().get("output", {})
            return {
                "현재가": float(out.get("stck_prpr")) if out.get("stck_prpr") else None,
                "전일 종가": float(out.get("stck_sdpr")) if out.get("stck_sdpr") else None,
            }
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as exc:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ [{ticker}] KIS API 통신 오류 발생. 재시도 {attempt}/{max_retries}, {delay}초 대기: {exc}")
                time.sleep(delay)
                attempt += 1
                continue
            print(f"❌ [{ticker}] KIS API 요청 실패 (최대 재시도 초과): {exc}")
            return {}
            
        except Exception as exc:
            print(f"❌ [{ticker}] 시스템 에러 파싱 실패: {exc}")
            return {}
            
    return {}


# ==============================================================================
# 3. 개별 페이지 가격 분석 및 페이로드 빌더
# ==============================================================================
def build_update_for_page(
    page: Dict[str, Any],
    kis_ctx: Dict[str, Any]
) -> Optional[Tuple[str, str, Dict[str, Any]]]:
    """페이지별 속성을 추출해 한투 가격 데이터와 매핑 구조를 빌드합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker or not is_kr_ticker(ticker):
        return None

    price_data = get_price_data(ticker, kis_ctx)
    if not price_data:
        print(f"⚠️ [{ticker}] 가격 데이터 미수신")
        return None

    update_props: Dict[str, Any] = {}
    if is_valid_num(price_data.get("현재가")):
        update_props["현재가"] = {"number": price_data["현재가"]}
    if is_valid_num(price_data.get("전일 종가")):
        update_props["전일 종가"] = {"number": price_data["전일 종가"]}
        
    set_page_date_property(update_props, props)

    if not update_props:
        print(f"⚠️ [{ticker}] 업데이트할 유효한 데이터 없음")
        return None

    return (page["id"], ticker, update_props)


# ==============================================================================
# 4. 배치 수집 및 노션 다중 스레드 반영
# ==============================================================================
def batch_collect_price_data(
    pages: List[Dict[str, Any]],
    kis_ctx: Dict[str, Any],
    max_workers: int = 3
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """여러 페이지의 국내 주식 가격 데이터를 병렬로 수집합니다."""
    updates: List[Tuple[str, str, Dict[str, Any]]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_update_for_page, page, kis_ctx): page for page in pages}
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    updates.append(result)
            except Exception as exc:
                page = futures[fut]
                props = page.get("properties", {})
                ticker = get_page_text(props, ["티커", "Ticker"]).upper() or "UNKNOWN"
                print(f"❌ [{ticker}] 데이터 수집 중 예외 발생: {exc}")
    return updates


def batch_update_pages(
    notion_client: Any,
    updates: List[Tuple[str, str, Dict[str, Any]]],
    batch_size: int = 10,
    delay_between_batches: float = 0.3
) -> None:
    """수집된 가격 정보를 배치화하여 노션에 안전하게 반영합니다."""
    if not updates:
        return
    for i in range(0, len(updates), batch_size):
        chunk = updates[i : i + batch_size]
        with ThreadPoolExecutor(max_workers=min(len(chunk), 5)) as exe:
            futures = {exe.submit(safe_page_update, notion_client, pid, props): (pid, ticker) for pid, ticker, props in chunk}
            for fut in as_completed(futures):
                pid, ticker = futures[fut]
                try:
                    ok = fut.result()
                    if ok:
                        print(f"✅ [Price] {ticker} 업데이트 완료")
                    else:
                        print(f"❌ [Price] {ticker} 업데이트 실패")
                except Exception as exc:
                    print(f"❌ [Price] {ticker} 업데이트 중 예외: {exc}")
                    
        time.sleep(delay_between_batches)


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """국내 주식 현재가 일괄 업데이트 메인 파이프라인"""
    notion = build_notion_client(NOTION_TOKEN)
    kis_ctx = get_kis_auth_context()
    if not kis_ctx:
        print("❌ KIS 인증 컨텍스트를 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    print(f"🚀 한투 가격 정보 수집 시작 (활성 서버: {kis_ctx['env_type']} - {kis_ctx['url_base']})")
    all_pages = []
    
    print("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        all_pages.append(page)
        
    print(f"📊 총 {len(all_pages)}개 항목 발견")
    
    batch_collect_size = 20
    updates: List[Tuple[str, str, Dict[str, Any]]] = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        print(f"\n🔄 가격 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_price_data(batch, kis_ctx, max_workers=3)
        updates.extend(batch_updates)
        
        if i + batch_collect_size < len(all_pages):
            time.sleep(0.5)
            
    if updates:
        print(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_pages(notion, updates, batch_size=10, delay_between_batches=0.3)
    else:
        print("⚠️ 업데이트할 항목이 없습니다.")
        
    print("✨ 모든 국내 주식 현재가 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()
