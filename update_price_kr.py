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
    batch_update_pages,
    build_dirty_payload,
    is_market_holiday,
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
                print(f"   ⚠️ [{ticker}] KIS API {status} 에러. {delay}초 대기 후 재시도 ({attempt}/{max_retries})")
                time.sleep(delay)
                attempt += 1
                continue
                
            response.raise_for_status()
            data = response.json()
            output = data.get("output", {})
            if not output:
                raise ValueError("응답 데이터(output)가 비어 있습니다.")

            curr_price = float(output.get("stck_prpr", 0.0))
            prev_close = float(output.get("stck_sdpr", 0.0))
            
            if curr_price <= 0 and prev_close > 0:
                curr_price = prev_close

            return {
                "현재가": curr_price if curr_price > 0 else None,
                "전일 종가": prev_close if prev_close > 0 else None,
            }

        except (requests.exceptions.RequestException, ValueError) as exc:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ [{ticker}] KIS 통신 에러. {delay}초 대기 후 재시도 ({attempt}/{max_retries}): {exc}")
                time.sleep(delay)
                attempt += 1
                continue
            print(f"❌ [{ticker}] KIS API 요청 실패 (최대 재시도 초과): {exc}")
            return {}

    return {}


# ==============================================================================
# 3. 개별 페이지 가격 분석 및 페이로드 빌더
# ==============================================================================
def build_update_for_page(
    page: Dict[str, Any],
    kis_ctx: Dict[str, Any]
) -> Optional[Tuple[str, Dict[str, Any], str, str]]:
    """개별 노션 페이지의 티커를 추출하여 한투 가격 정보를 조회하고 변경된 경우에만 업데이트 페이로드를 생성합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    name = get_page_text(props, ["종목명", "Name"]) or ticker
    
    if not ticker or not is_kr_ticker(ticker):
        return None

    price_dict = get_price_data(ticker, kis_ctx)
    if not price_dict:
        return None

    dirty_props = build_dirty_payload(
        existing_props=props,
        candidate_data=price_dict,
        num_fields=["현재가", "전일 종가"],
        select_fields=[],
    )

    if dirty_props:
        return page["id"], dirty_props, ticker, name

    return None


# ==============================================================================
# 4. 일괄 데이터 수집
# ==============================================================================
def batch_collect_price_data(
    pages: List[Dict[str, Any]],
    kis_ctx: Dict[str, Any],
    max_workers: int = 3
) -> List[Tuple[str, Dict[str, Any], str, str]]:
    """페이지 청크 목록에 대해 멀티스레드로 시세를 병렬 수집합니다."""
    updates: List[Tuple[str, Dict[str, Any], str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_update_for_page, page, kis_ctx): page for page in pages}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if res:
                    updates.append(res)
            except Exception as exc:
                page = futures[fut]
                props = page.get("properties", {})
                ticker = get_page_text(props, ["티커", "Ticker"]).upper() or "UNKNOWN"
                print(f"❌ [{ticker}] 데이터 수집 중 예외 발생: {exc}")
    return updates


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """국내 주식 현재가 일괄 업데이트 메인 파이프라인"""
    # 0. 휴장일 감지 및 조기 종료 (리소스 및 액션스 사용량 절감)
    force_run = os.environ.get("FORCE_RUN", "").lower() in ("true", "1") or "--force" in sys.argv
    is_closed, reason = is_market_holiday("KR")
    if is_closed and not force_run:
        print(f"🛑 [KRX 휴장일 감지] 오늘은 {reason}입니다. 불필요한 API 호출 및 리소스를 절약하기 위해 작업을 즉시 종료합니다. (강제실행: FORCE_RUN=true 또는 --force)")
        return

    notion = build_notion_client(NOTION_TOKEN)
    kis_ctx = get_kis_auth_context()
    if not kis_ctx:
        print("❌ KIS 인증 컨텍스트를 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    print(f"🚀 한투 가격 정보 수집 시작 (활성 서버: {kis_ctx['env_type']} - {kis_ctx['url_base']})")
    all_pages = []
    
    print("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.05):
        all_pages.append(page)
        
    print(f"📊 총 {len(all_pages)}개 항목 발견")
    
    batch_collect_size = 20
    updates: List[Tuple[str, Dict[str, Any], str, str]] = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        print(f"\n🔄 가격 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_price_data(batch, kis_ctx, max_workers=3)
        updates.extend(batch_updates)
        
        if i + batch_collect_size < len(all_pages):
            time.sleep(0.5)
            
    if updates:
        batch_update_pages(notion, updates, max_workers=3, delay=0.05)
    else:
        print("⚠️ 업데이트할 항목이 없습니다.")
        
    print("✨ 모든 국내 주식 현재가 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()
