"""
update_finance_kr.py
=====================
한국투자증권(KIS) Open API를 호출하여 국내 상장 주식의 재무 지표, 투자의견, 52주 신고/신저가,
및 최근 20영업일 직전 고점/저점을 수집하여 노션 데이터베이스에 배치 업데이트합니다.
- 데이터 소스:
  1. KIS 기본 시세/투자지표 API (FHKST01010100) : PER, PBR, EPS, BPS, 배당수익률, 52주 최고/최저, 업종PER
  2. KIS 투자의견 API (HHDFS76700100) : 목표주가, 추정PER, 추정EPS, 투자의견
  3. KIS 일봉 차트 API (FHKST03010100) : 최근 20영업일 스윙 직전고점/직전저점
- 안정성: 지수 백오프 기반 재시도 및 안전 배치 업데이트
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import sys
import time
from datetime import datetime, timedelta
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
    get_kis_auth_context,
    get_http_session,
    is_kr_ticker,
    safe_float,
)


# ==============================================================================
# 1. 환경 변수 및 공통 세션 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = get_env_var("DATABASE_ID")

SESSION = get_http_session()


# ==============================================================================
# 2. 한국투자증권 다단계 재무/기술 지표 수집부
# ==============================================================================
def get_finance_data(
    ticker: str,
    kis_ctx: Dict[str, Any],
    max_retries: int = 4,
    base_delay: float = 3.0
) -> Dict[str, Any]:
    """한투 API에서 국내 주식 재무 데이터(기본정보, 투자의견, 일봉차트 직전고저점)를 조회합니다."""
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
        "custtype": "P",
    }

    # 1단계: 기본 정보 조회 (필수 - 지수 백오프 재시도 적용)
    output = {}
    for attempt in range(1, max_retries + 1):
        try:
            response = SESSION.get(
                url=f"{url_base}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers={**headers, "tr_id": "FHKST01010100"},
                params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker},
                timeout=10,
            )
            if response.status_code in RETRY_STATUS_CODES:
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    print(f"   ⚠️ [{ticker}] KIS API(기본정보) {response.status_code} 에러. {delay}초 대기 후 재시도 ({attempt}/{max_retries})")
                    time.sleep(delay)
                    continue
            response.raise_for_status()
            output = response.json().get("output", {})
            if not output:
                raise ValueError("응답 데이터(output)가 비어 있습니다.")
            break
        except (requests.exceptions.RequestException, ValueError) as exc:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ [{ticker}] KIS 기본정보 통신 에러. {delay}초 대기 후 재시도 ({attempt}/{max_retries}): {exc}")
                time.sleep(delay)
                continue
            print(f"❌ [{ticker}] KIS API(기본정보) 요청 실패 (최대 재시도 초과): {exc}")
            return {}

    time.sleep(0.1)

    # 2단계: 투자의견 조회 (비필수)
    opinion = {}
    try:
        response = SESSION.get(
            url=f"{url_base}/uapi/domestic-stock/v1/quotations/invest-opinion",
            headers={**headers, "tr_id": "HHDFS76700100"},
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker},
            timeout=10,
        )
        response.raise_for_status()
        output2 = response.json().get("output", [])
        opinion = output2[0] if isinstance(output2, list) and output2 else {}
    except Exception:
        pass

    time.sleep(0.1)

    # 3단계: 일봉 차트 조회 및 최근 20영업일 직전 고점/저점 계산
    swing_high = None
    swing_low = None
    try:
        end_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
        start_date = (datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=40)).strftime("%Y%m%d")
        response = SESSION.get(
            url=f"{url_base}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            headers={**headers, "tr_id": "FHKST03010100"},
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": clean_ticker,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0"
            },
            timeout=10,
        )
        response.raise_for_status()
        output3 = response.json().get("output2", [])
        
        if isinstance(output3, list) and output3:
            candles = list(reversed(output3))
            formatted_candles = []
            for day in candles:
                try:
                    formatted_candles.append({
                        "high": int(day["stck_hgpr"]),
                        "low": int(day["stck_lwpr"])
                    })
                except (KeyError, ValueError, TypeError):
                    continue
            
            recent_candles = formatted_candles[-20:]
            if recent_candles:
                swing_high = max(day["high"] for day in recent_candles)
                swing_low = min(day["low"] for day in recent_candles)
    except Exception:
        pass

    return {
        "현재가": safe_float(output.get("stck_prpr")),
        "PER": safe_float(output.get("per")),
        "PBR": safe_float(output.get("pbr")),
        "EPS": safe_float(output.get("eps")),
        "BPS": safe_float(output.get("bps")),
        "배당수익률": safe_float(output.get("dydt")),
        "52주 최고가": safe_float(output.get("w52_hgpr")),
        "52주 최저가": safe_float(output.get("w52_lwpr")),
        "업종PER": safe_float(output.get("bts_per")),
        "추정PER": safe_float(opinion.get("est_per")),
        "추정EPS": safe_float(opinion.get("est_eps")),
        "목표주가": safe_float(opinion.get("dstn_prce")) or safe_float(output.get("dstn_prce")),
        "의견": opinion.get("invt_opnn_nm"),
        "직전고점": safe_float(swing_high),
        "직전저점": safe_float(swing_low),
    }


# ==============================================================================
# 3. 개별 페이지 재무 분석 및 페이로드 빌더
# ==============================================================================
def build_finance_update_for_page(
    page: Dict[str, Any],
    kis_ctx: Dict[str, Any]
) -> Optional[Tuple[str, str, Dict[str, Any], str]]:
    """개별 노션 페이지의 티커를 추출하여 데이터를 수집하고 구조화합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker or not is_kr_ticker(ticker):
        return None

    data = get_finance_data(ticker, kis_ctx)
    if not data:
        print(f"⚠️ [{ticker}] 재무 데이터 미수신")
        return None

    num_fields = [
        "현재가", "PER", "PBR", "EPS", "BPS", "배당수익률",
        "52주 최고가", "52주 최저가", "업종PER", "추정PER", "추정EPS", "목표주가",
        "직전고점", "직전저점",
    ]

    update_props = {
        field: {"number": data[field]}
        for field in num_fields
        if data.get(field) is not None and field in props
    }
    
    if data.get("의견") and "목표가 범위" in props:
        update_props["목표가 범위"] = {"select": {"name": data["의견"]}}
    
    if "마지막 업데이트" in props:
        now_str = datetime.now(ZoneInfo("Asia/Seoul")).isoformat()
        update_props["마지막 업데이트"] = {"date": {"start": now_str}}

    if not update_props:
        print(f"⚠️ [{ticker}] 업데이트할 유효한 데이터 없음")
        return None

    preview = ", ".join([f"{k}={v}" for k, v in list(data.items())[:3]])
    
    curr_price_str = f"{int(data['현재가']):,}" if data.get('현재가') else 'None'
    swing_high_str = f"{int(data['직전고점']):,}" if data.get('직전고점') else 'None'
    swing_low_str = f"{int(data['직전저점']):,}" if data.get('직전저점') else 'None'
    print(f"   ✅ [Collect] {ticker} 완료 (현재가: {curr_price_str}원, 직전고점: {swing_high_str}, 직전저점: {swing_low_str})")

    return (page["id"], ticker, update_props, preview)


# ==============================================================================
# 4. 배치 수집 및 노션 다중 스레드 반영
# ==============================================================================
def batch_collect_finance_data(
    pages: List[Dict[str, Any]],
    kis_ctx: Dict[str, Any],
    max_workers: int = 3
) -> List[Tuple[str, str, Dict[str, Any], str]]:
    """여러 페이지의 국내 주식 재무 데이터를 병렬로 수집합니다."""
    updates: List[Tuple[str, str, Dict[str, Any], str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_finance_update_for_page, page, kis_ctx): page for page in pages}
        
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    updates.append(result)
            except Exception as exc:
                page = futures[fut]
                ticker = get_page_text(page.get("properties", {}), ["티커", "Ticker"]).upper() or "UNKNOWN"
                print(f"❌ [{ticker}] 데이터 수집 중 예외 발생: {exc}")
    
    return updates


def batch_update_finance_pages(
    notion_client: Any,
    updates: List[Tuple[str, str, Dict[str, Any], str]],
    batch_size: int = 10,
    delay_between_batches: float = 0.3
) -> None:
    """수집된 재무 정보를 배치화하여 노션에 안전하게 반영합니다."""
    if not updates:
        return
    
    print(f"📦 [{len(updates)}개 항목] 재무 정보 배치 업데이트 시작 (배치 크기: {batch_size})")
    success_count = 0
    fail_count = 0
    
    for batch_idx, i in enumerate(range(0, len(updates), batch_size), 1):
        chunk = updates[i : i + batch_size]
        print(f"   📤 배치 {batch_idx}/{(len(updates) + batch_size - 1) // batch_size} 처리 중 ({len(chunk)}개)...")
        
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
                        print(f"      ✅ [Finance] {ticker} | {preview}...")
                        success_count += 1
                    else:
                        print(f"      ❌ [Finance] {ticker} - 업데이트 실패")
                        fail_count += 1
                except Exception as exc:
                    print(f"      ❌ [Finance] {ticker} - 예외 발생: {exc}")
                    fail_count += 1
        
        if batch_idx < (len(updates) + batch_size - 1) // batch_size:
            time.sleep(delay_between_batches)
    
    print(f"\n✨ 재무 정보 배치 업데이트 완료: 성공 {success_count}개, 실패 {fail_count}개")


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """국내 주식 재무 정보 일괄 업데이트 메인 파이프라인"""
    notion = build_notion_client(NOTION_TOKEN)
    kis_ctx = get_kis_auth_context()
    if not kis_ctx:
        print("❌ KIS 인증 컨텍스트를 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    print(f"🚀 한투 재무 정보 대량 업데이트 시작 (활성 서버: {kis_ctx['env_type']} - {kis_ctx['url_base']})")
    all_pages = []
    
    print("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.4):
        all_pages.append(page)
    
    print(f"📊 총 {len(all_pages)}개 항목 발견")
    
    batch_collect_size = 15
    updates: List[Tuple[str, str, Dict[str, Any], str]] = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        print(f"\n🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_finance_data(batch, kis_ctx, max_workers=6)
        updates.extend(batch_updates)
        
        if i + batch_collect_size < len(all_pages):
            time.sleep(0.5)
    
    if updates:
        print(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_finance_pages(notion, updates, batch_size=10, delay_between_batches=0.5)
    else:
        print("⚠️ 업데이트할 항목이 없습니다.")

    print("✨ 국내 주식 재무 정보 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()
