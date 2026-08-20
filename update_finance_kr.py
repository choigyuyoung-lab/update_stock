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
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests
import numpy as np
import pandas as pd
import FinanceDataReader as fdr

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
    safe_float,
    calculate_quant_indicators,
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

    # 2단계: 1년치 일봉 데이터(FDR)로 직전고저점 및 5대 퀀트 지표(200일선, 추세, 12M모멘텀, 52주낙폭, 60일변동성) 계산
    curr_p = safe_float(output.get("stck_prpr"))
    w52_h = safe_float(output.get("w52_hgpr"))
    df_chart = None

    try:
        fdr_start = (datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=400)).strftime("%Y-%m-%d")
        df_chart = fdr.DataReader(clean_ticker, fdr_start)
    except Exception:
        pass

    # 공통 5대 퀀트 엔진 호출
    quant = calculate_quant_indicators(df_chart, current_price=curr_p, is_kr=True, high_52w_override=w52_h)

    # FDR 실패 시 KIS 일봉 API로 폴백
    if quant["swing_high"] is None:
        try:
            end_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
            start_date = (datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=120)).strftime("%Y%m%d")
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
            if response.status_code == 200:
                output3 = response.json().get("output2", [])
                if isinstance(output3, list) and output3:
                    candles = list(reversed(output3))
                    recent_candles = candles[-20:]
                    if recent_candles:
                        quant["swing_high"] = max(safe_float(day.get("stck_hgpr")) or 0 for day in recent_candles)
                        quant["swing_low"] = min(safe_float(day.get("stck_lwpr")) or 999999999 for day in recent_candles)
                    closes = [safe_float(c.get("stck_clpr")) or 0 for c in candles if c.get("stck_clpr")]
                    if len(closes) >= 10:
                        returns = pd.Series(closes[-60:]).pct_change().dropna()
                        quant["vol_60d"] = float(returns.std() * np.sqrt(252))
        except Exception:
            pass

    return {
        "현재가": curr_p,
        "PER": safe_float(output.get("per")),
        "PBR": safe_float(output.get("pbr")),
        "EPS": safe_float(output.get("eps")),
        "BPS": safe_float(output.get("bps")),
        "배당수익률": safe_float(output.get("dydt")),
        "52주 최고가": w52_h,
        "52주 최저가": safe_float(output.get("w52_lwpr")),
        "업종PER": safe_float(output.get("bts_per")),
        "직전고점": safe_float(quant["swing_high"]),
        "직전저점": safe_float(quant["swing_low"]),
        "200일선": safe_float(quant["ma200"]),
        "60일선": safe_float(quant["ma_supply"]),
        "수급선": safe_float(quant["ma_supply"]),
        "추세": quant["trend"],
        "스마트 가이드": quant["smart_guide"],
        "모멘텀 진단": quant["mom_diag"],
        "위험도 등급": quant["risk_grade"],
        "12M 모멘텀": safe_float(quant["mom_12m"]),
        "52주 낙폭": safe_float(quant["drawdown_52w"]),
        "낙폭율": safe_float(quant["drawdown_52w"]),
        "60일 변동성": safe_float(quant["vol_60d"]),
    }


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
    page: Dict[str, Any],
    kis_ctx: Dict[str, Any]
) -> Optional[Tuple[str, str, Dict[str, Any], str]]:
    """개별 노션 페이지의 티커를 추출하여 데이터를 수집하고 변경된 경우에만 구조화합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker or not is_kr_ticker(ticker):
        return None

    data = get_finance_data(ticker, kis_ctx)
    if not data:
        print(f"⚠️ [{ticker}] 재무 데이터 미수신")
        return None

    num_fields = [
        "현재가", "PER", "PBR", "EPS", "BPS", "배당수익률", "업종PER",
        "직전고점", "직전저점", "60일 변동성", "52주 낙폭", "낙폭율", "200일선", "60일선", "수급선", "12M 모멘텀"
    ]
    select_fields = ["추세", "스마트 가이드", "모멘텀 진단", "위험도 등급"]

    dirty_props = build_dirty_payload(
        existing_props=props,
        candidate_data=data,
        num_fields=num_fields,
        select_fields=select_fields,
        diagnostic_color_fn=get_diagnostic_color
    )

    if not dirty_props:
        return None

    preview = ", ".join([f"{k}={v}" for k, v in list(data.items())[:3]])
    
    curr_price_str = f"{int(data['현재가']):,}" if data.get('현재가') else 'None'
    swing_high_str = f"{int(data['직전고점']):,}" if data.get('직전고점') else 'None'
    swing_low_str = f"{int(data['직전저점']):,}" if data.get('직전저점') else 'None'
    vol_str = f"{data['60일 변동성']*100:.1f}%" if data.get('60일 변동성') is not None else 'None'
    print(f"   ✅ [Collect] {ticker} 완료 (현재가: {curr_price_str}원, 직전고점: {swing_high_str}, 직전저점: {swing_low_str}, 60일변동성: {vol_str})")

    return (page["id"], ticker, dirty_props, preview)


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

    print(f"🚀 한투 재무 정보 대량 업데이트 시작 (활성 서버: {kis_ctx['env_type']} - {kis_ctx['url_base']})")
    all_pages = []
    
    print("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.05):
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
            time.sleep(0.2)
    
    if updates:
        print(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_finance_pages(notion, updates, batch_size=10, delay_between_batches=0.1)
    else:
        print("⚠️ 업데이트할 항목이 없습니다.")

    print("✨ 국내 주식 재무 정보 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()
