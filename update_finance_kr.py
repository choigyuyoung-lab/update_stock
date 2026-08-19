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
    swing_high = None
    swing_low = None
    vol_60d = None
    drawdown_52w = None
    ma200 = None
    ma60 = None
    trend = None
    mom_12m = None
    mom_diag = None
    risk_grade = None
    smart_guide = None

    try:
        fdr_start = (datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=400)).strftime("%Y-%m-%d")
        df_chart = fdr.DataReader(clean_ticker, fdr_start)
        if df_chart is not None and not df_chart.empty:
            c = df_chart["Close"].dropna() if "Close" in df_chart.columns else df_chart.iloc[:, 0].dropna()
            if not c.empty:
                curr_p_chart = float(c.iloc[-1])
                ma60 = float(c.rolling(60).mean().iloc[-1]) if len(c) >= 60 else float(c.mean())
                ma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else float(c.mean())
                
                # 🇰🇷 한국 특화 추세 판정 (60일 수급선 + 200일 대세선)
                if curr_p_chart >= ma60 and curr_p_chart >= ma200:
                    trend = "▲ 수급유입"
                elif curr_p_chart >= ma200:
                    trend = "━ 박스권세"
                else:
                    trend = "▼ 하락추세"

                mom_12m = ((curr_p_chart - float(c.iloc[0])) / float(c.iloc[0])) if len(c) > 0 else 0.0
                
                # 모멘텀 직관적 진단 (5단계 정밀 분류)
                if mom_12m >= 0.50:
                    mom_diag = "▲ 초강력세"
                elif mom_12m >= 0.20:
                    mom_diag = "▲ 성장강세"
                elif mom_12m >= 0.05:
                    mom_diag = "▲ 순항상승"
                elif mom_12m >= -0.10:
                    mom_diag = "━ 보합횡보"
                else:
                    mom_diag = "▼ 침체하락"

                returns_60 = c.pct_change().tail(60).dropna()
                if len(returns_60) >= 5:
                    vol_60d = float(returns_60.std() * np.sqrt(252))

                if "High" in df_chart.columns and "Low" in df_chart.columns:
                    recent_20 = df_chart.tail(20)
                    swing_high = float(recent_20["High"].max())
                    swing_low = float(recent_20["Low"].min())
    except Exception:
        # FDR 예외 시 KIS 일봉 API로 스윙고저점 및 60일 변동성 폴백
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
            response.raise_for_status()
            output3 = response.json().get("output2", [])
            if isinstance(output3, list) and output3:
                candles = list(reversed(output3))
                formatted_candles = []
                for day in candles:
                    try:
                        formatted_candles.append({
                            "high": int(day["stck_hgpr"]),
                            "low": int(day["stck_lwpr"]),
                            "close": int(day["stck_clpr"])
                        })
                    except (KeyError, ValueError, TypeError):
                        continue
                recent_candles = formatted_candles[-20:]
                if recent_candles:
                    swing_high = max(day["high"] for day in recent_candles)
                    swing_low = min(day["low"] for day in recent_candles)
                closes = [c["close"] for c in formatted_candles]
                if len(closes) >= 10:
                    returns = pd.Series(closes[-60:]).pct_change().dropna()
                    vol_60d = float(returns.std() * np.sqrt(252))
        except Exception:
            pass

    # 변동성 체감 위험도 등급 (어떤 경로로 계산되든 100% 산출)
    if vol_60d is not None:
        if vol_60d < 0.20:
            risk_grade = "▲ 안심비중"
        elif vol_60d < 0.35:
            risk_grade = "━ 표준비중"
        elif vol_60d < 0.60:
            risk_grade = "▼ 주의비중"
        else:
            risk_grade = "▼ 경계소액"

    curr_p = safe_float(output.get("stck_prpr"))
    w52_h = safe_float(output.get("w52_hgpr"))
    if curr_p is not None and w52_h is not None and w52_h > 0:
        drawdown_52w = (curr_p - w52_h) / w52_h  # 노션 백분율 형식 (-0.15 = -15%)

    # 스마트 가이드 (표준 태그 6종)
    if curr_p is not None and ma200 is not None:
        if curr_p >= ma200:
            if drawdown_52w is not None and drawdown_52w <= -0.20:
                smart_guide = "▲ 분할매수"
            elif ma60 is not None and curr_p >= ma60 and mom_12m is not None and mom_12m >= 0.50:
                smart_guide = "▲ 추세탑승"
            elif ma60 is not None and curr_p < ma60:
                smart_guide = "━ 눌림지지"
            else:
                smart_guide = "▲ 상승유지"
        else:
            if drawdown_52w is not None and drawdown_52w <= -0.35:
                smart_guide = "▼ 바닥확인"
            else:
                smart_guide = "▼ 하락관망"

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
        "직전고점": safe_float(swing_high),
        "직전저점": safe_float(swing_low),
        "200일선": safe_float(round(ma200, 2)) if ma200 else None,
        "60일선": safe_float(round(ma60, 2)) if ma60 else None,
        "수급선": safe_float(round(ma60, 2)) if ma60 else None,
        "추세": trend,
        "스마트 가이드": smart_guide,
        "모멘텀 진단": mom_diag,
        "위험도 등급": risk_grade,
        "12M 모멘텀": safe_float(round(mom_12m, 4)) if mom_12m is not None else None,
        "52주 낙폭": safe_float(drawdown_52w),
        "낙폭율": safe_float(drawdown_52w),
        "60일 변동성": safe_float(vol_60d),
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
        "현재가", "PER", "PBR", "EPS", "BPS", "배당수익률", "업종PER",
        "직전고점", "직전저점", "60일 변동성", "52주 낙폭", "낙폭율", "200일선", "60일선", "수급선", "12M 모멘텀"
    ]
    select_fields = ["추세", "스마트 가이드", "모멘텀 진단", "위험도 등급"]

    update_props = {
        field: {"number": data[field]}
        for field in num_fields
        if data.get(field) is not None and field in props
    }
    
    # 노션 속성 타입(Select vs Status vs Rich_text) 자동 감지 방어 매핑
    for s_field in select_fields:
        val = data.get(s_field)
        if val and s_field in props:
            p_type = props[s_field].get("type", "select")
            if p_type == "status":
                update_props[s_field] = {"status": {"name": val}}
            elif p_type == "rich_text":
                color = get_diagnostic_color(val)
                update_props[s_field] = {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": val},
                            "annotations": {"color": color, "bold": True}
                        }
                    ]
                }
            else:
                update_props[s_field] = {"select": {"name": val}}
    
    set_page_date_property(update_props, props)

    if not update_props:
        print(f"⚠️ [{ticker}] 업데이트할 유효한 데이터 없음")
        return None

    preview = ", ".join([f"{k}={v}" for k, v in list(data.items())[:3]])
    
    curr_price_str = f"{int(data['현재가']):,}" if data.get('현재가') else 'None'
    swing_high_str = f"{int(data['직전고점']):,}" if data.get('직전고점') else 'None'
    swing_low_str = f"{int(data['직전저점']):,}" if data.get('직전저점') else 'None'
    vol_str = f"{data['60일 변동성']*100:.1f}%" if data.get('60일 변동성') is not None else 'None'
    print(f"   ✅ [Collect] {ticker} 완료 (현재가: {curr_price_str}원, 직전고점: {swing_high_str}, 직전저점: {swing_low_str}, 60일변동성: {vol_str})")

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
