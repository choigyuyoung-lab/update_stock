"""
update_price_kr.py
===================
한국투자증권(KIS) Open API를 호출하여 국내 상장 주식의 현재가 및 전일 종가를 수집하고
노션(Notion) 데이터베이스에 안전하게 배치(Batch) 업데이트합니다.
- 데이터 소스: 한국투자증권(KIS) 멀티시세 API (FHKST11300006, 30종목 묶음 일괄 수집) + 기본 시세 API (FHKST01010100 폴백)
- 기능: 실시간 시세 수집, 전일 종가 매핑, 마지막 업데이트 일시(KST) 기록
- 안정성: 지수 백오프 기반 재시도, 30개 묶음 초고속 수집 및 더티 체크 기반 안전 배치 쓰기
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import re
from typing import Any, Dict, List, Optional, Tuple


# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from core.notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    paginate_database,
    RETRY_STATUS_CODES,
    get_kis_auth_context,
    get_http_session,
    is_kr_ticker,
    safe_float,
    batch_update_pages,
    build_dirty_payload,
    ensure_database_properties,
)
from core.local_db_manager import upsert_finances_batch, export_all_tables_to_csv


PRICE_KR_SCHEMA: Dict[str, Dict[str, Any]] = {
    "현재가": {"number": {"format": "number"}},
    "전일 종가": {"number": {"format": "number"}},
    "마지막 업데이트": {"date": {}},
}

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
# 2. 한국투자증권 30종목 묶음 멀티시세 수집부
# ==============================================================================
def fetch_multprice_batch(
    tickers: List[str],
    kis_ctx: Dict[str, Any],
    max_retries: int = 3,
    base_delay: float = 1.5
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    최대 30개 종목을 1회 HTTP 요청(FHKST11300006)으로 일괄 조회하여
    {ticker: {'현재가': float, '전일 종가': float}} 맵을 반환합니다.
    """
    if not kis_ctx or not tickers:
        return {}

    token = kis_ctx["token"]
    url_base = kis_ctx["url_base"]
    app_key = kis_ctx["app_key"]
    app_secret = kis_ctx["app_secret"]

    headers = {
        "authorization": f"Bearer {token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST11300006",
        "custtype": "P",
    }

    results: Dict[str, Dict[str, Optional[float]]] = {}
    
    # 30개 단위 청크로 분할
    chunk_size = 30
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        params: Dict[str, str] = {}
        for idx, t in enumerate(chunk, start=1):
            clean_t = t.split(".")[0].strip()
            params[f"FID_COND_MRKT_DIV_CODE_{idx}"] = "J"
            params[f"FID_INPUT_ISCD_{idx}"] = clean_t

        for attempt in range(1, max_retries + 1):
            try:
                res = SESSION.get(
                    url=f"{url_base}/uapi/domestic-stock/v1/quotations/intstock-multprice",
                    headers=headers,
                    params=params,
                    timeout=10
                )
                if res.status_code in RETRY_STATUS_CODES and attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    time.sleep(delay)
                    continue
                res.raise_for_status()
                data = res.json()
                outputs = data.get("output", [])
                for item in outputs:
                    iscd = item.get("inter_shrn_iscd", "").strip()
                    if not iscd:
                        continue
                    curr_p = safe_float(item.get("inter2_prpr"))
                    prev_c = safe_float(item.get("inter2_sdpr") or item.get("inter2_prdy_clpr"))
                    if curr_p and curr_p > 0:
                        results[iscd] = {
                            "현재가": curr_p,
                            "전일 종가": prev_c if prev_c and prev_c > 0 else curr_p
                        }
                break
            except Exception as exc:
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    time.sleep(delay)
                else:
                    print(f"   ⚠️ 멀티시세 배치 {i//chunk_size + 1}회차 요청 실패 (단일 폴백 대기): {exc}")

    return results


def get_single_price_data(
    ticker: str,
    kis_ctx: Dict[str, Any],
    max_retries: int = 2
) -> Dict[str, Optional[float]]:
    """멀티시세에서 누락된 개별 종목에 대한 단일 시세 폴백 조회 (FHKST01010100)"""
    if not kis_ctx:
        return {}

    clean_ticker = ticker.split(".")[0].strip()
    headers = {
        "authorization": f"Bearer {kis_ctx['token']}",
        "appkey": kis_ctx["app_key"],
        "appsecret": kis_ctx["app_secret"],
        "tr_id": "FHKST01010100",
        "custtype": "P",
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker}

    for attempt in range(1, max_retries + 1):
        try:
            res = SESSION.get(
                url=f"{kis_ctx['url_base']}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=headers,
                params=params,
                timeout=8,
            )
            if res.status_code == 200:
                out = res.json().get("output", {})
                curr_p = safe_float(out.get("stck_prpr"))
                prev_c = safe_float(out.get("stck_sdpr"))
                if curr_p and curr_p > 0:
                    return {
                        "현재가": curr_p,
                        "전일 종가": prev_c if prev_c and prev_c > 0 else curr_p
                    }
            break
        except Exception:
            pass
    return {}


# ==============================================================================
# 3. 메인 파이프라인
# ==============================================================================
def main() -> None:
    """국내 주식 현재가 30개 묶음 초고속 일괄 업데이트 메인 파이프라인"""
    start_time = time.time()
    print("=" * 80)
    print("🚀 [국내 주식 시세 동기화] KIS 30종목 묶음 초고속 멀티시세 엔진 가동")
    print("=" * 80)

    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    ensure_database_properties(client, DATABASE_ID, PRICE_KR_SCHEMA)
    kis_ctx = get_kis_auth_context()
    if not kis_ctx:
        print("❌ KIS 인증 컨텍스트 생성 실패. 프로세스를 중단합니다.")
        return

    # 1. 노션 대상 페이지 전체 스캔
    all_pages = []
    print("📋 노션 대상 페이지 로드 중...")
    for p in paginate_database(client, DATABASE_ID, page_size=100, retry_delay=0.1):
        all_pages.append(p)
    print(f"   ✅ 총 {len(all_pages)}개 페이지 확인 완료")

    # 2. 국내 티커 목록 추출
    kr_pages = []
    kr_tickers = []
    for p in all_pages:
        props = p.get("properties", {})
        ticker_val = get_page_text(props, ["티커", "Ticker"]).upper()
        match = re.search(r'(\d{6}[A-Z]?)', ticker_val)
        clean_t = match.group(1) if match else ticker_val
        if clean_t and is_kr_ticker(clean_t):
            kr_pages.append((p, clean_t))
            kr_tickers.append(clean_t)

    unique_tickers = list(dict.fromkeys(kr_tickers))
    print(f"📊 국내 대상 종목: 총 {len(unique_tickers)}개 (고유 티커 기준)")

    # 3. KIS 30종목 묶음 멀티시세 일괄 수집
    print("⚡ 30종목 단위 묶음 시세 수집 시작...")
    price_map = fetch_multprice_batch(unique_tickers, kis_ctx)
    print(f"   ✅ 멀티시세 수집 완료: {len(price_map)}/{len(unique_tickers)}개 종목 확보")

    # 4. 누락 종목 단일 폴백 조회
    missing_tickers = [t for t in unique_tickers if t not in price_map]
    if missing_tickers:
        print(f"   ℹ️ 누락 {len(missing_tickers)}개 종목 단일 폴백 조회 중...")
        for mt in missing_tickers:
            sp = get_single_price_data(mt, kis_ctx)
            if sp:
                price_map[mt] = sp

    # 5. 더티 체크 및 노션 업데이트 페이로드 생성
    update_payloads: List[Tuple[str, Dict[str, Any], str, str]] = []
    for p, clean_t in kr_pages:
        props = p.get("properties", {})
        name = get_page_text(props, ["종목명", "Name"]) or clean_t
        p_data = price_map.get(clean_t)
        if not p_data:
            continue

        dirty_props = build_dirty_payload(
            existing_props=props,
            candidate_data=p_data,
            num_fields=["현재가", "전일 종가"],
            select_fields=[],
        )
        if dirty_props:
            update_payloads.append((p["id"], dirty_props, clean_t, name))

    print(f"📦 노션 실제 변경 대상: {len(update_payloads)}개 페이지 (더티 체크 완료)")

    # 6. 노션 안전 배치 업데이트
    if update_payloads:
        batch_update_pages(client, update_payloads, max_workers=3, delay=0.1)

    # 7. 통합 로컬 SQLite DB 캐싱 및 CSV 내보내기
    if price_map:
        fin_records = [
            {"ticker": t, "current_price": d.get("현재가")}
            for t, d in price_map.items() if d.get("현재가")
        ]
        upsert_finances_batch(fin_records)
        export_all_tables_to_csv()
        print(f"💾 [통합 로컬 SQLite DB] {len(fin_records)}개 국내 종목 현재가 캐싱 및 CSV 내보내기 완료")

    elapsed = time.time() - start_time
    print(f"🎉 국내 주식 시세 업데이트 완료! (소요 시간: {elapsed:.2f}초)")


if __name__ == "__main__":
    main()