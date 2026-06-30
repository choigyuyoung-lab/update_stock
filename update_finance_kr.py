import json
import math
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # 🌟 해외 서버 시간 왜곡 차단용 표준 라이브러리
from concurrent.futures import ThreadPoolExecutor, as_completed
from notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    paginate_database,
    safe_page_update,
    RETRY_STATUS_CODES,
)

# 1. 환경 변수 및 설정 로드 (.env 연동)
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = get_env_var("DATABASE_ID")
KIS_APP_KEY = get_env_var("KIS_APP_KEY")
KIS_APP_SECRET = get_env_var("KIS_APP_SECRET")

# 한국투자증권 모의투자 주소 설정
URL_BASE = "https://openapivts.koreainvestment.com:29443"

SESSION = requests.Session()
SESSION.headers.update({
    "Content-Type": "application/json",
    "Connection": "close"  # 🌟 Keep-Alive 해제하여 서버의 불시 연결 끊김 방지
})

# 🌟 네트워크 에러 및 429(트래픽 제한), 5xx 서버 에러 발생 시 즉시 재시도하도록 설정
retries = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))


def sf(value):
    """문자열 숫자를 안전하게 float로 변환하고, 0이거나 유효하지 않으면 None을 반환합니다."""
    try:
        converted = float(value)
        return converted if converted != 0 else None
    except (TypeError, ValueError):
        return None


def get_access_token(max_retries: int = 3, base_delay: float = 2.0) -> str | None:
    """ 한투 API 액세스 토큰을 발급받습니다. 지수 백오프가 적용되어 있습니다. """
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}

    attempt = 1
    while attempt <= max_retries:
        try:
            response = SESSION.post(url, data=json.dumps(body), timeout=10)
            status = response.status_code
            
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ KIS 토큰 재시도 {attempt}/{max_retries} - status={status}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
                
            response.raise_for_status()
            token = response.json().get("access_token")
            if token:
                return token
            else:
                print(f"❌ KIS 토큰 응답에서 access_token을 찾을 수 없습니다")
                return None
                
        except requests.RequestException as exc:
            status = None
            if hasattr(exc, "response") and exc.response is not None:
                status = exc.response.status_code
                
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ KIS 토큰 재시도 {attempt}/{max_retries} - status={status}: {exc}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
                
            print(f"❌ KIS 토큰 발급 실패 (시도 {attempt}/{max_retries}): {exc}")
            return None
            
        except ValueError as exc:
            print(f"❌ KIS 토큰 파싱 실패: {exc}")
            return None
    
    print(f"❌ KIS 토큰 발급: 최대 재시도 횟수 초과")
    return None


def get_finance_data(ticker: str, token: str, max_retries: int = 4, base_delay: float = 3.0) -> dict:
    """ 한투 API에서 국내 주식 재무 데이터를 조회합니다. """
    clean_ticker = ticker.split(".")[0]
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "custtype": "P",
    }

    # 1단계: 기본 정보 조회 (필수 - 재시도 적용)
    output = {}
    for attempt in range(1, max_retries + 1):
        try:
            response = SESSION.get(
                url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
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
                print(f"   ⚠️ [{ticker}] KIS 기본정보 네트워크/통신 에러. {delay}초 대기 후 재시도 ({attempt}/{max_retries}): {exc}")
                time.sleep(delay)
                continue
            print(f"❌ [{ticker}] KIS API(기본정보) 요청 실패 (최대 재시도 초과): {exc}")
            return {}

    time.sleep(0.1)

    # 2단계: 투자의견 조회 (비필수 - 재시도 없음)
    opinion = {}
    try:
        response = SESSION.get(
            url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/invest-opinion",
            headers={**headers, "tr_id": "HHDFS76700100"},
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker},
            timeout=10,
        )
        response.raise_for_status()
        output2 = response.json().get("output", [])
        opinion = output2[0] if isinstance(output2, list) and output2 else {}
    except Exception:
        # 500 에러 또는 에러 발생 시 대기 시간 없이 즉시 통과
        pass

    time.sleep(0.1)

    # 3단계: 일봉 차트 조회 및 직전 고점/저점 계산 (비필수 - 재시도 없음, 기간 단축 40일)
    swing_high = None
    swing_low = None
    try:
        end_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
        start_date = (datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(days=40)).strftime("%Y%m%d")
        response = SESSION.get(
            url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
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
            
            # 최근 20영업일 동안의 단순 최고가 및 최저가를 직전 고점/저점으로 반영
            recent_candles = formatted_candles[-20:]
            if recent_candles:
                swing_high = max(day["high"] for day in recent_candles)
                swing_low = min(day["low"] for day in recent_candles)
    except Exception:
        # 에러 발생 시 대기 시간 없이 즉시 통과
        pass

    return {
        "현재가": sf(output.get("stck_prpr")),
        "PER": sf(output.get("per")),
        "PBR": sf(output.get("pbr")),
        "EPS": sf(output.get("eps")),
        "BPS": sf(output.get("bps")),
        "배당수익률": sf(output.get("dydt")),
        "52주 최고가": sf(output.get("w52_hgpr")),
        "52주 최저가": sf(output.get("w52_lwpr")),
        "업종PER": sf(output.get("bts_per")),
        "추정PER": sf(opinion.get("est_per")),
        "추정EPS": sf(opinion.get("est_eps")),
        "목표주가": sf(opinion.get("dstn_prce")) or sf(output.get("dstn_prce")),
        "의견": opinion.get("invt_opnn_nm"),
        "직전고점": sf(swing_high),
        "직전저점": sf(swing_low),
    }


def is_kr_ticker(ticker: str) -> bool:
    if not ticker:
        return False
    return (ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith((".T", ".TA", ".TW"))


def build_finance_update_for_page(page, token: str):
    """개별 노션 페이지의 티커를 추출하여 데이터를 수집하고 구조화합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not is_kr_ticker(ticker):
        return None

    # 1. 오늘 이미 업데이트 되었는지 확인하여 중복 수집 방지 (스마트 캐싱)
    last_update_prop = props.get("마지막 업데이트", {}).get("date", {})
    if last_update_prop:
        last_update_val = last_update_prop.get("start")
        if last_update_val:
            last_update_date = last_update_val.split("T")[0]
            today_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
            if last_update_date == today_date:
                print(f"   ⏭️ [{ticker}] 오늘 이미 업데이트됨 (스킵)")
                return None

    data = get_finance_data(ticker, token)
    if not data:
        print(f"⚠️ [{ticker}] 재무 데이터 미수신")
        return None

    num_fields = [
        "현재가", "PER", "PBR", "EPS", "BPS", "배당수익률",
        "52주 최고가", "52주 최저가", "업종PER", "추정PER", "추정EPS", "목표주가",
        "직전고점", "직전저점",
    ]

    # 노션 데이터베이스 스키마에 컬럼이 있는 경우에만 안전하게 매핑 (에러 방지)
    update_props = {
        field: {"number": data[field]}
        for field in num_fields
        if data.get(field) is not None and field in props
    }
    
    if data.get("의견"):
        update_props["목표가 범위"] = {"select": {"name": data["의견"]}}
    
    if "마지막 업데이트" in props:
        # 🌟 타임존 이슈 원천 봉쇄: 해외 리눅스 서버에서도 무조건 Asia/Seoul 타임존의 ISO 포맷 강제 추출
        now_str = datetime.now(ZoneInfo("Asia/Seoul")).isoformat()
        update_props["마지막 업데이트"] = {"date": {"start": now_str}}

    if not update_props:
        print(f"⚠️ [{ticker}] 업데이트할 유효한 데이터 없음")
        return None

    preview = ", ".join([f"{k}={v}" for k, v in list(data.items())[:3]])
    
    # 실시간 수집 완료 로그 출력
    curr_price_str = f"{int(data['현재가']):,}" if data.get('현재가') else 'None'
    swing_high_str = f"{int(data['직전고점']):,}" if data.get('직전고점') else 'None'
    swing_low_str = f"{int(data['직전저점']):,}" if data.get('직전저점') else 'None'
    print(f"   ✅ [Collect] {ticker} 완료 (현재가: {curr_price_str}원, 직전고점: {swing_high_str}, 직전저점: {swing_low_str})")

    # 🌟 불필요한 대기 시간(0.2s) 제거로 전체 성능 대폭 향상
    return (page["id"], ticker, update_props, preview)


def batch_collect_finance_data(pages: list, token: str, max_workers: int = 3):
    updates = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(build_finance_update_for_page, page, token): page for page in pages}
        
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


def batch_update_finance_pages(notion_client, updates: list, batch_size: int = 10, delay_between_batches: float = 0.3):
    if not updates:
        return
    
    print(f"📦 [{len(updates)}개 항목] 재무 정보 배치 업데이트 시작 (배치 크기: {batch_size})")
    success_count = 0
    fail_count = 0
    
    for batch_idx, i in enumerate(range(0, len(updates), batch_size), 1):
        chunk = updates[i : i + batch_size]
        print(f"   📤 배치 {batch_idx}/{(len(updates) + batch_size - 1) // batch_size} 처리 중 ({len(chunk)}개)...")
        
        # 🌟 노션 API 속도 최적화를 위해 max_workers=5로 패싱
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


def main() -> None:
    notion = build_notion_client(NOTION_TOKEN)
    token = get_access_token()
    if not token:
        print("❌ KIS 액세스 토큰을 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    print("🚀 한투 재무 정보 대량 업데이트 시작...")
    all_pages = []
    
    print("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.4):
        all_pages.append(page)
    
    print(f"📊 총 {len(all_pages)}개 항목 발견")
    
    batch_collect_size = 15
    updates = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        print(f"\n🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_finance_data(batch, token, max_workers=6)
        updates.extend(batch_updates)
        
        if i + batch_collect_size < len(all_pages):
            time.sleep(0.5)  # 🌟 대기 시간 단축 (1.5s -> 0.5s)
    
    if updates:
        print(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_finance_pages(notion, updates, batch_size=10, delay_between_batches=0.5)
    else:
        print("⚠️ 업데이트할 항목이 없습니다.")


if __name__ == "__main__":
    main()