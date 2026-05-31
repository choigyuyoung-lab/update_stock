import json
import math
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    kst_isoformat,
    paginate_database,
    safe_page_update,
    RETRY_STATUS_CODES,
)

NOTION_TOKEN = get_env_var("NOTION_TOKEN")
DATABASE_ID = get_env_var("DATABASE_ID")
KIS_APP_KEY = get_env_var("KIS_APP_KEY")
KIS_APP_SECRET = get_env_var("KIS_APP_SECRET")
URL_BASE = "https://openapivts.koreainvestment.com:29443"

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})


def sf(value):
    try:
        converted = float(value)
        return converted if converted != 0 else None
    except (TypeError, ValueError):
        return None


def get_access_token(max_retries: int = 3, base_delay: float = 2.0) -> str:
    """
    한투 API 액세스 토큰을 발급받습니다.
    서버 에러(500, 429 등)가 발생하면 재시도합니다.
    """
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}

    attempt = 1
    while attempt <= max_retries:
        try:
            response = SESSION.post(url, data=json.dumps(body), timeout=10)
            
            # 재시도 가능한 서버 에러 확인
            status = response.status_code
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                print(f"   ⚠️ KIS 토큰 재시도 {attempt}/{max_retries} - status={status}")
                time.sleep(base_delay * attempt)
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
            # HTTPError 예외에서 상태 코드 추출
            status = None
            if hasattr(exc, "response") and exc.response is not None:
                status = exc.response.status_code
                
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                print(f"   ⚠️ KIS 토큰 재시도 {attempt}/{max_retries} - status={status}: {exc}")
                time.sleep(base_delay * attempt)
                attempt += 1
                continue
                
            print(f"❌ KIS 토큰 발급 실패 (시도 {attempt}/{max_retries}): {exc}")
            return None
            
        except ValueError as exc:
            print(f"❌ KIS 토큰 파싱 실패: {exc}")
            return None
    
    print(f"❌ KIS 토큰 발급: 최대 재시도 횟수 초과")
    return None


def get_finance_data(ticker: str, token: str, max_retries: int = 3, base_delay: float = 2.0) -> dict:
    """
    한투 API에서 국내 주식 재무 데이터를 조회합니다.
    서버 에러(500, 429 등)가 발생하면 2~3초 대기 후 최대 3번까지 재시도합니다.
    """
    clean_ticker = ticker.split(".")[0]
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "custtype": "P",
    }

    attempt = 1
    while attempt <= max_retries:
        try:
            # 첫 번째 API 호출: 기본 정보
            response = SESSION.get(
                url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers={**headers, "tr_id": "FHKST01010100"},
                params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker},
                timeout=10,
            )
            
            # 상태 코드 확인
            status = response.status_code
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * attempt
                print(f"   ⚠️ [{ticker}] KIS API(기본정보) 재시도 {attempt}/{max_retries} - status={status}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            
            response.raise_for_status()
            output = response.json().get("output", {})

            time.sleep(0.15)
            
            # 두 번째 API 호출: 투자의견
            response = SESSION.get(
                url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/invest-opinion",
                headers={**headers, "tr_id": "HHDFS76700100"},
                params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker},
                timeout=10,
            )
            
            # 상태 코드 확인
            status = response.status_code
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * attempt
                print(f"   ⚠️ [{ticker}] KIS API(투자의견) 재시도 {attempt}/{max_retries} - status={status}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            
            response.raise_for_status()
            output2 = response.json().get("output", [])
            opinion = output2[0] if isinstance(output2, list) and output2 else {}

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
            }
            
        except requests.exceptions.Timeout as exc:
            # 타임아웃 에러는 재시도 가능
            if attempt < max_retries:
                delay = base_delay * attempt
                print(f"   ⚠️ [{ticker}] KIS API 타임아웃 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            print(f"❌ [{ticker}] KIS API 요청 타임아웃 (최대 재시도 초과): {exc}")
            return {}
            
        except requests.exceptions.ConnectionError as exc:
            # 연결 에러도 재시도 가능
            if attempt < max_retries:
                delay = base_delay * attempt
                print(f"   ⚠️ [{ticker}] KIS API 연결 에러 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            print(f"❌ [{ticker}] KIS API 연결 실패 (최대 재시도 초과): {exc}")
            return {}
            
        except requests.exceptions.HTTPError as exc:
            # HTTP 에러에서 상태 코드 추출
            status = None
            if hasattr(exc, "response") and exc.response is not None:
                status = exc.response.status_code
            
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * attempt
                print(f"   ⚠️ [{ticker}] KIS API HTTP {status} 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            
            print(f"❌ [{ticker}] KIS API HTTP 요청 실패 (시도 {attempt}/{max_retries}): {exc}")
            return {}
            
        except requests.RequestException as exc:
            # 기타 요청 에러
            print(f"❌ [{ticker}] KIS API 요청 실패 (시도 {attempt}/{max_retries}): {exc}")
            return {}
            
        except ValueError as exc:
            print(f"❌ [{ticker}] KIS API 응답 파싱 실패: {exc}")
            return {}
            
    print(f"❌ [{ticker}] KIS API: 최대 재시도 횟수 초과")
    return {}



def is_kr_ticker(ticker: str) -> bool:
    if not ticker:
        return False
    return (ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith((".T", ".TA", ".TW"))


def build_finance_update_for_page(page, token: str):
    """개별 페이지의 재무 데이터를 수집하고 업데이트 정보를 반환합니다."""
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not is_kr_ticker(ticker):
        return None

    data = get_finance_data(ticker, token)
    if not data:
        print(f"⚠️ [{ticker}] 재무 데이터 미수신")
        return None

    num_fields = [
        "현재가", "PER", "PBR", "EPS", "BPS", "배당수익률",
        "52주 최고가", "52주 최저가", "업종PER", "추정PER", "추정EPS", "목표주가",
    ]

    update_props = {
        field: {"number": data[field]}
        for field in num_fields
        if data.get(field) is not None
    }
    
    if data.get("의견"):
        update_props["목표가 범위"] = {"select": {"name": data["의견"]}}
    
    if "마지막 업데이트" in props:
        update_props["마지막 업데이트"] = {"date": {"start": kst_isoformat()}}

    if not update_props:
        print(f"⚠️ [{ticker}] 업데이트할 유효한 데이터 없음")
        return None

    preview = ", ".join([f"{k}={v}" for k, v in list(data.items())[:3]])
    return (page["id"], ticker, update_props, preview)


def batch_collect_finance_data(pages: list, token: str, max_workers: int = 5):
    """
    여러 페이지의 재무 데이터를 병렬로 수집합니다.
    ThreadPoolExecutor를 사용하여 API 호출을 동시에 처리합니다.
    """
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
                print(f"❌ [{ticker}] 데이터 수집 중 에러: {exc}")
    
    return updates


def batch_update_finance_pages(notion_client, updates: list, batch_size: int = 10, delay_between_batches: float = 0.3):
    """
    배치 단위로 노션 재무 정보 페이지를 업데이트합니다.
    각 배치는 parallel로 처리되며, 배치 간에는 delay를 두어 API 제한을 준수합니다.
    """
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


def main() -> None:
    notion = build_notion_client(NOTION_TOKEN)
    token = get_access_token()
    if not token:
        print("❌ KIS 액세스 토큰을 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    print("🚀 한투 재무 정보 대량 업데이트 시작...")
    all_pages = []
    
    # 1단계: 모든 페이지 수집
    print("📋 노션 데이터베이스 스캔 중...")
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        all_pages.append(page)
    
    print(f"📊 총 {len(all_pages)}개 항목 발견")
    
    # 2단계: 배치 크기로 그룹화하여 데이터 수집 (병렬화)
    batch_collect_size = 25  # 한 번에 25개씩 병렬 수집 (API 호출 2번 필요하므로 더 작음)
    updates = []
    
    for batch_idx, i in enumerate(range(0, len(all_pages), batch_collect_size), 1):
        batch = all_pages[i : i + batch_collect_size]
        print(f"\n🔄 데이터 수집 배치 {batch_idx}/{(len(all_pages) + batch_collect_size - 1) // batch_collect_size} ({len(batch)}개 항목)")
        
        batch_updates = batch_collect_finance_data(batch, token, max_workers=4)
        updates.extend(batch_updates)
        
        # 배치 간에 짧은 대기 (API 제한 준수)
        if i + batch_collect_size < len(all_pages):
            time.sleep(1.0)
    
    # 3단계: 수집된 데이터를 배치로 노션에 업데이트
    if updates:
        print(f"\n📝 {len(updates)}개 항목을 노션에 업데이트합니다...")
        batch_update_finance_pages(notion, updates, batch_size=10, delay_between_batches=0.3)
    else:
        print("⚠️ 업데이트할 항목이 없습니다.")



if __name__ == "__main__":
    main()
