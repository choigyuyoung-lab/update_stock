import json
import math
import time
import requests
from datetime import timedelta, timezone
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

notion = build_notion_client(NOTION_TOKEN)

def is_valid(value):
    if value is None:
        return False
    try:
        if isinstance(value, str):
            return False
        return not (math.isnan(value) or math.isinf(value))
    except (TypeError, ValueError):
        return False


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


def get_price_data(ticker: str, token: str, max_retries: int = 3, base_delay: float = 2.0) -> dict:
    """
    한투 API에서 국내 주식 가격 데이터를 조회합니다.
    서버 에러(500, 429 등)가 발생하면 2~3초 대기 후 최대 3번까지 재시도합니다.
    """
    clean_ticker = ticker.split(".")[0]
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100",
        "custtype": "P",
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker}
    
    attempt = 1
    while attempt <= max_retries:
        try:
            response = SESSION.get(
                url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers={**headers},
                params=params,
                timeout=10,
            )
            
            status = response.status_code
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * attempt
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
            
        except requests.exceptions.Timeout as exc:
            if attempt < max_retries:
                delay = base_delay * attempt
                print(f"   ⚠️ [{ticker}] KIS API 타임아웃 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            print(f"❌ [{ticker}] KIS API 요청 타임아웃 (최대 재시도 초과): {exc}")
            return {}
            
        except requests.exceptions.ConnectionError as exc:
            if attempt < max_retries:
                delay = base_delay * attempt
                print(f"   ⚠️ [{ticker}] KIS API 연결 에러 재시도 {attempt}/{max_retries}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            print(f"❌ [{ticker}] KIS API 연결 실패 (최대 재시도 초과): {exc}")
            return {}
            
        except requests.exceptions.HTTPError as exc:
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
            print(f"❌ [{ticker}] KIS API 요청 실패 (시도 {attempt}/{max_retries}): {exc}")
            return {}
            
        except ValueError as exc:
            print(f"❌ [{ticker}] KIS API 응답 파싱 실패: {exc}")
            return {}
            
    print(f"❌ [{ticker}] KIS API: 최대 재시도 횟수 초과")
    return {}


def build_update_for_page(page, token: str):
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker:
        return None

    is_kr = ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())
    if not is_kr or ticker.endswith((".T", ".TA", ".TW")):
        return None

    price_data = get_price_data(ticker, token)
    if not price_data:
        print(f"⚠️ [{ticker}] 가격 데이터 미수신")
        return None

    update_props: dict = {}
    if is_valid(price_data.get("현재가")):
        update_props["현재가"] = {"number": price_data["현재가"]}
    if is_valid(price_data.get("전일 종가")):
        update_props["전일 종가"] = {"number": price_data["전일 종가"]}
    if "마지막 업데이트" in props:
        update_props["마지막 업데이트"] = {"date": {"start": kst_isoformat()}}

    if not update_props:
        print(f"⚠️ [{ticker}] 업데이트할 유효한 데이터 없음")
        return None

    return (page["id"], ticker, update_props)


def batch_update_pages(notion_client, updates: list, batch_size: int = 5, delay_between_batches: float = 0.25):
    if not updates:
        return
    for i in range(0, len(updates), batch_size):
        chunk = updates[i : i + batch_size]
        with ThreadPoolExecutor(max_workers=len(chunk)) as exe:
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


def main() -> None:
    notion = build_notion_client(NOTION_TOKEN)
    token = get_access_token()
    if not token:
        print("❌ KIS 액세스 토큰을 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    print("🚀 한투 가격 정보 실시간 수집 및 배치 업데이트 시작...")
    updates = []
    batch_threshold = 20  
    
    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        item = build_update_for_page(page, token)
        
        if item:
            updates.append(item)
            # 🛡️ [지연 보완] 한국 주식 데이터 수집이 성공적으로 진행되었다면, 
            # 모의투자 초당 2건 제한 규칙(500ms)을 안전하게 준수하기 위해 0.5초 대기를 보장합니다.
            time.sleep(0.5)
        else:
            # 주식이 아니거나 업데이트 대상이 아닐 때는 노션 스캔 제한 부하 유지를 위해 짧게 대기
            time.sleep(0.1)
            
        # 버퍼 플러시 분기점
        if len(updates) >= batch_threshold:
            batch_update_pages(notion, updates, batch_size=5, delay_between_batches=0.25)
            updates.clear()
            
    # 남아있는 잔여 데이터 처리
    if updates:
        batch_update_pages(notion, updates, batch_size=5, delay_between_batches=0.25)
        
    print("✨ 모든 국내 주식 현재가 업데이트 프로세스가 완료되었습니다.")


if __name__ == "__main__":
    main()