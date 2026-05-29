import json
import math
import time
import requests
from datetime import timedelta, timezone

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    kst_isoformat,
    paginate_database,
    safe_page_update,
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


def get_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    try:
        response = SESSION.post(url, data=json.dumps(body), timeout=10)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.RequestException as exc:
        print(f"❌ KIS 토큰 발급 실패: {exc}")
        return None
    except ValueError as exc:
        print(f"❌ KIS 토큰 파싱 실패: {exc}")
        return None


def get_price_data(ticker: str, token: str) -> dict:
    clean_ticker = ticker.split(".")[0]
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100",
        "custtype": "P",
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker}

    try:
        response = SESSION.get(
            url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=headers,
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        out = response.json().get("output", {})
        return {
            "현재가": float(out.get("stck_prpr")) if out.get("stck_prpr") else None,
            "전일 종가": float(out.get("stck_sdpr")) if out.get("stck_sdpr") else None,
        }
    except requests.RequestException as exc:
        print(f"❌ [{ticker}] KIS API 요청 실패: {exc}")
        return {}
    except ValueError as exc:
        print(f"❌ [{ticker}] KIS API 응답 파싱 실패: {exc}")
        return {}

def process_page(page, token: str):
    props = page.get("properties", {})
    ticker = get_page_text(props, ["티커", "Ticker"]).upper()
    if not ticker:
        return

    is_kr = ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())
    if not is_kr or ticker.endswith((".T", ".TA", ".TW")):
        return

    price_data = get_price_data(ticker, token)
    if not price_data:
        print(f"⚠️ [{ticker}] 가격 데이터 미수신")
        return

    update_props = {}
    if is_valid(price_data.get("현재가")):
        update_props["현재가"] = {"number": price_data["현재가"]}
    if is_valid(price_data.get("전일 종가")):
        update_props["전일 종가"] = {"number": price_data["전일 종가"]}
    if "마지막 업데이트" in props:
        update_props["마지막 업데이트"] = {"date": {"start": kst_isoformat()}}

    if not update_props:
        print(f"⚠️ [{ticker}] 업데이트할 유효한 데이터 없음")
        return

    if safe_page_update(notion, page["id"], update_props):
        print(f"✅ [Price] {ticker} 업데이트 완료")


def main() -> None:
    notion = build_notion_client(NOTION_TOKEN)
    token = get_access_token()
    if not token:
        print("❌ KIS 액세스 토큰을 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        process_page(page, token)
        time.sleep(0.25)


if __name__ == "__main__":
    main()
