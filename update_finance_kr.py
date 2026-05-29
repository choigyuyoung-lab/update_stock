import json
import math
import time
import requests

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


def sf(value):
    try:
        converted = float(value)
        return converted if converted != 0 else None
    except (TypeError, ValueError):
        return None


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

def get_finance_data(ticker: str, token: str) -> dict:
    clean_ticker = ticker.split(".")[0]
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "custtype": "P",
    }

    try:
        response = SESSION.get(
            url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers={**headers, "tr_id": "FHKST01010100"},
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker},
            timeout=10,
        )
        response.raise_for_status()
        output = response.json().get("output", {})

        time.sleep(0.15)
        response = SESSION.get(
            url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/invest-opinion",
            headers={**headers, "tr_id": "HHDFS76700100"},
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker},
            timeout=10,
        )
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
    except requests.RequestException as exc:
        print(f"❌ [{ticker}] KIS API 요청 실패: {exc}")
        return {}
    except ValueError as exc:
        print(f"❌ [{ticker}] KIS API 응답 파싱 실패: {exc}")
        return {}


def is_kr_ticker(ticker: str) -> bool:
    if not ticker:
        return False
    return (ticker.endswith((".KS", ".KQ")) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith((".T", ".TA", ".TW"))


def main() -> None:
    notion = build_notion_client(NOTION_TOKEN)
    token = get_access_token()
    if not token:
        print("❌ KIS 액세스 토큰을 가져오지 못했습니다. 환경 변수를 확인하세요.")
        return

    num_fields = [
        "현재가",
        "PER",
        "PBR",
        "EPS",
        "BPS",
        "배당수익률",
        "52주 최고가",
        "52주 최저가",
        "업종PER",
        "추정PER",
        "추정EPS",
        "목표주가",
    ]

    for page in paginate_database(notion, DATABASE_ID, page_size=100, retry_delay=0.3):
        props = page.get("properties", {})
        ticker = get_page_text(props, ["티커", "Ticker"]).upper()
        if not is_kr_ticker(ticker):
            continue

        data = get_finance_data(ticker, token)
        if not data:
            print(f"⚠️ [미취득] {ticker}: 데이터 없음")
            continue

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
            print(f"⚠️ [미취득] {ticker}: 노션에 업데이트할 값 없음")
            continue

        preview = ", ".join([f"{k}={v}" for k, v in list(data.items())[:5]])
        print(f"📊 [{ticker}] {preview}... 업데이트 항목 {len(update_props)}개")

        if safe_page_update(notion, page["id"], update_props):
            print(f"✅ [Finance] {ticker} 완료")

        time.sleep(0.25)


if __name__ == "__main__":
    main()
