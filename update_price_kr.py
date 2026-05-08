import os, time, math, requests, json
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 변수 설정 (GitHub Secrets 이름과 일치)
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

# 모의투자 공식 주소 및 포트
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except: return False

def get_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        return res.json().get('access_token')
    except: return None

def get_price_data(ticker, token):
    clean_ticker = ticker.split('.')[0]
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100",
        "custtype": "P"
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker}
    try:
        res = requests.get(url=f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", headers=headers, params=params)
        out = res.json().get('output', {})
        return {
            "현재가": float(out.get('stck_prpr', 0)) if out.get('stck_prpr') else None,
            "전일 종가": float(out.get('stck_sdpr', 0)) if out.get('stck_sdpr') else None
        }
    except: return {}

def main():
    kst = timezone(timedelta(hours=9))
    token = get_access_token()
    if not token:
        print("❌ 토큰 발급 실패. 키 설정을 확인하세요.")
        return

    next_cursor = None
    while True:
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        pages = res.get("results", [])
        for page in pages:
            props = page["properties"]
            ticker = ""
            for name in ["티커", "Ticker"]:
                if name in props:
                    content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        break
            
            # 사용자님의 티커 분류 규칙 유지
            is_kr = ticker and (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
            if not is_kr: continue

            price_data = get_price_data(ticker, token)
            upd = {}
            if is_valid(price_data.get("현재가")): upd["현재가"] = {"number": price_data["현재가"]}
            if is_valid(price_data.get("전일 종가")): upd["전일 종가"] = {"number": price_data["전일 종가"]}
            if "마지막 업데이트" in props: upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
            
            if upd:
                notion.pages.update(page_id=page["id"], properties=upd)
                print(f"✅ [Price] {ticker} 업데이트 완료")
            time.sleep(0.1)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

if __name__ == "__main__":
    main()
