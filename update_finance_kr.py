import os, time, math, requests, json
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

URL_BASE = "https://openapivts.koreainvestment.com:29443" 
notion = Client(auth=NOTION_TOKEN)

def get_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers=headers, data=json.dumps(body))
    return res.json().get('access_token')

def get_finance_data(ticker, token):
    clean_ticker = ticker.split('.')[0]
    headers = {"Content-Type":"application/json", "authorization":f"Bearer {token}", "appkey":KIS_APP_KEY, "appsecret":KIS_APP_SECRET, "custtype":"P"}
    
    # 1. 시세 및 기본재무
    res1 = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", headers={**headers, "tr_id":"FHKST01010100"}, params={"fid_cond_mrkt_div_code":"J", "fid_input_iscd":clean_ticker})
    out1 = res1.json().get('output', {})
    
    # 2. 투자의견 및 추정치
    time.sleep(0.1)
    res2 = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/invest-opinion", headers={**headers, "tr_id":"HHDFS76700100"}, params={"fid_cond_mrkt_div_code":"J", "fid_input_iscd":clean_ticker})
    out2_list = res2.json().get('output', [])
    out2 = out2_list[0] if isinstance(out2_list, list) and len(out2_list) > 0 else {}

    def sf(v): # Safe Float
        try: return float(v) if v and float(v) != 0 else None
        except: return None

    return {
        "현재가": sf(out1.get('stck_prpr')), "PER": sf(out1.get('per')), "PBR": sf(out1.get('pbr')),
        "EPS": sf(out1.get('eps')), "BPS": sf(out1.get('bps')), "배당수익률": sf(out1.get('dydt')),
        "52주 최고가": sf(out1.get('w52_hgpr')), "52주 최저가": sf(out1.get('w52_lwpr')),
        "업종PER": sf(out1.get('bts_per')), "추정PER": sf(out2.get('est_per')),
        "추정EPS": sf(out2.get('est_eps')), "목표주가": sf(out2.get('dstn_prce')) or sf(out1.get('dstn_prce')),
        "의견": out2.get('invt_opnn_nm')
    }

def main():
    kst = timezone(timedelta(hours=9))
    token = get_access_token()
    if not token: return

    next_cursor = None
    num_fields = ["현재가", "PER", "PBR", "EPS", "BPS", "배당수익률", "52주 최고가", "52주 최저가", "업종PER", "추정PER", "추정EPS", "목표주가"]

    while True:
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        for page in res.get("results", []):
            props = page["properties"]
            ticker = ""
            for name in ["티커", "Ticker"]:
                if name in props:
                    content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                    if content: ticker = content[0].get("plain_text", "").strip().upper(); break
            
            if not ticker or not ((ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))): continue

            data = get_finance_data(ticker, token)
            upd = {f: {"number": data[f]} for f in num_fields if data.get(f) is not None}
            if data.get("의견"): upd["목표가 범위"] = {"select": {"name": data["의견"]}}
            if "마지막 업데이트" in props: upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
            
            if upd:
                notion.pages.update(page_id=page["id"], properties=upd)
                print(f"✅ [Finance] {ticker} 완료")
            time.sleep(0.2)
        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

if __name__ == "__main__":
    main()
