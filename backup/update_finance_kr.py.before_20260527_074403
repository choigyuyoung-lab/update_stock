import os, time, math, requests, json
from datetime import datetime, timedelta, timezone
from notion_client import Client
from notion_client.errors import HTTPResponseError # 에러 처리를 위해 추가

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

URL_BASE = "https://openapivts.koreainvestment.com:29443" 
notion = Client(auth=NOTION_TOKEN)

# 2. 유틸리티 함수
def sf(v): # 숫자 변환 안전 함수
    try: return float(v) if v and float(v) != 0 else None
    except: return None

def get_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        return res.json().get('access_token')
    except: return None

def get_finance_data(ticker, token):
    clean_ticker = ticker.split('.')[0]
    headers = {"Content-Type":"application/json", "authorization":f"Bearer {token}", "appkey":KIS_APP_KEY, "appsecret":KIS_APP_SECRET, "custtype":"P"}
    try:
        # 1. 시세/재무
        r1 = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", 
                          headers={**headers, "tr_id":"FHKST01010100"}, 
                          params={"fid_cond_mrkt_div_code":"J", "fid_input_iscd":clean_ticker}, timeout=10)
        o1 = r1.json().get('output', {})
        time.sleep(0.15)
        # 2. 의견/추정치
        r2 = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/invest-opinion", 
                          headers={**headers, "tr_id":"HHDFS76700100"}, 
                          params={"fid_cond_mrkt_div_code":"J", "fid_input_iscd":clean_ticker}, timeout=10)
        o2_list = r2.json().get('output', [])
        o2 = o2_list[0] if isinstance(o2_list, list) and len(o2_list) > 0 else {}

        return {
            "현재가": sf(o1.get('stck_prpr')), "PER": sf(o1.get('per')), "PBR": sf(o1.get('pbr')),
            "EPS": sf(o1.get('eps')), "BPS": sf(o1.get('bps')), "배당수익률": sf(o1.get('dydt')),
            "52주 최고가": sf(o1.get('w52_hgpr')), "52주 최저가": sf(o1.get('w52_lwpr')),
            "업종PER": sf(o1.get('bts_per')), "추정PER": sf(o2.get('est_per')),
            "추정EPS": sf(o2.get('est_eps')), "목표주가": sf(o2.get('dstn_prce')) or sf(o1.get('dstn_prce')),
            "의견": o2.get('invt_opnn_nm')
        }
    except: return {}

# 3. 메인 로직 (재시도 기능 포함)
def main():
    kst = timezone(timedelta(hours=9))
    token = get_access_token()
    if not token: return

    next_cursor = None
    num_fields = ["현재가", "PER", "PBR", "EPS", "BPS", "배당수익률", "52주 최고가", "52주 최저가", "업종PER", "추정PER", "추정EPS", "목표주가"]

    while True:
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        for page in res.get("results", []):
            try:
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
                
                # 🌟 [보완] 노션 업데이트 재시도 로직
                if upd:
                    success = False
                    for attempt in range(3): # 최대 3번 시도
                        try:
                            notion.pages.update(page_id=page["id"], properties=upd)
                            print(f"✅ [Finance] {ticker} 완료")
                            success = True
                            break
                        except HTTPResponseError as e:
                            if e.status == 504:
                                print(f"   ⏳ [노션 지연] {ticker} 재시도 중... ({attempt+1}/3)")
                                time.sleep(3) # 3초 쉬고 다시 시도
                            else:
                                raise e # 504 이외의 에러는 밖으로 던짐
                    
                    if not success:
                        print(f"   ❌ [결국 실패] {ticker}: 노션 서버 응답 없음")
                
                time.sleep(0.3) # 노션 부하를 줄이기 위해 간격을 살짝 늘림

            except Exception as e:
                print(f"❌ [에러] {ticker} 스킵: {e}")
                continue

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

if __name__ == "__main__":
    main()
