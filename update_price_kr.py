import os, time, math, requests, json
import concurrent.futures
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

# 병렬로 실행할 단일 페이지 처리 함수 분리
def process_page(page, token, kst):
    props = page["properties"]
    ticker = ""
    for name in ["티커", "Ticker"]:
        if name in props:
            content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
            if content:
                ticker = content[0].get("plain_text", "").strip().upper()
                break
                
    is_kr = ticker and (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
    if not is_kr: return

    price_data = get_price_data(ticker, token)
    upd = {}
    if is_valid(price_data.get("현재가")): upd["현재가"] = {"number": price_data["현재가"]}
    if is_valid(price_data.get("전일 종가")): upd["전일 종가"] = {"number": price_data["전일 종가"]}
    if "마지막 업데이트" in props: upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
    
    if upd:
        notion.pages.update(page_id=page["id"], properties=upd)
        print(f"✅ [Price] {ticker} 업데이트 완료")

def main():
    kst = timezone(timedelta(hours=9))
    token = get_access_token()
    if not token:
        print("❌ 토큰 발급 실패. 키 설정을 확인하세요.")
        return

    print("🔍 노션 데이터베이스 페이지 수집 중...")
    pages_to_process = []
    next_cursor = None
    
    # 1. 수정할 페이지를 먼저 한 번에 모두 수집합니다.
    while True:
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        pages_to_process.extend(res.get("results", []))
        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"총 {len(pages_to_process)}개의 페이지를 확인했습니다. 병렬 업데이트를 시작합니다...")

    # 2. ThreadPoolExecutor를 이용해 병렬(스레드)로 처리합니다.
    # 노션 API Rate Limit(초당 약 3회)를 고려하여 작업자(worker) 수를 4 정도로 제한합니다.
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_page, page, token, kst) for page in pages_to_process]
        
        # 처리 중 발생하는 예외를 확인하기 위한 구문
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ 업데이트 중 에러 발생: {e}")

if __name__ == "__main__":
    main()
