import os, time, math, requests, json
from datetime import datetime, timedelta, timezone
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 및 KIS API 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_VTS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_VTS_APP_SECRET")

# 모의투자 공식 접속 주소 및 포트
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

notion = Client(auth=NOTION_TOKEN)

# ---------------------------------------------------------
# 2. 유틸리티 함수
# ---------------------------------------------------------
def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

# ---------------------------------------------------------
# 3. KIS API 호출 엔진 (가격 전용)
# ---------------------------------------------------------
def get_access_token():
    """OAuth2.0 접근 토큰 발급"""
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET
    }
    try:
        res = requests.post(url, headers=headers, data=json.dumps(body))
        return res.json().get('access_token')
    except Exception as e:
        print(f"❌ 토큰 발급 에러: {e}")
        return None

def get_price_data_api(ticker, token):
    """현재가와 전일 종가만 가져오는 최적화 API 호출"""
    # 접미사(.KS 등)만 제거하여 영문 포함 본래 티커 보존
    clean_ticker = ticker.split('.')[0]
    
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100", # 주식 현재가 시세 TR
        "custtype": "P"
    }
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": clean_ticker
    }
    
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json().get('output', {})
        
        return {
            "현재가": float(data.get('stck_prpr', 0)) if data.get('stck_prpr') else None,
            "전일 종가": float(data.get('stck_sdpr', 0)) if data.get('stck_sdpr') else None
        }
    except Exception as e:
        print(f"   ⚠️ [API 에러] {ticker}: {e}")
        return {}

# ---------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    print(f"⚡ [Price Update] KIS API 시작 - {datetime.now(kst)}")
    
    token = get_access_token()
    if not token:
        print("❌ 토큰 발급 실패. 환경 변수 또는 서버 주소를 확인하세요.")
        return

    next_cursor = None
    success_cnt = 0

    while True:
        try:
            # 가격 업데이트는 속도가 중요하므로 한 번에 100개씩 처리
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
            print(f"❌ 노션 연결 실패: {e}")
            break

        pages = res.get("results", [])
        for page in pages:
            props = page["properties"]
            ticker = ""
            is_kr = False
            
            # 🌟 [기존 로직 보존] 티커 판별 규칙
            for name in ["티커", "Ticker"]:
                if name in props:
                    content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        # 시행착오 끝에 완성하신 소중한 판별 로직입니다.
                        is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
                        break
            
            if not ticker or not is_kr: continue

            # API 데이터 수집
            price_data = get_price_data_api(ticker, token)

            upd = {}
            if is_valid(price_data.get("현재가")):
                upd["현재가"] = {"number": price_data["현재가"]}
            if is_valid(price_data.get("전일 종가")):
                upd["전일 종가"] = {"number": price_data["전일 종가"]}

            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
            
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    print(f"   ✅ [Price: {ticker}] 완료")
                    success_cnt += 1
            except Exception as e:
                print(f"   ❌ [{ticker}] 전송 실패: {e}")
            
            # 초당 요청 제한 준수를 위한 미세 대기
            time.sleep(0.1)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")
        time.sleep(1)

    print(f"\n✨ 가격 업데이트 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
