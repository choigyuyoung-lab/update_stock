import os, re, time, logging, json, requests
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 변수
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

# 모의투자 주소
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

notion = Client(auth=NOTION_TOKEN)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_kis_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers=headers, data=json.dumps(body))
    return res.json().get('access_token')

def get_master_data(ticker, token):
    """성공했던 update_price_kr.py와 동일한 호출 구조"""
    clean_ticker = ticker.split('.')[0]
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100", # 성공했던 TR ID 사용
        "custtype": "P"
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": clean_ticker}
    
    try:
        # 타임아웃을 넉넉히 15초로 설정 (MCI 오류 방지)
        res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", 
                          headers=headers, params=params, timeout=15)
        data = res.json()
        
        if data.get('rt_cd') != '0':
            return None, data.get('msg1') # 에러 메시지 반환
        return data.get('output'), None
    except Exception as e:
        return None, str(e)

def main():
    token = get_kis_token()
    if not token:
        logger.error("❌ 토큰 발급 실패")
        return

    # 마스터 DB 쿼리
    res = notion.databases.query(database_id=MASTER_DATABASE_ID)
    pages = res.get("results", [])

    for page in pages:
        props = page["properties"]
        ticker = ""
        # 티커 추출 로직 (성공 코드 기반)
        for name in ["티커", "Ticker"]:
            if name in props:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content:
                    ticker = content[0].get("plain_text", "").strip().upper()
                    break
        
        if not ticker: continue

        # KIS 데이터 호출
        output, error_msg = get_master_data(ticker, token)
        
        if error_msg:
            logger.warning(f"⏩ {ticker} 건너뜀: {error_msg}")
            time.sleep(1) # 서버 과부하 방지
            continue

        if output:
            m_raw = output.get('rprs_mrkt_kor_name', '')
            is_etf = "ETF" in m_raw.upper()
            market_label = "ETF(KR)" if is_etf else ("KOSPI" if "KOSPI" in m_raw.upper() else "KOSDAQ")
            industry = output.get('bstp_kor_isnm', '기타')

            # 노션 업데이트
            def make_rt(v): return {"rich_text": [{"text": {"content": str(v)}}]}
            upd = {
                "Market": {"select": {"name": market_label}},
                "KR_섹터": make_rt(industry if not is_etf else "ETF"),
                "KR_산업": make_rt(industry if not is_etf else "ETF"),
                "업데이트 일자": {"date": {"start": datetime.now(timezone(timedelta(hours=9))).isoformat()}}
            }
            notion.pages.update(page_id=page["id"], properties=upd)
            logger.info(f"✅ {ticker} ({industry}) 업데이트 완료")
        
        time.sleep(0.2) # API 호출 간격

if __name__ == "__main__":
    main()
