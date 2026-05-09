import os, re, time, logging, json, requests
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = os.environ.get("BENCHMARK_DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

# 모의투자 공식 주소
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_kis_token():
    """토큰 발급 및 서버 상태 확인"""
    url = f"{URL_BASE}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials", 
        "appkey": KIS_APP_KEY, 
        "appsecret": KIS_APP_SECRET
    }
    
    try:
        # json= 인자를 사용하여 객체를 바로 전송
        res = requests.post(url, headers=headers, json=body, timeout=15)
        
        logger.info(f"📡 [Auth] HTTP 상태 코드: {res.status_code}")
        
        if not res.text.strip():
            logger.error("❌ [Auth] 서버 응답이 텅 비어있습니다. (점검 가능성 99%)")
            return None
            
        if res.status_code != 200:
            logger.error(f"❌ [Auth] 토큰 발급 실패! 서버 응답: {res.text[:300]}")
            return None
            
        return res.json().get('access_token')
        
    except Exception as e:
        logger.error(f"❌ [Auth] 통신 중 에러 발생: {e}")
        return None

def get_stock_info(ticker, token):
    """현재가 시세 API를 이용한 정보 수집"""
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100",
        "custtype": "P"
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": ticker}
    
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()
        if data.get('rt_cd') != '0':
            return None, data.get('msg1')
        return data.get('output'), None
    except:
        return None, "통신 에러"

def main():
    logger.info("🚀 업데이트 프로세스 시작")
    
    # 1. 토큰 발급
    token = get_kis_token()
    if not token:
        logger.error("🛑 토큰을 받지 못해 작업을 중단합니다.")
        return

    # 2. 노션 연결
    notion = Client(auth=NOTION_TOKEN)
    
    # 3. 데이터 로드 및 루프
    try:
        res = notion.databases.query(database_id=MASTER_DATABASE_ID)
        pages = res.get("results", [])
        logger.info(f"📡 노션에서 {len(pages)}개 종목을 찾았습니다.")
    except Exception as e:
        logger.error(f"❌ 노션 쿼리 실패: {e}")
        return

    for page in pages:
        props = page["properties"]
        ticker = ""
        for name in ["티커", "Ticker"]:
            if name in props:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content:
                    ticker = content[0].get("plain_text", "").strip().upper().split('.')[0]
                    break
        
        if not (ticker and len(ticker) == 6): continue

        output, err = get_stock_info(ticker, token)
        if output:
            m_raw = output.get('rprs_mrkt_kor_name', '')
            is_etf = "ETF" in m_raw.upper()
            market_label = "ETF(KR)" if is_etf else ("KOSPI" if "KOSPI" in m_raw.upper() else "KOSDAQ")
            industry = output.get('bstp_kor_isnm', '기타')

            upd = {
                "Market": {"select": {"name": market_label}},
                "KR_섹터": {"rich_text": [{"text": {"content": industry if not is_etf else "ETF"}}]},
                "KR_산업": {"rich_text": [{"text": {"content": industry if not is_etf else "ETF"}}]},
                "업데이트 일자": {"date": {"start": datetime.now(timezone(timedelta(hours=9))).isoformat()}}
            }
            notion.pages.update(page_id=page["id"], properties=upd)
            logger.info(f"✅ {ticker} 업데이트 완료")
        
        time.sleep(0.5) # 안전을 위해 0.5초 대기

if __name__ == "__main__":
    main()
