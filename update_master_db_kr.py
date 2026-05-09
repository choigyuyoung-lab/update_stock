import os, re, time, logging, json, requests
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

URL_BASE = "https://openapivts.koreainvestment.com:29443" 

# 로그 레벨을 INFO로 설정하여 진행 상황 확인
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class KISDebugEngine:
    def __init__(self):
        self.token = self._get_token()
        self.headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P"
        }

    def _get_token(self):
        url = f"{URL_BASE}/oauth2/tokenP"
        try:
            res = requests.post(url, json={"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET})
            return res.json().get('access_token')
        except: return None

    def get_stock_debug_info(self, ticker):
        """시세 API를 통해 원본 데이터를 가져오고 로그에 출력"""
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": ticker}
        headers = {**self.headers, "tr_id": "FHKST01010100"}
        
        try:
            res = requests.get(url, headers=headers, params=params, timeout=10)
            data = res.json()
            
            # 🔍 디버깅용 로그: KIS에서 받은 실제 응답 확인
            if data.get('rt_cd') != '0':
                logger.warning(f"⚠️ {ticker} KIS 응답 에러: {data.get('msg1')}")
                return None
            
            return data.get('output')
        except Exception as e:
            logger.error(f"❌ {ticker} 호출 중 물리적 에러: {e}")
            return None

def main():
    engine = KISDebugEngine()
    if not engine.token:
        logger.error("❌ KIS 토큰 발급 실패. 환경 변수를 확인하세요.")
        return
        
    client = Client(auth=NOTION_TOKEN)
    
    # 마스터 DB 모든 페이지 로드
    all_pages = []
    cursor = None
    logger.info("📡 노션 데이터베이스 읽는 중...")
    while True:
        res = client.databases.query(database_id=MASTER_DATABASE_ID, start_cursor=cursor)
        all_pages.extend(res.get("results", []))
        if not res.get("has_more"): break
        cursor = res.get("next_cursor")

    logger.info(f"🚀 총 {len(all_pages)}개 종목 분석 시작 (기초 정보 전용)")

    for page in all_pages:
        pid = page["id"]
        props = page["properties"]
        
        # 1. 티커 파싱
        ticker = ""
        for name in ["티커", "Ticker"]:
            if name in props:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content:
                    ticker = content[0].get("plain_text", "").strip().upper().split('.')[0]
                    break
        
        if not (ticker and len(ticker) == 6):
            continue

        # 2. KIS API 데이터 호출
        item = engine.get_stock_debug_info(ticker)
        
        if not item:
            logger.warning(f"⏩ {ticker}: 데이터를 가져오지 못함 (Skip)")
            time.sleep(1) # 에러 시에도 서버 부하 방지 위해 대기
            continue

        # 3. 데이터 추출
        # rprs_mrkt_kor_name: 대표 시장명 (KOSPI/KOSDAQ)
        # bstp_kor_isnm: 업종 한글명 (섹터/산업 대용)
        m_raw = item.get('rprs_mrkt_kor_name', '알수없음')
        is_etf = "ETF" in m_raw.upper()
        market_label = "ETF(KR)" if is_etf else ("KOSPI" if "KOSPI" in m_raw.upper() else "KOSDAQ")
        industry_name = item.get('bstp_kor_isnm', '기타')

        # 4. 노션 업데이트 (기초 4종 세트)
        def make_rt(v): return {"rich_text": [{"text": {"content": str(v)}}]}
        
        upd = {
            "Market": {"select": {"name": market_label}},
            "KR_섹터": make_rt(industry_name if not is_etf else "ETF"),
            "KR_산업": make_rt(industry_name if not is_etf else "ETF"),
            "업데이트 일자": {"date": {"start": datetime.now(timezone(timedelta(hours=9))).isoformat()}}
        }

        try:
            client.pages.update(page_id=pid, properties=upd)
            logger.info(f"✅ {ticker} | 시장: {market_label} | 업종: {industry_name}")
        except Exception as e:
            logger.error(f"❌ {ticker} 노션 업데이트 실패: {e}")
            
        # 🌟 모의투자 서버 배려를 위한 1초 대기
        time.sleep(1)

if __name__ == "__main__":
    main()
