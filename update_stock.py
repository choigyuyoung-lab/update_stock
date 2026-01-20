import os
import requests
from notion_client import Client
import time

# 1. Notion 및 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def get_domestic_price(ticker):
    """국내 주식(KOSPI/KOSDAQ) API 호출"""
    # 네이버 실시간 시세 API 주소
    url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{ticker}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        # 현재가 데이터 추출 (nv)
        price = data['result']['areas'][0]['datas'][0]['nv']
        return float(price)
    except Exception as e:
        print(f"국내 API 오류 ({ticker}): {e}")
        return None

def get_overseas_price(ticker, market):
    """해외 주식(NYSE/NASDAQ 등) API 호출"""
    # 티커 정제 (예: PATH.K -> PATH / CSCO.O -> CSCO.O)
    clean_ticker = ticker.split('.')[0]
    
    # 나스닥 종목은 티커 뒤에 .O를 붙여야 네이버 API가 인식함
    if market != "NYSE":
        clean_ticker = f"{clean_ticker}.O"
    
    url = f"https://api.stock.naver.com/stock/{clean_ticker}/basic"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        # 종가/현재가 데이터 추출 (closePrice)
        price_str = str(data['closePrice']).replace(",", "")
        return float(price_str)
    except Exception as e:
        print(f"해외 API 오류 ({clean_ticker}): {e}")
        return None

def main():
    print("🚀 네이버 데이터 API를 통해 업데이트를 시작합니다.")
    try:
        response = notion.databases.query(database_id=DATABASE_ID)
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            
            # Market 정보 가져오기
            market_data = props.get("Market", {}).get("select")
            market = market_data.get("name") if market_data else ""
            
            # 티커 정보 가져오기 (제목 속성)
            ticker_data = props.get("티커", {}).get("title", [])
            raw_ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
            
            if market and raw_ticker:
                # 시장 구분별로 다른 API 호출
                if market in ["KOSPI", "KOSDAQ"]:
                    price = get_domestic_price(raw_ticker)
                else:
                    price = get_overseas_price(raw_ticker, market)
                
                if price is not None:
                    # 노션 현재가 열 업데이트
                    notion.pages.update(
                        page_id=page["id"],
                        properties={"현재가": {"number": price}}
                    )
                    print(f"✅ {raw_ticker} ({market}) -> {price}원 업데이트 완료")
                else:
                    print(f"⚠️ {raw_ticker} ({market}) 데이터 추출 실패")
                
                # API 부하 방지 및 안전한 실행을 위해 대기
                time.sleep(0.5)
    except Exception as e:
        print(f"메인 프로세스 에러: {e}")

if __name__ == "__main__":
    main()
