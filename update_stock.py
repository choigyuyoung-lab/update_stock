import os
import requests
from bs4 import BeautifulSoup
from notion_client import Client
import time

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def get_naver_price(url, is_overseas=False):
    """국내(PC 클래식)와 해외(모바일) 각각에 맞는 방식으로 주가를 추출합니다."""
    try:
        # 모바일 페이지에 접속할 때는 모바일 브라우저인 것처럼 속여야 합니다.
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
        }
        res = requests.get(url, headers=headers, timeout=10)
        
        # 국내 클래식 페이지는 EUC-KR, 모바일은 UTF-8을 주로 사용합니다.
        res.encoding = 'utf-8' if is_overseas else 'euc-kr'
        soup = BeautifulSoup(res.text, 'html.parser')
        
        if is_overseas:
            # 모바일 해외 주식 페이지 (m.stock.naver.com)
            # 가격 정보가 들어있는 클래스 (실시간으로 변동되는 해시값이 포함될 수 있어 부분 일치 선택자 사용)
            price_tag = soup.select_one("span[class^='StockEndPrice_price']") 
            # 만약 위 태그로 안 잡힐 경우를 대비한 2차 선택자
            if not price_tag:
                price_tag = soup.select_one(".GraphMain_price__")
        else:
            # 국내 PC 클래식 페이지 (finance.naver.com)
            price_tag = soup.select_one(".no_today .blind")
            
        if price_tag:
            price_str = price_tag.text.strip().replace(",", "")
            return float(price_str)
        return None
    except Exception as e:
        print(f"❌ 접속 에러 ({url}): {e}")
        return None

def main():
    print("🚀 업데이트 시작 (국내: 클래식 / 해외: 모바일)")
    response = notion.databases.query(database_id=DATABASE_ID)
    pages = response.get("results", [])
    
    for page in pages:
        props = page["properties"]
        
        # 1. Market 정보
        market_data = props.get("Market", {}).get("select")
        market = market_data.get("name") if market_data else ""
        
        # 2. 티커 정보 (제목 속성)
        ticker_data = props.get("티커", {}).get("title", [])
        raw_ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
        
        if market and raw_ticker:
            is_overseas = market not in ["KOSPI", "KOSDAQ"]
            
            if not is_overseas:
                # 국내: PC 클래식 주소
                url = f"https://finance.naver.com/item/main.naver?code={raw_ticker}"
            else:
                # 해외: 사용자 제안 모바일 주소
                # 티커 정제 (PATH.K -> PATH.O 등 네이버 모바일 형식에 맞춤)
                clean_ticker = raw_ticker.split(".")[0]
                symbol = f"{clean_ticker}.O" if market != "NYSE" else clean_ticker
                url = f"https://m.stock.naver.com/worldstock/stock/{symbol}/total"
            
            price = get_naver_price(url, is_overseas)
            
            if price is not None:
                notion.pages.update(
                    page_id=page["id"],
                    properties={"현재가": {"number": price}}
                )
                print(f"✅ {raw_ticker} 업데이트 완료: {price}")
            else:
                print(f"⚠️ {raw_ticker} 가격 추출 실패 (URL: {url})")
            
            time.sleep(1)

if __name__ == "__main__":
    main()
