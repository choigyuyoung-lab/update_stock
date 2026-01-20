import os
import requests
from bs4 import BeautifulSoup
from notion_client import Client
import time

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def get_naver_price(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        price_tag = soup.select_one(".no_today .blind")
        return int(price_tag.text.replace(",", "")) if price_tag else None
    except:
        return None

def main():
    print("--- 노션 데이터베이스 조회를 시작합니다 ---")
    response = notion.databases.query(database_id=DATABASE_ID)
    pages = response.get("results", [])
    
    if not pages:
        print("데이터베이스에서 페이지를 하나도 찾지 못했습니다. ID를 확인하세요.")
        return

    for page in pages:
        props = page["properties"]
        # 점검용: 노션에서 인식한 모든 열 이름을 출력합니다.
        print(f"찾은 열 이름들: {list(props.keys())}")
        
        # 'Market' 열 확인
        market_data = props.get("Market", {}).get("select")
        market = market_data.get("name") if market_data else None
        
        # '티커' 열 확인 (제목 속성)
        ticker_data = props.get("티커", {}).get("title", [])
        ticker = ticker_data[0].get("plain_text", "") if ticker_data else ""
        
        print(f"읽어온 데이터 -> Market: {market}, 티커: {ticker}")
        
        if market and ticker:
            if market in ["KOSPI", "KOSDAQ"]:
                url = f"https://stock.naver.com/domestic/stock/{ticker}/price"
            elif market == "NYSE":
                url = f"https://stock.naver.com/worldstock/stock/{ticker}/price"
            else:
                url = f"https://stock.naver.com/worldstock/stock/{ticker}.O/price"
            
            price = get_naver_price(url)
            if price:
                notion.pages.update(
                    page_id=page["id"],
                    properties={"현재가": {"number": price}}
                )
                print(f"✅ 업데이트 성공: {ticker} -> {price}원")
            else:
                print(f"❌ 가격 추출 실패: {url}")
        else:
            print("⚠️ Market 또는 티커 데이터가 비어있어 건너뜁니다.")
        print("-" * 30)

if __name__ == "__main__":
    main()
