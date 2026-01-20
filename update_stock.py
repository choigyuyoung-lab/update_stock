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
    response = notion.databases.query(database_id=DATABASE_ID)
    pages = response.get("results", [])
    
    for page in pages:
        props = page["properties"]
        
        # 'Market' (선택 속성)
        market_data = props.get("Market", {}).get("select")
        market = market_data.get("name") if market_data else None
        
        # '티커' (제목 속성 - 이미지 2 기준)
        ticker_data = props.get("티커", {}).get("title", [])
        ticker = ticker_data[0].get("plain_text", "") if ticker_data else ""
        
        if market and ticker:
            # URL 생성 로직
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
                print(f"업데이트: {ticker} -> {price}")
                time.sleep(0.5)

if __name__ == "__main__":
    main()
