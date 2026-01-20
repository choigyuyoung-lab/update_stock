import os
import requests
from bs4 import BeautifulSoup
from notion_client import Client

# GitHub Secrets 설정
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["DATABASE_ID"]

notion = Client(auth=NOTION_TOKEN)

def build_naver_url(market, ticker):
    """노션 수식과 동일한 로직으로 네이버 증권 URL을 생성합니다."""
    if market in ["KOSPI", "KOSDAQ"]:
        return f"https://stock.naver.com/domestic/stock/{ticker}/price"
    elif market == "NYSE":
        return f"https://stock.naver.com/worldstock/stock/{ticker}/price"
    else:
        # 그 외(나스닥 등)는 티커 뒤에 .O가 붙는 로직 반영
        return f"https://stock.naver.com/worldstock/stock/{ticker}.O/price"

def get_naver_price(url):
    """URL에서 현재가를 추출합니다."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        price_tag = soup.select_one(".no_today .blind")
        return int(price_tag.text.replace(",", "")) if price_tag else None
    except:
        return None

def main():
    pages = notion.databases.query(database_id=DATABASE_ID).get("results")
    
    for page in pages:
        props = page["properties"]
        
        # 1. Market(선택 또는 텍스트)과 티커(텍스트) 가져오기
        # Market이 '선택(Select)' 타입인 경우를 가정합니다.
        market_data = props.get("Market", {}).get("select")
        market = market_data.get("name") if market_data else ""
        
        # 티커가 '텍스트(rich_text)' 타입인 경우
        ticker_list = props.get("티커", {}).get("rich_text", [])
        ticker = ticker_list[0].get("plain_text", "") if ticker_list else ""
        
        if market and ticker:
            # 2. 수식 로직에 따라 URL 생성
            target_url = build_naver_url(market, ticker)
            price = get_naver_price(target_url)
            
            if price:
                # 3. 현재가 열 업데이트
                notion.pages.update(
                    page_id=page["id"],
                    properties={"현재가": {"number": price}}
                )
                print(f"성공: {market}({ticker}) -> {price}원")
            else:
                print(f"실패: {target_url}에서 정보를 찾을 수 없음")

if __name__ == "__main__":
    main()
