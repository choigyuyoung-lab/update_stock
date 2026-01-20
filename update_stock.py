import os
import requests
from bs4 import BeautifulSoup
from notion_client import Client
import time

# 1. GitHub Secrets에서 설정한 토큰과 데이터베이스 ID를 가져옵니다.
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")

# 노션 클라이언트 초기화
notion = Client(auth=NOTION_TOKEN)

def build_naver_url(market, ticker):
    """
    사용자의 노션 수식 로직을 파이썬으로 구현한 함수입니다.
    시장 구분(Market)에 따라 네이버 증권 URL을 생성합니다.
    """
    if market in ["KOSPI", "KOSDAQ"]:
        # 국내 주식 경로
        return f"https://stock.naver.com/domestic/stock/{ticker}/price"
    elif market == "NYSE":
        # 뉴욕 거래소(NYSE) 경로
        return f"https://stock.naver.com/worldstock/stock/{ticker}/price"
    else:
        # 그 외(NASDAQ 등) 티커 뒤에 .O가 붙는 해외 주식 경로
        return f"https://stock.naver.com/worldstock/stock/{ticker}.O/price"

def get_naver_price(url):
    """네이버 증권 웹페이지에서 실시간 현재가를 크롤링합니다."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        }
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status() # 접속 실패 시 에러 발생
        
        soup = BeautifulSoup(res.text, 'html.parser')
        # 네이버 증권의 현재가 숫자 영역 추출
        price_tag = soup.select_one(".no_today .blind")
        
        if price_tag:
            # 쉼표(,) 제거 후 정수형(int)으로 변환
            return int(price_tag.text.replace(",", ""))
        return None
    except Exception as e:
        print(f"크롤링 에러 ({url}): {e}")
        return None

def main():
    try:
        # 데이터베이스에 있는 모든 데이터를 조회합니다.
        response = notion.databases.query(database_id=DATABASE_ID)
        pages = response.get("results", [])
        
        if not pages:
            print("데이터베이스에 데이터가 없거나 ID가 올바르지 않습니다.")
            return

        for page in pages:
            props = page["properties"]
            
            # [중요] 노션의 실제 열 이름과 대소문자/띄어쓰기까지 일치해야 합니다.
            
            # 1. 'Market' 열에서 값 추출 (선택/Select 타입 기준)
            market_data = props.get("Market", {}).get("select")
            market = market_data.get("name") if market_data else None
            
            # 2. '티커' 열에서 값 추출 (텍스트/Rich Text 타입 기준)
            ticker_list = props.get("티커", {}).get("rich_text", [])
            ticker = ticker_list[0].get("plain_text", "") if ticker_list else None
            
            # Market과 티커 정보가 모두 있을 때만 가격 업데이트 실행
            if market and ticker:
                target_url = build_naver_url(market, ticker)
                current_price = get_naver_price(target_url)
                
                if current_price:
                    # 3. '현재가' 열 업데이트 (숫자/Number 타입)
                    notion.pages.update(
                        page_id=page["id"],
                        properties={
                            "현재가": {"number": current_price}
                        }
                    )
                    print(f"성공: {market}({ticker}) -> {current_price}원")
                    
                    # API 호출 제한을 방지하기 위한 짧은 대기 (0.5초)
                    time.sleep(0.5)
                else:
                    print(f"실패: {market}({ticker}) 가격 정보를 가져올 수 없습니다.")
            else:
                print("Market 또는 티커 정보가 누락된 행입니다.")
                
    except Exception as e:
        print(f"실행 중 치명적 오류 발생: {e}")

if __name__ == "__main__":
    main()
