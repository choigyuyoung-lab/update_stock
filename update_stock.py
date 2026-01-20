import os
import requests
from notion_client import Client
import time
from datetime import datetime, timedelta

# 1. Notion 및 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def get_domestic_price(ticker):
    """국내 주식(KOSPI/KOSDAQ) API 호출"""
    url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{ticker}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        price = data['result']['areas'][0]['datas'][0]['nv']
        return float(price)
    except:
        return None

def get_overseas_price(ticker, market):
    """해외 주식(NYSE/NASDAQ 등) API 호출"""
    clean_ticker = ticker.split('.')[0]
    if market != "NYSE":
        clean_ticker = f"{clean_ticker}.O"
    
    url = f"https://api.stock.naver.com/stock/{clean_ticker}/basic"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        price_str = str(data['closePrice']).replace(",", "")
        return float(price_str)
    except:
        return None

def main():
    # 한국 시간(KST) 계산
    now = datetime.utcnow() + timedelta(hours=9)
    now_iso = now.isoformat()
    now_display = now.strftime('%Y-%m-%d %H:%M:%S')

    print(f"🚀 전체 종목 업데이트 시작 (KST: {now_display})")
    
    has_more = True
    next_cursor = None
    total_count = 0

    while has_more:
        try:
            # 페이지네이션 처리: start_cursor를 사용하여 다음 100개를 가져옵니다.
            response = notion.databases.query(
                database_id=DATABASE_ID,
                start_cursor=next_cursor
            )
            pages = response.get("results", [])
            
            for page in pages:
                props = page["properties"]
                
                market_data = props.get("Market", {}).get("select")
                market = market_data.get("name") if market_data else ""
                
                ticker_data = props.get("티커", {}).get("title", [])
                raw_ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                
                if market and raw_ticker:
                    if market in ["KOSPI", "KOSDAQ"]:
                        price = get_domestic_price(raw_ticker)
                    else:
                        price = get_overseas_price(raw_ticker, market)
                    
                    if price is not None:
                        # 현재가 및 마지막 업데이트 시간 동시 기록
                        notion.pages.update(
                            page_id=page["id"],
                            properties={
                                "현재가": {"number": price},
                                "마지막 업데이트": {"date": {"start": now_iso}}
                            }
                        )
                        total_count += 1
                        if total_count % 10 == 0:
                            print(f"진행 중... {total_count}개 완료")
                    
                    # Notion API 속도 제한 준수 (초당 약 3개 권장)
                    time.sleep(0.4) 
            
            # 다음 페이지가 있는지 확인
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            break

    print(f"✨ 총 {total_count}개의 종목 업데이트가 완료되었습니다!")

if __name__ == "__main__":
    main()
