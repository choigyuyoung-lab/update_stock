import os
import requests
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

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
    """해외 주식 API 호출 - 시장별 특성에 맞춰 심볼 최적화"""
    symbol = ticker
    
    # [지능형 심볼 처리]
    # 1. 나스닥(NASDAQ) 종목인데 마침표(.)가 없다면 자동으로 .O를 붙여줍니다.
    if market == "NASDAQ" and "." not in ticker:
        symbol = f"{ticker}.O"
    
    # 2. 아멕스(AMEX) 종목인데 마침표(.)가 없다면 자동으로 .A를 붙여줍니다.
    elif market == "AMEX" and "." not in ticker:
        symbol = f"{ticker}.A"
    
    # 3. NYSE는 사용자님의 말씀대로 노션 값을 100% 신뢰하여 그대로 사용합니다.
    # (이미 .K가 있든 없든 적힌 그대로 조회함)

    url = f"https://api.stock.naver.com/stock/{symbol}/basic"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        price_str = str(data['closePrice']).replace(",", "")
        return float(price_str)
    except Exception as e:
        print(f"❌ API 조회 실패 ({symbol}): {e}")
        return None

def main():
    # 한국 시간대(KST) 설정 (노션 시간 오차 해결)
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    now_display = now.strftime('%Y-%m-%d %H:%M:%S')

    print(f"🚀 전체 종목 업데이트 시작 (KST: {now_display})")
    
    has_more = True
    next_cursor = None
    total_count = 0

    while has_more:
        try:
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
                        # 해외 주식은 티커와 시장 정보를 함께 전달하여 처리
                        price = get_overseas_price(raw_ticker, market)
                    
                    if price is not None:
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
                    
                    time.sleep(0.4) 
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 실행 중 오류 발생: {e}")
            break

    print(f"✨ 총 {total_count}개의 종목 업데이트 완료!")

if __name__ == "__main__":
    main()
