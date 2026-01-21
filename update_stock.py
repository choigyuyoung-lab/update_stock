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
    except Exception:
        return None

def get_overseas_price(ticker, market):
    """해외 주식(NYSE/NASDAQ 등) API 호출"""
    # 티커 정제 (예: PATH.K -> PATH)
    clean_ticker = ticker.split('.')[0]
    
    # 나스닥/아멕스 등은 티커 뒤에 .O가 붙어야 네이버 API에서 인식함
    if market != "NYSE":
        clean_ticker = f"{clean_ticker}.O"
    
    url = f"https://api.stock.naver.com/stock/{clean_ticker}/basic"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        # closePrice가 문자열일 경우 대비하여 쉼표 제거 후 변환
        price_str = str(data['closePrice']).replace(",", "")
        return float(price_str)
    except Exception:
        return None

def main():
    # 2. 한국 시간대(KST) 정의 및 설정
    # timezone 정보를 포함해야 노션에서 시간이 중복으로 더해지지 않습니다.
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() # 출력 예: 2026-01-21T12:01:00+09:00
    now_display = now.strftime('%Y-%m-%d %H:%M:%S')

    print(f"🚀 전체 종목 업데이트 시작 (KST: {now_display})")
    
    has_more = True
    next_cursor = None
    total_count = 0

    while has_more:
        try:
            # 페이지네이션 처리: 한 번에 100개씩 가져옵니다.
            response = notion.databases.query(
                database_id=DATABASE_ID,
                start_cursor=next_cursor
            )
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
                    
                    # Notion API 속도 제한 준수 (초당 약 3개 처리)
                    time.sleep(0.4) 
            
            # 다음 페이지(커서)가 있는지 확인
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 루프 실행 중 오류 발생: {e}")
            break

    print(f"✨ 총 {total_count}개의 종목 업데이트가 완료되었습니다!")

if __name__ == "__main__":
    main()
