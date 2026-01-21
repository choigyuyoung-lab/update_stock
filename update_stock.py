import os
import requests
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. Notion 및 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def get_stock_info(ticker, market):
    """국내/해외 주식의 가격 및 재무 지표를 가져오는 통합 함수"""
    # 기본값 설정
    info = {
        "price": None, "per": None, "pbr": None, 
        "eps": None, "high52w": None, "low52w": None
    }
    
    # 시장별 심볼 및 API URL 설정
    if market in ["KOSPI", "KOSDAQ"]:
        # 국내 주식 통합 API
        url = f"https://api.stock.naver.com/stock/{ticker}/integration"
        symbol = ticker
    else:
        # 해외 주식 (사용자 제안 로직: 마침표가 있으면 그대로, 없으면 시장별 부여)
        symbol = ticker
        if "." not in ticker:
            if market == "NYSE": symbol = f"{ticker}.K"
            elif market == "NASDAQ": symbol = f"{ticker}.O"
            elif market == "AMEX": symbol = f"{ticker}.A"
        url = f"https://api.stock.naver.com/stock/{symbol}/basic"

    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        # 1. 현재가 추출
        if market in ["KOSPI", "KOSDAQ"]:
            info["price"] = float(data['total']['currentPrice'].replace(",", ""))
            # 국내 재무 정보는 보통 'stockFina' 항목에 있음
            fina = data.get('stockFina', {})
            info["per"] = float(fina.get('per', 0)) or None
            info["pbr"] = float(fina.get('pbr', 0)) or None
            info["eps"] = float(fina.get('eps', 0)) or None
            info["high52w"] = float(data['total'].get('high52wPrice', 0).replace(",", ""))
            info["low52w"] = float(data['total'].get('low52wPrice', 0).replace(",", ""))
        else:
            info["price"] = float(str(data['closePrice']).replace(",", ""))
            # 해외 재무 정보 추출
            info["per"] = data.get('per')
            info["pbr"] = data.get('pbr')
            info["eps"] = data.get('eps')
            info["high52w"] = float(str(data.get('high52wPrice', 0)).replace(",", ""))
            info["low52w"] = float(str(data.get('low52wPrice', 0)).replace(",", ""))
            
        return info
    except Exception as e:
        print(f"❌ 데이터 추출 실패 ({symbol}): {e}")
        return None

def main():
    # 한국 시간대(KST) 및 ISO 포맷 설정 (시간 오차 해결)
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    now_display = now.strftime('%Y-%m-%d %H:%M:%S')

    print(f"🚀 전체 종목 심층 업데이트 시작 (KST: {now_display})")
    
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
                    stock_data = get_stock_info(raw_ticker, market)
                    
                    if stock_data and stock_data["price"] is not None:
                        # 노션 속성 업데이트 (값이 있는 것만 골라서 업데이트)
                        update_props = {
                            "현재가": {"number": stock_data["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        # 재무 지표들 추가 (데이터가 존재할 경우에만)
                        if stock_data["per"]: update_props["PER"] = {"number": stock_data["per"]}
                        if stock_data["pbr"]: update_props["PBR"] = {"number": stock_data["pbr"]}
                        if stock_data["eps"]: update_props["EPS"] = {"number": stock_data["eps"]}
                        if stock_data["high52w"]: update_props["52주 최고가"] = {"number": stock_data["high52w"]}
                        if stock_data["low52w"]: update_props["52주 최저가"] = {"number": stock_data["low52w"]}

                        notion.pages.update(page_id=page["id"], properties=update_props)
                        
                        total_count += 1
                        if total_count % 10 == 0:
                            print(f"진행 중... {total_count}개 완료")
                    
                    time.sleep(0.4) 
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 실행 중 오류 발생: {e}")
            break

    print(f"✨ 총 {total_count}개의 종목 업데이트가 완료되었습니다!")

if __name__ == "__main__":
    main()
