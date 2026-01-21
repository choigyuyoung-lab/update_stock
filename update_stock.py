import os
import warnings
warnings.filterwarnings("ignore") # 경고 무시

import yfinance as yf
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_stock_data_from_yahoo(ticker, market):
    """
    모든 주식(한국/미국/ETF)을 야후 파이낸스에서 조회
    """
    symbol = str(ticker).strip().upper()
    
    # [핵심] 한국 주식은 야후 양식에 맞게 꼬리표(.KS / .KQ) 부착
    # 노션에 '005930'이라고 적혀있으면 -> '005930.KS'로 변환
    if market == "KOSPI":
        if not symbol.endswith(".KS"):
            symbol = f"{symbol}.KS"
    elif market == "KOSDAQ":
        if not symbol.endswith(".KQ"):
            symbol = f"{symbol}.KQ"
    
    try:
        # 야후 파이낸스 접속
        stock = yf.Ticker(symbol)
        d = stock.info
        
        # 가격 정보 (현재가 or 정규장 종가)
        price = d.get("currentPrice") or d.get("regularMarketPrice")
        
        if price is None:
            return None

        # 모든 데이터 리턴 (PER, PBR, 52주 등등)
        return {
            "price": price,
            "per": d.get("trailingPE"),
            "pbr": d.get("priceToBook"),
            "eps": d.get("trailingEps"),
            "high52w": d.get("fiftyTwoWeekHigh"),
            "low52w": d.get("fiftyTwoWeekLow")
        }
    except Exception as e:
        # print(f"에러 상세: {e}")
        return None

def main():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 [통합 모드] 야후 파이낸스 전체 업데이트 시작 - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    
    while has_more:
        try:
            print(f"\n📡 노션 페이지 조회 중... (Cursor: {next_cursor})")
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            
            if not pages and success == 0 and fail == 0:
                print("🚨 가져온 페이지가 0개입니다.")
                break

            for page in pages:
                try:
                    props = page["properties"]
                    
                    # 1. Market 확인
                    market_obj = props.get("Market", {}).get("select")
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    # 2. 티커 확인
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    if not market or not ticker: continue
                    
                    # 3. 데이터 가져오기 (야후 단일 통일)
                    data = get_stock_data_from_yahoo(ticker, market)

                    if data is not None:
                        # 4. 노션 업데이트
                        upd = {
                            "현재가": {"number": data["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        # 재무 지표 및 52주 데이터 일괄 업데이트
                        fields = {
                            "PER": "per", 
                            "PBR": "
