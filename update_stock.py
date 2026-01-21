import os
import yfinance as yf
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone
import pandas as pd

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 전역 변수
KRX_PRICE = None
KRX_FUND = None

def load_krx_data():
    """PER/PBR 데이터가 있는 날짜를 찾을 때까지 과거로 탐색"""
    global KRX_PRICE, KRX_FUND
    print("📥 한국 주식 데이터(KRX) 로드 시작...")
    
    try:
        # 1. 가격 정보 (FDR)
        KRX_PRICE = fdr.StockListing('KRX')
        KRX_PRICE['Code'] = KRX_PRICE['Code'].astype(str)
        KRX_PRICE.set_index('Code', inplace=True)
        print("✅ 가격 데이터 로드 완료")

        # 2. 재무 지표 (Pykrx) - [핵심: 유효한 데이터 찾을 때까지 루프]
        kst = timezone(timedelta(hours=9))
        target_date = datetime.now(kst)
        
        found = False
        
        # 최대 7일 전까지 뒤지면서 'PER' 컬럼이 있는 데이터를 찾음
        for i in range(7):
            date_str = target_date.strftime("%Y%m%d")
            print(f"🔎 재무 데이터 탐색 중... ({date_str})")
            
            try:
                df = stock.get_market_fundamental_by_ticker(date=date_str, market="ALL")
                
                # 데이터가 있고, 핵심 컬럼(PER)이 존재하는지 확인
                if not df.empty and 'PER' in df.columns:
                    KRX_FUND = df
                    print(f"✅ {date_str}일자 유효한 재무 데이터 확보 완료! (총 {len(df)}개)")
                    found = True
                    break # 찾았으면 중단
                else:
                    print(f"⚠️ {date_str}일자 데이터는 비어있거나 지표가 없습니다.")
            except Exception as e:
                print(f"⚠️ {date_str}일자 조회 실패: {e}")
            
            # 하루 전으로 이동
            target_date -= timedelta(days=1)
            time.sleep(1) # 차단 방지용 살짝 대기

        if not found:
            print("🚨 최근 7일간 유효한 재무 데이터를 찾지 못했습니다. (재무 정보 업데이트 건너뜀)")
            KRX_FUND = None
        
    except Exception as e:
        print(f"🚨 데이터 로드 중 치명적 오류: {e}")
        if KRX_PRICE is None: KRX_PRICE = None
        KRX_FUND = None

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_korean_stock_info(ticker):
    """메모리 캐시에서 조회"""
    global KRX_PRICE, KRX_FUND
    
    if KRX_PRICE is None: return None
    
    ticker_clean = str(ticker).strip().zfill(6)
    
    info = {
        "price": None, "per": None, "pbr": None, "eps": None, 
        "high52w": None, "low52w": None 
    }
    
    # 1. 가격 (FDR)
    if ticker_clean in KRX_PRICE.index:
        row = KRX_PRICE.loc[ticker_clean]
        info["price"] = safe_float(row.get('Close'))
    
    # 2. 재무 (Pykrx)
    if KRX_FUND is not None and ticker_clean in KRX_FUND.index:
        row = KRX_FUND.loc[ticker_clean]
        # 컬럼 이름이 확실히 존재할 때만 가져옴
        if 'PER' in row: info["per"] = safe_float(row['PER'])
        if 'PBR' in row: info["pbr"] = safe_float(row['PBR'])
        if 'EPS' in row: info["eps"] = safe_float(row['EPS'])
        
    return info

def get_overseas_stock_info(ticker):
    symbol = ticker.split('.')[0]
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        return {
            "price": d.get("currentPrice") or d.get("regularMarketPrice"),
            "per": d.get("trailingPE"),
            "pbr": d.get("priceToBook"),
            "eps": d.get("trailingEps"),
            "high52w": d.get("fiftyTwoWeekHigh"),
            "low52w": d.get("fiftyTwoWeekLow")
        }
    except:
        return None

def main():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 벌크 업데이트 (스마트 탐색) 시작 - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    load_krx_data()
    
    has_more, next_cursor, success, fail = True, None, 0, 0

    while has_more:
        try:
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            
            for page in pages:
                ticker = ""
                try:
                    props = page["properties"]
                    market_obj = props.get("Market", {}).get("select")
                    market = market
