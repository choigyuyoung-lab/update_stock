import os
import warnings
# 경고 메시지 무시
warnings.filterwarnings("ignore", category=UserWarning)

import yfinance as yf
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 전역 변수
KRX_PRICE = None
KRX_FUND = None

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def load_krx_data():
    global KRX_PRICE, KRX_FUND
    print("---------------------------------------------------")
    print("📥 [진단] 한국 주식 데이터(KRX) 로드 시작...")
    
    try:
        KRX_PRICE = fdr.StockListing('KRX')
        KRX_PRICE['Code'] = KRX_PRICE['Code'].astype(str)
        KRX_PRICE.set_index('Code', inplace=True)
        print(f"✅ 가격 데이터 확보: 총 {len(KRX_PRICE)}개 종목")

        kst = timezone(timedelta(hours=9))
        target_date = datetime.now(kst)
        found = False
        
        # 재무 데이터 찾기 루프
        for i in range(7):
            date_str = target_date.strftime("%Y%m%d")
            try:
                df = stock.get_market_fundamental_by_ticker(date=date_str, market="ALL")
                if not df.empty and 'PER' in df.columns:
                    KRX_FUND = df
                    print(f"✅ 재무 데이터 확보({date_str}): 총 {len(df)}개 종목")
                    found = True
                    break 
            except:
                pass
            target_date -= timedelta(days=1)

        if not found:
            print("⚠️ [경고] 재무 데이터를 찾지 못했습니다. (가격만 업데이트 됩니다)")
            KRX_FUND = None
        
    except Exception as e:
        print(f"🚨 [치명적 오류] 데이터 로드 실패: {e}")
    print("---------------------------------------------------")

def get_korean_stock_info(ticker):
    global KRX_PRICE, KRX_FUND
    if KRX_PRICE is None: return None
    
    ticker_clean = str(ticker).strip().zfill(6)
    
    # [진단] 리스트 존재 여부 확인
    if ticker_clean not in KRX_PRICE.index:
        print(f"      ㄴ ⚠️ KRX 명부에 없는 티커입니다: '{ticker_clean}' (티커 확인 필요)")
        return None

    info = { "price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None }
    
    row = KRX_PRICE.loc[ticker_clean]
    info["price"] = safe_float(row.get('Close'))
    
    if KRX_FUND is not None and ticker_clean in KRX_FUND.index:
        row_f = KRX_FUND.loc[ticker_clean]
        if 'PER' in row_f: info["per"] = safe_float(row_f['PER'])
        if 'PBR' in row_f: info["pbr"] = safe_float(row_f['PBR'])
        if 'EPS' in row_f: info["eps"] = safe_float(row_f['EPS'])
        
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
    print(f"🚀 [진단 모드] 업데이트 시작 - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    load_krx_data()
    
    has_more, next_cursor, success, fail = True, None, 0, 0
    total_pages = 0

    while has_more:
        try:
            print(f"\n📡 노션 페이지 가져오는 중... (Cursor: {next_cursor})")
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            page_count = len(pages)
            total_pages += page_count
            print(f"📄 이번 페이지 수: {page_count}개")

            if total_pages == 0 and page_count == 0:
                print("🚨 [중요] 노션에서 아무것도 가져오지 못했습니다! DATABASE_ID를 확인하거나 봇 초대를 확인하세요.")
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
                    
                    # [진단 로그 출력]
                    print(f"🔍 검사 중: [{market}] {ticker}")

                    if not market:
                        print("   => ❌ Market 값이 비어있어 건너뜁니다.")
                        continue
                    if not ticker:
                        print("   => ❌ 티커 값이 비어있어 건너뜁니다.")
                        continue

                    stock_info = None
                    if market in ["KOSPI", "KOSDAQ"]:
                        stock_info = get_korean_stock_info(ticker)
                    else:
                        stock_info = get_overseas_stock_info(ticker)

                    if stock_info and stock
