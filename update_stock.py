import os
import yfinance as yf
import FinanceDataReader as fdr
from pykrx import stock  # [핵심] 재무지표 전용 라이브러리
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone
import pandas as pd

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 전역 변수: 데이터를 담아둘 그릇
KRX_PRICE = None # 가격 정보
KRX_FUND = None  # 재무 정보 (PER, PBR 등)

def get_latest_business_day():
    """주식 시장이 열린 가장 최근 평일을 찾습니다 (주말/공휴일 대비)"""
    kst = timezone(timedelta(hours=9))
    date = datetime.now(kst)
    
    # 최대 7일 전까지 뒤져서 데이터가 있는 날을 찾음
    for _ in range(7):
        date_str = date.strftime("%Y%m%d")
        try:
            # 코스피 시가총액 상위 1개만 조회해서 장이 열렸는지 확인
            check = stock.get_market_cap(date_str, market="KOSPI")
            if not check.empty:
                return date_str
        except:
            pass
        date -= timedelta(days=1)
    
    # 실패 시 오늘 날짜 반환 (어차피 에러 처리됨)
    return datetime.now(kst).strftime("%Y%m%d")

def load_krx_data():
    """한국 주식 데이터 로드 (가격 + 재무지표)"""
    global KRX_PRICE, KRX_FUND
    print("📥 한국 주식 데이터(KRX) 다운로드 중...")
    
    try:
        # 1. 가격 정보 (FinanceDataReader가 가장 빠름)
        KRX_PRICE = fdr.StockListing('KRX')
        KRX_PRICE['Code'] = KRX_PRICE['Code'].astype(str)
        KRX_PRICE.set_index('Code', inplace=True)
        print("✅ 가격 데이터 로드 완료")

        # 2. 재무 지표 (Pykrx 사용 - PER, PBR, EPS 등)
        target_date = get_latest_business_day()
        print(f"📥 재무 데이터 로드 중 (기준일: {target_date})...")
        
        # 전체 종목의 PER, PBR, EPS 등을 한 번에 가져옴
        KRX_FUND = stock.get_market_fundamental_by_ticker(date=target_date, market="ALL")
        # 인덱스가 티커로 되어있음. 티커 형식 보장 필요 없음 (이미 6자리)
        print(f"✅ 재무 데이터 로드 완료! (총 {len(KRX_FUND)}개 종목)")
        
    except Exception as e:
        print(f"🚨 데이터 로드 중 오류 발생: {e}")
        KRX_PRICE = None
        KRX_FUND = None

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        # 0.0 인 경우도 데이터가 있는 것이므로 반환
        return float(str(value).replace(",", ""))
    except:
        return None

def get_korean_stock_info(ticker):
    """메모리에 있는 데이터에서 조회"""
    global KRX_PRICE, KRX_FUND
    
    if KRX_PRICE is None or KRX_FUND is None: return None
    
    # 티커 포맷 통일 (005930)
    ticker_clean = str(ticker).strip().zfill(6)
    
    info = {
        "price": None, "per": None, "pbr": None, "eps": None, 
        "high52w": None, "low52w": None 
    }
    
    # 1. 가격 정보 가져오기
    if ticker_clean in KRX_PRICE.index:
        row = KRX_PRICE.loc[ticker_clean]
        info["price"] = safe_float(row.get('Close'))
    
    # 2. 재무 정보 가져오기 (Pykrx)
    if ticker_clean in KRX_FUND.index:
        row = KRX_FUND.loc[ticker_clean]
        # Pykrx 컬럼명: PER, PBR, EPS
        info["per"] = safe_float(row.get('PER'))
        info["pbr"] = safe_float(row.get('PBR'))
        info["eps"] = safe_float(row.get('EPS'))
        
    return info

def get_overseas_stock_info(ticker):
    """해외 주식: 야후 파이낸스"""
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
    print(f"🚀 벌크 업데이트(Pykrx + FDR) 시작 - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 프로그램 시작 시 데이터 로드
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
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    if not market or not ticker: continue

                    if market in ["KOSPI", "KOSDAQ"]:
                        stock = get_korean_stock_info(ticker)
                    else:
                        stock = get_overseas_stock_info(ticker)

                    if stock and stock["price"] is not None:
                        upd = {
                            "현재가": {"number": stock["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        fields = {"PER": "per", "PBR": "pbr", "EPS": "eps", "52주 최고가": "high52w", "52주 최저가": "low52w"}
                        for n_key, d_key in fields.items():
                            val = safe_float(stock[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        if success % 10 == 0: print(f"✅ {success}개 완료 (최근: {ticker})")
                    else:
                        fail += 1
                    
                    if market not in ["KOSPI", "KOSDAQ"]:
                        time.sleep(0.3) 
                        
                except Exception as e:
                    print(f"❌ {ticker} 에러: {e}")
                    fail += 1
                    continue
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"🚨 노션 쿼리 오류: {e}"); break

    print(f"✨ 최종 결과: 성공 {success} / 실패 {fail}")

if __name__ == "__main__":
    main()
