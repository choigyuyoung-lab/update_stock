import os
import yfinance as yf
import FinanceDataReader as fdr
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone
import pandas as pd

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 전역 변수: 한국 주식 전체 데이터를 담을 그릇
KRX_DATA = None

def load_krx_data():
    """한국거래소(KRX) 전 종목 데이터를 단 1번만 가져와서 메모리에 저장"""
    global KRX_DATA
    print("📥 한국 주식 전 종목 데이터(KRX) 다운로드 중... (약 5~10초 소요)")
    try:
        # KRX: 코스피, 코스닥, 코넥스 통합 조회 (가격, PER, PBR, EPS 등 포함됨)
        df = fdr.StockListing('KRX')
        
        # 검색 속도를 높이기 위해 티커(Code)를 인덱스로 설정
        df['Code'] = df['Code'].astype(str) # 코드를 문자로 변환
        df.set_index('Code', inplace=True)
        
        KRX_DATA = df
        print(f"✅ KRX 데이터 로드 완료! (총 {len(df)}개 종목)")
    except Exception as e:
        print(f"🚨 KRX 데이터 로드 실패: {e}")
        KRX_DATA = None

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def get_korean_stock_info(ticker):
    """메모리에 저장된 KRX 데이터에서 조회 (네이버 접속 X)"""
    global KRX_DATA
    
    # 1. 데이터가 없으면 실패
    if KRX_DATA is None: return None
    
    # 2. 티커 포맷 통일 (005930 처럼 6자리 문자열로)
    ticker_clean = str(ticker).strip().zfill(6)
    
    # 3. 데이터프레임에서 조회
    if ticker_clean not in KRX_DATA.index:
        return None
    
    try:
        row = KRX_DATA.loc[ticker_clean]
        
        # KRX 데이터 컬럼 매핑 ('Close', 'PER', 'PBR', 'EPS' 등은 StockListing에서 제공)
        # 52주 데이터는 KRX 리스트에 없을 수 있으므로 가격 위주로 처리하거나
        # 필요시 별도 처리하지만, 일단 핵심 지표 위주로 가져옵니다.
        
        info = {
            "price": safe_float(row.get('Close')),
            "per": safe_float(row.get('PER')),
            "pbr": safe_float(row.get('PBR')),
            "eps": safe_float(row.get('EPS')),
            # KRX 리스트는 52주 데이터를 바로 주지 않을 수 있음 (None 처리)
            "high52w": None, 
            "low52w": None 
        }
        return info
    except Exception as e:
        print(f"⚠️ 매핑 에러 ({ticker_clean}): {e}")
        return None

def get_overseas_stock_info(ticker):
    """해외 주식: 야후 파이낸스 (기존 유지)"""
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
    print(f"🚀 벌크(Bulk) 업데이트 시작 - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # [핵심] 프로그램 시작 시 딱 한 번 한국 주식 전체를 가져옴
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

                    # 시장 구분에 따른 분기
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
                        # 한국 주식인데 실패했다면 티커 문제일 가능성이 높음
                        fail += 1
                    
                    # 한국 주식은 API 호출을 안하므로 딜레이가 거의 필요 없음
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
