import os
import warnings
# 불필요한 경고 메시지 제거
warnings.filterwarnings("ignore")

import yfinance as yf
import FinanceDataReader as fdr
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 전역 변수 (한국 주식 데이터 저장소)
KRX_DATA = None

def safe_float(value):
    """문자열이나 지저분한 데이터를 안전한 숫자로 변환"""
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def load_krx_data():
    """한국 주식 전체 시세 데이터 로드 (속도 최우선)"""
    global KRX_DATA
    print("---------------------------------------------------")
    print("📥 [KRX] 한국 주식 전체 시세 다운로드 중...")
    
    try:
        # FinanceDataReader는 KRX 전체 종목의 가격 정보를 가장 빨리 가져옵니다.
        # (PER/PBR 등은 제공되지 않을 수 있으나, 가격 업데이트 속도는 최고입니다.)
        df = fdr.StockListing('KRX')
        
        # 티커(Code)를 문자로 변환하고 인덱스로 설정 (검색 속도 향상)
        df['Code'] = df['Code'].astype(str)
        df.set_index('Code', inplace=True)
        
        KRX_DATA = df
        print(f"✅ KRX 데이터 확보 완료: 총 {len(df)}개 종목")
        
    except Exception as e:
        print(f"🚨 KRX 데이터 로드 실패: {e}")
        KRX_DATA = None
    print("---------------------------------------------------")

def get_korean_stock_info(ticker):
    """메모리에 있는 KRX 데이터에서 조회 (가격 위주)"""
    global KRX_DATA
    
    if KRX_DATA is None: return None
    
    # [핵심] 티커 6자리 자동 보정 (예: '5930' -> '005930')
    ticker_clean = str(ticker).strip().zfill(6)
    
    # KRX 명부에 있는지 확인
    if ticker_clean not in KRX_DATA.index:
        # ETF나 ETN 등의 경우 티커가 다를 수 있음. 로그만 남기고 패스
        print(f"      ㄴ ⚠️ KRX 명부에 없는 티커: '{ticker_clean}'")
        return None

    info = { "price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None }
    
    # 데이터 추출
    row = KRX_DATA.loc[ticker_clean]
    
    # 가격 정보 (필수)
    # 컬럼명이 'Close'인 경우가 일반적임
    if 'Close' in row:
        info["price"] = safe_float(row['Close'])
    
    # PER, PBR 등은 FDR 데이터 버전에 따라 있을 수도 있고 없을 수도 있음.
    # 있으면 넣고, 없으면 굳이 에러내지 않고 넘어감 (사용자 요청 반영)
    if 'PER' in row: info["per"] = safe_float(row['PER'])
    if 'PBR' in row: info["pbr"] = safe_float(row['PBR'])
    if 'EPS' in row: info["eps"] = safe_float(row['EPS'])
        
    return info

def get_overseas_stock_info(ticker):
    """미국 주식 정보 추출 (야후 파이낸스)"""
    symbol = ticker.split('.')[0] # .K 같은 접미사 제거
    try:
        stock_data = yf.Ticker(symbol)
        d = stock_data.info
        
        # 가격 정보
        price = d.get("currentPrice") or d.get("regularMarketPrice")
        
        return {
            "price": price,
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
    print(f"🚀 주식 업데이트 시작 (KRX:속도 / US:야후) - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 한국 주식 데이터 미리 로드 (Bulk Fetch)
    load_krx_data()
    
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
                print("🚨 노션에서 가져온 페이지가 없습니다. (DB ID 확인 필요)")
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
                    
                    # 진단 로그
                    # print(f"🔍 처리 중: [{market}] {ticker}") 

                    if not market or not ticker:
                        continue

                    # 3. 데이터 가져오기 분기
                    stock_info = None
                    
                    if market in ["KOSPI", "KOSDAQ"]:
                        # 한국 주식: 메모리에서 즉시 조회 (Fast)
                        stock_info = get_korean_stock_info(ticker)
                    else:
                        # 해외 주식: 야후 파이낸스 접속 (Detailed)
                        stock_info = get_overseas_stock_info(ticker)

                    # 4. 노션 업데이트 수행
                    if stock_info is not None and stock_info["price"] is not None:
                        upd = {
                            "현재가": {"number": stock_info["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        # 재무 지표 업데이트 (값이 있는 경우에만)
                        fields = {"PER": "per", "PBR": "pbr", "EPS": "eps", "52주 최고가": "high52w", "52주 최저가": "low52w"}
                        for n_key, d_key in fields.items():
                            val = safe_float(stock_info[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        print(f"   => ✅ [{market}] {ticker} : {stock_info['price']:,.0f}원 (업데이트 완료)")
                    else:
                        print(f"   => ❌ [{market}] {ticker} : 데이터 없음")
                        fail += 1
                    
                    # 해외 주식일 경우에만 서버 부하 방지용 딜레이
                    if market not in ["KOSPI", "KOSDAQ"]:
                        time.sleep(0.3) 
                        
                except Exception as e:
                    print(f"   => 🚨 [{market}] {ticker} 에러: {e}")
                    fail += 1
                    continue
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 노션 연결 중 치명적 오류: {e}")
            break

    print("\n---------------------------------------------------")
    print(f"✨ 최종 결과: 성공 {success}건 / 실패 {fail}건")

if __name__ == "__main__":
    main()
