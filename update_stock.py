import os
import warnings
# 경고 메시지 무시 (지저분한 로그 제거)
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

# 전역 변수 (한국 주식 데이터 저장용)
KRX_PRICE = None
KRX_FUND = None

def safe_float(value):
    """지저분한 데이터를 안전한 숫자로 변환"""
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def load_krx_data():
    """한국 주식 데이터(가격+재무)를 메모리에 로드"""
    global KRX_PRICE, KRX_FUND
    print("---------------------------------------------------")
    print("📥 [진단] 한국 주식 데이터(KRX) 로드 시작...")
    
    try:
        # 1. 가격 데이터 (FDR 사용)
        KRX_PRICE = fdr.StockListing('KRX')
        KRX_PRICE['Code'] = KRX_PRICE['Code'].astype(str)
        KRX_PRICE.set_index('Code', inplace=True)
        print(f"✅ 가격 데이터 확보: 총 {len(KRX_PRICE)}개 종목")

        # 2. 재무 데이터 (Pykrx 사용) - 최근 7일간 데이터 탐색
        kst = timezone(timedelta(hours=9))
        target_date = datetime.now(kst)
        found = False
        
        for i in range(7):
            date_str = target_date.strftime("%Y%m%d")
            try:
                # 해당 날짜의 전체 재무제표 가져오기
                df = stock.get_market_fundamental_by_ticker(date=date_str, market="ALL")
                
                # 데이터가 있고, 'PER' 컬럼이 존재하는지 확인
                if not df.empty and 'PER' in df.columns:
                    KRX_FUND = df
                    print(f"✅ 재무 데이터 확보({date_str}): 총 {len(df)}개 종목")
                    found = True
                    break 
            except:
                pass
            # 실패하면 하루 전으로 이동
            target_date -= timedelta(days=1)

        if not found:
            print("⚠️ [경고] 최근 7일간 유효한 재무 데이터를 찾지 못했습니다. (가격만 업데이트 됩니다)")
            KRX_FUND = None
        
    except Exception as e:
        print(f"🚨 [치명적 오류] 데이터 로드 실패: {e}")
    print("---------------------------------------------------")

def get_korean_stock_info(ticker):
    """메모리에 로드된 KRX 데이터에서 정보 추출"""
    global KRX_PRICE, KRX_FUND
    
    # 가격 데이터가 없으면 검색 불가
    if KRX_PRICE is None: return None
    
    # 티커 정리 (예: "5930" -> "005930")
    ticker_clean = str(ticker).strip().zfill(6)
    
    # [진단] 티커가 명부에 있는지 확인
    if ticker_clean not in KRX_PRICE.index:
        print(f"      ㄴ ⚠️ KRX 명부에 없는 티커입니다: '{ticker_clean}'")
        return None

    info = { "price": None, "per": None, "pbr": None, "eps": None, "high52w": None, "low52w": None }
    
    # 가격 정보 추출
    row = KRX_PRICE.loc[ticker_clean]
    info["price"] = safe_float(row.get('Close'))
    
    # 재무 정보 추출 (데이터가 있을 경우에만)
    if KRX_FUND is not None and ticker_clean in KRX_FUND.index:
        row_f = KRX_FUND.loc[ticker_clean]
        if 'PER' in row_f: info["per"] = safe_float(row_f['PER'])
        if 'PBR' in row_f: info["pbr"] = safe_float(row_f['PBR'])
        if 'EPS' in row_f: info["eps"] = safe_float(row_f['EPS'])
        
    return info

def get_overseas_stock_info(ticker):
    """해외 주식 정보 추출 (야후 파이낸스)"""
    symbol = ticker.split('.')[0] # .K 같은 접미사 제거
    try:
        stock_data = yf.Ticker(symbol)
        d = stock_data.info
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
    print(f"🚀 [최종 수정본] 업데이트 시작 - KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 한국 주식 데이터 미리 가져오기
    load_krx_data()
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
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
                print("🚨 [중요] 노션에서 아무것도 가져오지 못했습니다! DATABASE_ID 확인 필요.")
                break

            for page in pages:
                try:
                    props = page["properties"]
                    
                    # 1. Market 값 읽기
                    market_obj = props.get("Market", {}).get("select")
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    # 2. 티커 값 읽기
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    # [진단] 처리 중인 종목 출력
                    print(f"🔍 검사 중: [{market}] {ticker}")

                    if not market:
                        print("   => ❌ Market 값이 비어있어 건너뜁니다.")
                        continue
                    if not ticker:
                        print("   => ❌ 티커 값이 비어있어 건너뜁니다.")
                        continue

                    # 3. 데이터 가져오기
                    stock_info = None
                    if market in ["KOSPI", "KOSDAQ"]:
                        stock_info = get_korean_stock_info(ticker)
                    else:
                        stock_info = get_overseas_stock_info(ticker)

                    # 4. 노션 업데이트
                    # [수정 완료] 변수명 통일 및 콜론(:) 추가됨
                    if stock_info is not None and stock_info["price"] is not None:
                        upd = {
                            "현재가": {"number": stock_info["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        fields = {"PER": "per", "PBR": "pbr", "EPS": "eps", "52주 최고가": "high52w", "52주 최저가": "low52w"}
                        for n_key, d_key in fields.items():
                            val = safe_float(stock_info[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        print(f"   => ✅ 업데이트 성공! (가격: {stock_info['price']})")
                    else:
                        print(f"   => ❌ 데이터 가져오기 실패 (종목을 못 찾았거나 데이터 없음)")
                        fail += 1
                    
                    # 해외 주식일 경우에만 딜레이 (국내는 메모리에서 가져오므로 필요 없음)
                    if market not in ["KOSPI", "KOSDAQ"]:
                        time.sleep(0.3) 
                        
                except Exception as e:
                    print(f"   => 🚨 처리 중 에러 발생: {e}")
                    fail += 1
                    continue
            
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 노션 연결 오류: {e}")
            break

    print("\n---------------------------------------------------")
    print(f"✨ 최종 결과: 성공 {success}건 / 실패 {fail}건")

if __name__ == "__main__":
    main()
