import os
import time
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def get_us_finance_with_logs(ticker):
    """
    미국 주식 데이터를 가져오며, 실패 시 구체적인 이유를 반환합니다.
    """
    try:
        stock = yf.Ticker(ticker)
        # fast_info와 info를 교차 확인하여 데이터 가용성 체크
        info = stock.info
        
        if not info or len(info) < 5:
            return None, None, "❌ 티커를 찾을 수 없거나 Yahoo Finance에 해당 종목 정보가 부족함"

        # 1. EPS 추출 (TTM -> 연간 안전장치)
        eps = info.get("trailingEps")  # TTM
        eps_source = "TTM"
        
        if eps is None:
            eps = info.get("forwardEps") or info.get("epsActual")
            eps_source = "Annual/Est"

        # 2. BPS 추출
        bps = info.get("bookValue")
        
        # 로그를 위한 상세 상태 메시지 생성
        reasons = []
        if eps is None: reasons.append("EPS 누락")
        if bps is None: reasons.append("BPS 누락")
        
        if not reasons:
            return eps, bps, f"✅ 성공 (EPS:{eps_source})"
        else:
            return eps, bps, f"⚠️ 일부 누락: {', '.join(reasons)}"

    except Exception as e:
        return None, None, f"🚨 시스템 에러: {str(e)}"

def is_korean_ticker(ticker):
    """한국 종목 필터링 로직 (0104P0 등 우선주 포함)"""
    ticker = ticker.strip().upper()
    if len(ticker) == 6 and ticker[0].isdigit(): return True
    if any(ext in ticker for ext in [".KS", ".KQ"]): return True
    if ticker.isdigit(): return True
    return False

def extract_ticker(props):
    for name in ["티커", "Ticker"]:
        prop = props.get(name, {})
        content = prop.get("title") or prop.get("rich_text")
        if content:
            ticker = content[0].get("plain_text", "").strip().upper()
            if not ticker or is_korean_ticker(ticker): continue
            return ticker
    return None

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇺🇸 [미국 재무 업데이트] 상세 로그 모드 시작")
    print(f"⏰ 실행 시간: {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    success, partial, fail, skip = 0, 0, 0, 0
    next_cursor = None
    
    while True:
        response = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = extract_ticker(props)
            
            if not ticker:
                skip += 1
                continue

            eps, bps, log_msg = get_us_finance_with_logs(ticker)
            
            # 노션 업데이트 로직
            if eps is not None or bps is not None:
                try:
                    upd = {}
                    if eps is not None: upd["EPS"] = {"number": eps}
                    if bps is not None: upd["BPS"] = {"number": bps}
                    
                    notion.pages.update(page_id=page["id"], properties=upd)
                    
                    if "✅" in log_msg:
                        success += 1
                    else:
                        partial += 1
                    print(f"   [{ticker}] {log_msg}")
                except Exception as e:
                    print(f"   [{ticker}] 🚨 노션 기록 에러: {e}")
                    fail += 1
            else:
                print(f"   [{ticker}] {log_msg}")
                fail += 1
            
            time.sleep(0.5)

        if not response.get("has_more"): break
        next_cursor = response.get("next_cursor")

    print("-" * 50)
    print(f"✨ 최종 결과 요약")
    print(f"   - 전체 성공: {success}")
    print(f"   - 일부 누락(부분 성공): {partial}")
    print(f"   - 완전 실패: {fail}")
    print(f"   - 한국종목 등 건너뜀: {skip}")

if __name__ == "__main__":
    main()
