import os
import time
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def get_us_finance_data(ticker):
    """
    yfinance를 사용하여 미국 주식의 EPS와 BPS를 가져옵니다.
    """
    try:
        stock = yf.Ticker(ticker)
        # 404 에러 방지를 위해 데이터 존재 여부를 먼저 확인하는 로직 강화
        info = stock.info
        
        if not info or 'quoteType' not in info:
            return None, None
            
        eps = info.get("trailingEps")
        bps = info.get("bookValue")
        
        return eps, bps
    except Exception:
        # 에러 발생 시 로그를 남기지 않고 조용히 넘어가도록 처리
        return None, None

def extract_ticker(props):
    """
    노션에서 미국 주식 티커를 추출합니다. 
    한국 종목(숫자 6자리, 우선주 포함)은 철저히 제외합니다.
    """
    for name in ["티커", "Ticker"]:
        prop = props.get(name, {})
        content = prop.get("title") or prop.get("rich_text")
        if content:
            ticker = content[0].get("plain_text", "").strip().upper()
            
            # [강화된 한국 종목 필터링]
            # 1. 6자리이면서 숫자로 시작하면 한국 종목(0104P0 등 우선주 포함)으로 간주
            if len(ticker) == 6 and ticker[0].isdigit():
                continue
            # 2. .KS 나 .KQ가 붙어있는 경우 제외
            if any(ext in ticker for ext in [".KS", ".KQ"]):
                continue
            # 3. 순수 숫자로만 된 경우 제외
            if ticker.isdigit():
                continue
            # 4. 티커가 너무 짧거나 없으면 제외
            if not ticker or len(ticker) > 5: # 미국 주식은 보통 1~5글자
                # 단, 6글자 중 숫자로 시작하지 않는 특수 케이스가 있을 수 있어 1번 조건이 우선임
                if len(ticker) >= 6: continue
                        
            return ticker
    return None

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇺🇸 [미국 재무 전용 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None
    
    while True:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = extract_ticker(props)
            
            if not ticker:
                skip += 1
                continue

            # 데이터 수집 (yfinance)
            eps, bps = get_us_finance_data(ticker)
            
            if eps is not None or bps is not None:
                upd = {}
                if eps is not None: upd["EPS"] = {"number": eps}
                if bps is not None: upd["BPS"] = {"number": bps}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                success += 1
                print(f"   => ✅ {ticker} | EPS: {eps} | BPS: {bps}")
            else:
                # 미국 주식인데 데이터를 못 가져온 경우만 실패로 처리
                print(f"   => ❌ {ticker} | 데이터 없음")
                fail += 1
            
            time.sleep(0.5)

        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 완료 | 성공: {success} | 실패: {fail} | 건너뜀(한국 종목 등): {skip}")

if __name__ == "__main__":
    main()
