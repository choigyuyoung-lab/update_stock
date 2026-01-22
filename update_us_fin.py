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
        info = stock.info
        
        # EPS (Trailing EPS)
        eps = info.get("trailingEps")
        # BPS (Book Value Per Share)
        bps = info.get("bookValue")
        
        return eps, bps
    except Exception as e:
        print(f"      ⚠️ {ticker} 데이터 추출 중 오류: {e}")
        return None, None

def extract_ticker(props):
    """노션에서 미국 주식 티커(알파벳)를 추출합니다."""
    for name in ["티커", "Ticker"]:
        prop = props.get(name, {})
        content = prop.get("title") or prop.get("rich_text")
        if content:
            ticker = content[0].get("plain_text", "").strip().upper()
            # 숫자가 아닌 알파벳 형색일 때 미국 주식으로 간주 (또는 .KS/.KQ가 없는 경우)
            if not ticker.isdigit() and not any(ext in ticker for ext in [".KS", ".KQ"]):
                return ticker
    return None

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇺🇸 [미국 재무 전용 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None
    
    # [핵심] 100개 제한 해제를 위한 페이지네이션 루프
    while True:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = extract_ticker(props)
            
            # 미국 주식이 아니면(한국 주식이거나 티커가 없으면) 건너뜀
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
                print(f"   => ❌ {ticker} | 데이터 누락")
                fail += 1
            
            time.sleep(0.5) # API 부하 방지

        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 완료 | 성공: {success} | 실패: {fail} | 건너뜀(한국 주식 등): {skip}")

if __name__ == "__main__":
    main()
