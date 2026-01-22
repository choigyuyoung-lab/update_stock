import os, time, math, requests, io, pandas as pd, yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    """유효한 숫자인지 체크 (NaN, Inf 방지)"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def get_kr_fin(ticker):
    """한국 주식 재무 정보 추출 (TTM -> FY)"""
    eps, bps = None, None
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # 1. 모바일 API
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(url, headers=headers, timeout=10).json()
        for item in res.get("result", {}).get("totalInfos", []):
            val = str(item.get("value", "")).replace(",", "").replace("원", "").strip()
            key = item.get("key", "").upper()
            if "EPS" in key: eps = float(val) if val.replace(".","").isdigit() else None
            if "BPS" in key: bps = float(val) if val.replace(".","").isdigit() else None
        
        # 2. PC 웹 백업
        if eps is None or bps is None:
            pc_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            resp = requests.get(pc_url, headers=headers)
            try: html = resp.content.decode('cp949')
            except: html = resp.content.decode('utf-8', errors='ignore')
            tables = pd.read_html(io.StringIO(html))
            for table in tables:
                if any("주요재무정보" in str(col) for col in table.columns):
                    table.columns = table.columns.get_level_values(-1)
                    table = table.set_index(table.columns[0])
                    eps = float(str(table.loc["EPS(원)"].iloc[3]).replace(",", ""))
                    bps = float(str(table.loc["BPS(원)"].iloc[3]).replace(",", ""))
                    break
    except: pass
    return eps, bps

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"📊 [재무 업데이트 상세 모드] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    success_cnt = 0

    while True:
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = ""; is_kr = False
            
            for name in ["티커", "Ticker"]:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content:
                    ticker = content[0].get("plain_text", "").strip().upper()
                    is_kr = len(ticker) == 6 and ticker[0].isdigit()
                    break
            
            if not ticker:
                continue

            # 데이터 추출
            if is_kr:
                eps, bps = get_kr_fin(ticker)
            else:
                try:
                    stock = yf.Ticker(ticker)
                    eps = stock.info.get("trailingEps") or stock.info.get("forwardEps")
                    bps = stock.info.get("bookValue")
                except: eps, bps = None, None

            # 노션 업데이트 (날짜 갱신 포함으로 시각적 확인 가능하게 변경)
            try:
                upd = {}
                if is_valid(eps): upd["EPS"] = {"number": eps}
                if is_valid(bps): upd["BPS"] = {"number": bps}
                
                # 수치가 없더라도 '마지막 업데이트' 날짜를 찍어줘서 작동 여부를 확인하게 함
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                print(f"   => [{ticker}] 재무 확인 완료 (EPS: {eps}, BPS: {bps})")
                success_cnt += 1
            except Exception as e:
                print(f"   => [{ticker}] 업데이트 실패: {e}")
            
            time.sleep(0.4)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"✨ 재무 업데이트 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
