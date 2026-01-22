import os, time, math, requests, io, pandas as pd, yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    return val is not None and not (math.isnan(val) or math.isinf(val))

def get_kr_fin(ticker):
    eps, bps = None, None
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        # 1순위: 모바일 API
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        data = requests.get(url, headers=headers, timeout=10).json()
        for item in data.get("result", {}).get("totalInfos", []):
            val = str(item.get("value", "")).replace(",", "").replace("원", "").strip()
            if "EPS" in item.get("key", "").upper(): eps = float(val)
            if "BPS" in item.get("key", "").upper(): bps = float(val)
        
        # 2순위: PC 웹 표 (안전장치 & 인코딩 해결)
        if eps is None or bps is None:
            pc_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            resp = requests.get(pc_url, headers=headers)
            try: html = resp.content.decode('cp949')
            except: html = resp.content.decode('utf-8', errors='ignore')
            
            tables = pd.read_html(io.StringIO(html)) # StringIO 적용
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
    print(f"📊 [재무 업데이트] 시작 - {datetime.now()}")
    next_cursor = None
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
            
            if not ticker: continue
            
            # 재무 데이터 추출 (KR/US 분기)
            if is_kr:
                eps, bps = get_kr_fin(ticker)
            else:
                stock = yf.Ticker(ticker)
                eps = stock.info.get("trailingEps") or stock.info.get("forwardEps")
                bps = stock.info.get("bookValue")
            
            try:
                upd = {}
                if is_valid(eps): upd["EPS"] = {"number": eps}
                if is_valid(bps): upd["BPS"] = {"number": bps}
                if upd: 
                    notion.pages.update(page_id=page["id"], properties=upd)
                    print(f"   [{ticker}] 재무 업데이트 완료")
            except: pass
            time.sleep(0.4)
        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")
