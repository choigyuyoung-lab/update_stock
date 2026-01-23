import os, time, math, requests, io, pandas as pd, yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    """노션 API 전송 전 숫자 유효성 검사 (NaN, Inf 방지)"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def get_kr_price(ticker):
    """한국 주식 전용: 네이버를 통해 실시간 주가 및 52주 최고/최저 추출"""
    price_data = {'price': None, 'high': None, 'low': None}
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        # 1. 네이버 모바일 API (실시간 현재가 추출 - 지연 없음)
        api_url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(api_url, headers=headers, timeout=10).json()
        stock_item = res.get("result", {}).get("stockItem", {})
        if stock_item:
            price_data['price'] = float(stock_item.get("closePrice", "0").replace(",", ""))
        
        # 2. PC 웹 백업 (52주 최고/최저가 추출)
        pc_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        resp = requests.get(pc_url, headers=headers)
        try: html = resp.content.decode('cp949')
        except: html = resp.content.decode('utf-8', errors='ignore')
        
        tables = pd.read_html(io.StringIO(html))
        for table in tables:
            # '52주최고' 텍스트가 포함된 테이블 탐색
            if any("52주최고" in str(col) for col in table.columns) or any("52주최고" in str(idx) for idx in table.index):
                # 네이버 금융 페이지 구조상 특정 위치의 52주 데이터를 파싱
                # 보통 시세 정보 테이블에 위치함
                table_str = str(table)
                if "52주최고" in table_str:
                    # 'tab_con1' 영역의 데이터를 판다스로 읽었을 때의 처리
                    target_row = table[table.iloc[:, 0].str.contains("52주최고", na=False)]
                    if not target_row.empty:
                        # 52주 최고/최저 수치 추출 (예: "80,000l60,000")
                        raw_val = str(target_row.iloc[0, 1])
                        if 'l' in raw_val:
                            high, low = raw_val.split('l')
                            price_data['high'] = float(high.replace(",", "").strip())
                            price_data['low'] = float(low.replace(",", "").strip())
    except Exception as e:
        print(f"   ⚠️ [Naver Price Error] {ticker}: {e}")
        
    return price_data

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"💰 [주가 업데이트] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    while True:
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = ""; is_kr = False
            
            # 티커 속성명 대응 (티커/Ticker)
            for name in ["티커", "Ticker"]:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content:
                    ticker = content[0].get("plain_text", "").strip().upper()
                    is_kr = len(ticker) == 6 and ticker[0].isdigit()
                    break
            
            if not ticker: continue
            
            try:
                upd = {}
                if is_kr:
                    # --- 한국 주식: 네이버 엔진 ---
                    d = get_kr_price(ticker)
                    if is_valid(d['price']): upd["현재가"] = {"number": d['price']}
                    if is_valid(d['high']): upd["52주 최고가"] = {"number": d['high']}
                    if is_valid(d['low']): upd["52주 최저가"] = {"number": d['low']}
                else:
                    # --- 미국 주식: 야후 엔진 ---
                    stock = yf.Ticker(ticker)
                    fast = stock.fast_info # fast_info가 일반 info보다 속도가 빠름
                    if is_valid(fast.get("last_price")): upd["현재가"] = {"number": fast["last_price"]}
                    if is_valid(fast.get("year_high")): upd["52주 최고가"] = {"number": fast["year_high"]}
                    if is_valid(fast.get("year_low")): upd["52주 최저가"] = {"number": fast["year_low"]}

                # 업데이트 시간 기록
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                print(f"   ✅ [{ticker}] 가격 업데이트 완료")
                
            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.4) # API 속도 제한 준수

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

if __name__ == "__main__":
    main()
