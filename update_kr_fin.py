import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def get_kr_finance_data(ticker):
    """
    네이버 API(블로그 가이드 방식)와 웹 페이지 표 분석을 결합하여 
    EPS, BPS를 누락 없이 가져옵니다.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': f'https://finance.naver.com/item/main.naver?code={ticker}'
    }
    eps, bps = None, None
    
    try:
        # [1단계] 네이버 모바일 통합 API 시도
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(url, headers=headers, timeout=10).json()
        
        items = res.get("result", {}).get("totalInfos", [])
        for item in items:
            key = item.get("key", "").upper()
            val = str(item.get("value", "")).replace(",", "").replace("원", "").strip()
            
            if "EPS" in key and val not in ["", "-", "N/A"]:
                try: eps = float(val)
                except: pass
            if "BPS" in key and val not in ["", "-", "N/A"]:
                try: bps = float(val)
                except: pass

        # [2단계] API 데이터가 없을 경우 PC용 주요재무정보 표 분석 (대형주/지주사 대응)
        if eps is None or bps is None:
            pc_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            tables = pd.read_html(pc_url, encoding='cp949')
            for table in tables:
                if any("주요재무정보" in str(col) for col in table.columns):
                    table.columns = table.columns.get_level_values(-1)
                    table = table.set_index(table.columns[0])
                    
                    # 최근 결산 열(4번째 열)에서 데이터 추출
                    if "EPS(원)" in table.index and eps is None:
                        val = str(table.loc["EPS(원)"].iloc[3]).replace(",", "")
                        if val.replace(".","").replace("-","").replace("nan","").isdigit(): eps = float(val)
                    if "BPS(원)" in table.index and bps is None:
                        val = str(table.loc["BPS(원)"].iloc[3]).replace(",", "")
                        if val.replace(".","").replace("-","").replace("nan","").isdigit(): bps = float(val)
                    break
    except:
        pass
        
    return eps, bps

def extract_ticker(props):
    """노션에서 한국 주식 티커(6자리 숫자)를 추출합니다."""
    for name in ["티커", "Ticker"]:
        prop = props.get(name, {})
        content = prop.get("title") or prop.get("rich_text")
        if content:
            ticker = content[0].get("plain_text", "").strip()
            # 6자리 숫자 형식일 때만 한국 주식으로 간주
            if ticker.isdigit() and len(ticker) == 6:
                return ticker
    return None

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇰🇷 [한국 재무 전용 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None
    
    # [핵심] 100개 제한 해제를 위한 페이지네이션 무한 루프
    while True:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = extract_ticker(props)
            
            # 한국 주식이 아니면 건너뜀
            if not ticker:
                skip += 1
                continue

            # 데이터 수집 (API + HTML)
            eps, bps = get_kr_finance_data(ticker)
            
            if eps is not None or bps is not None:
                # 노션 속성 이름이 'EPS', 'BPS' (대문자)인지 확인 필수
                upd = {}
                if eps is not None: upd["EPS"] = {"number": eps}
                if bps is not None: upd["BPS"] = {"number": bps}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                success += 1
                print(f"   => ✅ {ticker} | EPS: {eps} | BPS: {bps}")
            else:
                print(f"   => ❌ {ticker} | 데이터 누락 (재확인 필요)")
                fail += 1
            
            time.sleep(0.4) # 네이버 서버 부하 방지용 지연

        # 다음 페이지가 없으면 루프 종료
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 완료 | 성공: {success} | 실패: {fail} | 건너뜀: {skip}")

if __name__ == "__main__":
    main()
