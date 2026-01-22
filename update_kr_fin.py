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

def clean_value(val_str):
    """문자열에서 숫자만 추출 (마이너스, 소수점 포함)"""
    if not val_str: return None
    # 'N/A', '-', '원' 등 불필요한 문자 제거
    clean_val = str(val_str).replace(",", "").replace("원", "").replace(" ", "").strip()
    try:
        # 마이너스 기호가 포함된 숫자도 변환 가능하도록 처리
        return float(clean_val)
    except ValueError:
        return None

def get_kr_financials(ticker):
    """네이버 통합 API와 웹 페이지를 중첩 검색하여 데이터 확보"""
    eps, bps = None, None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    
    try:
        # 방법 1: 모바일 API (가장 빠름)
        api_url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(api_url, headers=headers, timeout=10).json()
        infos = res.get("result", {}).get("totalInfos", [])
        
        for item in infos:
            key = item.get("key", "").upper()
            if "EPS" in key: eps = clean_value(item.get("value"))
            if "BPS" in key: bps = clean_value(item.get("value"))

        # 방법 2: API에 데이터가 없을 경우 PC용 웹페이지 표 분석
        if eps is None or bps is None:
            pc_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            # lxml 엔진으로 표 전체 로드
            tables = pd.read_html(pc_url, encoding='cp949')
            for table in tables:
                table_str = table.to_string()
                if "EPS" in table_str or "BPS" in table_str:
                    table = table.set_index(table.columns[0])
                    # 현재 실적 행에서 데이터 추출
                    if "EPS" in table.index and eps is None:
                        eps = clean_value(table.loc["EPS"].iloc[0])
                    if "BPS" in table.index and bps is None:
                        bps = clean_value(table.loc["BPS"].iloc[0])
    except Exception as e:
        print(f"      ⚠️ {ticker} 데이터 추출 중 오류: {e}")
    
    return {"eps": eps, "bps": bps}

def extract_ticker(props):
    """티커 속성에서 문자열 추출 (다양한 타입 대응)"""
    prop = props.get("티커", {})
    p_type = prop.get("type")
    if p_type == "title":
        return prop.get("title", [{}])[0].get("plain_text", "")
    elif p_type == "rich_text":
        return prop.get("rich_text", [{}])[0].get("plain_text", "")
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇰🇷 [한국 재무 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None
    
    while True:
        # [중요] 100개 제한 없는 페이지네이션
        response = notion.databases.query(
            database_id=DATABASE_ID,
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
            try:
                props = page["properties"]
                ticker = extract_ticker(props).strip()
                
                # 한국 종목 판별 (6글자)
                if len(ticker) != 6:
                    skip += 1
                    continue

                # 데이터 가져오기
                data = get_kr_financials(ticker)
                
                if data["eps"] is not None or data["bps"] is not None:
                    # 노션 속성 업데이트 (데이터가 있는 것만)
                    upd_props = {}
                    if data["eps"] is not None: upd_props["EPS"] = {"number": data["eps"]}
                    if data["bps"] is not None: upd_props["BPS"] = {"number": data["bps"]}
                    
                    notion.pages.update(page_id=page["id"], properties=upd_props)
                    success += 1
                    print(f"   => ✅ {ticker} | EPS: {data['eps']} | BPS: {data['bps']}")
                else:
                    print(f"   => ❌ {ticker} | 데이터 찾지 못함")
                    fail += 1
                
                time.sleep(0.3) # 서버 부하 방지

            except Exception as e:
                print(f"   => 🚨 {ticker} 업데이트 에러: {e}")
                fail += 1
                continue

        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 업데이트 완료 | 성공: {success} | 실패: {fail} | 건너뜀: {skip}")

if __name__ == "__main__":
    main()
