import os
import time
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def get_kr_financials(ticker):
    """네이버 모바일 API를 최우선으로 사용하여 EPS/BPS 추출"""
    eps, bps = None, None
    try:
        # 방식 1: 네이버 모바일 통합 API (가장 안정적)
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers).json()
        
        infos = res.get("result", {}).get("totalInfos", [])
        for item in infos:
            key = item.get("key", "").upper()
            val = str(item.get("value", "")).replace(",", "").replace("원", "").strip()
            if "EPS" in key and val.replace(".","").isdigit(): eps = float(val)
            if "BPS" in key and val.replace(".","").isdigit(): bps = float(val)

        # 방식 2: API 실패 시 웹 페이지 직접 파싱 (Pandas)
        if eps is None or bps is None:
            web_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            dfs = pd.read_html(web_url, encoding='cp949', header=0)
            for df in dfs:
                df = df.set_index(df.columns[0])
                if "EPS" in df.index:
                    v = str(df.loc["EPS"].iloc[0]).replace(",", "").split(" ")[0]
                    if v.replace(".","").isdigit(): eps = float(v)
                if "BPS" in df.index:
                    v = str(df.loc["BPS"].iloc[0]).replace(",", "").split(" ")[0]
                    if v.replace(".","").isdigit(): bps = float(v)
    except: pass
    return {"eps": eps, "bps": bps}

def extract_value(prop):
    if not prop: return ""
    p_type = prop.get("type")
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        return extract_value(array[0]) if array else ""
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        return text_list[0].get("plain_text", "") if text_list else ""
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇰🇷 [한국 재무 업데이트] 시작 (전체 데이터 모드)")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None
    
    while True:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
            try:
                props = page["properties"]
                ticker = extract_value(props.get("티커")).strip()
                
                # 한국 주식 판별 (6글자)
                if len(ticker) != 6:
                    skip += 1
                    continue

                data = get_kr_financials(ticker)
                
                if data["eps"] or data["bps"]:
                    upd = {}
                    if data["eps"]: upd["EPS"] = {"number": data["eps"]}
                    if data["bps"]: upd["BPS"] = {"number": data["bps"]}
                    
                    notion.pages.update(page_id=page["id"], properties=upd)
                    success += 1
                    print(f"   => ✅ {ticker} : EPS({data['eps']}), BPS({data['bps']})")
                else:
                    fail += 1
                time.sleep(0.4)
            except: fail += 1; continue

        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 재무 업데이트 완료: 성공 {success} / 실패 {fail} / 건너뜀 {skip}")

if __name__ == "__main__":
    main()
