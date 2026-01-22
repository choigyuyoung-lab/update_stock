import os
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def get_naver_financials(ticker):
    """
    [핵심] pandas를 이용해 네이버 금융 화면의 표를 직접 긁어옵니다.
    """
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        
        # 1. 화면에 있는 모든 표를 가져옴
        dfs = pd.read_html(url, encoding='cp949', header=0)
        
        eps = None
        bps = None
        
        for df in dfs:
            df_str = df.to_string()
            
            # 테이블 안에 EPS, BPS 정보가 있는지 확인
            if "EPS" in df_str or "BPS" in df_str:
                try:
                    # 표 구조에 따라 데이터 추출
                    df = df.set_index(df.columns[0])
                    
                    if "EPS" in df.index:
                        val = str(df.loc["EPS"].iloc[0]).replace(",", "").split(" ")[0] 
                        if val.replace("-","").isdigit(): eps = float(val)
                            
                    if "BPS" in df.index:
                        val = str(df.loc["BPS"].iloc[0]).replace(",", "").split(" ")[0]
                        if val.replace("-","").isdigit(): bps = float(val)
                except:
                    continue
        
        # 못 찾았으면 모바일 API 시도
        if eps is None or bps is None:
            return get_financial_mobile(ticker)

        return {"eps": eps, "bps": bps}

    except Exception as e:
        return get_financial_mobile(ticker)

def get_financial_mobile(ticker):
    """예비용: 네이버 모바일 API"""
    import urllib.request
    import json
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        res = urllib.request.urlopen(req).read()
        data = json.loads(res)
        
        infos = data.get("result", {}).get("totalInfos", [])
        eps, bps = None, None
        
        for item in infos:
            key = item.get("key", "").upper()
            val = str(item.get("value", "")).replace(",", "").replace("원", "")
            
            if "EPS" in key and val.replace("-","").replace(".","").isdigit():
                eps = float(val)
            if "BPS" in key and val.replace("-","").replace(".","").isdigit():
                bps = float(val)
                
        return {"eps": eps, "bps": bps}
    except:
        return None

def extract_ticker(props):
    """티커 추출 (롤업/텍스트)"""
    if props.get("티커", {}).get("type") == "rollup":
        array = props.get("티커", {}).get("rollup", {}).get("array", [])
        if array and array[0].get("type") in ["rich_text", "title"]:
            return array[0].get(array[0].get("type"), [])[0].get("plain_text", "")
    
    ticker_data = props.get("티커", {}).get("title", []) or props.get("티커", {}).get("rich_text", [])
    if ticker_data: return ticker_data[0].get("plain_text", "")
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇰🇷 [한국 재무정보(Pandas)] 업데이트 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    skip = 0
    
    while has_more:
        try:
            response = notion.databases.query(
                database_id=DATABASE_ID,
                start_cursor=next_cursor
            )
            pages = response.get("results", [])
            if not pages: break

            for page in pages:
                try:
                    props = page["properties"]
                    ticker = extract_ticker(props).strip()
                    
                    # 한국 주식(숫자 6자리)만
                    if not (ticker.isdigit() and len(ticker) == 6):
                        skip += 1
                        continue

                    # 데이터 가져오기
                    data = get_naver_financials(ticker)
                    
                    if data and (data["eps"] or data["bps"]):
                        upd = {}
                        if data["eps"]: upd["EPS"] = {"number": data["eps"]}
                        if data["bps"]: upd["BPS"] = {"number": round(data["bps"])}
                        
                        if upd:
                            notion.pages.update(page_id=page["id"], properties=upd)
                            success += 1
                            print(f"   => ✅ {ticker} : EPS {data['eps']} / BPS {data['bps']}")
                        else:
                             fail += 1
                    else:
                        print(f"   => ❌ {ticker} : 조회 실패")
                        fail += 1
                    
                    time.sleep(0.2) 

                except:
                    fail += 1
                    continue
            
            if not has_more: break
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 오류: {e}")
            break

    print(f"✨ 결과: 성공 {success} / 실패 {fail} / 건너뜀 {skip}")

if __name__ == "__main__":
    main()
