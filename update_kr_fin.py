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
    [핵심] pandas를 이용해 네이버 금융 화면의 '투자지표' 표를 직접 읽어옵니다.
    0104P0 같은 특수 코드도 네이버 메인 페이지에서 정확히 읽어올 수 있습니다.
    """
    try:
        # 네이버 금융 메인 페이지 URL
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        
        # lxml 엔진을 사용하여 표 읽기 (cp949 인코딩 필수)
        dfs = pd.read_html(url, encoding='cp949', header=0)
        
        eps, bps = None, None
        
        for df in dfs:
            # 데이터프레임을 문자열로 변환하여 검색
            df_str = df.to_string()
            
            if "EPS" in df_str or "BPS" in df_str:
                try:
                    # 첫 번째 열을 인덱스로 설정 (EPS, BPS 행을 찾기 위함)
                    df = df.set_index(df.columns[0])
                    
                    # EPS 추출
                    if "EPS" in df.index:
                        val = str(df.loc["EPS"].iloc[0]).replace(",", "").split(" ")[0]
                        if val.replace("-","").replace(".","").isdigit(): 
                            eps = float(val)
                            
                    # BPS 추출
                    if "BPS" in df.index:
                        val = str(df.loc["BPS"].iloc[0]).replace(",", "").split(" ")[0]
                        if val.replace("-","").replace(".","").isdigit(): 
                            bps = float(val)
                except:
                    continue
        
        # 만약 Pandas로 실패했다면 모바일 API로 2차 시도
        if eps is None and bps is None:
            return get_financial_mobile(ticker)

        return {"eps": eps, "bps": bps}

    except:
        return get_financial_mobile(ticker)

def get_financial_mobile(ticker):
    """예비용: 네이버 모바일 API (JSON 방식)"""
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
            
            if "EPS" in key and val.replace("-","").replace(".","").replace(" ", "").replace("N/A","").isdigit():
                eps = float(val)
            if "BPS" in key and val.replace("-","").replace(".","").replace(" ", "").replace("N/A","").isdigit():
                bps = float(val)
                
        return {"eps": eps, "bps": bps}
    except:
        return None

def extract_value(prop):
    """노션 속성값 안전 추출 (update_stock.py와 동일한 로직 적용)"""
    if not prop: return ""
    p_type = prop.get("type")
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        return extract_value(array[0]) if array else ""
    if p_type == "select": return prop.get("select", {}).get("name", "")
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        return text_list[0].get("plain_text", "") if text_list else ""
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇰🇷 [한국 재무정보] 업데이트 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 데이터베이스 전체 쿼리
        response = notion.databases.query(database_id=DATABASE_ID)
        pages = response.get("results", [])
    except Exception as e:
        print(f"🚨 노션 연결 오류: {e}")
        return

    success, fail, skip = 0, 0, 0
    
    for page in pages:
        try:
            props = page["properties"]
            ticker = extract_value(props.get("티커")).strip()
            
            # 한국 주식 판별 (6글자)
            if len(ticker) != 6:
                skip += 1
                continue

            # 데이터 조회
            data = get_naver_financials(ticker)
            
            if data and (data["eps"] or data["bps"]):
                upd = {}
                if data["eps"]: upd["EPS"] = {"number": data["eps"]}
                if data["bps"]: upd["BPS"] = {"number": data["bps"]}
                
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    success += 1
                    print(f"   => ✅ {ticker} : EPS {data['eps']} / BPS {data['bps']}")
                else:
                    fail += 1
            else:
                fail += 1
            
            # 네이버 차단 방지를 위한 짧은 대기
            time.sleep(0.3)

        except:
            fail += 1
            continue

    print(f"\n✨ 완료: 성공 {success} / 실패 {fail} / 건너뜀(미국 등) {skip}")

if __name__ == "__main__":
    main()
