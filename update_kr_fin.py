import os
import time
import requests
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def debug_naver_api(ticker):
    """
    API 응답의 원본 데이터를 출력하여 어디서 막히는지 확인합니다.
    """
    print(f"\n🔍 [{ticker}] 탐색 시작...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    eps, bps = None, None
    
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        res = requests.get(url, headers=headers, timeout=10)
        
        if res.status_code != 200:
            print(f"   ❌ API 연결 실패 (HTTP {res.status_code})")
            return None, None

        data = res.json()
        items = data.get("result", {}).get("totalInfos", [])
        
        if not items:
            print(f"   ❌ API 응답에 재무 정보(totalInfos)가 아예 없습니다.")
            return None, None

        for item in items:
            key = item.get("key", "").upper()
            val = str(item.get("value", "")).replace(",", "").replace("원", "").strip()
            
            # 로그에 키와 값 표시
            if "EPS" in key:
                print(f"   -> API에서 찾은 EPS 키: '{item.get('key')}', 값: '{item.get('value')}'")
                try: eps = float(val)
                except: print(f"      ⚠️ '{val}'을 숫자로 변환하지 못했습니다.")
            
            if "BPS" in key:
                print(f"   -> API에서 찾은 BPS 키: '{item.get('key')}', 값: '{item.get('value')}'")
                try: bps = float(val)
                except: print(f"      ⚠️ '{val}'을 숫자로 변환하지 못했습니다.")

    except Exception as e:
        print(f"   🚨 네트워크 또는 JSON 파싱 에러: {e}")
        
    return eps, bps

def extract_ticker(props):
    """노션 속성 이름과 타입을 로그로 남깁니다."""
    # 사용자님의 노션 컬럼명을 확인하기 위한 출력
    print(f"   📊 노션 속성 목록: {list(props.keys())}")
    
    for name in ["티커", "Ticker"]:
        prop = props.get(name)
        if not prop: continue
        
        p_type = prop.get("type")
        content = prop.get("title") or prop.get("rich_text")
        if content:
            ticker = content[0].get("plain_text", "").strip()
            print(f"   📌 추출된 티커: {ticker} (속성명: {name})")
            return ticker
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🛠️ [디버깅 모드] 한국 재무 업데이트 분석 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    while True:
        # 노션 페이지네이션 적용
        response = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        pages = response.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = extract_ticker(props)
            
            if len(ticker) == 6:
                eps, bps = debug_naver_api(ticker)
                
                # 노션에 반영 시도 시 로그
                if eps is not None or bps is not None:
                    print(f"   ✅ 데이터 확보 성공! 노션 업데이트 시도...")
                    try:
                        upd = {}
                        if eps is not None: upd["EPS"] = {"number": eps}
                        if bps is not None: upd["BPS"] = {"number": bps}
                        
                        notion.pages.update(page_id=page["id"], properties=upd)
                        print(f"      🚀 노션 업데이트 완료!")
                    except Exception as e:
                        print(f"      🚨 노션 업데이트 에러 (컬럼명이 'EPS', 'BPS'가 맞는지 확인): {e}")
                else:
                    print(f"   ❌ 최종 데이터 없음 (기록 스킵)")
            
            time.sleep(1) # 상세 로그 확인을 위해 천천히 진행

        if not response.get("has_more"): break
        next_cursor = response.get("next_cursor")

if __name__ == "__main__":
    main()
