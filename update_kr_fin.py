import os
import time
import requests
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def get_naver_api_data(ticker):
    """
    [블로그 가이드 반영] 네이버 증권 JSON API를 직접 호출하여
    가장 정확한 EPS와 BPS 데이터를 가져옵니다.
    """
    eps, bps = None, None
    try:
        # 네이버 모바일 통합 API 엔드포인트
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        res = requests.get(url, headers=headers, timeout=10).json()
        
        # JSON 결과 내의 totalInfos 리스트에서 EPS/BPS 탐색
        items = res.get("result", {}).get("totalInfos", [])
        for item in items:
            key = item.get("key", "").upper()
            val = item.get("value", "").replace(",", "").replace("원", "").strip()
            
            # 유효한 숫자인 경우만 float 변환 (마이너스 포함)
            if "EPS" in key and val not in ["", "-", "N/A"]:
                try: eps = float(val)
                except: pass
            if "BPS" in key and val not in ["", "-", "N/A"]:
                try: bps = float(val)
                except: pass
    except Exception as e:
        print(f"      ⚠️ {ticker} API 호출 중 오류: {e}")
        
    return {"eps": eps, "bps": bps}

def extract_ticker(props):
    """노션 속성(제목 또는 텍스트)에서 티커 추출"""
    for name in ["티커", "Ticker"]:
        prop = props.get(name, {})
        content = prop.get("title") or prop.get("rich_text")
        if content:
            return content[0].get("plain_text", "").strip()
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇰🇷 [한국 재무 업데이트] 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    success, fail, skip = 0, 0, 0
    next_cursor = None
    
    # [핵심] 100개 제한을 풀기 위한 무한 루프 페이지네이션
    while True:
        response = notion.databases.query(
            database_id=DATABASE_ID,
            start_cursor=next_cursor
        )
        pages = response.get("results", [])
        
        for page in pages:
            try:
                props = page["properties"]
                ticker = extract_ticker(props)
                
                # 티커가 없거나 한국 주식(6자리)이 아니면 건너뜀
                if not ticker or len(ticker) != 6:
                    skip += 1
                    continue

                # API 데이터 호출
                data = get_naver_api_data(ticker)
                
                # 데이터가 하나라도 있는 경우만 노션 업데이트
                if data["eps"] is not None or data["bps"] is not None:
                    upd_props = {}
                    if data["eps"] is not None: upd_props["EPS"] = {"number": data["eps"]}
                    if data["bps"] is not None: upd_props["BPS"] = {"number": data["bps"]}
                    
                    notion.pages.update(page_id=page["id"], properties=upd_props)
                    success += 1
                    print(f"   => ✅ {ticker} | EPS: {data['eps']} | BPS: {data['bps']}")
                else:
                    print(f"   => ❌ {ticker} | 데이터 누락")
                    fail += 1
                
                time.sleep(0.3) # API 호출 매너 딜레이

            except Exception as e:
                print(f"   => 🚨 {ticker} 처리 중 에러: {e}")
                fail += 1
                continue

        # [핵심] 다음 페이지가 없으면 루프 탈출
        if not response.get("has_more"):
            break
        next_cursor = response.get("next_cursor")

    print(f"\n✨ 완료 | 성공: {success} | 실패: {fail} | 건너뜀: {skip}")

if __name__ == "__main__":
    main()
