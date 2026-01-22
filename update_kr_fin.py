import os
import json
import urllib.request
import time
from datetime import datetime, timedelta, timezone
from notion_client import Client

# 1. 환경 설정 (기존 키 그대로 사용)
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def get_financial_info(ticker):
    """네이버 금융에서 EPS와 BPS(역산) 가져오기"""
    try:
        url = f"https://api.finance.naver.com/service/itemSummary.nhn?itemcode={ticker}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        response = urllib.request.urlopen(req).read()
        data = json.loads(response)
        
        if not data: return None
        
        price = data.get("now")
        eps = data.get("eps")
        pbr = data.get("pbr")
        
        # BPS 계산 (주가 / PBR)
        bps = None
        if price and pbr and pbr > 0:
            bps = price / pbr
            
        return {"eps": eps, "bps": bps}
    except:
        return None

def extract_ticker(props):
    """티커 추출 (롤업, 텍스트 모두 대응)"""
    # 1. 롤업인 경우
    if props.get("티커", {}).get("type") == "rollup":
        array = props.get("티커", {}).get("rollup", {}).get("array", [])
        if array:
            # 롤업 내부가 텍스트/타이틀인 경우
            if array[0].get("type") in ["rich_text", "title"]:
                return array[0].get(array[0].get("type"), [])[0].get("plain_text", "")
    
    # 2. 텍스트/타이틀인 경우
    ticker_data = props.get("티커", {}).get("title", []) or props.get("티커", {}).get("rich_text", [])
    if ticker_data:
        return ticker_data[0].get("plain_text", "")
        
    return ""

def main():
    kst = timezone(timedelta(hours=9))
    print(f"🇰🇷 [관심주 한국 재무정보] 업데이트 시작 - {datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
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
                    
                    # [핵심] 한국 주식(숫자 6자리)만 골라냄
                    if not (ticker.isdigit() and len(ticker) == 6):
                        skip += 1
                        continue # 미국 주식은 패스

                    # 데이터 가져오기
                    data = get_financial_info(ticker)
                    
                    if data:
                        upd = {}
                        if data["eps"]: upd["EPS"] = {"number": data["eps"]}
                        if data["bps"]: upd["BPS"] = {"number": round(data["bps"])}
                        
                        if upd:
                            notion.pages.update(page_id=page["id"], properties=upd)
                            success += 1
                            print(f"   => ✅ {ticker} : EPS {data['eps']} / BPS {round(data['bps'] or 0)}")
                    
                    time.sleep(0.1) # 네이버 예의상 대기

                except Exception as e:
                    continue
            
            if not has_more: break
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 오류: {e}")
            break

    print(f"✨ 결과: 한국주식 업데이트 {success}건 / 건너뜀(미국 등) {skip}건")

if __name__ == "__main__":
    main()
