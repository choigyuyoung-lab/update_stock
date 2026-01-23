import os, time, yfinance as yf
from notion_client import Client

# 환경 변수 로드
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def main():
    print("🏭 [산업 정보 업데이트] 시작...")
    
    next_cursor = None
    while True:
        # 데이터베이스 조회
        res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            # 티커 추출 (티커 또는 Ticker 속성 확인)
            t_list = props.get("티커", {}).get("title") or props.get("Ticker", {}).get("rich_text")
            if not t_list: continue
            
            ticker = t_list[0]["plain_text"].strip().upper()
            # 한국 종목 판별 및 심볼 변환
            is_kr = len(ticker) == 6 and ticker[0].isdigit()
            symbol = ticker + (".KS" if is_kr else "")
            
            try:
                # 야후 파이낸스에서 산업 정보 가져오기
                info = yf.Ticker(symbol).info
                sector = info.get("sector") # 섹터 정보
                industry = info.get("industry") # 세부 산업 정보
                
                if sector or industry:
                    industry_text = f"{sector} - {industry}" if sector and industry else (sector or industry)
                    # 노션 '산업' 속성 업데이트 (속성명이 다르면 수정 필요)
                    notion.pages.update(
                        page_id=page["id"],
                        properties={
                            "산업": {"rich_text": [{"text": {"content": industry_text}}]}
                        }
                    )
                    print(f"   ✅ {ticker}: {industry_text}")
            except Exception as e:
                print(f"   ❌ {ticker}: 정보 검색 실패 ({e})")
            
            time.sleep(0.5) # API 부하 방지

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

if __name__ == "__main__":
    main()
