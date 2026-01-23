import os, time, requests, yfinance as yf
from notion_client import Client

# 환경 변수 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def get_kr_industry(ticker):
    """한국 종목 업종 추출 (네이버 금융)"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers)
        # 업종 정보가 포함된 부분을 간단하게 파싱 (정규식 대용)
        text = resp.text
        if 'h4 class="h_sub"' in text:
            industry = text.split('h4 class="h_sub"')[1].split('em>')[1].split('</em')[0].strip()
            return industry
    except: pass
    return None

def get_us_industry(ticker):
    """미국 종목 섹터/산업 추출 (yfinance)"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        sector = info.get('sector', '')
        industry = info.get('industry', '')
        if sector and industry: return f"{sector} | {industry}"
        return sector or industry
    except: return None

def main():
    print("🏗️ [산업분류 업데이트] 시작...")
    next_cursor = None
    update_count = 0

    while True:
        # 산업분류가 비어 있는 페이지만 필터링하여 쿼리
        query_params = {
            "database_id": MASTER_DB_ID,
            "start_cursor": next_cursor,
            "filter": {
                "property": "산업분류", # 노션의 속성 이름과 일치해야 함
                "rich_text": {"is_empty": True}
            }
        }
        res = notion.databases.query(**query_params)
        pages = res.get("results", [])

        for page in pages:
            props = page["properties"]
            ticker = ""
            # 티커 찾기 (사용자님의 기존 필드명 규칙 적용)
            for name in ["티커", "Ticker"]:
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content: ticker = content[0].get("plain_text", "").strip().upper(); break
            
            if not ticker: continue
            
            is_kr = len(ticker) == 6 and ticker[0].isdigit()
            industry_info = get_kr_industry(ticker) if is_kr else get_us_industry(ticker)

            if industry_info:
                try:
                    notion.pages.update(
                        page_id=page["id"],
                        properties={"산업분류": {"rich_text": [{"text": {"content": industry_info}}]}}
                    )
                    print(f"   ✅ [{ticker}] -> {industry_info}")
                    update_count += 1
                except Exception as e:
                    print(f"   ❌ [{ticker}] 업데이트 실패: {e}")
            
            time.sleep(0.5) # API 속도 제한 준수

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"✨ 업데이트 완료! 총 {update_count}개 종목의 산업분류를 채웠습니다.")

if __name__ == "__main__":
    main()
