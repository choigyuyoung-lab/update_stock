import os
import time
import math
import requests
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client
from bs4 import BeautifulSoup

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    """노션 API 전송 전 숫자 유효성 검사 (NaN, Inf, None 방지)"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def get_kr_price(ticker):
    """
    [한국 주식] 네이버 금융 PC 페이지(HTML)를 직접 크롤링
    - 화면에 보이는 '빨간색 큰 숫자(현재가)' 추출 (div.today)
    - 52주 최고/최저가 추출 (테이블 파싱)
    """
    price_data = {'price': None, 'high': None, 'low': None}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=10)
        
        # 인코딩 처리
        res.encoding = res.apparent_encoding 
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. 현재가 추출
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area:
            price_data['price'] = float(today_area.text.replace(',', '').strip())

        # 2. 52주 최고/최저가 추출
        th_tags = soup.find_all('th')
        for th in th_tags:
            if "52주최고" in th.text:
                td = th.find_next_sibling('td')
                if td:
                    ems = td.select('em')
                    if len(ems) >= 2:
                        high_str = ems[0].text.strip().replace(',', '')
                        low_str = ems[1].text.strip().replace(',', '')
                        price_data['high'] = float(high_str)
                        price_data['low'] = float(low_str)
                break 

    except Exception as e:
        print(f"   ⚠️ [Naver Parsing Error] {ticker}: {e}")
        
    return price_data

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    
    print(f"💰 [주가 업데이트] 최종 점검 버전 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
            pages = res.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 페이지가 없습니다.")
                break

            for page in pages:
                props = page["properties"]
                ticker =
