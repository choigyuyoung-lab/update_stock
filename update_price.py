import os
import time
import math
import requests
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client
from bs4 import BeautifulSoup  # [핵심] HTML 파싱 도구

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
    - API가 아닌 화면에 보이는 '빨간색 큰 숫자(현재가)'를 가져옴
    - 52주 최고/최저가도 HTML 태그를 분석해서 추출
    """
    price_data = {'price': None, 'high': None, 'low': None}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=10)
        
        # 인코딩 처리 (한글 깨짐 방지)
        res.encoding = res.apparent_encoding 
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # -----------------------------------------------------
        # 1. 현재가 추출 (화면 상단의 큰 숫자)
        # 위치: div.today > p.no_today > em > span.blind
        # -----------------------------------------------------
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area:
            # 쉼표(,) 제거 후 실수형(float)으로 변환
            price_data['price'] = float(today_area.text.replace(',', '').strip())

        # -----------------------------------------------------
        # 2. 52주 최고/최저가 추출
        # 위치: 테이블(table) 내 '52주최고' 텍스트가 있는 행(tr) 찾기
        # -----------------------------------------------------
        # '52주최고'라는 글자를 포함한 <th> 태그 등을 찾음
        th_tags = soup.find_all('th')
        for th in th_tags:
            if "52주최고" in th.text:
                # th의 형제 태그인 td를 찾음
                td = th.find_next_sibling('td')
                if td:
                    # 구조: <em>80,000</em> l <em>60,000</em>
                    ems = td.select('em')
                    if len(ems) >= 2:
                        high_str = ems[0].text.strip().replace(',', '')
                        low_str = ems[1].text.strip().replace(',', '')
                        price_data['high'] = float(high_str)
                        price_data['low'] = float(low_str)
                break # 찾았으면 루프 종료

    except Exception as e:
        print(f"   ⚠️ [Naver Parsing Error] {ticker}: {e}")
        
    return price_data

def main():
    # 한국 시간 설정
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    
    print(f"💰 [주가 업데이트] HTML 크롤링 방식 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # 노션 DB 조회
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
            pages = res.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 페이지가 없습니다.")
                break

            for page in pages:
                props = page["properties"]
                ticker = ""; is_kr = False
                
                # 티커 추출 (속성명이 '티커' 또는 'Ticker')
                for name in ["티커", "Ticker"]:
                    target = props.get(name)
                    if target:
                        content = target.get("title") or target.get("rich_text")
                        if content:
                            ticker = content[0].get("plain_text", "").strip().upper()
                            # 한국 주식 판별 (숫자 6자리)
                            is_kr = len(ticker) == 6 and ticker.isdigit()
                            break
                
                if not ticker: continue
                
                try:
                    upd = {}
                    current_price_log = 0
                    
                    if is_kr:
                        # [한국] 네이버 PC HTML 크롤링
                        d = get_kr_price(ticker)
                        if is_valid(d['price']): 
                            upd["현재가"] = {"number": d['price']}
                            current_price_log = d['price']
                        if is_valid(d['high']): upd["52주 최고가"] = {"number": d['high']}
                        if is_valid(d['low']): upd["52주 최저가"] = {"number": d['low']}
                    else:
                        # [미국] 야후 파이낸스
                        stock = yf.Ticker(ticker)
                        fast = stock.fast_info
                        last_price = fast.get("last_price")
                        
                        if is_valid(last_price): 
                            upd["현재가"] = {"number": last_price}
                            current_price_log = last_price
                        if is_valid(fast.get("year_high")): upd["52주 최고가"] = {"number": fast["year_high"]}
                        if is_valid(fast.get("year_low")): upd["52주 최저가"] = {"number": fast["year_low"]}

                    # 업데이트 시간 기록
                    upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                    
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [{ticker}] 완료 (현재가: {current_price_log})")
                    
                except Exception as e:
                    print(f"   ❌ [{ticker}] 실패: {e}")
                
                # 네이버 차단 방지를 위한 대기
                time.sleep(0.5) 

            if not res.get("has_more"): break
            next_cursor = res.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 에러: {e}")
            break

    print(f"🏁 작업 완료: 총 {processed_count}건 업데이트됨")

if __name__ == "__main__":
    main()
