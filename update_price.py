import os
import time
import math
import re
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

def get_kr_stock_data(ticker):
    """
    [한국 주식] 네이버 금융에서 현재가, 52주 고/저, 목표주가, 투자의견 추출
    """
    data = {
        'price': None, 'high': None, 'low': None, 
        'target_price': None, 'opinion': None
    }
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = res.apparent_encoding 
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. 현재가 추출
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area:
            data['price'] = float(today_area.text.replace(',', '').strip())

        # 2. 52주 최고/최저가 추출
        th_tags = soup.find_all('th')
        for th in th_tags:
            if "52주최고" in th.text:
                td = th.find_next_sibling('td')
                if td:
                    ems = td.select('em')
                    if len(ems) >= 2:
                        data['high'] = float(ems[0].text.strip().replace(',', ''))
                        data['low'] = float(ems[1].text.strip().replace(',', ''))
                break 

        # 3. [신규] 목표주가 및 투자의견 추출 (summary="투자의견 정보" 테이블 타겟)
        target_table = soup.find('table', summary="투자의견 정보")
        if target_table:
            td = target_table.find('td')
            if td:
                ems = td.find_all('em')
                if ems:
                    # 마지막 em 태그가 목표주가 (예: 77,889)
                    p_raw = ems[-1].get_text(strip=True).replace(',', '')
                    if p_raw.replace('.', '').isdigit():
                        data['target_price'] = float(p_raw)
                
                # 투자의견 추출 (4.00매수 등)
                opinion_span = td.find('span', class_='f_up')
                if opinion_span:
                    data['opinion'] = opinion_span.get_text(strip=True)

    except Exception as e:
        print(f"   ⚠️ [Naver Error] {ticker}: {e}")
        
    return data

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    
    print(f"💰 [주가 업데이트] 목표주가 통합 버전 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
            pages = res.get("results", [])
            
            for page in pages:
                props = page["properties"]
                ticker = ""
                is_kr = False
                
                # 티커 추출 로직 (기존 유지)
                for name in ["티커", "Ticker"]:
                    target = props.get(name)
                    if target:
                        content = target.get("title") or target.get("rich_text")
                        if content:
                            ticker = content[0].get("plain_text", "").strip().upper()
                            # 스마트 분류
                            if ticker.endswith('.KS') or ticker.endswith('.KQ') or any(char.isdigit() for char in ticker):
                                is_kr = True
                            else:
                                is_kr = False
                            break
                
                if not ticker: continue
                
                try:
                    upd = {}
                    
                    if is_kr:
                        # [한국]
                        d = get_kr_stock_data(ticker)
                        if is_valid(d['price']): upd["현재가"] = {"number": d['price']}
                        if is_valid(d['high']): upd["52주 최고가"] = {"number": d['high']}
                        if is_valid(d['low']): upd["52주 최저가"] = {"number": d['low']}
                        # [신규 추가]
                        if is_valid(d['target_price']): upd["목표주가"] = {"number": d['target_price']}
                        if d['opinion']: upd["목표가 범위"] = {"rich_text": [{"text": {"content": d['opinion']}}]}
                    else:
                        # [미국] yfinance
                        stock = yf.Ticker(ticker)
                        info = stock.info # 목표가 데이터를 위해 fast_info 대신 info 사용
                        
                        last_price = info.get('currentPrice') or info.get('regularMarketPrice')
                        if is_valid(last_price): upd["현재가"] = {"number": last_price}
                        if is_valid(info.get('fiftyTwoWeekHigh')): upd["52주 최고가"] = {"number": info.get('fiftyTwoWeekHigh')}
                        if is_valid(info.get('fiftyTwoWeekLow')): upd["52주 최저가"] = {"number": info.get('fiftyTwoWeekLow')}
                        
                        # [신규 추가] 미국 목표가 및 범위
                        if is_valid(info.get('targetMeanPrice')): 
                            upd["목표주가"] = {"number": info.get('targetMeanPrice')}
                        
                        low = info.get('targetLowPrice')
                        high = info.get('targetHighPrice')
                        if low and high:
                            range_str = f"{low} ~ {high}"
                            upd["목표가 범위"] = {"rich_text": [{"text": {"content": range_str}}]}

                    # 공통: 업데이트 시간 기록
                    upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                    
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [{ticker}] 업데이트 완료")
                    
                except Exception as e:
                    print(f"   ❌ [{ticker}] 실패: {e}")
                
                time.sleep(0.5) 

            if not res.get("has_more"): break
            next_cursor = res.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 에러: {e}")
            break

    print(f"🏁 작업 완료: 총 {processed_count}건 업데이트됨")

if __name__ == "__main__":
    main()
