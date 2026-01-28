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
    """노션 API 전송 전 숫자 유효성 검사"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def get_kr_stock_data(ticker):
    """한국 주식 데이터 추출 (네이버 금융)"""
    data = {'price': None, 'high': None, 'low': None, 'target_price': None, 'opinion': None}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = res.apparent_encoding 
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 현재가 및 52주 고저 (기존 로직)
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area: data['price'] = float(today_area.text.replace(',', '').strip())

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

        # 목표주가 및 투자의견 (요청하신 summary 테이블 기준)
        target_table = soup.find('table', summary="투자의견 정보")
        if target_table:
            td = target_table.find('td')
            if td:
                ems = td.find_all('em')
                if ems: data['target_price'] = float(ems[-1].get_text(strip=True).replace(',', ''))
                opinion_span = td.find('span', class_='f_up')
                if opinion_span: data['opinion'] = opinion_span.get_text(strip=True)
    except Exception as e:
        print(f"   ⚠️ [Naver Error] {ticker}: {e}")
    return data

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"💰 [주가 업데이트] 최종 문법 검증 완료 버전 시작 - {datetime.now(kst)}")
    
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
                
                # 티커 추출 및 한국/미국 분류
                for name in ["티커", "Ticker"]:
                    target = props.get(name)
                    if target:
                        content = target.get("title") or target.get("rich_text")
                        if content:
                            ticker = content[0].get("plain_text", "").strip().upper()
                            is_kr = ticker.endswith(('.KS', '.KQ')) or any(char.isdigit() for char in ticker)
                            break
                if not ticker: continue
                
                try:
                    upd = {}
                    if is_kr:
                        d = get_kr_stock_data(ticker)
                        if is_valid(d['price']): upd["현재가"] = {"number": d['price']}
                        if is_valid(d['high']): upd["52주 최고가"] = {"number": d['high']}
                        if is_valid(d['low']): upd["52주 최저가"] = {"number": d['low']}
                        if is_valid(d['target_price']): upd["목표주가"] = {"number": d['target_price']}
                        if d['opinion']: upd["목표가 범위"] = {"rich_text": [{"text": {"content": d['opinion']}}]}
                    else:
                        stock = yf.Ticker(ticker)
                        info = stock.info
                        last_price = info.get('currentPrice') or info.get('regularMarketPrice')
                        if is_valid(last_price): upd["현재가"] = {"number": last_price}
                        if is_valid(info.get('fiftyTwoWeekHigh')): upd["52주 최고가"] = {"number": info.get('fiftyTwoWeekHigh')}
                        if is_valid(info.get('fiftyTwoWeekLow')): upd["52주 최저가"] = {"number": info.get('fiftyTwoWeekLow')}
                        
                        # 미국 주식 포맷팅 반영 (소수점 2자리 및 $)
                        target_mean = info.get('targetMeanPrice')
                        if is_valid(target_mean): upd["목표주가"] = {"number": round(target_mean, 2)}
                        
                        low, high = info.get('targetLowPrice'), info.get('targetHighPrice')
                        if is_valid(low) and is_valid(high):
                            range_str = f"${low:.2f} ~ ${high:.2f}"
                            upd["목표가 범위"] = {"rich_text": [{"text": {"content": range_str}}]}

                    upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                    notion.pages.update(page_id=page["id"], properties=upd)
                    processed_count += 1
                    print(f"   ✅ [{ticker}] 완료")
                except Exception as e:
                    print(f"   ❌ [{ticker}] 실패: {e}")
                time.sleep(0.5) 

            if not res.get("has_more"): break
            next_cursor = res.get("next_cursor")
        except Exception as e:
            print(f"❌ 시스템 에러: {e}"); break
    print(f"🏁 작업 완료: 총 {processed_count}건 업데이트됨")

if __name__ == "__main__":
    main()
