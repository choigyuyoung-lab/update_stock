import os
import time
import math
import requests
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client
from bs4 import BeautifulSoup

# [추가됨] 사용자님이 주신 성공 코드의 필수 라이브러리
import pandas as pd
from io import StringIO

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")

if not NOTION_TOKEN or not DATABASE_ID:
    print("❌ 오류: NOTION_TOKEN 또는 DATABASE_ID 환경 변수가 설정되지 않았습니다.")
    exit()

notion = Client(auth=NOTION_TOKEN)

# ---------------------------------------------------------
# 2. 유틸리티 함수
# ---------------------------------------------------------
def is_valid(val):
    """노션 API 전송 전 숫자 유효성 검사 (NaN, Inf 제외)"""
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

# ---------------------------------------------------------
# [기존 함수] BeautifulSoup 사용 (가격, 등락폭, 의견 등)
# ---------------------------------------------------------
def get_kr_stock_data(ticker):
    """한국 주식 데이터 추출 (네이버 금융) - BS4"""
    data = {'price': None, 'high': None, 'low': None, 'target_price': None, 'opinion': None}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = res.apparent_encoding 
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. 현재가
        today_area = soup.select_one('div.today p.no_today em .blind')
        if today_area: 
            data['price'] = float(today_area.text.replace(',', '').strip())

        # 2. 52주 최고/최저
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

        # 3. 목표주가 및 투자의견
        target_table = soup.find('table', summary="투자의견 정보")
        if target_table:
            td = target_table.find('td')
            if td:
                ems = td.find_all('em')
                if ems: 
                    try:
                        data['target_price'] = float(ems[-1].get_text(strip=True).replace(',', ''))
                    except: pass 

                opinion_span = td.find('span', class_='f_up')
                if opinion_span:
                    raw_text = opinion_span.get_text(strip=True)
                    try:
                        score_str = "".join([c for c in raw_text if c.isdigit() or c == '.'])
                        score = float(score_str)
                        if score >= 4.5: clean_opinion = "적극매수"
                        elif score >= 3.5: clean_opinion = "매수"
                        elif score >= 3.0: clean_opinion = "중립"
                        elif score >= 2.0: clean_opinion = "매도"
                        else: clean_opinion = "적극매도"
                    except:
                        clean_opinion = "".join([c for c in raw_text if not c.isdigit() and c != '.']).strip()
                    data['opinion'] = clean_opinion

    except Exception as e:
        print(f"   ⚠️ [Naver Error] {ticker}: {e}")
    return data

# ---------------------------------------------------------
# [추가된 함수] 사용자님이 주신 성공한 Pandas 코드 (그대로 복사)
# ---------------------------------------------------------
def get_sector_per_pandas(item_code: str):
    url = f"https://finance.naver.com/item/main.naver?code={item_code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/'
    }

    data = {"동일업종PER": "N/A"}

    try:
        res = requests.get(url, headers=headers)
        
        # 사용자님이 성공하신 방식 그대로 (StringIO + res.text)
        dfs = pd.read_html(StringIO(res.text), encoding='euc-kr')

        for df in dfs:
            if "동일업종 PER" in df.to_string():
                for idx, row in df.iterrows():
                    row_str = str(row.values)
                    if "동일업종 PER" in row_str:
                        raw_val = str(row.iloc[-1])
                        data["동일업종PER"] = raw_val.replace('배', '').strip()
                        break
                break
    except Exception as e:
        print(f"Pandas 추출 중 에러: {e}")

    return data

# ---------------------------------------------------------
# 3. 메인 실행 함수
# ---------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"💰 [주가 업데이트] 실행 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        except Exception as e:
            print(f"❌ 노션 연결 실패: {e}")
            break

        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = ""
            is_kr = False
            
            # 티커 추출
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        is_kr = ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())
                        break
            
            if not ticker: continue
            
            try:
                upd = {}
                opinion_val = None 

                # --- [1] 한국 주식 (KR) ---
                if is_kr:
                    # 1-1. 기존 함수 실행 (가격 등)
                    d = get_kr_stock_data(ticker)
                    
                    if is_valid(d['price']): upd["현재가"] = {"number": d['price']}
                    if is_valid(d['high']): upd["52주 최고가"] = {"number": d['high']}
                    if is_valid(d['low']): upd["52주 최저가"] = {"number": d['low']}
                    if is_valid(d['target_price']): upd["목표주가"] = {"number": d['target_price']}
                    if d['opinion']: opinion_val = d['opinion']

                    # 1-2. [추가] Pandas 함수 실행 (동일업종 PER) - 섞지 않고 따로 호출
                    per_data = get_sector_per_pandas(ticker)
                    per_val = per_data.get("동일업종PER")
                    
                    # 값이 유효하면 노션 업데이트 목록에 추가
                    if per_val and per_val != "N/A":
                        try:
                            # 콤마 제거 후 실수형 변환
                            upd["동일업종 PER"] = {"number": float(per_val.replace(',', ''))}
                        except:
                            pass

                # --- [2] 미국 주식 (US) ---
                else:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    
                    last_price = info.get('currentPrice') or info.get('regularMarketPrice')
                    if is_valid(last_price): upd["현재가"] = {"number": last_price}
                    if is_valid(info.get('fiftyTwoWeekHigh')): upd["52주 최고가"] = {"number": info.get('fiftyTwoWeekHigh')}
                    if is_valid(info.get('fiftyTwoWeekLow')): upd["52주 최저가"] = {"number": info.get('fiftyTwoWeekLow')}
                    
                    target_mean = info.get('targetMeanPrice')
                    if is_valid(target_mean): upd["목표주가"] = {"number": round(target_mean, 2)}
                    
                    rec_key = info.get('recommendationKey', '').lower()
                    opinion_map = {
                        "strong_buy": "적극매수", "buy": "매수", "hold": "중립",
                        "underperform": "매도", "sell": "적극매도"
                    }
                    translated_opinion = opinion_map.get(rec_key, rec_key.upper())
                    if translated_opinion and translated_opinion != "NONE":
                        opinion_val = translated_opinion

                # --- [3] 공통 업데이트 ---
                if opinion_val:
                    upd["목표가 범위"] = {"select": {"name": opinion_val}}

                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                processed_count += 1
                
                # 로그 확인용 (업종PER이 잘 들어갔는지 화면에 표시)
                per_check = upd.get("동일업종 PER", {}).get("number", "없음")
                print(f"   ✅ [{ticker}] 완료 ({'KR' if is_kr else 'US'}) - 의견: {opinion_val}, PER: {per_check}")

            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.5)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"\n✨ 종료. 총 {processed_count}건 업데이트 완료.")

if __name__ == "__main__":
    main()
