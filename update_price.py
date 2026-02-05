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

# 토큰이 없을 경우 안내
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
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def get_kr_stock_data(ticker):
    """한국 주식 데이터 추출 (네이버 금융) - 5단계 의견 통일"""
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

        # 3. 목표주가 및 투자의견 (summary="투자의견 정보" 테이블)
        target_table = soup.find('table', summary="투자의견 정보")
        if target_table:
            td = target_table.find('td')
            if td:
                # 목표주가 추출
                ems = td.find_all('em')
                if ems: 
                    try:
                        data['target_price'] = float(ems[-1].get_text(strip=True).replace(',', ''))
                    except:
                        pass # 목표가 없음

                # --- [핵심] 투자의견 5단계 변환 로직 ---
                opinion_span = td.find('span', class_='f_up')
                if opinion_span:
                    raw_text = opinion_span.get_text(strip=True)
                    try:
                        # '4.00매수' -> 4.00 추출
                        score_str = "".join([c for c in raw_text if c.isdigit() or c == '.'])
                        score = float(score_str)
                        
                        # 점수 기준 매핑 (사용자 요청 반영)
                        if score >= 4.5:
                            clean_opinion = "적극매수"
                        elif score >= 3.5:
                            clean_opinion = "매수"
                        elif score >= 3.0:  # 3.0 이상 3.5 미만은 중립
                            clean_opinion = "중립"
                        elif score >= 2.0:
                            clean_opinion = "매도"
                        else:
                            clean_opinion = "적극매도"
                    except:
                        # 점수 파싱 실패 시 텍스트만 추출 (예외처리)
                        clean_opinion = "".join([c for c in raw_text if not c.isdigit() and c != '.']).strip()
                    
                    data['opinion'] = clean_opinion
                # -------------------------------------

    except Exception as e:
        print(f"   ⚠️ [Naver Error] {ticker}: {e}")
    return data

# ---------------------------------------------------------
# 3. 메인 실행 함수
# ---------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"💰 [주가 업데이트] 최종 통합 버전 시작 - {datetime.now(kst)}")
    
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
            
            # 티커 추출 로직
            for name in ["티커", "Ticker"]:
                target = props.get(name)
                if target:
                    content = target.get("title") or target.get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        # 한국 주식 판별 (숫자로 시작하거나 .KS/.KQ로 끝남)
                        is_kr = ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())
                        break
            
            if not ticker: continue
            
            try:
                upd = {}
                opinion_val = None # 투자의견 임시 저장

                # --- 1. 한국 주식 처리 ---
                if is_kr:
                    d = get_kr_stock_data(ticker)
                    if is_valid(d['price']): upd["현재가"] = {"number": d['price']}
                    if is_valid(d['high']): upd["52주 최고가"] = {"number": d['high']}
                    if is_valid(d['low']): upd["52주 최저가"] = {"number": d['low']}
                    if is_valid(d['target_price']): upd["목표주가"] = {"number": d['target_price']}
                    
                    if d['opinion']: 
                        opinion_val = d['opinion']

                # --- 2. 미국 주식 처리 ---
                else:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    
                    # 가격 정보
                    last_price = info.get('currentPrice') or info.get('regularMarketPrice')
                    if is_valid(last_price): upd["현재가"] = {"number": last_price}
                    if is_valid(info.get('fiftyTwoWeekHigh')): upd["52주 최고가"] = {"number": info.get('fiftyTwoWeekHigh')}
                    if is_valid(info.get('fiftyTwoWeekLow')): upd["52주 최저가"] = {"number": info.get('fiftyTwoWeekLow')}
                    
                    # 목표주가 (평균)
                    target_mean = info.get('targetMeanPrice')
                    if is_valid(target_mean): upd["목표주가"] = {"number": round(target_mean, 2)}
                    
                    # 투자의견 매핑 (영어 -> 한글 5단계)
                    rec_key = info.get('recommendationKey', '').lower()
                    opinion_map = {
                        "strong_buy": "적극매수",
                        "buy": "매수",
                        "hold": "중립",
                        "underperform": "매도",
                        "sell": "적극매도"
                    }
                    translated_opinion = opinion_map.get(rec_key, rec_key.upper()) # 매핑 없으면 원문
                    
                    # 값이 유효한 경우만 저장
                    if translated_opinion and translated_opinion != "NONE":
                        opinion_val = translated_opinion

                # --- 3. 공통: 투자의견 노션 전송 (Select 속성 사용) ---
                if opinion_val:
                    # 주의: 노션의 '목표가 범위' 컬럼이 '선택(Select)' 유형이어야 함
                    upd["목표가 범위"] = {"select": {"name": opinion_val}}

                # --- 4. 업데이트 실행 ---
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                
                notion.pages.update(page_id=page["id"], properties=upd)
                processed_count += 1
                print(f"   ✅ [{ticker}] 완료 ({'KR' if is_kr else 'US'}) - 의견: {opinion_val}")

            except Exception as e:
                print(f"   ❌ [{ticker}] 실패: {e}")
            
            time.sleep(0.5) # API 부하 방지

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"\n✨ 종료. 총 {processed_count}건 업데이트 완료.")

if __name__ == "__main__":
    main()
