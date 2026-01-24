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
    """유효한 숫자인지 체크 (NaN, Inf, None 방지)"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def get_kr_fin(ticker):
    """
    [한국 주식] 네이버 금융 PC 페이지 크롤링 (BeautifulSoup)
    - 종목분석 > 주요재무정보 테이블 파싱
    - 최근 연간 실적 또는 추정치(EPS, BPS) 추출
    """
    eps, bps = None, None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = res.apparent_encoding
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # '주요재무정보' 테이블 섹션 찾기
        analysis_div = soup.select_one('div.section.cop_analysis')
        if not analysis_div: return None, None
        
        # 테이블 내 행(tr) 순회
        rows = analysis_div.select('table tbody tr')
        
        for row in rows:
            header = row.select_one('th')
            if not header: continue
            
            title = header.text.strip()
            
            # 데이터 컬럼(td) 추출
            cols = row.select('td')
            
            # 유효한 숫자 값만 리스트에 담기
            valid_vals = []
            for col in cols:
                txt = col.text.strip().replace(',', '')
                try:
                    val = float(txt)
                    valid_vals.append(val)
                except:
                    continue
            
            if not valid_vals: continue
            
            # 가장 오른쪽(최신/추정치) 값 선택
            latest_val = valid_vals[-1]
            
            if "EPS" in title:
                eps = latest_val
            elif "BPS" in title:
                bps = latest_val
                
    except Exception as e:
        print(f"   ⚠️ [Naver Fin Error] {ticker}: {e}")
        
    return eps, bps

def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"📊 [재무 정보(EPS/BPS) 업데이트] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    success_cnt = 0

    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
            pages = res.get("results", [])
            
            if not pages and success_cnt == 0:
                print("✨ 업데이트할 페이지가 없습니다.")
                break

            for page in pages:
                props = page["properties"]
                
                # 변수 초기화 (문법 오류 방지)
                ticker = ""
                is_kr = False
                
                # 티커 추출
                for name in ["티커", "Ticker"]:
                    target = props.get(name)
                    if target:
                        content = target.get("title") or target.get("rich_text")
                        if content:
                            ticker = content[0].get("plain_text", "").strip().upper()
                            is_kr = len(ticker) == 6 and ticker.isdigit()
                            break
                
                if not ticker: continue

                # 데이터 추출
                eps = None
                bps = None
                
                if is_kr:
                    eps, bps = get_kr_fin(ticker)
                else:
                    try:
                        stock = yf.Ticker(ticker)
                        # 재무 정보는 fast_info가 아니라 일반 info에 있음
                        info = stock.info
                        eps = info.get("trailingEps") or info.get("forwardEps")
                        bps = info.get("bookValue")
                    except: 
                        pass

                # 노션 업데이트
                try:
                    upd = {}
                    if is_valid(eps): upd["EPS"] = {"number": eps}
                    if is_valid(bps): upd["BPS"] = {"number": bps}
                    
                    # 재무 정보 확인 날짜 갱신
                    upd["마지막 업데이트"] = {"date": {"start": now_iso}}
                    
                    # 수치가 하나라도 있거나, 업데이트 날짜라도 갱신할 경우
                    if upd:
                        notion.pages.update(page_id=page["id"], properties=upd)
                        print(f"   ✅ [{ticker}] 재무 완료 (EPS: {eps}, BPS: {bps})")
                        success_cnt += 1
                        
                except Exception as e:
                    print(f"   ❌ [{ticker}] 업데이트 실패: {e}")
                
                time.sleep(0.5) 

            if not res.get("has_more"): break
            next_cursor = res.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 에러: {e}")
            break

    print(f"✨ 재무 업데이트 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
