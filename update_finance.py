import os, time, math, requests
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# 1. 환경 설정
# ---------------------------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

# ---------------------------------------------------------------------------
# 2. 유틸리티 함수 (숫자 변환기)
# ---------------------------------------------------------------------------
def is_valid(val):
    """유효한 숫자인지 체크 (NaN, Inf, None 방지)"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def to_numeric(val_str):
    """
    [핵심] 텍스트("1,234", "N/A", "12.50")를 숫자(1234.0, None, 12.5)로 변환
    """
    if not val_str: return None
    try:
        # 쉼표, 원, %, 공백 제거
        clean_str = str(val_str).replace(",", "").replace("원", "").replace("%", "").strip()
        # N/A 이거나 빈 문자열이면 None 반환
        if clean_str.upper() == "N/A" or clean_str == "":
            return None
        return float(clean_str)
    except:
        return None

# ---------------------------------------------------------------------------
# 3. 데이터 수집 함수 (신규 코드 로직 적용 + 숫자 변환)
# ---------------------------------------------------------------------------
def get_kr_fin(ticker):
    """
    [신규 코드 로직 적용]
    get_stock_data_master 함수의 CSS Selector 로직을 변형 없이 사용하되,
    마지막에 to_numeric 함수로 숫자 변환만 수행.
    """
    # 1. 페이지 요청
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- [신규 코드의 추출 로직: 텍스트 수집] ---
        raw_data = {}
        
        # ID 기반 추출
        selectors = {
            "현재PER": "#_per", "현재EPS": "#_eps",
            "추정PER": "#_cns_per", "추정EPS": "#_cns_eps",
            "현재PBR": "#_pbr", "배당수익률": "#_dvr"
        }
        for key, sel in selectors.items():
            el = soup.select_one(sel)
            raw_data[key] = el.get_text(strip=True) if el else "N/A"

        # BPS 추출 (ID가 없으므로 PBR 부모 td 추적)
        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            raw_data["현재BPS"] = ems[1].get_text(strip=True) if len(ems) > 1 else "N/A"
        else:
            raw_data["현재BPS"] = "N/A"
        
        # --- [데이터 변환: 텍스트 -> 숫자] ---
        # 노션 필드명("EPS")과 수집된 키("현재EPS")를 매핑하며 변환
        final_data = {
            "EPS": to_numeric(raw_data.get("현재EPS")),           # TTM 기준
            "추정EPS": to_numeric(raw_data.get("추정EPS")),       # 증권사 컨센서스
            "BPS": to_numeric(raw_data.get("현재BPS")),           # 최근 분기 자산 기준
            "배당수익률": to_numeric(raw_data.get("배당수익률"))   # 현 주가 기준 배당률
        }
        
        return final_data

    except Exception as e:
        print(f"   [Error] {ticker} 파싱 중 오류: {e}")
        return {"EPS": None, "추정EPS": None, "BPS": None, "배당수익률": None}

def get_us_fin(ticker):
    """미국 주식 (Yahoo Finance) - 기존 유지"""
    data = {"EPS": None, "추정EPS": None, "BPS": None, "배당수익률": None}
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        data["EPS"] = info.get("trailingEps")
        data["추정EPS"] = info.get("forwardEps")
        data["BPS"] = info.get("bookValue")
        
        div = info.get("dividendYield")
        if div:
             data["배당수익률"] = div * 100
    except:
        pass
    return data

# ---------------------------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"📊 [재무 업데이트: 신규 로직(TTM) 적용] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    success_cnt = 0

    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
        except Exception as e:
            print(f"❌ 노션 연결 실패: {e}")
            break

        pages = res.get("results", [])
        
        for page in pages:
            props = page["properties"]
            ticker = ""; is_kr = False
            
            # 티커 확인
            for name in ["티커", "Ticker"]:
                if name in props:
                    content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        is_kr = len(ticker) == 6 and ticker[0].isdigit()
                        break
            
            if not ticker: continue

            # 데이터 수집
            if is_kr:
                fin_data = get_kr_fin(ticker)
            else:
                fin_data = get_us_fin(ticker)

            # 노션 업데이트
            upd = {}
            for key, val in fin_data.items():
                if is_valid(val):
                    upd[key] = {"number": val}
            
            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
            
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    
                    # 로그 출력 (EPS, 추정EPS, BPS, 배당 확인)
                    log_items = [f"{k}:{v}" for k, v in fin_data.items() if is_valid(v)]
                    print(f"   => [{ticker}] 업데이트 완료 ({', '.join(log_items)})")
                    success_cnt += 1
                else:
                    print(f"   => [{ticker}] 업데이트 할 유효 데이터 없음")
                    
            except Exception as e:
                print(f"   => [{ticker}] 노션 전송 실패: {e}")
            
            time.sleep(0.5)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"✨ 업데이트 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
