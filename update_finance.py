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
# 2. 유틸리티 함수
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
    문자열(예: '1,234', 'N/A')을 숫자(float)로 변환.
    변환 실패 시 None 반환.
    """
    if not val_str:
        return None
    try:
        # 쉼표, 원, % 등 제거
        clean_str = str(val_str).replace(",", "").replace("원", "").replace("%", "").strip()
        # 숫자로 변환
        return float(clean_str)
    except:
        return None

# ---------------------------------------------------------------------------
# 3. 데이터 수집 함수 (기초 데이터 위주)
# ---------------------------------------------------------------------------
def get_kr_fin(ticker):
    """
    한국 주식: EPS, 추정EPS, BPS, 배당수익률 추출
    (PER, PBR은 노션 수식 계산을 위해 수집 제외)
    """
    data = {
        "EPS": None, 
        "추정EPS": None, 
        "BPS": None, 
        "배당수익률": None
    }
    
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/'
    }

    try:
        # 1. 페이지 요청
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr' 
        soup = BeautifulSoup(response.text, 'html.parser')

        # 2. CSS Selector 매핑 (PER, PBR 관련 선택자 제거)
        selectors = {
            "EPS": "#_eps",          # 현재 EPS
            "추정EPS": "#_cns_eps",   # 추정 EPS (컨센서스)
            "배당수익률": "#_dvr"     # 배당수익률
        }

        for key, sel in selectors.items():
            el = soup.select_one(sel)
            if el:
                data[key] = to_numeric(el.get_text(strip=True))

        # 3. BPS 추출
        # (주의: BPS 텍스트를 찾기 위해 #_pbr 태그를 '위치 찾기용'으로만 사용하고, PBR 값은 저장하지 않음)
        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            # 보통 두 번째 em 태그가 BPS
            bps_text_val = ems[1].get_text(strip=True) if len(ems) > 1 else "N/A"
            data["BPS"] = to_numeric(bps_text_val)

    except Exception as e:
        print(f"   [Error] {ticker} 파싱 중 오류: {e}")
    
    return data

def get_us_fin(ticker):
    """
    미국 주식: EPS, 추정EPS, BPS, 배당수익률 추출 (Yahoo Finance)
    """
    data = {
        "EPS": None, 
        "추정EPS": None, 
        "BPS": None, 
        "배당수익률": None
    }
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Yahoo Finance 데이터 매핑
        data["EPS"] = info.get("trailingEps")      # EPS
        data["추정EPS"] = info.get("forwardEps")   # 추정 EPS
        data["BPS"] = info.get("bookValue")        # BPS
        
        # 배당수익률 (% 단위로 변환)
        div_yield = info.get("dividendYield")
        if div_yield:
             data["배당수익률"] = div_yield * 100

    except:
        pass
        
    return data

# ---------------------------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"📊 [재무 업데이트: 기초 데이터 모드] 시작 - {datetime.now(kst)}")
    
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
                if name not in props: continue
                content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                if content:
                    ticker = content[0].get("plain_text", "").strip().upper()
                    is_kr = len(ticker) == 6 and ticker[0].isdigit()
                    break
            
            if not ticker:
                continue

            # 데이터 수집 (EPS, 추정EPS, BPS, 배당수익률)
            if is_kr:
                fin_data = get_kr_fin(ticker)
            else:
                fin_data = get_us_fin(ticker)

            # 노션 업데이트 페이로드 생성
            upd = {}
            
            for key, val in fin_data.items():
                if is_valid(val):
                    upd[key] = {"number": val}
            
            # 날짜 갱신
            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
            
            # 노션 API 호출
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    
                    # 로그 출력
                    log_items = [f"{k}:{v:.2f}" for k, v in fin_data.items() if is_valid(v)]
                    print(f"   => [{ticker}] 완료 ({', '.join(log_items)})")
                    success_cnt += 1
                else:
                    print(f"   => [{ticker}] 업데이트 할 유효 데이터 없음")
                    
            except Exception as e:
                print(f"   => [{ticker}] 노션 업데이트 실패: {e}")
            
            time.sleep(0.5) 

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"✨ 업데이트 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
