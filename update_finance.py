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
    [데이터 정제]
    텍스트("1,234", "N/A", "12.50")를 순수 숫자(1234.0, None, 12.5)로 1차 변환
    """
    if not val_str: return None
    try:
        clean_str = str(val_str).replace(",", "").replace("원", "").replace("%", "").strip()
        if clean_str.upper() == "N/A" or clean_str == "":
            return None
        return float(clean_str)
    except:
        return None

def format_value(key, val, is_kr):
    """
    [디자인 적용]
    숫자를 노션에 보여줄 '예쁜 텍스트'로 최종 변환
    """
    if not is_valid(val):
        return None

    # 1. 금액/가치 관련 (EPS, 추정EPS, BPS) -> 통화 기호 + 콤마
    if key in ["EPS", "추정EPS", "BPS"]:
        if is_kr:
            # 한국: 소수점 없이 콤마 (예: ₩1,234)
            return f"₩{int(val):,}"
        else:
            # 미국: 소수점 2자리 + 콤마 (예: $12.50)
            return f"${val:,.2f}"

    # 2. 배당수익률 -> 퍼센트 붙이기
    elif key == "배당수익률":
        return f"{val:.2f}%"

    # 3. 비율 지표 (PER, PBR 등) -> 깔끔한 숫자 문자열
    else:
        return f"{val:.2f}"

# ---------------------------------------------------------------------------
# 3. 데이터 수집 함수
# ---------------------------------------------------------------------------
def get_kr_fin(ticker):
    """
    [한국 주식] 네이버 금융 크롤링
    """
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/'
    }

    # 수집할 항목 정의
    data_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률"]
    final_data = {k: None for k in data_keys}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'
        soup = BeautifulSoup(response.text, 'html.parser')

        # [ID 기반 기본 지표 추출]
        selectors = {
            "PER": "#_per",
            "EPS": "#_eps",
            "추정PER": "#_cns_per",
            "추정EPS": "#_cns_eps",
            "PBR": "#_pbr",
            "배당수익률": "#_dvr"
        }
        
        raw_data = {}
        for key, sel in selectors.items():
            el = soup.select_one(sel)
            # 여기가 수정된 부분입니다 (대괄호 닫기 확인)
            raw_data[key] = el.get_text(strip=True) if el else "N/A"

        # [BPS 추출] (PBR 부모 td -> em 태그 추적)
        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            # ems[1]이 BPS에 해당함
            raw_data["BPS"] = ems[1].get_text(strip=True) if len(ems) > 1 else "N/A"
        else:
            raw_data["BPS"] = "N/A"

        # [데이터 변환] 텍스트 -> 숫자(float)로 변환하여 저장
        for key in data_keys:
            final_data[key] = to_numeric(raw_data.get(key))

        return final_data

    except Exception as e:
        print(f"   [KR Error] {ticker} 파싱 실패: {e}")
        return final_data

def get_us_fin(ticker):
    """
    [미국 주식] Yahoo Finance API 사용
    """
    data_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률"]
    final_data = {k: None for k in data_keys}

    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # 야후 데이터 매핑
        final_data["PER"] = info.get("trailingPE")
        final_data["추정PER"] = info.get("forwardPE")
        final_data["EPS"] = info.get("trailingEps")
        final_data["추정EPS"] = info.get("forwardEps")
        final_data["PBR"] = info.get("priceToBook")
        final_data["BPS"] = info.get("bookValue")
        
        # 배당수익률 (0.05 -> 5.0 변환)
        div_yield = info.get("dividendYield")
        if div_yield is not None:
            final_data["배당수익률"] = div_yield * 100
            
        return final_data

    except Exception as e:
        print(f"   [US Error] {ticker} 가져오기 실패: {e}")
        return final_data

# ---------------------------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"📊 [재무 업데이트: 텍스트/통화 포맷 적용] 시작 - {datetime.now(kst)}")
    
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
                        # 한국 주식 판별: 6자리 숫자 & 숫자로 시작
                        is_kr = len(ticker) == 6 and ticker[0].isdigit()
                        break
            
            if not ticker: continue

            # 1. 데이터 수집 (숫자 형태)
            if is_kr:
                fin_data = get_kr_fin(ticker)
            else:
                fin_data = get_us_fin(ticker)

            # 2. 노션 전송용 포맷팅 (텍스트 형태)
            upd = {}
            for key, val in fin_data.items():
                # 여기서 원화(₩), 달러($), 콤마(,) 처리가 수행됨
                formatted_text = format_value(key, val, is_kr)
                
                if formatted_text:
                    # 노션 '텍스트' 속성 업데이트 페이로드
                    upd[key] = {
                        "rich_text": [
                            {"text": {"content": formatted_text}}
                        ]
                    }
            
            # 마지막 업데이트 시간
            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
            
            # 3. 노션 API 전송
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    
                    # 로그 메시지 생성
                    log_items = []
                    for k, v in fin_data.items():
                        fmt = format_value(k, v, is_kr)
                        if fmt: log_items.append(f"{k}:{fmt}")
                        
                    print(f"   => [{ticker}] 완료 ({', '.join(log_items)})")
                    success_cnt += 1
                else:
                    print(f"   => [{ticker}] 업데이트 할 유효 데이터 없음")
                    
            except Exception as e:
                print(f"   => [{ticker}] 전송 실패: {e}")
                print("      (Tip: 노션 속성 타입이 '텍스트'인지 꼭 확인하세요!)")
            
            time.sleep(0.5)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"✨ 업데이트 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
