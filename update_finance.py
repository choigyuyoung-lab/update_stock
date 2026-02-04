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
    """유효한 숫자인지 체크"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def to_numeric(val_str):
    """텍스트를 숫자(float)로 변환"""
    if not val_str: return None
    try:
        clean_str = str(val_str).replace(",", "").replace("원", "").replace("%", "").strip()
        if clean_str.upper() in ["N/A", "-", "", "IFRS", "GAAP"]:
            return None
        return float(clean_str)
    except:
        return None

def format_value_raw(key, val, is_kr):
    """
    [데이터 전처리]
    1. 마이너스 부호를 '숫자 바로 앞'에 붙임 (예: -1,000)
    2. 통화 기호는 맨 왼쪽 고정 (예: ₩)
    3. 사이를 공백으로 채움 -> [₩     -1,000]
    """
    if not is_valid(val):
        return None

    # [설정]
    # 타자기체에서는 일반 하이픈(-)이 가장 깔끔합니다.
    MINUS_CHAR = "-" 
    TOTAL_WIDTH = 13  # 전체 폭 (타자기체는 글자가 커서 13~14 정도가 적당)

    # 1. 숫자 부분 포맷팅 (부호 포함)
    # val 자체가 음수면 f-string 포맷팅에서 자동으로 앞에 -가 붙음
    # 하지만 명시적으로 제어하기 위해 절대값 사용 후 붙임
    is_negative = val < 0
    abs_val = abs(val)
    
    number_part = ""
    
    # (1) 금액 (EPS, BPS)
    if key in ["EPS", "추정EPS", "BPS"]:
        if is_kr:
            number_part = f"{int(abs_val):,}"
        else:
            number_part = f"{abs_val:,.2f}"

    # (2) 퍼센트 (배당수익률)
    elif key == "배당수익률":
        number_part = f"{abs_val:,.1f}%"

    # (3) 일반 비율 (PER, PBR)
    else:
        number_part = f"{abs_val:,.1f}배"
    
    # 2. 마이너스 부호 결합 (숫자 바로 앞)
    if is_negative:
        number_part = f"{MINUS_CHAR}{number_part}"

    # 3. 통화 기호(Symbol) 설정
    symbol = ""
    if key in ["EPS", "추정EPS", "BPS"]:
        symbol = "₩" if is_kr else "$"
    
    # 4. 정렬 로직 (양쪽 채우기)
    # [심볼] + [공백] + [숫자(부호포함)]
    padding_len = TOTAL_WIDTH - len(symbol) - len(number_part)
    
    # 원화(₩) 보정: 타자기체에서도 원화는 약간 넓을 수 있음
    if "₩" in symbol:
        padding_len -= 1

    if padding_len < 0: padding_len = 0
    
    padding_str = " " * padding_len  # 일반 공백 (나중에 LaTeX ~로 변환)
    
    # 최종 문자열 형태 예시: "[₩       -1,000]"
    return f"[{symbol}{padding_str}{number_part}]"

# ---------------------------------------------------------------------------
# 3. 데이터 수집 함수
# ---------------------------------------------------------------------------
def get_kr_fin(ticker):
    """한국 주식 데이터 수집"""
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://finance.naver.com/'
    }
    data_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률"]
    final_data = {k: None for k in data_keys}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        selectors = {
            "PER": "#_per", "EPS": "#_eps",
            "추정PER": "#_cns_per", "추정EPS": "#_cns_eps",
            "PBR": "#_pbr", "배당수익률": "#_dvr"
        }
        
        raw_data = {}
        for key, sel in selectors.items():
            el = soup.select_one(sel)
            raw_data[key] = el.get_text(strip=True) if el else "N/A"

        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            raw_data["BPS"] = ems[1].get_text(strip=True) if len(ems) > 1 else "N/A"
        else:
            raw_data["BPS"] = "N/A"

        for key in data_keys:
            final_data[key] = to_numeric(raw_data.get(key))

        return final_data
    except Exception as e:
        print(f"   ❌ [KR Error] {ticker}: {e}")
        return final_data

def get_us_fin(ticker):
    """미국 주식 데이터 수집"""
    data_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률"]
    final_data = {k: None for k in data_keys}

    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        if not info or len(info) < 5:
             print(f"   ⚠️ [{ticker}] 야후 정보 없음")
             return final_data

        final_data["PER"] = info.get("trailingPE")
        final_data["추정PER"] = info.get("forwardPE")
        final_data["EPS"] = info.get("trailingEps")
        final_data["추정EPS"] = info.get("forwardEps")
        final_data["PBR"] = info.get("priceToBook")
        final_data["BPS"] = info.get("bookValue")
        
        div_yield = info.get("dividendYield")
        if div_yield is not None:
            final_data["배당수익률"] = div_yield * 100
            
        return final_data
    except Exception as e:
        print(f"   ❌ [US Error] {ticker}: {e}")
        return final_data

# ---------------------------------------------------------------------------
# 4. 메인 실행 함수
# ---------------------------------------------------------------------------
def main():
    kst = timezone(timedelta(hours=9))
    now_iso = datetime.now(kst).isoformat()
    print(f"📊 [재무 업데이트: 마이너스 숫자 앞 배치] 시작 - {datetime.now(kst)}")
    
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
            
            for name in ["티커", "Ticker"]:
                if name in props:
                    content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        is_kr = len(ticker) == 6 and ticker[0].isdigit()
                        break
            
            if not ticker: continue

            # 1. 데이터 수집
            if is_kr:
                fin_data = get_kr_fin(ticker)
            else:
                fin_data = get_us_fin(ticker)

            # 2. 노션 업데이트 준비 (Equation 변환)
            upd = {}
            valid_cnt = 0

            for key, val in fin_data.items():
                raw_str = format_value_raw(key, val, is_kr)
                
                if raw_str:
                    valid_cnt += 1
                    
                    # [LaTeX 변환]
                    tex_str = raw_str.replace("$", "\\$").replace("%", "\\%")
                    # 공백을 LaTeX 공백(~)으로 변환 (화면엔 빈칸으로 나옴)
                    tex_str = tex_str.replace(" ", "~") 
                    
                    # ₩ 처리
                    tex_str = tex_str.replace("₩", "\\text{₩}")
                    
                    # 타자기체 적용
                    expression = f"\\texttt{{{tex_str}}}"
                    
                    # 적자(음수)일 경우 빨간색 (전체 적용)
                    if is_valid(val) and val < 0:
                        expression = f"\\color{{red}}{expression}"

                    upd[key] = {
                        "rich_text": [
                            {
                                "type": "equation",
                                "equation": {"expression": expression}
                            }
                        ]
                    }
                else:
                    upd[key] = {"rich_text": []}
            
            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
            
            # 3. 전송
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    
                    if valid_cnt > 0:
                         print(f"   ✅ [{ticker}] 완료")
                    else:
                         print(f"   🧹 [{ticker}] 데이터 없음 -> 초기화")
                    
                    success_cnt += 1
                else:
                    print(f"   ⚠️ [{ticker}] 처리할 내용 없음")
                    
            except Exception as e:
                print(f"   ❌ [{ticker}] 전송 실패: {e}")
            
            time.sleep(0.5)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"\n✨ 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
