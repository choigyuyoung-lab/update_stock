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

def format_value(key, val, is_kr):
    """
    [최종 디자인 적용: 회계 스타일]
    1. 전체 폭 10자리 고정
    2. 마이너스 부호 가시성 개선 ('−')
    3. 배당수익률: 소수점 1자리
    4. PER/PBR: 천 단위 콤마 + 소수점 1자리
    5. 양쪽 정렬: [기호    숫자]
    """
    if not is_valid(val):
        return None

    # [설정]
    MINUS_CHAR = "−"  # 굵은 마이너스
    FILL_CHAR = "\u2007" # 피겨 스페이스 (숫자 너비 공백)
    TOTAL_WIDTH = 10     # 전체 폭

    # 1. 부호 처리
    sign_str = ""
    if val < 0:
        sign_str = MINUS_CHAR
        val = abs(val)

    # 2. 기호(prefix)와 숫자(value_str) 분리
    prefix = ""
    value_str = ""

    # (1) 금액 (EPS, BPS)
    if key in ["EPS", "추정EPS", "BPS"]:
        if is_kr:
            prefix = "₩"
            value_str = f"{sign_str}{int(val):,}"
        else:
            prefix = "$"
            value_str = f"{sign_str}{val:,.2f}"

    # (2) 퍼센트 (배당수익률) -> [수정] 소수점 1자리
    elif key == "배당수익률":
        prefix = ""
        value_str = f"{sign_str}{val:,.1f}%"

    # (3) 일반 비율 (PER, PBR) -> [수정] 천 단위 콤마 + 소수점 1자리
    else:
        prefix = ""
        value_str = f"{sign_str}{val:,.1f}배"
    
    # 3. 정렬 로직 (양쪽 채우기)
    # [prefix + 공백 + value_str]
    padding_len = TOTAL_WIDTH - len(prefix) - len(value_str)
    if padding_len < 0: padding_len = 0
    
    padding_str = FILL_CHAR * padding_len
    
    return f"[{prefix}{padding_str}{value_str}]"

# ---------------------------------------------------------------------------
# 3. 데이터 수집 함수
# ---------------------------------------------------------------------------
def get_kr_fin(ticker):
    """한국 주식 데이터 수집"""
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
        found_elements = False
        for key, sel in selectors.items():
            el = soup.select_one(sel)
            if el:
                raw_data[key] = el.get_text(strip=True)
                found_elements = True
            else:
                raw_data[key] = "N/A"

        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            raw_data["BPS"] = ems[1].get_text(strip=True) if len(ems) > 1 else "N/A"
        else:
            raw_data["BPS"] = "N/A"

        if not found_elements:
            print(f"   ⚠️ [{ticker}] 데이터 태그 없음")

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
    print(f"📊 [재무 업데이트: 최종 완성본] 시작 - {datetime.now(kst)}")
    
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

            # 2. 노션 업데이트 준비
            upd = {}
            valid_cnt = 0

            for key, val in fin_data.items():
                formatted_text = format_value(key, val, is_kr)
                
                if formatted_text:
                    valid_cnt += 1
                    # 음수면 빨간색
                    text_color = "default"
                    if is_valid(val) and val < 0:
                        text_color = "red"

                    upd[key] = {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": formatted_text},
                                "annotations": {"color": text_color}
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
