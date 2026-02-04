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
    [디자인 적용] 12자리 고정 폭 우측 정렬 + 음수 처리
    값이 없으면 None 반환
    """
    if not is_valid(val):
        return None

    # 음수 처리
    sign = ""
    if val < 0:
        sign = "-"
        val = abs(val)

    final_str = ""

    # 1. 금액 (EPS, BPS)
    if key in ["EPS", "추정EPS", "BPS"]:
        if is_kr:
            final_str = f"{sign}₩{int(val):,}"
        else:
            final_str = f"{sign}${val:,.2f}"

    # 2. 퍼센트 (배당수익률)
    elif key == "배당수익률":
        final_str = f"{sign}{val:.2f}%"

    # 3. 일반 비율 (PER, PBR)
    else:
        final_str = f"{sign}{val:.2f}"
    
    # 12자리 확보 후 우측 정렬
    return final_str.rjust(12)

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

        # 1. 일반 주식 Selector
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

        # BPS 별도 처리
        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            raw_data["BPS"] = ems[1].get_text(strip=True) if len(ems) > 1 else "N/A"
        else:
            raw_data["BPS"] = "N/A"

        if not found_elements:
            print(f"   ⚠️ [{ticker}] 데이터 태그 없음 (ETF/관리종목 가능성)")

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
    print(f"📊 [재무 업데이트: 누락 데이터 공백 처리] 시작 - {datetime.now(kst)}")
    
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

            # 2. 노션 업데이트 준비 (공백 처리 로직 포함)
            upd = {}
            log_details = []

            for key, val in fin_data.items():
                formatted_text = format_value(key, val, is_kr)
                
                if formatted_text:
                    # [값 있음] 정상 업데이트 (빨간색/기본색 적용)
                    text_color = "default"
                    if is_valid(val) and val < 0:
                        text_color = "red"

                    upd[key] = {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": formatted_text},
                                "annotations": {"code": True, "color": text_color}
                            }
                        ]
                    }
                    log_details.append(f"{key}:O") # 로그에 O 표시
                else:
                    # [값 없음] ⚠️ 빈 리스트([])를 보내서 노션 값을 강제로 지움
                    upd[key] = {"rich_text": []}
                    # 로그에는 X 표시 (너무 길어지면 생략 가능)
                    # log_details.append(f"{key}:X") 
            
            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
            
            # 3. 전송
            try:
                # 데이터가 하나라도 있거나, 공백 처리라도 해야 하면 업데이트 수행
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    
                    # 성공 로그 출력
                    # (값이 있는 항목 개수와 없는 항목 개수를 파악)
                    valid_count = len([v for v in fin_data.values() if is_valid(v)])
                    if valid_count > 0:
                         print(f"   ✅ [{ticker}] 업데이트 완료 ({valid_count}개 항목 성공)")
                    else:
                         print(f"   🧹 [{ticker}] 데이터 없음 -> 전체 공백(초기화) 처리 완료")
                    
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
