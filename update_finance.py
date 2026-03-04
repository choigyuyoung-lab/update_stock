import os, time, math, requests
import yfinance as yf
import pandas as pd  # [추가됨]
from io import StringIO # [추가됨]
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
# ---------------------------------------------------------
def to_numeric(val_str):
    if not val_str: return None
    try:
        clean_str = str(val_str).replace(",", "").replace("원", "").replace("%", "").strip()
        if clean_str.upper() in ["N/A", "-", "", "IFRS", "GAAP"]: return None
        return float(clean_str)
    except:
        return None

def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

# ---------------------------------------------------------
# [이전된 함수] 성공한 코드: 동일업종 PER 추출 (Pandas)
# ---------------------------------------------------------
def get_sector_per_pandas(item_code: str):
    url = f"https://finance.naver.com/item/main.naver?code={item_code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://finance.naver.com/'
    }
    data = {"동일업종PER": None}
    try:
        res = requests.get(url, headers=headers)
        dfs = pd.read_html(StringIO(res.text), encoding='euc-kr')
        for df in dfs:
            if "동일업종 PER" in df.to_string():
                for idx, row in df.iterrows():
                    row_str = str(row.values)
                    if "동일업종 PER" in row_str:
                        raw_val = str(row.iloc[-1])
                        try:
                            data["동일업종PER"] = float(raw_val.replace('배', '').replace(',', '').strip())
                        except: pass
                        break
                break
    except Exception as e:
        print(f"   ⚠️ [Pandas Error] {item_code}: {e}")
    return data

# ---------------------------------------------------------------------------
# 3. 데이터 수집 함수 (기존 재무 + 목표주가/의견/52주 통합)
# ---------------------------------------------------------------------------
def get_kr_fin(ticker):
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 수집할 데이터 키 통합
    final_data = {
        "PER": None, "추정PER": None, "EPS": None, "추정EPS": None, 
        "PBR": None, "BPS": None, "배당수익률": None,
        "52주 최고가": None, "52주 최저가": None, "목표주가": None, "의견": None
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        # [기존] 재무 데이터 추출
        selectors = {
            "PER": "#_per", "EPS": "#_eps", "추정PER": "#_cns_per", 
            "추정EPS": "#_cns_eps", "PBR": "#_pbr", "배당수익률": "#_dvr"
        }
        for key, sel in selectors.items():
            el = soup.select_one(sel)
            final_data[key] = to_numeric(el.get_text(strip=True) if el else None)

        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            final_data["BPS"] = to_numeric(ems[1].get_text(strip=True) if len(ems) > 1 else None)

        # [통합] 52주 최고/최저가
        th_tags = soup.find_all('th')
        for th in th_tags:
            if "52주최고" in th.text:
                td = th.find_next_sibling('td')
                if td:
                    ems = td.select('em')
                    if len(ems) >= 2:
                        final_data['52주 최고가'] = to_numeric(ems[0].text)
                        final_data['52주 최저가'] = to_numeric(ems[1].text)
                break 

        # [통합] 목표주가 및 투자의견
        target_table = soup.find('table', summary="투자의견 정보")
        if target_table:
            td = target_table.find('td')
            if td:
                ems = td.find_all('em')
                if ems: 
                    final_data['목표주가'] = to_numeric(ems[-1].get_text(strip=True))

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
                    final_data['의견'] = clean_opinion

        return final_data
    except Exception as e:
        print(f"   ❌ [KR Error] {ticker}: {e}")
        return final_data

def get_us_fin(ticker):
    final_data = {
        "PER": None, "추정PER": None, "EPS": None, "추정EPS": None, 
        "PBR": None, "BPS": None, "배당수익률": None,
        "52주 최고가": None, "52주 최저가": None, "목표주가": None, "의견": None
    }

    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        if not info or len(info) < 5: return final_data

        # [기존] 재무
        final_data["PER"] = info.get("trailingPE")
        final_data["추정PER"] = info.get("forwardPE")
        final_data["EPS"] = info.get("trailingEps")
        final_data["추정EPS"] = info.get("forwardEps")
        final_data["PBR"] = info.get("priceToBook")
        final_data["BPS"] = info.get("bookValue")
        if info.get("dividendYield") is not None:
            final_data["배당수익률"] = info.get("dividendYield") * 100
            
        # [통합] 52주 & 목표주가 & 의견
        final_data["52주 최고가"] = info.get('fiftyTwoWeekHigh')
        final_data["52주 최저가"] = info.get('fiftyTwoWeekLow')
        if info.get('targetMeanPrice') is not None:
            final_data["목표주가"] = round(info.get('targetMeanPrice'), 2)
            
        rec_key = info.get('recommendationKey', '').lower()
        opinion_map = {"strong_buy": "적극매수", "buy": "매수", "hold": "중립", "underperform": "매도", "sell": "적극매도"}
        translated_opinion = opinion_map.get(rec_key, rec_key.upper())
        if translated_opinion and translated_opinion != "NONE":
            final_data['의견'] = translated_opinion

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
    print(f"📊 [재무/분석 종합 업데이트 모드] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    success_cnt = 0

    # 노션 속성 매핑을 위한 키 분류 (Number vs Select)
    number_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률", "52주 최고가", "52주 최저가", "목표주가"]

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
            
            for name in ["티커", "Ticker"]:
                if name in props:
                    content = props.get(name, {}).get("title") or props.get(name, {}).get("rich_text")
                    if content:
                        ticker = content[0].get("plain_text", "").strip().upper()
                        is_kr = ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())
                        break
            
            if not ticker: continue

            # 1. 데이터 수집
            if is_kr:
                fin_data = get_kr_fin(ticker)
                # 동일업종 PER 병합
                per_data = get_sector_per_pandas(ticker)
                fin_data["동일업종 PER"] = per_data.get("동일업종PER")
            else:
                fin_data = get_us_fin(ticker)
                fin_data["동일업종 PER"] = None # 미국 주식은 제외

            # 2. 노션 업데이트 준비
            upd = {}
            valid_cnt = 0

            # 숫자형 필드 처리
            for key in number_keys + ["동일업종 PER"]:
                val = fin_data.get(key)
                if is_valid(val):
                    valid_cnt += 1
                    upd[key] = {"number": val}
                else:
                    upd[key] = {"number": None}

            # 선택형(Select) 필드 처리 (투자의견)
            opinion_val = fin_data.get("의견")
            if opinion_val:
                upd["목표가 범위"] = {"select": {"name": opinion_val}}

            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": now_iso}}
            
            # 3. 전송
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    if valid_cnt > 0:
                         print(f"   ✅ [{ticker}] 완료 (의견: {opinion_val})")
                    else:
                         print(f"   🧹 [{ticker}] 데이터 없음 (빈값 처리)")
                    success_cnt += 1
            except Exception as e:
                print(f"   ❌ [{ticker}] 전송 실패: {e}")
            
            time.sleep(0.5)

        if not res.get("has_more"): break
        next_cursor = res.get("next_cursor")

    print(f"\n✨ 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
