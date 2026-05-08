import os, time, math, requests, re
from datetime import datetime, timedelta, timezone
from notion_client import Client
from bs4 import BeautifulSoup

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def to_numeric(val_str):
    if not val_str: return None
    try:
        clean_str = str(val_str).replace(",", "").replace("원", "").replace("%", "").replace("배", "").strip()
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

# 🌟 [최적화] Session을 인자로 받아 연결을 재사용하고, 1번의 접속으로 모든 데이터를 뽑아냅니다.
def get_kr_fin_optimized(ticker, session):
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.naver.com/'}
    
    final_data = {
        "PER": None, "추정PER": None, "EPS": None, "추정EPS": None, 
        "PBR": None, "BPS": None, "배당수익률": None,
        "52주 최고가": None, "52주 최저가": None, "목표주가": None, "의견": None,
        "업종PER": None # 업종PER도 여기서 한 번에 처리!
    }
    
    try:
        res = session.get(url, headers=headers, timeout=10)
        # 🌟 html.parser보다 압도적으로 빠른 lxml 파서 사용
        soup = BeautifulSoup(res.text, 'lxml')
        
        # 1. 기본 재무 지표
        selectors = {"PER": "#_per", "EPS": "#_eps", "추정PER": "#_cns_per", "추정EPS": "#_cns_eps", "PBR": "#_pbr", "배당수익률": "#_dvr"}
        for key, sel in selectors.items():
            el = soup.select_one(sel)
            final_data[key] = to_numeric(el.get_text(strip=True) if el else None)

        # 2. BPS 추출
        pbr_el = soup.select_one("#_pbr")
        if pbr_el:
            ems = pbr_el.find_parent("td").find_all("em")
            final_data["BPS"] = to_numeric(ems[1].get_text(strip=True) if len(ems) > 1 else None)

        # 3. 52주 최고/최저가
        for th in soup.find_all('th'):
            if "52주최고" in th.text:
                td = th.find_next_sibling('td')
                if td:
                    ems = td.select('em')
                    if len(ems) >= 2:
                        final_data['52주 최고가'] = to_numeric(ems[0].text)
                        final_data['52주 최저가'] = to_numeric(ems[1].text)
                break 

        # 4. 투자의견 및 목표주가
        target_table = soup.find('table', summary="투자의견 정보")
        if target_table:
            td = target_table.find('td')
            if td:
                ems = td.find_all('em')
                if ems: final_data['목표주가'] = to_numeric(ems[-1].get_text(strip=True))
                opinion_span = td.find('span', class_='f_up')
                if opinion_span:
                    raw_text = opinion_span.get_text(strip=True)
                    try:
                        score = float("".join([c for c in raw_text if c.isdigit() or c == '.']))
                        if score >= 4.5: clean_opinion = "적극매수"
                        elif score >= 3.5: clean_opinion = "매수"
                        elif score >= 3.0: clean_opinion = "중립"
                        elif score >= 2.0: clean_opinion = "매도"
                        else: clean_opinion = "적극매도"
                    except:
                        clean_opinion = "".join([c for c in raw_text if not c.isdigit() and c != '.']).strip()
                    final_data['의견'] = clean_opinion

        # 5. 🌟 [최적화] 무거운 Pandas 대신 BS4로 업종 PER 추출
        for th in soup.find_all('th'):
            if th.text and "업종" in th.text and "PER" in th.text:
                td = th.find_next_sibling('td')
                if td:
                    em = td.find('em')
                    final_data['업종PER'] = to_numeric(em.text if em else td.text)
                break

        return final_data
    except Exception as e:
        print(f"   ⚠️ [크롤링 에러] {ticker}: {e}")
        return final_data

def main():
    kst = timezone(timedelta(hours=9))
    print(f"⚡ [최적화 테스트] 재무 데이터: 한국 주식 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    success_cnt = 0
    number_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률", "52주 최고가", "52주 최저가", "목표주가", "업종PER"]

    # 🌟 [최적화] Session 객체 생성 (통신 속도 극대화)
    session = requests.Session()

    while True:
        try:
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
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
                        is_kr = (ticker.endswith(('.KS', '.KQ')) or (len(ticker) >= 6 and ticker[0].isdigit())) and not ticker.endswith(('.T', '.TA', '.TW'))
                        break
            
            if not ticker or not is_kr: continue

            # 🌟 [최적화] 함수 하나로 모든 데이터를 한 번에 가져옴
            fin_data = get_kr_fin_optimized(ticker, session)

            upd = {}
            valid_cnt = 0
            for key in number_keys:
                val = fin_data.get(key)
                if is_valid(val):
                    valid_cnt += 1
                    upd[key] = {"number": val}
                else:
                    upd[key] = {"number": None}

            opinion_val = fin_data.get("의견")
            if opinion_val:
                upd["목표가 범위"] = {"select": {"name": opinion_val}}

            if "마지막 업데이트" in props:
                upd["마지막 업데이트"] = {"date": {"start": datetime.now(kst).isoformat()}}
            
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    if valid_cnt > 0: print(f"   ✅ [KR: {ticker}] 완료")
                    else: print(f"   🧹 [KR: {ticker}] 데이터 없음")
                    success_cnt += 1
            except Exception as e:
                print(f"   ❌ [{ticker}] 전송 실패: {e}")
            
            time.sleep(0.5)

        if not res.get("has_more"): 
            break
            
        next_cursor = res.get("next_cursor")
        print(f"--- 현재까지 {success_cnt}건 완료. 다음 페이지로 이동 전 3초 휴식 ---")
        time.sleep(3)

    print(f"\n✨ 최적화 테스트 종료. 총 {success_cnt}건 처리됨.")

if __name__ == "__main__":
    main()
