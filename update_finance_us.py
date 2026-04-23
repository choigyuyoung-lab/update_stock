import os, time, math
import yfinance as yf
from datetime import datetime, timedelta, timezone
from notion_client import Client

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") 
notion = Client(auth=NOTION_TOKEN)

def is_valid(val):
    if val is None: return False
    try:
        if isinstance(val, str): return False
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

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

        final_data["PER"] = info.get("trailingPE")
        final_data["추정PER"] = info.get("forwardPE")
        final_data["EPS"] = info.get("trailingEps")
        final_data["추정EPS"] = info.get("forwardEps")
        final_data["PBR"] = info.get("priceToBook")
        final_data["BPS"] = info.get("bookValue")
        if info.get("dividendYield") is not None:
            final_data["배당수익률"] = info.get("dividendYield") * 100
            
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
        return final_data

def main():
    kst = timezone(timedelta(hours=9))
    print(f"📊 [재무 데이터: 미국 주식] 시작 - {datetime.now(kst)}")
    
    next_cursor = None
    success_cnt = 0
    number_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률", "52주 최고가", "52주 최저가", "목표주가"]

    while True:
        try:
            # [수정 전]
            # res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
            
            # 🌟 [수정 후] 100개씩 가져오라고 명확히 지시 (page_size=100 추가)
            res = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor, page_size=100)
        except Exception as e:
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
            
            if not ticker or is_kr: continue # 미국 주식이 아니면 패스

            fin_data = get_us_fin(ticker)
            fin_data["동일업종 PER"] = None

            upd = {}
            valid_cnt = 0
            for key in number_keys + ["동일업종 PER"]:
                val = fin_data.get(key)
                if is_valid(val):
                    valid_cnt += 1
                    upd[key] = {"number": val}
                else:
                    upd[key] = {"number": None}

            opinion_val = fin_data.get("의견")
            if opinion_val:
                upd["목표가 범위"] = {"select": {"name": opinion_val}}

            # 🌟 [수정됨] 실시간 업데이트 시간 기록
            if "마지막 업데이트" in props:
                current_now_iso = datetime.now(kst).isoformat()
                upd["마지막 업데이트"] = {"date": {"start": current_now_iso}}
            
            try:
                if upd:
                    notion.pages.update(page_id=page["id"], properties=upd)
                    if valid_cnt > 0: print(f"   ✅ [US: {ticker}] 완료")
                    else: print(f"   🧹 [US: {ticker}] 데이터 없음")
                    success_cnt += 1
            except Exception as e:
                print(f"   ❌ [{ticker}] 전송 실패: {e}")
            time.sleep(0.5)

        # [수정 전]
        # if not res.get("has_more"): break
        # next_cursor = res.get("next_cursor")

        # 🌟 [수정 후] 페이지를 넘길 때 진행 상황을 출력하고 3초간 달콤한 휴식
        if not res.get("has_more"): 
            break
            
        next_cursor = res.get("next_cursor")
        print(f"--- 현재까지 {success_cnt}건 완료. 다음 페이지로 이동 전 3초 휴식 ---")
        time.sleep(3) # API 과부하를 막는 핵심 방어막

    print(f"\n✨ 종료. 총 {success_cnt}건 처리됨.")


if __name__ == "__main__":
    main()
