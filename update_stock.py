import os
import warnings
warnings.filterwarnings("ignore")

import yfinance as yf
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID") # [중요] 여기에 '관심주 DB'의 ID가 들어가야 합니다.
notion = Client(auth=NOTION_TOKEN)

# 안전장치: 20분
MAX_RUNTIME_SEC = 1200 

def safe_float(value):
    try:
        if value is None or str(value).strip() in ["", "-", "N/A", "nan"]: return None
        return float(str(value).replace(",", ""))
    except:
        return None

def extract_value_from_property(prop):
    """
    [핵심 함수] 노션 속성이 롤업이든, 수식이든, 선택이든 상관없이
    무조건 '문자열' 알맹이를 끄집어내는 만능 함수
    """
    if not prop: return ""
    
    p_type = prop.get("type")
    
    # 1. 롤업 (Rollup) - 상장주식 DB에서 끌어온 값
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        if not array: return ""
        # 롤업된 배열의 첫 번째 값 재귀 호출 (껍질 까기)
        return extract_value_from_property(array[0])

    # 2. 선택 (Select)
    if p_type == "select":
        return prop.get("select", {}).get("name", "")
    
    # 3. 텍스트 (Rich Text) / 제목 (Title)
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        if text_list:
            return text_list[0].get("plain_text", "")
        return ""

    # 4. 수식 (Formula)
    if p_type == "formula":
        f_type = prop.get("formula", {}).get("type")
        if f_type == "string":
            return prop.get("formula", {}).get("string", "")
        elif f_type == "number": # 숫자로 된 티커일 경우 대비
            return str(prop.get("formula", {}).get("number", ""))

    return ""

def get_stock_data_from_yahoo(ticker, market):
    symbol = str(ticker).strip().upper()
    
    # [오타 보정 및 시장 매핑]
    if "KOSPI" in market.upper(): 
        if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
    elif "KOSDAQ" in market.upper(): 
        if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
    else:
        # 미국/해외: 꼬리표 제거
        symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
    
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        price = d.get("currentPrice") or d.get("regularMarketPrice")
        
        if price is None: return None

        return {
            "price": price,
            "per": d.get("trailingPE"),
            "pbr": d.get("priceToBook"),
            "eps": d.get("trailingEps"),
            "high52w": d.get("fiftyTwoWeekHigh"),
            "low52w": d.get("fiftyTwoWeekLow")
        }
    except:
        return None

def main():
    start_time = time.time()
    
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 [관심주 DB 전용] 업데이트 시작 - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    has_more = True
    next_cursor = None
    success = 0
    fail = 0
    skip = 0
    
    while has_more:
        if time.time() - start_time > MAX_RUNTIME_SEC:
            print(f"\n⏰ 20분 경과. 안전 종료.")
            break

        try:
            response = notion.databases.query(
                **{"database_id": DATABASE_ID, "start_cursor": next_cursor}
            )
            pages = response.get("results", [])
            
            if not pages and success == 0 and fail == 0:
                print("🚨 가져온 페이지가 0개입니다. (DB ID가 '관심주 DB'인지 확인하세요)")
                break

            for page in pages:
                if time.time() - start_time > MAX_RUNTIME_SEC:
                    has_more = False; break 

                try:
                    props = page["properties"]
                    
                    # 1. Market 추출 (롤업 대응)
                    market = extract_value_from_property(props.get("Market"))
                    
                    # 2. 티커 추출 (롤업 대응 - 혹시 티커도 롤업일 수 있으니)
                    ticker = extract_value_from_property(props.get("티커"))
                    
                    # 데이터 검증 로그
                    # print(f"🔍 검사: {ticker} ({market})") 

                    if not market or not ticker:
                        skip += 1
                        continue
                    
                    # 3. 야후 조회
                    data = get_stock_data_from_yahoo(ticker, market)

                    if data is not None:
                        upd = {
                            "현재가": {"number": data["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        
                        fields = {"PER": "per", "PBR": "pbr", "EPS": "eps", "52주 최고가": "high52w", "52주 최저가": "low52w"}
                        for n_key, d_key in fields.items():
                            val = safe_float(data[d_key])
                            if val is not None: upd[n_key] = {"number": val}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success += 1
                        print(f"   => ✅ [{market}] {ticker} : {data['price']:,.0f}")
                    else:
                        print(f"   => ❌ [{market}] {ticker} : 검색 실패")
                        fail += 1
                    
                    time.sleep(0.5) 
                        
                except Exception as e:
                    # print(f"에러: {e}")
                    fail += 1
                    continue
            
            if not has_more: break
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"🚨 노션 연결 오류: {e}")
            break

    print("\n---------------------------------------------------")
    print(f"✨ 결과: 성공 {success} / 실패 {fail} / 스킵 {skip}")
    print(f"⏱️ 총 소요 시간: {time.time() - start_time:.1f}초")

if __name__ == "__main__":
    main()
