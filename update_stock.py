import os
import warnings
warnings.filterwarnings("ignore")
import json 

from notion_client import Client

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

def extract_value_from_property(prop):
    if not prop: return ""
    p_type = prop.get("type")
    
    # 1. 롤업 (Rollup)
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        if not array: return ""
        # 재귀 호출로 내부 값 확인
        return extract_value_from_property(array[0])

    # 2. 선택 (Select)
    if p_type == "select":
        return prop.get("select", {}).get("name", "")
    
    # 3. 텍스트/제목
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
        elif f_type == "number":
            return str(prop.get("formula", {}).get("number", ""))

    return ""

def main():
    print(f"🔍 [데이터 구조 진단] 시작...")
    
    try:
        # 딱 1개의 페이지만 가져와서 분석
        response = notion.databases.query(
            **{"database_id": DATABASE_ID, "page_size": 1} 
        )
        pages = response.get("results", [])
        
        if not pages:
            print("🚨 페이지를 찾을 수 없습니다. DB ID를 확인하세요.")
            return

        page = pages[0]
        props = page["properties"]
        
        print("\n================ [진단 리포트] ================")
        print(f"1. 발견된 속성 이름 목록:\n{list(props.keys())}")
        
        # Market 분석
        market_prop = props.get("Market")
        print(f"\n2. 'Market' 속성 분석:")
        if market_prop:
            print(f"   - Type: {market_prop.get('type')}")
            # JSON 형태로 데이터 구조 전체 출력
            print(f"   - Raw Data: {json.dumps(market_prop, indent=2, ensure_ascii=False)}")
            extracted = extract_value_from_property(market_prop)
            print(f"   - 프로그램 추출 시도값: '{extracted}'")
        else:
            print("   - ❌ 'Market' 속성이 없습니다. (대소문자/띄어쓰기 확인)")

        # 티커 분석
        ticker_prop = props.get("티커")
        print(f"\n3. '티커' 속성 분석:")
        if ticker_prop:
            print(f"   - Type: {ticker_prop.get('type')}")
            print(f"   - Raw Data: {json.dumps(ticker_prop, indent=2, ensure_ascii=False)}")
            extracted = extract_value_from_property(ticker_prop)
            print(f"   - 프로그램 추출 시도값: '{extracted}'")
        else:
            print("   - ❌ '티커' 속성이 없습니다. (이름 확인)")
            
        print("===============================================")

    except Exception as e:
        print(f"🚨 에러 발생: {e}")

if __name__ == "__main__":
    main()
