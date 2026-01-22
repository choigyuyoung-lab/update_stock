import os
import warnings
warnings.filterwarnings("ignore")
import json # 데이터 구조를 보기 위해 추가

import yfinance as yf
from notion_client import Client
import time
from datetime import datetime, timedelta, timezone

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 안전장치
MAX_RUNTIME_SEC = 600 

def extract_value_from_property(prop):
    if not prop: return ""
    p_type = prop.get("type")
    
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        if not array: return ""
        return extract_value_from_property(array[0])

    if p_type == "select":
        return prop.get("select", {}).get("name", "")
    
    if p_type in ["rich_text", "title"]:
        text_list = prop.get(p_type, [])
        if text_list:
            return text_list[0].get("plain_text", "")
        return ""

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
        response = notion.databases.query(
            **{"database_id": DATABASE_ID, "page_size": 1} # 딱 1개만 가져옴
        )
        pages = response.get("results", [])
        
        if not pages:
            print("🚨 페이지를 찾을 수 없습니다. DB ID를 확인하세요.")
            return

        page = pages[0]
        props = page["properties"]
        
        print("\n================ [진단 리포트] ================")
        print(f"1. 발견된 속성 이름 목록: {list(props.keys())}")
        
        # Market 분석
        market_prop = props.get("Market")
        print(f"\n2. 'Market' 속성 분석:")
        if market_prop:
            print(f"   - Type: {market_prop.get('type')}")
            # JSON 형태로 적나라하게 출력
            print(f"   - Raw Data: {json.dumps(market_prop, indent=2, ensure_ascii=False)}")
            extracted = extract_value_from_property(market_prop)
            print(f"   - 프로그램이 추출한 값: '{extracted}'")
        else:
            print("   - ❌ 'Market'이라는 이름의 속성이 없습니다! (이름 확인 필요)")

        # 티커 분석
        ticker_prop = props.get("티커")
        print(f"\n3. '티커' 속성 분석:")
        if ticker_prop:
            print(f"   - Type: {ticker_prop.get('type')}")
            print(f"   - Raw Data: {json.dumps(ticker_prop, indent=2, ensure_ascii=False)}")
            extracted = extract_value_from_property(ticker_prop)
            print(f"   - 프로그램이 추출한 값: '{extracted}'")
        else:
            print("   - ❌ '티커'라는 이름의 속성이 없습니다! (이름 확인 필요)")
            
        print("===============================================")

    except Exception as e:
