import sys
import subprocess
import os
import time
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------
# 🧹 [초강력 클리닝] 시작하자마자 무조건 재설치 (좀비 박멸)
# ---------------------------------------------------------
print("🚑 [시스템 초기화] 기존 라이브러리 제거 및 재설치 중...")
try:
    # 1. 꼬인 라이브러리들 강제 삭제
    subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "notion", "notion-client"])
    # 2. 최신 정품 라이브러리 강제 설치 (버전 2.2.1 고정)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "notion-client==2.2.1", "yfinance"])
    print("✅ 라이브러리 재설치 완료! 이제 진짜 시작합니다.")
except Exception as e:
    print(f"⚠️ 설치 중 경고(무시 가능): {e}")

# 이제서야 라이브러리를 불러옵니다 (깨끗한 상태)
import notion_client
from notion_client import Client
import yfinance as yf
# ---------------------------------------------------------

# 1. 환경 설정
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DATABASE_ID = os.environ.get("DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# 안전장치
MAX_RUNTIME_SEC = 1200 

def fetch_yahoo_price(symbol):
    """가격 정보만 가져오기"""
    try:
        stock = yf.Ticker(symbol)
        d = stock.info
        price = d.get("currentPrice") or d.get("regularMarketPrice")
        if price is None: return None
        return {
            "price": price,
            "high52w": d.get("fiftyTwoWeekHigh"), 
            "low52w": d.get("fiftyTwoWeekLow")    
        }
    except:
        return None

def get_smart_stock_data(ticker, market_hint):
    """한/미 주식 자동 감지"""
    ticker = str(ticker).strip().upper()
    if market_hint:
        symbol = ticker
        if "KOSPI" in market_hint.upper(): 
            if not symbol.endswith(".KS"): symbol = f"{symbol}.KS"
        elif "KOSDAQ" in market_hint.upper(): 
            if not symbol.endswith(".KQ"): symbol = f"{symbol}.KQ"
        else:
            symbol = symbol.replace(".KS", "").replace(".KQ", "").replace(".K", "")
        return fetch_yahoo_price(symbol), market_hint
    else:
        if ticker.isdigit() and len(ticker) == 6:
            data = fetch_yahoo_price(f"{ticker}.KS")
            if data: return data, "KOSPI(Auto)"
            data = fetch_yahoo_price(f"{ticker}.KQ")
            if data: return data, "KOSDAQ(Auto)"
        else:
            clean_ticker = ticker.replace(".KS", "").replace(".KQ", "").replace(".K", "")
            data = fetch_yahoo_price(clean_ticker)
            if data: return data, "US(Auto)"
            data = fetch_yahoo_price(f"{clean_ticker}.KS")
            if data: return data, "KOSPI(Auto-Retry)"
            data = fetch_yahoo_price(f"{clean_ticker}.KQ")
            if data: return data, "KOSDAQ(Auto-Retry)"
    return None, "Unknown"

def extract_value(prop):
    """속성값 안전 추출"""
    if not prop: return ""
    p_type = prop.get("type")
    if p_type == "rollup":
        array = prop.get("rollup", {}).get("array", [])
        if array: return extract_value(array[0])
    if p_type == "select": return prop.get("select", {}).get("name", "")
    if p_type in ["rich_text", "title"]:
        return prop.get(p_type, [{}])[0].get("plain_text", "") if prop.get(p_type) else ""
    if p_type == "
