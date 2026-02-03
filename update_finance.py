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
    """유효한 숫자인지 체크 (NaN, Inf, None 방지)"""
    if val is None: return False
    try:
        return not (math.isnan(val) or math.isinf(val))
    except:
        return False

def to_numeric(val_str):
    """
    [데이터 정제]
    텍스트("1,234", "N/A", "12.50")를 순수 숫자(1234.0, None, 12.5)로 1차 변환
    """
    if not val_str: return None
    try:
        clean_str = str(val_str).replace(",", "").replace("원", "").replace("%", "").strip()
        if clean_str.upper() == "N/A" or clean_str == "":
            return None
        return float(clean_str)
    except:
        return None

def format_value(key, val, is_kr):
    """
    [디자인 적용]
    숫자를 노션에 보여줄 '예쁜 텍스트'로 최종 변환
    """
    if not is_valid(val):
        return None

    # 1. 금액/가치 관련 (EPS, 추정EPS, BPS) -> 통화 기호 + 콤마
    if key in ["EPS", "추정EPS", "BPS"]:
        if is_kr:
            # 한국: 소수점 없이 콤마 (예: ₩1,234)
            return f"₩{int(val):,}"
        else:
            # 미국: 소수점 2자리 + 콤마 (예: $12.50)
            return f"${val:,.2f}"

    # 2. 배당수익률 -> 퍼센트 붙이기
    elif key == "배당수익률":
        return f"{val:.2f}%"

    # 3. 비율 지표 (PER, PBR 등) -> 깔끔한 숫자 문자열
    else:
        return f"{val:.2f}"

# ---------------------------------------------------------------------------
# 3. 데이터 수집 함수
# ---------------------------------------------------------------------------
def get_kr_fin(ticker):
    """
    [한국 주식] '1. 비교해야 할 코드'의 로직을 그대로 적용
    """
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/'
    }

    # 수집할 항목 정의
    data_keys = ["PER", "추정PER", "EPS", "추정EPS", "PBR", "BPS", "배당수익률"]
    final_data = {k: None for k in data_keys}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'
        soup = BeautifulSoup(response.text, 'html.parser')

        # [비교 코드 로직 1] ID 기반 기본 지표 추출
        selectors = {
            "PER": "#_per",
            "EPS": "#_eps",
            "추정PER": "#_cns_per",
            "추정EPS": "#_cns_eps",
            "PBR": "#_pbr",
            "배당수익률": "#_dvr"
        }
        
        raw_data = {}
        for key, sel in selectors.items():
            el = soup.select_one(sel)
            raw_data[key
