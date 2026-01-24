import os
import time
import requests
import re
from notion_client import Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# 재시도 및 타임아웃 설정 (시스템 안정성)
MAX_RETRIES = 3
TIMEOUT = 10
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

class NaverStockClient:
    """
    네이버 통합 검색 로직을 사용하여 
    국내/해외 주식의 '한글 데이터'를 수집하는 전담 클래스
    """
    def __init__(self):
        self.session = requests.Session()
        # 네트워크 불안정 시 3회 재시도 설정
        retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({'User-Agent': USER_AGENT})

    def search_and_fetch(self, ticker):
        """
        티커 -> 네이버 검색 -> 정확한 코드 식별 -> 한글 상세 데이터 반환
        """
        if not ticker:
            return None

        # 1. 검색어 정제 (접미어 제거: 005930.KS -> 005930)
        clean_ticker = ticker.strip().upper()
        search_query = clean_ticker.split('.')[0]

        try:
            # -----------------------------------------------------
            # STEP A: 네이버 검색 API로 '실제 코드(reutersCode)' 조회
            # -----------------------------------------------------
            search_url = f"https://m.stock.naver.com/api/search/all?query={search_query}"
            res = self.session.get(search_url, timeout=TIMEOUT)
            
            if res.status_code != 200:
                return None

            search_result = res.json().get("searchList", [])
            if not search_result:
                return None

            # 검색 결과 중 가장 적합한 코드 찾기
            target_code = None
            
            # 1순위: 검색어와 코드가 정확히 일치하거나 포함되는 경우
            for item in search_result:
                code = item.get("reutersCode", "") or item.get("stockId", "")
                if search_query == code or search_query in code:
                    target_code = code
                    break
            
            # 2순위: 없으면 가장 상단 결과 선택
            if not target_code:
                first_item = search_result[0]
                target_code = first_item.get("reutersCode", "") or first_item.get("stockId", "")

            # -----------------------------------------------------
            # STEP B: 상세 정보(Integration) 수집 - 한글 데이터 원천
            # -----------------------------------------------------
            detail_url = f"https://m.stock.naver.com/api/stock/{target_code}/integration"
            
            # 차단 방지용 Referer 설정
            self.session.headers.update({'Referer': f'https://m.stock.naver.com/domestic/stock/{target_code}/total'})
            
            res_detail = self.session.get(detail_url, timeout=TIMEOUT)
            if res_detail.status_code == 200:
                data = res_detail.json()
                
                # 데이터 위치 찾기 (주식, ETF, ETN, 리츠 등)
                r = data.get("result", {})
                item = (r.get("stockItem") or r.get("etfItem") or 
                        r.get("etnItem") or r.get("reitItem"))
                
                if item:
                    # 1. 종목명 (한글 우선)
                    korean_name = item.get("stockName") or item.get("itemname") or item.get("gname")
                    
                    # 2. 산업분류
                    industry = item.get("industryName", "") or item.get("industryCodeName
