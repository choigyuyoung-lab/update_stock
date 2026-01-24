import os
import time
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from notion_client import Client

# ---------------------------------------------------------
# 1. 환경 변수 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")

# [설정] 전체 업데이트 (특정 티커 테스트 시 여기에 리스트 작성)
TARGET_TICKERS = [] 

class StockCrawler:
    """
    복잡한 API 호출 대신, 네이버 웹페이지(HTML)를 직접 분석하는 
    가장 전통적이고 안정적인 방식의 크롤러
    """
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    def get_korea_stock(self, ticker):
        """
        [한국 주식] 네이버 금융(PC버전) HTML 크롤링
        출처: https://finance.naver.com/item/main.naver?code=...
        """
        try:
            url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            res = requests.get(url, headers=self.headers, timeout=10)
            
            # 네이버 금융은 EUC-KR 인코딩을 사용 (깨짐 방지)
            res.encoding = 'euc-kr' 
            soup = BeautifulSoup(res.text, 'html.parser')

            # 1. 종목명 (h2 태그)
            name_tag = soup.select_one('.wrap_company h2 a')
            if not name_tag:
                return None, "페이지 구조 다름(종목명 실패)"
            name = name_tag.text.strip()

            # 2. 산업분류 (하이라이트 섹션 등에서 유추하거나 업종 란 파싱)
            # 보통 '업종' 란이 상단에 있음
            industry = ""
            ind_tag = soup.select_one('.first .blind') # '전일' 등의 텍스트가 걸릴 수 있어 상세 파싱 필요
            # 더 확실한 방법: 기업개요 섹션 근처의 업종 확인
            # 네이버 금융 메인에서는 업종 찾기가 까다로워 WICS(섹터) 정보를 많이 씁니다.
            # 여기서는 '투자의견/목표주가' 테이블 옆이나 '동일업종비교' 탭을 봐야하는데,
            # 간단하게 기업개요 텍스트에서 추출 시도 혹은 빈칸.
            # (네이버 블로그 방식: 보통 ETF가 아니면 업종란이 명확치 않아 '코스피/코스닥'만 구분하기도 함)
            # 여기서는 안정성을 위해 데이터를 비워두거나, 아래 기업개요에서 가져옵니다.
            
            # 3. 회사개요 (기업개요 div)
            summary_div = soup.select_one('#summary_info p')
            summary = summary_div.text.strip() if summary_div else "기업개요 없음"

            return {
                "name": name,
                "industry": "한국증시", # HTML 파싱으로 정확한 업종 찾기는 복잡하여 일단 국가로 표기
                "summary": summary
            }, "✅ 네이버 크롤링 성공"

        except Exception as e:
            return None, f"크롤링 에러: {e}"

    def get_usa_stock_summary_kr(self, ticker):
        """
        [미국 주식 보조] 네이버 해외주식에서 '한글 개요'만 살짝 긁어오기
        실패하면 None 반환
        """
        try:
            # 네이버 해외주식 모바일 페이지 (여기가 구조가 제일 단순함)
            url = f"https://m.stock.naver.com/api/stock/{ticker}.O/integration" # .O는 나스닥/NYSE 등 자동매칭됨
            # 만약 .O가 안먹히면 검색 API를 써야하므로, 여기서는 단순 시도만 함.
            # 이번엔 API 말고 순수 yfinance로 가되, 한글 개요가 꼭 필요하면 아래 로직 사용.
            pass 
        except:
            pass
        return None

    def get_usa_stock(self, ticker):
        """
        [미국 주식] yfinance 라이브러리 사용 (세계 표준, 가장 안정적)
        단, 기본 정보는 영어로 나옵니다.
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # 데이터가 없으면 실패
            if 'regularMarketPrice' not in info and 'symbol' not in info:
                 return None, "yfinance 데이터 없음"

            # 1. 종목명 (영어)
            name = info.get('longName') or info.get('shortName') or ticker
            
            # 2. 산업분류 (한글 매핑 시도)
            sector_map = {
                "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
                "Consumer Cyclical": "경기소비재", "Communication Services": "통신",
                "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
                "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
            }
            eng_sector = info.get('sector', '')
            industry = sector_map.get(eng_sector, eng_sector) # 매핑 없으면 영어 그대로

            # 3. 회사개요 (영어 -> 한글 번역은 구글 API 없이 불가능하므로 영어 원문 or 네이버 시도)
            summary = info.get('longBusinessSummary') or "개요 없음"
            
            # [옵션] 여기서 네이버에 한 번 물어봐서 한글 개요가 있으면 바꿔치기 할 수 있습니다.
            # 하지만 '뒤죽박죽'을 피하기 위해, 미국 주식은 일단 yfinance(영어)로 확실하게 채우는 걸 추천합니다.
            
            return {
                "name": name,
                "industry": industry,
                "summary": summary
            }, "✅ 야후(yfinance) 성공"

        except Exception as e:
            return None, f"yfinance 에러: {e}"

    def fetch(self, ticker):
        """티커 형태를 보고 한국/미국 분류하여 데이터 수집"""
        clean_ticker = ticker.strip().upper()
        
        # 한국 주식 판별: 6자리 숫자 (또는 뒤에 한글자 알파벳)
        # 예: 005930, 0057H0 (알파벳 섞인 코드도 한국주식 로직 태움)
        is_korea = False
        if len(clean_ticker) >= 6 and clean_ticker[:5].isdigit(): 
            is_korea = True
        
        if is_korea:
            # 005930.KS 등 접미어가 있으면 제거하고 순수 코드로 변환
            code = clean_ticker.split('.')[0]
            return self.get_korea_stock(code)
        else:
            # 미국 주식 (알파벳 티커)
            return self.get_usa_stock(clean_ticker)

def main():
    print(f"🚀 [Master DB] Classic Mode 업데이트 시작 (Naver Crawling + yfinance)")
    
    try:
        notion = Client(auth=NOTION_TOKEN)
        crawler = StockCrawler()
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")
        return

    next_cursor = None
    processed_count = 0
    
    while True:
        try:
            # 필터: 검증되지 않은 항목만
            query_params = {
                "database_id": MASTER_DATABASE_ID,
                "filter": {"property": "데이터 상태", "select": {"does_not_equal": "✅ 검증완료"}},
                "page_size": 30
            }
            if next_cursor: query_params["start_cursor"] = next_cursor
            
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            if not pages and processed_count == 0:
                print("✨ 업데이트할 대상이 없습니다.")
                break

            for page in pages:
                page_id = page["id"]
                props = page["properties"]
                
                ticker_list = props.get("티커", {}).get("title", [])
                if not ticker_list: continue
                raw_ticker = ticker_list[0].get("plain_text", "").strip().upper()
                
                # 타겟 필터링
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS:
                    continue

                print(f"🔍 조회 중: {raw_ticker} ...")
                
                # 데이터 수집 (크롤러 결정)
                data, log_msg = crawler.fetch(raw_ticker)
                
                status = ""
                upd_props = {}
                
                if data:
                    status = "✅ 검증완료"
                    # 요약본 안전 처리
                    summary_text = data['summary']
                    safe_summary = summary_text[:1900] + "..." if len(summary_text) > 1900 else summary_text
                    
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]},
                        "종목명": {"rich_text": [{"text": {"content": data['name']}}]},
                        "산업분류": {"rich_text": [{"text": {"content": data['industry']}}]}
                    }
                    if "회사개요" in props:
                        upd_props["회사개요"] = {"rich_text": [{"text": {"content": safe_summary}}]}
                        print(f"   └ [완료] {data['name']}")
                    else:
                        print(f"   └ [완료] {data['name']} (개요 열 없음)")
                else:
                    status = "⚠️ 확인필요"
                    upd_props = {
                        "데이터 상태": {"select": {"name": status}},
                        "검증로그": {"rich_text": [{"text": {"content": log_msg}}]}
                    }
                    print(f"   └ [실패] {log_msg}")

                notion.pages.update(page_id=page_id, properties=upd_props)
                processed_count += 1
                time.sleep(0.5) 

            if not response.get("has_more"): break
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            print(f"❌ 시스템 오류: {e}")
            break
            
    print(f"🏁 작업 완료: 총 {processed_count}건")

if __name__ == "__main__":
    main()
