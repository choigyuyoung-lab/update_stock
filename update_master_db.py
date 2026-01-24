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
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    def get_korea_stock(self, ticker):
        """
        [한국 주식] 네이버 금융 HTML 크롤링 (한글 깨짐 방지 적용)
        """
        try:
            url = f"https://finance.naver.com/item/main.naver?code={ticker}"
            res = requests.get(url, headers=self.headers, timeout=10)
            
            # [핵심 수정] 인코딩을 강제하지 않고, 실제 페이지 내용에 맞춰 자동으로 찾습니다.
            # 이 코드가 '' 깨짐 현상을 해결합니다.
            res.encoding = res.apparent_encoding 
            
            if res.status_code != 200:
                return None, f"페이지 접속 불가({res.status_code})"

            soup = BeautifulSoup(res.text, 'html.parser')

            # 1. 종목명
            name_tag = soup.select_one('.wrap_company h2 a')
            if not name_tag:
                return None, "종목명 추출 실패 (페이지 구조 다름)"
            name = name_tag.text.strip()

            # 2. 산업분류 (네이버 금융 '업종'란 파싱)
            industry = ""
            try:
                # 기업개요 섹션 옆의 '업종' 링크 찾기 시도
                industry_tag = soup.select_one('div.section.trade_compare h4 em a')
                if industry_tag:
                    industry = industry_tag.text.strip()
                else:
                    # 실패 시 하단 기업개요 텍스트에서 유추하거나 '한국증시'로 대체
                    industry = "한국증시"
            except:
                industry = "한국증시"

            # 3. 회사개요
            summary = ""
            summary_div = soup.select_one('#summary_info p')
            if summary_div:
                summary = summary_div.text.strip()
            else:
                summary = "기업개요 정보 없음"

            return {
                "name": name,
                "industry": industry,
                "summary": summary
            }, "✅ 네이버 크롤링 성공"

        except Exception as e:
            return None, f"크롤링 에러: {e}"

    def get_usa_stock(self, ticker):
        """
        [미국 주식] yfinance 사용 (안정성 최우선)
        """
        try:
            # yfinance는 기본적으로 데이터를 잘 가져오지만, 
            # 티커가 'LENB' 처럼 점(.)이 빠진 경우 'LEN-B'나 'LEN.B'로 변환 시도 가능
            # 여기서는 기본 시도 후 실패 시 변환 시도 로직 추가
            
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # 데이터 없음 확인 (yfinance는 에러를 안 뱉고 빈 딕셔너리를 줄 때가 있음)
            if 'regularMarketPrice' not in info and 'symbol' not in info:
                # 점(.)이 있는 티커(BRK.B 등)를 위한 재시도 로직
                if len(ticker) > 3 and 'B' in ticker and '.' not in ticker:
                     # 예: LENB -> LEN-B (야후는 하이픈 사용)
                     retry_ticker = ticker.replace("B", "-B")
                     stock = yf.Ticker(retry_ticker)
                     info = stock.info
                
                if 'regularMarketPrice' not in info and 'symbol' not in info:
                    return None, "데이터 없음 (티커 확인 필요)"

            # 1. 종목명
            name = info.get('longName') or info.get('shortName') or ticker
            
            # 2. 산업분류 (영어 -> 한글 단순 매핑)
            sector_map = {
                "Technology": "기술", "Financial Services": "금융", "Healthcare": "헬스케어",
                "Consumer Cyclical": "경기소비재", "Communication Services": "통신",
                "Industrials": "산업재", "Consumer Defensive": "필수소비재", "Energy": "에너지",
                "Basic Materials": "소재", "Real Estate": "부동산", "Utilities": "유틸리티"
            }
            eng_sector = info.get('sector', '')
            industry = sector_map.get(eng_sector, eng_sector)

            # 3. 회사개요 (영어)
            summary = info.get('longBusinessSummary') or "개요 없음"
            
            return {
                "name": name,
                "industry": industry,
                "summary": summary
            }, "✅ 야후(yfinance) 성공"

        except Exception as e:
            return None, f"yfinance 에러: {e}"

    def fetch(self, ticker):
        """티커를 분석하여 한국/미국 분류 후 데이터 수집"""
        clean_ticker = ticker.strip().upper()
        
        # 한국 주식 판별 로직 (6자리 숫자 포함)
        is_korea = False
        # 숫자 6개가 포함되어 있으면 한국 주식으로 간주 (예: 005930, 0057H0)
        # 정규표현식으로 숫자 5개 이상 연속되면 한국으로 판단
        import re
        if re.search(r'\d{5,}', clean_ticker):
            is_korea = True
        
        if is_korea:
            # 접미어(.KS) 제거
            code = clean_ticker.split('.')[0]
            return self.get_korea_stock(code)
        else:
            return self.get_usa_stock(clean_ticker)

def main():
    print(f"🚀 [Master DB] 한글 깨짐 수정 완료 버전 시작")
    
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
            # 아직 검증되지 않은 항목만 가져오기
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
                
                # 타겟 필터링 (리스트가 비어있으면 전체 실행)
                if TARGET_TICKERS and raw_ticker not in TARGET_TICKERS:
                    continue

                print(f"🔍 조회 중: {raw_ticker} ...")
                
                # 데이터 수집
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
