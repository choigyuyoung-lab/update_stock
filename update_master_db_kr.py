import os, re, time, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client, errors # errors 추가

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
MASTER_DATABASE_ID = os.environ.get("MASTER_DATABASE_ID")
EXCLUDE_TICKERS = {"069500", "233740", "291680", "226490"}

BENCHMARK_IDS = {
    "KOSPI 200": "2f0f59dbdb5b81b98fecc95376dbc921",
    "KOSDAQ 150": "2f8f59dbdb5b80dc984ccb32f316dd1f",
    "KOSPI_TOTAL": "353f59dbdb5b80ba82ffc1f99413d759",
    "KODEX_300": "355f59dbdb5b80879573c5dce4d1e291"
}

# (중략: INDUSTRY_ETF_MAP 및 StockAutomationEngineKR 클래스는 이전과 동일)
# [생략된 엔진 부분은 이전 코드의 로직을 그대로 유지합니다]

# ---------------------------------------------------------
# 3. 유틸리티 (타임아웃 방지용 재시도 로직 추가)
# ---------------------------------------------------------
def safe_update(client, page_id, props, retries=3):
    """타임아웃 및 속도 제한 발생 시 재시도하는 안전한 업데이트 함수"""
    for i in range(retries):
        try:
            client.pages.update(page_id=page_id, properties=props)
            return True
        except (errors.RequestTimeoutError, errors.HTTPResponseError) as e:
            if i < retries - 1:
                wait_time = (i + 1) * 2  # 점진적 대기 시간 증가
                logging.warning(f"⚠️ API 지연 발생. {wait_time}초 후 재시도합니다... ({i+1}/{retries})")
                time.sleep(wait_time)
            else:
                logging.error(f"❌ 최대 재시도 횟수를 초과했습니다: {e}")
                return False

def format_notion_id(uid):
    if not uid: return None
    u = str(uid).replace("-", "")
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}" if len(u) == 32 else uid

def get_base_update(info):
    return {
        "종목명": {"rich_text": [{"text": {"content": info["name"]}}]},
        "Market": {"select": {"name": info["market"])}},
        "업데이트 일자": {"date": {"start": datetime.now().isoformat()}}
    }

# ---------------------------------------------------------
# 4. 단계별 실행 로직 (안정화 적용)
# ---------------------------------------------------------

def run_stock_phase(pages, engine, client):
    logger.info("🚀 Phase 1: 일반 주식 업데이트 시작")
    for page in pages:
        ticker = extract_ticker(page)
        if not ticker or ticker in EXCLUDE_TICKERS: continue
        
        info = engine.get_info(ticker)
        if not info or info["is_etf"]: continue

        update_props = get_base_update(info)
        # (중략: 마켓별 지표 할당 로직 동일)
        
        # 🌟 안전한 업데이트 호출 및 간격 조정
        if safe_update(client, page["id"], update_props):
            logger.info(f"   ✅ [STOCK] {info['name']} ({ticker})")
            time.sleep(0.3) # 노션 API 안정성을 위한 짧은 휴식

def run_etf_phase(pages, engine, client):
    logger.info("🚀 Phase 2: ETF 업데이트 시작")
    for page in pages:
        ticker = extract_ticker(page)
        if not ticker or ticker in EXCLUDE_TICKERS: continue
        
        info = engine.get_info(ticker)
        if not info or not info["is_etf"]: continue

        update_props = get_base_update(info)
        update_props["시장BM"] = {"relation": [{"id": format_notion_id(BENCHMARK_IDS["KODEX_300"])}]}
        
        # 🌟 안전한 업데이트 호출 및 간격 조정
        if safe_update(client, page["id"], update_props):
            logger.info(f"   ✅ [ETF] {info['name']} ({ticker})")
            time.sleep(0.3)

# (중략: extract_ticker 함수 동일)

# ---------------------------------------------------------
# 5. 메인 실행 (클라이언트 타임아웃 설정 추가)
# ---------------------------------------------------------
def main():
    # 🌟 타임아웃 설정을 60초로 연장 (기본값은 짧을 수 있음)
    client = Client(auth=NOTION_TOKEN, timeout_ms=60000)
    engine = StockAutomationEngineKR()
    
    logger.info("📦 노션 페이지 수집 중...")
    # (중략: 데이터 수집 로직 동일)

    run_stock_phase(all_pages, engine, client)
    run_etf_phase(all_pages, engine, client)
    
    logger.info("✨ 모든 작업이 완료되었습니다.")

if __name__ == "__main__":
    main()
