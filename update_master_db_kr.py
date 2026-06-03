import os
import re
import time
import logging
import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # 🌟 파이썬 3.9+ 타임존 표준 라이브러리
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any


import httpx
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
from notion_client import Client

from notion_utils import (
    build_notion_client,
    get_env_var,
    paginate_database,
    safe_page_update,
)

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
MASTER_DATABASE_ID = get_env_var("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = get_env_var("BENCHMARK_DATABASE_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 기초 정보 추출용 우선순위 헤더
HEADERS = {
    "KR_SECTOR": ['Sector', 'WICS 업종명', '업종'],
    "KR_INDUSTRY": ['Industry', '주요제품', 'WICS 제품']
}

# 최종 완성된 테마 ETF 판별 규칙
ETF_THEME_RULES = {
    "S&P500": {"tag": "S&P 500", "bm": "SPY"},
    "나스닥100": {"tag": "NASDAQ 100", "bm": "QQQ"},
    "미국배당": {"tag": "US Dividend", "bm": "SCHD"},
    "AI전력": {"tag": "US AI Power", "bm": "XLU"},
    "AI광통신": {"tag": "US AI Optical Network", "bm": "IGN"},
    "미국빅테크": {"tag": "US Big Tech", "bm": "XLK"},
    "구글밸류": {"tag": "Google Focused", "bm": "QQQ"},
    "마이크로소프트밸류": {"tag": "MS Focused", "bm": "QQQ"},
    "엔비디아밸류": {"tag": "Nvidia Focused", "bm": "SOXX"},
    "우주테크&방산": {"tag": "Global Aerospace & Defense", "bm": "XAR"},
    "우주항공": {"tag": "US Aerospace & Defense", "bm": "XAR"},
    "AI&로봇": {"tag": "Global AI & Robot", "bm": "BOTZ"},
    "HBM": {"tag": "Global AI Memory", "bm": "SOXX"},
    "AI메모리": {"tag": "Global AI Memory", "bm": "SOXX"},
    "팔란티어밸류": {"tag": "Palantir Focused", "bm": "QQQ"}
}

# ---------------------------------------------------------
# 2. 지표 DB 동적 분석
# ---------------------------------------------------------
def get_dynamic_config(client):
    logger.info("🔍 지표지수 DB 동적 분석 시작...")
    config = {"ticker_to_id": {}, "kr_industry_tickers": []}
    try:
        for page in paginate_database(client, BENCHMARK_DATABASE_ID, page_size=100, retry_delay=0.3):
            props = page.get("properties", {})
            ticker_list = props.get("이름", {}).get("title", [])
            if not ticker_list:
                continue

            ticker = ticker_list[0].get("plain_text", "").strip().upper()
            select_obj = props.get("구분", {}).get("select")
            category = select_obj.get("name", "") if select_obj else ""

            config["ticker_to_id"][ticker] = page["id"]
            if category == "KR산업":
                config["kr_industry_tickers"].append(ticker)

        logger.info(f"✅ 지표 로드 완료 (총 {len(config['ticker_to_id'])}개)")
    except Exception as e:
        logger.error(f"❌ 지표 DB 로드 실패: {e}")
    return config

# ---------------------------------------------------------
# 3. 데이터 엔진
# ---------------------------------------------------------
class StockAutomationEngineKR:
    def __init__(self, kr_industry_tickers):
        logger.info("📡 KRX 데이터 엔진 가동...")
        
        # 1번 피드백 반영: 대량 스캔 인프라 안정화 마진 구현
        self.df_kr_desc = fdr.StockListing('KRX-DESC').set_index('Code')
        self.kr_etf = fdr.StockListing('ETF/KR').set_index('Symbol').to_dict('index')
        
        # 지수 추적용 포트폴리오 로드 간격 배분 (KRX 서버 디도스 감지 회피)
        self.k200_list = self._get_index_list("1028")
        time.sleep(0.5)  
        self.kd150_list = self._get_index_list("2203")
        time.sleep(0.5)
        
        # ETF PDF 기반 종목별 산업 매핑 연산 가동
        self.kr_industry_lookup = self._build_industry_lookup(kr_industry_tickers)

    def _get_index_list(self, code):
        """KRX 인덱스 포트폴리오를 조회하되 실패 시 타임아웃 백오프를 수행합니다."""
        # 지보 자산 연산 시 한국 표준시 기준으로 백테스팅 날짜 산출
        kst_today = datetime.now(ZoneInfo("Asia/Seoul"))
        for i in range(5):
            date = (kst_today - timedelta(days=i)).strftime("%Y%m%d")
            try:
                res = stock.get_index_portfolio_deposit_file(code, date)
                if res and len(res) > 50: 
                    return res
            except Exception as e:
                logger.warning(f"⚠️ 지수 [{code}] 조회 시도 실패 ({date}): {e}")
                time.sleep(1.0 * (i + 1))
                continue
        return []

    def _build_industry_lookup(self, tickers):
        """산업별 ETF 구성을 파싱할 때 거래소 IP 차단을 완벽 방어하기 위한 슬립 타임을 둡니다."""
        lookup = {}
        logger.info(f"📦 총 {len(tickers)}개 산업 ETF의 PDF 구성 종목 분석 중...")
        for etf_t in tickers:
            try:
                pdf = stock.get_etf_portfolio_deposit_file(etf_t)
                # 🌟 1번 반영: KRX 수집 서버 트래픽 0.4초 댐퍼 장치
                time.sleep(0.4)
                
                if pdf is not None and not pdf.empty:
                    w_col = '비중' if '비중' in pdf.columns else pdf.columns[0]
                    for stock_t, row in pdf.iterrows():
                        try:
                            weight = float(row[w_col])
                            if stock_t not in lookup or weight > lookup[stock_t][1]:
                                lookup[stock_t] = (etf_t, weight)
                        except (ValueError, TypeError):
                            continue
            except Exception as e:
                logger.warning(f"⚠️ ETF [{etf_t}] PDF 수집 건너뜀: {e}")
                time.sleep(1.0)
                continue
        return {k: v[0] for k, v in lookup.items()}

    def _get_val_from_headers(self, row, candidates):
        for col in candidates:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
                return str(row[col]).strip()
        return None

# ---------------------------------------------------------
# 4. 페이지 처리 (기초 정보 + 테마 판별 + 지표 연결)
# ---------------------------------------------------------
def process_page_kr(page, engine, client, config):
    pid, props = page["id"], page["properties"]
    ticker_prop = props.get("티커") or props.get("Ticker")
    if not ticker_prop: 
        return None
    
    ticker_val = ticker_prop.get("title", [{}])[0].get("plain_text", "").strip()
    match = re.search(r'(\d{6})', ticker_val)
    clean_t = match.group(1) if match else ticker_val

    item, is_etf = None, False
    if clean_t in engine.df_kr_desc.index:
        item = engine.df_kr_desc.loc[clean_t]
    elif clean_t in engine.kr_etf:
        item = engine.kr_etf[clean_t]
        is_etf = True

    if item is not None:
        stock_name = item['Name']
        m_raw = str(item.get('Market', '')).upper()
        market_label = "ETF(KR)" if is_etf else ("KOSDAQ" if "KOSDAQ" in m_raw else "KOSPI")
        
        sec_val = engine._get_val_from_headers(item, HEADERS['KR_SECTOR']) if not is_etf else item.get('Category')
        ind_val = engine._get_val_from_headers(item, HEADERS['KR_INDUSTRY']) if not is_etf else "ETF"

        us_tracking_tag = None
        target_m_t = None
        
        if is_etf:
            name_no_space = stock_name.replace(" ", "").upper()
            for keyword, rule in ETF_THEME_RULES.items():
                if keyword.upper() in name_no_space:
                    us_tracking_tag = rule["tag"]
                    target_m_t = rule["bm"]
                    break

        if not target_m_t:
            if clean_t in engine.k200_list: target_m_t = "069500"
            elif clean_t in engine.kd150_list: target_m_t = "229200"
            elif is_etf: target_m_t = "292190"
            elif market_label == "KOSPI": target_m_t = "226490"

        target_ind_t = engine.kr_industry_lookup.get(clean_t)

        def make_rich_text(val):
            return {"rich_text": [{"text": {"content": str(val)}}]} if val else {"rich_text": []}

        # 🌟 3번 반영: 깃허브 가상 서버용 KST 강제 지정 ISO 8601 타임스탬프 산출법 교정
        now_str = datetime.now(ZoneInfo("Asia/Seoul")).isoformat()

        update_props: dict[str, Any] = {
            "종목명": make_rich_text(stock_name),
            "Market": {"select": {"name": market_label}},
            "KR_섹터": make_rich_text(sec_val),
            "KR_산업": make_rich_text(ind_val),
            "업데이트 일자": {"date": {"start": now_str}}
        }
        
        if us_tracking_tag:
            update_props["우량주"] = {"multi_select": [{"name": us_tracking_tag}]}
        
        if target_m_t and target_m_t != clean_t:
            if m_id := config["ticker_to_id"].get(target_m_t):
                update_props["시장BM"] = {"relation": [{"id": m_id}]}
        if target_ind_t and target_ind_t != clean_t:
            if ind_id := config["ticker_to_id"].get(target_ind_t):
                update_props["산업BM"] = {"relation": [{"id": ind_id}]}

        return pid, update_props, clean_t, stock_name
    return None

# ---------------------------------------------------------
# 5. 메인 실행 함수
# ---------------------------------------------------------
def main():
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)
    config = get_dynamic_config(client)
    engine = StockAutomationEngineKR(config["kr_industry_tickers"])

    all_pages = []
    logger.info("📋 마스터 DB 스캔 및 대상 페이지 추출 시작...")
    for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100, retry_delay=0.3):
        all_pages.append(page)

    logger.info(f"📊 총 {len(all_pages)}개의 동기화 대상 목록 확보 완료")

    # 가상 연산 스텝 (데이터만 추출하므로 단일 루프로 쾌속 처리)
    update_payloads = []
    for page in all_pages:
        res = process_page_kr(page, engine, client, config)
        if res:
            update_payloads.append(res)

    # 🌟 2번 반영: 노션 API 쓰기 차단 우회용 동시 워커 제어 장치 가동
    if update_payloads:
        logger.info(f"📝 {len(update_payloads)}개 종목 노션 DB 반영 시작 (안전 동시 워커 제어)...")
        
        # 노션 쓰기 작업은 3개의 제한된 워커로 분산하여 초당 요청 상한선(TPS)을 영리하게 준수합니다.
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(safe_page_update, client, pid, props): (ticker, name)
                for pid, props, ticker, name in update_payloads
            }
            
            for idx, future in enumerate(as_completed(futures), 1):
                ticker, name = futures[future]
                try:
                    success = future.result()
                    if success:
                        logger.info(f"   ✅ [{idx}/{len(update_payloads)}] [Master Sync] {ticker} ({name}) 동기화 성공")
                    else:
                        logger.warning(f"   ❌ [{idx}/{len(update_payloads)}] [Master Sync] {ticker} ({name}) 노션 반영 실패")
                except Exception as exc:
                    logger.error(f"   ❌ [{ticker}] 트랜잭션 에러 발생: {exc}")
                
                # 쓰레드 반환 후 마진 시간(0.1초) 부여로 레이트 리밋 이중 방어
                time.sleep(0.1)

    logger.info("✨ 한국 주식 마스터 DB 통합 업데이트 프로세스 완료")

if __name__ == "__main__":
    main()