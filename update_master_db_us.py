import logging
import io
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_page_text,
    paginate_database,
    safe_page_update,
    kst_isoformat,
    extract_short_brand_name,
    is_kr_ticker,
    make_rich_text,
    get_http_session,
    match_keyword,
    find_best_bm,
    parse_keywords,
)

# ---------------------------------------------------------
# 1. 환경 변수 및 설정
# ---------------------------------------------------------
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
MASTER_DATABASE_ID = get_env_var("MASTER_DATABASE_ID")
BENCHMARK_DATABASE_ID = get_env_var("BENCHMARK_DATABASE_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# 2. 지표 DB 분석 (관계형 ID 매핑 및 키워드 인덱싱)
# ---------------------------------------------------------
def get_dynamic_config_us(client: Any) -> Dict[str, Any]:
    """지표지수 DB의 티커 및 매칭키워드 수집"""
    config = {"ticker_to_id": {}, "benchmarks": []}
    try:
        for page in paginate_database(client, BENCHMARK_DATABASE_ID, page_size=100, retry_delay=0.2):
            props = page.get("properties", {})
            ticker = get_page_text(props, ["티커", "Ticker", "이름", "Name"]).upper()
            if not ticker:
                continue
            summary = get_page_text(props, ["요약명"]).strip()
            cat = props.get("구분", {}).get("select", {}).get("name", "") if props.get("구분", {}).get("select") else ""
            country = props.get("국가", {}).get("select", {}).get("name", "") if props.get("국가", {}).get("select") else ""
            kw_raw = get_page_text(props, ["매칭키워드", "키워드"])
            keywords = parse_keywords(kw_raw, fallback_summary=summary)
                
            config["ticker_to_id"][ticker] = page["id"]
            config["benchmarks"].append({
                "ticker": ticker,
                "summary": summary,
                "category": cat,
                "country": country,
                "keywords": keywords,
                "id": page["id"]
            })
        logger.info(f"✅ 총 {len(config['benchmarks'])}개의 지수 및 키워드 데이터 로드 완료")
    except Exception as e:
        logger.error(f"❌ 지표 로드 실패: {e}")
    return config

# ---------------------------------------------------------
# 3. 데이터 엔진 (인메모리 인덱스 & 실시간 캐시)
# ---------------------------------------------------------
class StockAutomationEngineUS:
    def __init__(self):
        logger.info("📡 미국 주식 마스터 엔진 가동 (S&P500 인메모리 패스트트랙)...")
        self.session = get_http_session()

        try:
            self.df_sp500 = fdr.StockListing('S&P500')
            self.sp500_dict = self.df_sp500.set_index('Symbol').to_dict('index')
        except Exception as e:
            logger.warning(f"⚠️ S&P500 로드 실패: {e}")
            self.df_sp500 = pd.DataFrame(columns=['Symbol', 'Name', 'Sector', 'Industry'])
            self.sp500_dict = {}

        self.nasdaq_100 = self._get_nas100()

    def _get_nas100(self):
        """StringIO와 pandas를 활용한 나스닥 100 스크래핑"""
        urls = [
            'https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies',
            'https://en.wikipedia.org/wiki/Nasdaq-100'
        ]
        for url in urls:
            try:
                res = self.session.get(url, timeout=10)
                if res.status_code == 200:
                    dfs = pd.read_html(io.StringIO(res.text))
                    for df in dfs:
                        for col in ['Ticker', 'Symbol']:
                            if col in df.columns and len(df) >= 50:
                                return set(df[col].astype(str).str.strip().str.upper().tolist())
            except Exception as e:
                logger.warning(f"⚠️ 나스닥 100 수집 실패 ({url}): {e}")
            return set()

    def get_market_label(self, clean_t: str) -> str:
        """상세 마켓 판별 로직"""
        if clean_t in self.nasdaq_100:
            return "NASDAQ"
        if clean_t in self.sp500_dict:
            return "NYSE"
        return "기타"

# ---------------------------------------------------------
# 4. 페이지 처리 (기본 정보 자동 기입 + 지표 동적 매핑)
# ---------------------------------------------------------
def process_page_us(page: Dict[str, Any], engine: StockAutomationEngineUS, client: Any, config: Dict[str, Any]):
    pid, props = page["id"], page.get("properties", {})
    raw_t = get_page_text(props, ["티커", "Ticker"]).upper()
    if not raw_t or is_kr_ticker(raw_t):
        return

    name = ""
    sec = ""
    ind = ""
    market_label = engine.get_market_label(raw_t)
    target_m_t, target_ind_t = None, None

    # 1. S&P 500 인메모리 패스트트랙 (느린 YFinance 웹 호출 생략 -> 1ms 처리)
    if raw_t in engine.sp500_dict:
        sp_item = engine.sp500_dict[raw_t]
        name = extract_short_brand_name(sp_item.get('Name', raw_t))
        sec = sp_item.get('Sector', '')
        ind = sp_item.get('Industry', '')
        market_label = "NASDAQ" if raw_t in engine.nasdaq_100 else "NYSE"
    else:
        # 2. S&P 500 외 종목(ADR, 중소형주, ETF)만 YFinance 조회
        try:
            stock_yf = yf.Ticker(raw_t, session=engine.session)
            info = stock_yf.info
            raw_name = info.get("longName") or info.get("shortName") or raw_t
            name = extract_short_brand_name(raw_name)
            sec = info.get("sector", "")
            ind = info.get("industry", "")
            if not market_label or market_label == "기타":
                qtype = info.get("quoteType", "")
                if qtype == "ETF":
                    market_label = "ETF(US)"
                else:
                    exch = (info.get("exchange") or "").upper()
                    market_label = "NASDAQ" if "NAS" in exch else ("NYSE" if "NY" in exch else "기타")
        except Exception as exc:
            logger.warning(f"⚠️ [{raw_t}] YFinance 조회 실패: {exc}")
            name = raw_t

    # 3. 시장BM 및 G산업BM 노션 지표 DB 매칭키워드 기반 순수 동적 매핑
    if market_label != "기타":
        if raw_t in engine.nasdaq_100:
            target_m_t = "QQQ"
        elif raw_t in engine.sp500_dict:
            target_m_t = "SPY"
        elif market_label == "NASDAQ":
            target_m_t = "ONEQ"
        elif market_label == "ETF(US)":
            target_m_t = "SPY"
        else:
            target_m_t = "VTI"

        text_corpus = f"{raw_t} {name} {sec} {ind}".upper()
        us_industry_bms = [bm for bm in config["benchmarks"] if bm["category"] == "산업" and bm["country"] == "US"]
        target_ind_t = find_best_bm(text_corpus, us_industry_bms)

    update_props: Dict[str, Any] = {
        "종목명": make_rich_text(name),
        "Market": {"select": {"name": market_label}},
        "US_섹터": make_rich_text(sec),
        "US_업종": make_rich_text(ind),
        "업데이트 일자": {"date": {"start": kst_isoformat()}},
    }

    # 4. 우량주 태깅 (S&P 500, NASDAQ 100)
    blue_chip_tags = []
    if raw_t in engine.sp500_dict:
        blue_chip_tags.append("S&P 500")
    if raw_t in engine.nasdaq_100:
        blue_chip_tags.append("NASDAQ 100")

    if blue_chip_tags:
        update_props["우량주"] = {"multi_select": [{"name": tag} for tag in blue_chip_tags]}

    # 5. 벤치마크 관계형 속성 반영 (미국 주식은 K산업BM을 항상 빈 값으로 초기화)
    update_props["K산업BM"] = {"relation": []}

    if market_label == "기타":
        update_props["시장BM"] = {"relation": []}
        update_props["G산업BM"] = {"relation": []}
    else:
        if target_m_t and target_m_t != raw_t and (m_id := config["ticker_to_id"].get(target_m_t)):
            update_props["시장BM"] = {"relation": [{"id": m_id}]}
        else:
            update_props["시장BM"] = {"relation": []}

        if target_ind_t and target_ind_t != raw_t and (ind_id := config["ticker_to_id"].get(target_ind_t)):
            update_props["G산업BM"] = {"relation": [{"id": ind_id}]}
        else:
            update_props["G산업BM"] = {"relation": []}

    if safe_page_update(client, pid, update_props):
        logger.info(f"   ✅ [US] {raw_t} ({name}) 업데이트 완료")

# ---------------------------------------------------------
# 5. 메인 함수 (페이지네이션 적용)
# ---------------------------------------------------------
def main():
    client = build_notion_client(NOTION_TOKEN, use_httpx=True, timeout=60.0)

    config = get_dynamic_config_us(client)
    engine = StockAutomationEngineUS()

    all_pages = [page for page in paginate_database(client, MASTER_DATABASE_ID, page_size=100, retry_delay=0.1)]
    logger.info("📡 노션 DB 수집 및 페이지네이션 처리 중...")

    if all_pages:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_page_us, page, engine, client, config) for page in all_pages]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"❌ 페이지 처리 중 에러: {exc}")

    logger.info("✨ 모든 US 종목 업데이트 프로세스가 완료되었습니다.")

if __name__ == "__main__":
    main()
