# -*- coding: utf-8 -*-
"""
generate_portfolio_report.py
=============================
노션(Notion)의 4대 포트폴리오 데이터베이스
(투자계좌현황, 종목별 보유현황, 계좌별 보유종목, 투자주 DB)와 숨김/계산 퀀트 열
(52주 위치, 안전마진, 투자가이드, 계좌별 예수금, 배당수익률, 평가비중 등)을 전방위로 수집 및 결합하고,
FinanceDataReader 기반 실시간 글로벌 매크로 지표(환율, 금리, 장단기금리차, 유가, 금) 및
Google Gemini API (Google Search Grounding 팩트체크)를 연동하여
전문적인 「K-올라운드 마스터」 자산배분 진단 리포트를 생성한 후
노션 포트폴리오 분석 리포트 DB에 자동 적재 및 로컬 백업을 수행하는 오케스트레이터입니다.
"""

# ==============================================================================
# 0. 라이브러리 임포트 및 시스템 설정
# ==============================================================================
import os
import sys
import time
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple, Set

from dotenv import load_dotenv

# .env 환경변수 로드
load_dotenv()

# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from notion_utils import (
    build_notion_client,
    get_env_var,
    get_all_portfolio_db_ids,
    get_kst_now,
    get_kst_str,
    paginate_database,
    get_prop_value,
    kst_isoformat,
    markdown_to_notion_blocks,
    safe_create_page,
    safe_float,
    calc_margin_of_safety,
    calc_52w_position,
)
from config_portfolio import (
    K_ALL_ROUND_MASTER_CONFIG,
    TARGET_ALLOCATION,
    ASSET_ORDER,
    REBALANCING_DRIFT_THRESHOLD_PCT,
    ACCOUNT_POLICIES,
    TAX_DEDUCTION_CONFIG,
    FX_MACRO_RULES,
    classify_asset,
)
from macro_service import MacroService
from ai_service import AIService

logger = logging.getLogger("PortfolioReport")


# ==============================================================================
# 1. 환경 변수 및 다중 DB ID 설정
# ==============================================================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")

# 7대 노션 데이터베이스 ID 일괄 로드 (notion_utils 헬퍼)
PORTFOLIO_DB_IDS = get_all_portfolio_db_ids()
INVESTMENT_DB_ID = PORTFOLIO_DB_IDS["investment_db_id"]
ACCOUNT_STATUS_DB_ID = PORTFOLIO_DB_IDS["account_status_db_id"]
STOCK_HOLDINGS_DB_ID = PORTFOLIO_DB_IDS["stock_holdings_db_id"]
ACCOUNT_HOLDINGS_DB_ID = PORTFOLIO_DB_IDS["account_holdings_db_id"]
TRADE_LOG_DB_ID = PORTFOLIO_DB_IDS["trade_log_db_id"]
CASH_FLOW_DB_ID = PORTFOLIO_DB_IDS["cash_flow_db_id"]
NOTION_REPORT_DB_ID = PORTFOLIO_DB_IDS["notion_report_db_id"]

REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")


# ==============================================================================
# 2. 다중 노션 DB 통합 데이터 수집부 (숨김/계산 퀀트 열 전수 수집)
# ==============================================================================
def collect_account_status(client: Any, db_id: str) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    [투자계좌현황 DB]를 조회하여 계좌별(ISA, 연금, IRP 등) 총자산, 현금 예수금, 투자원금,
    수익률, 현금비중, 확정손익을 수집하고 페이지 ID -> 계좌명 매핑 딕셔너리를 반환합니다.
    """
    if not db_id:
        print("ℹ️ [Notion] 1. ACCOUNT_STATUS_DB_ID가 설정되지 않아 계좌현황 조회를 건너뜁니다.")
        return {"accounts": [], "total_asset_val": 0.0, "total_cash_val": 0.0, "total_invest_val": 0.0}, {}

    print("🏦 [Notion] 1. 투자계좌현황 DB 스캔 시작...")
    accounts: List[Dict[str, Any]] = []
    account_id_to_name: Dict[str, str] = {}
    total_asset_val = 0.0
    total_cash_val = 0.0
    total_invest_val = 0.0
    total_realized_profit = 0.0
    total_dividend = 0.0

    try:
        for page in paginate_database(client, db_id, page_size=100, retry_delay=0.2):
            props = page.get("properties", {})
            name = str(get_prop_value(props, ["이름", "Name", "계좌명"]) or "").strip()
            page_id = page.get("id", "")
            if page_id and name:
                account_id_to_name[page_id] = name
                account_id_to_name[page_id.replace("-", "")] = name

            checked = get_prop_value(props, ["체크박스"])
            asset_eval = safe_float(get_prop_value(props, ["자산평가", "평가자산", "총자산"])) or 0.0
            cash = safe_float(get_prop_value(props, ["현금", "예수금"])) or 0.0
            invest_eval = safe_float(get_prop_value(props, ["평가총액", "투자중 금액", "투자금액"])) or 0.0
            total_deposit = safe_float(get_prop_value(props, ["총 입금"])) or 0.0
            total_withdraw = safe_float(get_prop_value(props, ["출금"])) or 0.0
            asset_change = safe_float(get_prop_value(props, ["자산증감"])) or 0.0
            realized_profit = safe_float(get_prop_value(props, ["수익확정"])) or 0.0
            dividend = safe_float(get_prop_value(props, ["배당"])) or 0.0
            fee = safe_float(get_prop_value(props, ["제수수료"])) or 0.0
            
            asset_yield = get_prop_value(props, ["자산수익률"])
            invest_yield = get_prop_value(props, ["투자수익률 "])
            cash_asset_ratio = get_prop_value(props, ["현금/자산"])
            cash_principal_ratio = get_prop_value(props, ["현금/원금"])

            if name:
                accounts.append({
                    "name": name,
                    "page_id": page_id,
                    "checked": checked,
                    "asset_eval": asset_eval,
                    "cash": cash,
                    "invest_eval": invest_eval,
                    "total_deposit": total_deposit,
                    "total_withdraw": total_withdraw,
                    "asset_change": asset_change,
                    "realized_profit": realized_profit,
                    "dividend": dividend,
                    "fee": fee,
                    "asset_yield": asset_yield,
                    "invest_yield": invest_yield,
                    "cash_asset_ratio": cash_asset_ratio,
                    "cash_principal_ratio": cash_principal_ratio,
                })
                total_asset_val += asset_eval
                total_cash_val += cash
                total_invest_val += invest_eval
                total_realized_profit += realized_profit
                total_dividend += dividend

        print(f"   ✅ 계좌 {len(accounts)}개 수집 완료 (자산평가 합계: {total_asset_val:,.0f}원, 현금 예수금: {total_cash_val:,.0f}원)")
    except Exception as e:
        print(f"   ⚠️ 투자계좌현황 DB 조회 실패: {e}")

    return {
        "accounts": accounts,
        "total_asset_val": total_asset_val,
        "total_cash_val": total_cash_val,
        "total_invest_val": total_invest_val,
        "total_realized_profit": total_realized_profit,
        "total_dividend": total_dividend,
    }, account_id_to_name


def collect_cash_flow_deposits(
    client: Any,
    db_id: str,
    account_id_to_name: Dict[str, str],
    target_year: str = ""
) -> Dict[str, float]:
    """
    [입출금현황 DB]를 조회하여 당해연도(target_year) 기준 구분열이 '입금'인 내역의
    계좌별 합산 입금액(연도별 입금액)을 정밀 집계합니다.
    """
    if not db_id:
        return {}

    if not target_year:
        target_year = get_kst_str("%Y")

    print(f"💳 [Notion] {target_year}년도 입출금 현황 DB(입금액) 스캔 시작...")
    yearly_deposits: Dict[str, float] = {}

    try:
        for page in paginate_database(client, db_id, page_size=100, retry_delay=0.2):
            props = page.get("properties", {})
            date_val = str(get_prop_value(props, ["날짜", "Date", "일자"]) or "").strip()
            type_val = str(get_prop_value(props, ["구분", "Type", "분류", "입출금구분"]) or "").strip()
            amt_val = safe_float(get_prop_value(props, ["금액", "입금액", "Amount"])) or 0.0
            acc_rels = props.get("투자계좌현황", {}).get("relation", [])

            if type_val == "입금" and date_val.startswith(target_year) and amt_val > 0:
                for rel in acc_rels:
                    r_id = rel.get("id", "")
                    a_name = account_id_to_name.get(r_id) or account_id_to_name.get(r_id.replace("-", "")) or "미지정"
                    yearly_deposits[a_name] = yearly_deposits.get(a_name, 0.0) + amt_val

        print(f"   ✅ {target_year}년 당해연도 계좌별 입금액 집계 완료: {yearly_deposits}")
    except Exception as e:
        print(f"   ⚠️ 입출금현황 DB 조회 실패: {e}")

    return yearly_deposits


def collect_stock_holdings_meta(client: Any, db_id: str) -> Dict[str, Dict[str, Any]]:
    """
    [종목별 보유현황 DB]를 조회하여 종목별 포트폴리오 테마(AI Infra, Core 등), 세부선택, 국가,
    총수익, 전일대비 등 숨김/계산 퀀트 메타 정보를 매핑합니다.
    """
    if not db_id:
        print("ℹ️ [Notion] 2. STOCK_HOLDINGS_DB_ID가 설정되지 않아 종목별 메타 조회를 건너뜁니다.")
        return {}

    print("📜 [Notion] 2. 종목별 보유현황 DB 스캔 시작...")
    stock_meta: Dict[str, Dict[str, Any]] = {}
    try:
        for page in paginate_database(client, db_id, page_size=100, retry_delay=0.2):
            props = page.get("properties", {})
            name = str(get_prop_value(props, ["이름", "Name"]) or "").strip()
            port = str(get_prop_value(props, ["포트폴리오"]) or "").strip()
            sel = str(get_prop_value(props, ["선택"]) or "").strip()
            country = str(get_prop_value(props, ["국가"]) or "").strip()
            qty = safe_float(get_prop_value(props, ["보유량"])) or 0.0
            eval_amt = safe_float(get_prop_value(props, ["평가금액"])) or 0.0
            profit = safe_float(get_prop_value(props, ["총수익"])) or 0.0
            day_change = get_prop_value(props, ["전일대비"])

            if name:
                stock_meta[name.upper()] = {
                    "raw_name": name,
                    "portfolio_theme": port,
                    "selection": sel,
                    "country": country,
                    "qty": qty,
                    "eval_amt": eval_amt,
                    "profit": profit,
                    "day_change": day_change,
                }
        print(f"   ✅ 종목 메타 {len(stock_meta)}개 수집 완료")
    except Exception as e:
        print(f"   ⚠️ 종목별 보유현황 DB 조회 실패: {e}")

    return stock_meta


def collect_account_holdings_detail(
    client: Any,
    db_id: str,
    account_id_to_name: Dict[str, str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    [계좌별 보유종목 DB]를 조회하여 계좌별 보유 종목, 실시간 투자가이드,
    투자계좌현황 relation 기반 정확한 소속 계좌명을 매핑합니다.
    """
    if not db_id:
        print("ℹ️ [Notion] 3. ACCOUNT_HOLDINGS_DB_ID가 설정되지 않아 계좌별 종목 상세 조회를 건너뜁니다.")
        return {}

    print("💎 [Notion] 3. 계좌별 보유종목 DB 스캔 시작...")
    account_holdings_map: Dict[str, List[Dict[str, Any]]] = {}
    try:
        for page in paginate_database(client, db_id, page_size=100, retry_delay=0.2):
            props = page.get("properties", {})
            name = str(get_prop_value(props, ["이름", "Name"]) or "").strip()
            guide = str(get_prop_value(props, ["투자가이드"]) or "").strip()
            port = str(get_prop_value(props, ["포트폴리오"]) or "").strip()
            qty = safe_float(get_prop_value(props, ["보유량"])) or 0.0
            eval_amt = safe_float(get_prop_value(props, ["평가금액"])) or 0.0
            cum_ret = get_prop_value(props, ["누적수익률"])
            price = safe_float(get_prop_value(props, ["현재가"])) or 0.0
            buy_price = safe_float(get_prop_value(props, ["매수단가New"])) or 0.0
            take_profit_price = safe_float(get_prop_value(props, ["익절가"])) or 0.0
            weight_in_acc = get_prop_value(props, ["평가비중"])
            pos_52w = get_prop_value(props, ["52주위치"])
            div_amt = safe_float(get_prop_value(props, ["배당"])) or 0.0

            # 투자계좌현황 Relation 기반 정확한 소속 계좌명 판별
            acc_rels = props.get("투자계좌현황", {}).get("relation", [])
            resolved_acc_names = [account_id_to_name.get(r["id"].replace("-", "")) or account_id_to_name.get(r["id"]) for r in acc_rels]
            target_acc_name = resolved_acc_names[0] if (resolved_acc_names and resolved_acc_names[0]) else port

            if name:
                base_name = name.split("#")[0].strip()
                if base_name not in account_holdings_map:
                    account_holdings_map[base_name] = []
                account_holdings_map[base_name].append({
                    "full_name": name,
                    "account_name": target_acc_name,
                    "guide": guide,
                    "portfolio": port,
                    "qty": qty,
                    "eval_amt": eval_amt,
                    "cum_ret": cum_ret,
                    "price": price,
                    "buy_price": buy_price,
                    "take_profit_price": take_profit_price,
                    "weight_in_acc": weight_in_acc,
                    "pos_52w": pos_52w,
                    "dividend": div_amt,
                })
        print(f"   ✅ 계좌별 종목 상세 {len(account_holdings_map)}개 종목군 매핑 완료")
    except Exception as e:
        print(f"   ⚠️ 계좌별 보유종목 DB 조회 실패: {e}")

    return account_holdings_map


def fetch_latest_previous_report(client: Any, db_id: str) -> Optional[Dict[str, Any]]:
    """
    [포트폴리오 분석 DB]를 조회하여 가장 최근에 생성된 직전(전주) 리포트 스냅샷을 수집합니다.
    """
    if not db_id:
        return None

    try:
        query_res = client.databases.query(
            database_id=db_id,
            sorts=[{"property": "날짜", "direction": "descending"}],
            page_size=5
        )
        results = query_res.get("results", [])
        if not results:
            return None

        # 가장 최근 직전 리포트 1건 선택
        prev_page = results[0]
        props = prev_page.get("properties", {})
        title = str(get_prop_value(props, ["이름", "Title", "Name"]) or "")
        date_raw = get_prop_value(props, ["날짜", "Date"]) or ""
        total_eval = safe_float(get_prop_value(props, ["총 평가자산", "총자산", "평가액"])) or 0.0
        cash_pct = safe_float(get_prop_value(props, ["현금 비중", "현금비중"])) or 0.0
        fitness = str(get_prop_value(props, ["올웨더 적합도", "적합도", "올라운드 적합도"]) or "")
        actions = get_prop_value(props, ["핵심 조치", "핵심조치"]) or []
        if isinstance(actions, str):
            actions = [actions]
        summary = str(get_prop_value(props, ["요약", "Summary"]) or "")

        date_str = str(date_raw)[:10] if date_raw else "직전"

        print(f"⏱️ [Notion] 직전(전주) 리포트 스냅샷 확인: {date_str} ('{title}', 자산: {total_eval:,.0f}원, 현금: {cash_pct:.1f}%)")

        return {
            "page_id": prev_page.get("id"),
            "title": title,
            "date": date_str,
            "total_eval_krw": total_eval,
            "cash_pct": cash_pct,
            "fitness": fitness,
            "actions": actions,
            "summary": summary
        }
    except Exception as e:
        print(f"   ⚠️ 직전 리포트 DB 조회 실패: {e}")
        return None


def collect_all_portfolio_data(client: Any) -> Dict[str, Any]:
    """
    4대 데이터베이스, 입출금현황 DB 및 직전 리포트 DB를 통합 조회하여
    퀀트 지표(안전마진, 52주 위치, 배당수익률, 투자가이드, 연도별 입금 실적)를 결합합니다.
    """
    # 0. 직전(전주) 리포트 스냅샷 수집
    prev_report = fetch_latest_previous_report(client, NOTION_REPORT_DB_ID)

    # 1. 계좌 현황 수집 및 계좌 ID 매퍼 생성
    account_status, account_id_to_name = collect_account_status(client, ACCOUNT_STATUS_DB_ID)
    
    # 2. 멀티스레드 병렬 수집 (입출금현황, 종목별메타, 계좌별상세)
    current_year = get_kst_str("%Y")
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        f_dep = executor.submit(collect_cash_flow_deposits, client, CASH_FLOW_DB_ID, account_id_to_name, current_year)
        f_meta = executor.submit(collect_stock_holdings_meta, client, STOCK_HOLDINGS_DB_ID)
        f_acct = executor.submit(collect_account_holdings_detail, client, ACCOUNT_HOLDINGS_DB_ID, account_id_to_name)
        
        yearly_deposits = f_dep.result()
        stock_meta = f_meta.result()
        account_holdings = f_acct.result()

    # 3. 투자주 DB(마스터) 전수 스캔 (숨김 퀀트 지표 포함)
    print("📋 [Notion] 4. 투자주 DB(마스터) 스캔 시작...")
    raw_holdings: List[Dict[str, Any]] = []
    
    for page in paginate_database(client, INVESTMENT_DB_ID, page_size=100, retry_delay=0.3):
        props = page.get("properties", {})
        ticker = str(get_prop_value(props, ["티커", "Ticker", "이름"]) or "").strip().upper()
        name = str(get_prop_value(props, ["종목명", "ETF이름", "이름", "Name"]) or ticker).strip()
        eval_asset = safe_float(get_prop_value(props, ["평가자산", "평가금액", "평가액"])) or 0.0
        current_price = safe_float(get_prop_value(props, ["현재가", "종가"])) or 0.0
        market = str(get_prop_value(props, ["Market", "마켓", "시장"]) or "").strip()
        invest_tags = get_prop_value(props, ["투자여부", "투자상태", "상태"]) or []
        if isinstance(invest_tags, str):
            invest_tags = [invest_tags]

        # 숨김/계산 퀀트 열 추출 (노션 수식 열이 없어도 Python 자체 산출로 100% 보장)
        high_52w = safe_float(get_prop_value(props, ["52주 최고가"])) or 0.0
        low_52w = safe_float(get_prop_value(props, ["52주 최저가"])) or 0.0
        pos_52w = get_prop_value(props, ["52주 위치", "52주위치"])
        if pos_52w is None:
            pos_52w = calc_52w_position(current_price, high_52w, low_52w)

        target_price = safe_float(get_prop_value(props, ["목표주가"])) or 0.0
        margin_of_safety = str(get_prop_value(props, ["안전마진"]) or "").strip()
        if not margin_of_safety and target_price > 0 and current_price > 0:
            margin_of_safety = calc_margin_of_safety(current_price, target_price)

        target_range = str(get_prop_value(props, ["목표가 범위"]) or "").strip()
        div_yield = get_prop_value(props, ["배당수익률"])
        per = safe_float(get_prop_value(props, ["PER", "추정PER"]))
        pbr = safe_float(get_prop_value(props, ["PBR"]))
        prev_close = safe_float(get_prop_value(props, ["전일 종가", "전일종가"])) or 0.0
        day_change = get_prop_value(props, ["전일대비"])
        if day_change is None and prev_close > 0 and current_price > 0:
            day_change = (current_price - prev_close) / prev_close

        # 노션 '투자주 DB' 3차원 속성 (자산군, 상품유형, 국가) 추출
        notion_asset_class = str(get_prop_value(props, ["자산군"]) or "").strip()
        notion_prod_type = str(get_prop_value(props, ["상품유형"]) or "").strip()
        notion_country = str(get_prop_value(props, ["국가"]) or "").strip()

        # 종목 메타 결합
        meta_info = stock_meta.get(name.upper()) or stock_meta.get(ticker.upper()) or {}
        country = notion_country or meta_info.get("country", "")
        portfolio_theme = meta_info.get("portfolio_theme", "")
        selection = meta_info.get("selection", "")

        # 계좌별 보유 세부내역 결합
        acc_details = account_holdings.get(name) or account_holdings.get(ticker) or []
        guides = list({d["guide"] for d in acc_details if d.get("guide")})

        # 실제 개인 보유 평가액 우선 적용
        actual_user_eval = 0.0
        if meta_info and meta_info.get("eval_amt", 0) > 0:
            actual_user_eval = meta_info["eval_amt"]
        elif acc_details:
            actual_user_eval = sum(d.get("eval_amt", 0) for d in acc_details if d.get("eval_amt", 0) > 0)

        is_invested = (
            actual_user_eval > 0
            or any("투자" in str(tag) for tag in invest_tags)
            or bool(meta_info.get("eval_amt", 0) > 0)
            or bool(acc_details)
        )
        is_sold = any("매도" in str(tag) for tag in invest_tags) and actual_user_eval <= 0
        if is_sold:
            continue

        if is_invested and (actual_user_eval > 0 or ticker):
            raw_holdings.append({
                "page_id": page.get("id"),
                "ticker": ticker,
                "name": name,
                "eval_asset": actual_user_eval,
                "current_price": current_price,
                "market": market,
                "country": country,
                "portfolio_theme": portfolio_theme,
                "selection": selection,
                "asset_class": notion_asset_class,
                "prod_type": notion_prod_type,
                "invest_tags": invest_tags,
                "pos_52w": pos_52w,
                "high_52w": high_52w,
                "low_52w": low_52w,
                "margin_of_safety": margin_of_safety,
                "target_price": target_price,
                "target_range": target_range,
                "div_yield": div_yield,
                "per": per,
                "pbr": pbr,
                "day_change": day_change,
                "guides": guides,
                "account_details": acc_details,
            })

    # 5. 종목별 보유현황 DB에 있으나 투자주 DB에 누락된 실보유 종목 보완
    added_names = {h["name"].upper() for h in raw_holdings}
    for m_key, m_val in stock_meta.items():
        if m_val.get("eval_amt", 0) > 0 and m_val.get("raw_name", "").upper() not in added_names:
            r_name = m_val.get("raw_name", "")
            raw_holdings.append({
                "page_id": "",
                "ticker": "",
                "name": r_name,
                "eval_asset": m_val["eval_amt"],
                "current_price": 0.0,
                "market": "",
                "country": m_val.get("country", ""),
                "portfolio_theme": m_val.get("portfolio_theme", ""),
                "selection": m_val.get("selection", ""),
                "invest_tags": ["투자"],
                "pos_52w": None,
                "high_52w": 0.0,
                "low_52w": 0.0,
                "margin_of_safety": "",
                "target_price": 0.0,
                "target_range": "",
                "div_yield": None,
                "per": None,
                "pbr": None,
                "day_change": m_val.get("day_change"),
                "guides": [],
                "account_details": account_holdings.get(r_name, []),
            })
            added_names.add(r_name.upper())

    print(f"📊 [Notion] 포트폴리오 집계 대상 {len(raw_holdings)}개 종목 수집 완료")

    return {
        "prev_report": prev_report,
        "account_status": account_status,
        "stock_meta": stock_meta,
        "account_holdings": account_holdings,
        "holdings": raw_holdings,
        "yearly_deposits": yearly_deposits,
        "current_year": current_year,
    }


def analyze_integrated_portfolio(
    portfolio_dataset: Dict[str, Any],
    macro_snapshot: Dict[str, Any]
) -> Dict[str, Any]:
    """
    통합 수집된 포트폴리오 데이터를 기반으로 7대 자산군 비중, 리밸런싱 필요액, 
    연간 세액공제 실시간 트래커, 6대 계좌별 독립 진단 마크다운을 산출합니다.
    """
    account_status = portfolio_dataset["account_status"]
    stock_meta = portfolio_dataset["stock_meta"]
    holdings = portfolio_dataset["holdings"]
    prev_report = portfolio_dataset.get("prev_report")
    yearly_deposits = portfolio_dataset.get("yearly_deposits", {})
    current_year = portfolio_dataset.get("current_year", get_kst_str("%Y"))

    # 1. 7대 자산군 컨테이너 초기화
    asset_groups: Dict[str, Dict[str, Any]] = {}
    for code, cfg in K_ALL_ROUND_MASTER_CONFIG.items():
        asset_groups[code] = {
            "name": cfg["name"],
            "target_pct": cfg["target_pct"],
            "role": cfg["role"],
            "currency_exposure": cfg["currency_exposure"],
            "eval_krw": 0.0,
            "actual_pct": 0.0,
            "drift_pct": 0.0,
            "rebalance_krw": 0.0,
            "holdings": [],
        }

    # 2. 총 평가자산 및 현금 합산
    total_eval_krw = account_status.get("total_asset_val", 0.0)
    cash_total_krw = account_status.get("total_cash_val", 0.0)
    stock_total_krw = total_eval_krw - cash_total_krw if total_eval_krw >= cash_total_krw else total_eval_krw

    # 만약 계좌현황 DB 총자산이 0이면 실보유 종목 합산으로 대체
    if total_eval_krw <= 0:
        stock_total_krw = sum(h.get("eval_asset", 0.0) for h in holdings)
        total_eval_krw = stock_total_krw + cash_total_krw

    cash_pct = (cash_total_krw / total_eval_krw * 100.0) if total_eval_krw > 0 else 0.0

    # 3. 현금 예수금을 7대 자산군에 자동 배분
    krw_cash = cash_total_krw
    usd_cash = 0.0
    for h in holdings:
        if h.get("name") in ["달러예수금", "USD 현금", "달러 현금"]:
            usd_cash += h.get("eval_asset", 0.0)
            krw_cash = max(0.0, krw_cash - h.get("eval_asset", 0.0))

    if krw_cash > 0:
        asset_groups["KR_BOND_SHORT"]["eval_krw"] += krw_cash
    if usd_cash > 0:
        asset_groups["COMMODITY_CASH"]["eval_krw"] += usd_cash

    # 4. 종목별 자산군 분류 및 테마별 비중 집계
    theme_distribution: Dict[str, float] = {}

    for h in holdings:
        code, name = classify_asset(
            name=h["name"],
            ticker=h.get("ticker", ""),
            market=h.get("market", ""),
            country=h.get("country", ""),
            custom_portfolio=h.get("portfolio_theme", ""),
            custom_selection=h.get("selection", ""),
            custom_asset_class=h.get("asset_class", ""),
            custom_product_type=h.get("prod_type", ""),
        )

        h_info = {
            "name": h["name"],
            "ticker": h.get("ticker", ""),
            "eval_asset": h["eval_asset"],
            "current_price": h["current_price"],
            "portfolio_theme": h.get("portfolio_theme", ""),
            "selection": h.get("selection", ""),
            "country": h.get("country", ""),
            "theme": h.get("portfolio_theme") or "기타",
            "pos_52w": h.get("pos_52w"),
            "high_52w": h.get("high_52w"),
            "low_52w": h.get("low_52w"),
            "margin_of_safety": h.get("margin_of_safety"),
            "target_price": h.get("target_price"),
            "div_yield": h.get("div_yield"),
            "guides": h.get("guides", []),
            "account_details": h.get("account_details", []),
        }
        asset_groups[code]["eval_krw"] += h["eval_asset"]
        asset_groups[code]["holdings"].append(h_info)

        # 테마별 비중 집계
        t_key = h_info["theme"]
        theme_distribution[t_key] = theme_distribution.get(t_key, 0.0) + h["eval_asset"]

    # 5. 7대 자산군 비중 및 괴리율(Drift, 임계치 ±3.0%p) 계산
    for code in ASSET_ORDER:
        grp = asset_groups[code]
        if total_eval_krw > 0:
            grp["actual_pct"] = (grp["eval_krw"] / total_eval_krw) * 100.0
            grp["drift_pct"] = grp["actual_pct"] - grp["target_pct"]
            target_amount = total_eval_krw * (grp["target_pct"] / 100.0)
            grp["rebalance_krw"] = target_amount - grp["eval_krw"]
        else:
            grp["actual_pct"] = 0.0
            grp["drift_pct"] = -grp["target_pct"]
            grp["rebalance_krw"] = 0.0

    # 6. 마크다운 요약표 생성
    summary_lines = [
        "| 자산군 | 역할 & 통화 | 목표비중(%) | 현재평가액(원) | 현재비중(%) | 괴리율(%p) | 상태 (임계치 ±3.0%p) | 리밸런싱 필요액(원) |",
        "|:---|:---|:---:|:---:|:---:|:---:|:---:|:---:|",
    ]
    for code in ASSET_ORDER:
        grp = asset_groups[code]
        drift = grp["drift_pct"]
        if abs(drift) <= REBALANCING_DRIFT_THRESHOLD_PCT:
            status = "🟢 적정"
        elif drift > REBALANCING_DRIFT_THRESHOLD_PCT:
            status = f"🔴 과다 (+{drift:.1f}%p)"
        else:
            status = f"🔵 부족 ({drift:.1f}%p)"
            
        rebal_str = f"{grp['rebalance_krw']:+,.0f} 원" if total_eval_krw > 0 else "0 원"
        summary_lines.append(
            f"| **{grp['name']}** | {grp['currency_exposure']} | {grp['target_pct']:.1f}% | {grp['eval_krw']:,.0f} 원 | "
            f"{grp['actual_pct']:.1f}% | {drift:+.1f}%p | {status} | {rebal_str} |"
        )
    asset_summary_table = "\n".join(summary_lines)

    # 7. 6대 계좌별 매핑 및 데이터 분류
    account_map_data: Dict[str, Dict[str, Any]] = {
        "삼성연금": {"info": None, "holdings": [], "policy": ACCOUNT_POLICIES["삼성연금"]},
        "미래연금": {"info": None, "holdings": [], "policy": ACCOUNT_POLICIES["미래연금"]},
        "삼성IRP": {"info": None, "holdings": [], "policy": ACCOUNT_POLICIES["삼성IRP"]},
        "삼성이전": {"info": None, "holdings": [], "policy": ACCOUNT_POLICIES["삼성이전"]},
        "삼성ISA": {"info": None, "holdings": [], "policy": ACCOUNT_POLICIES["삼성ISA"]},
        "삼성종합": {"info": None, "holdings": [], "policy": ACCOUNT_POLICIES["삼성종합"]},
    }

    def match_account_key(raw_name: str) -> str:
        raw = str(raw_name or "").strip()
        if any(k in raw for k in ["삼성이전", "연금이전", "이전연금", "이전"]):
            return "삼성이전"
        if any(k in raw for k in ["미래연금", "미래에셋", "미래"]):
            return "미래연금"
        if any(k in raw for k in ["삼성IRP", "IRP", "개인형IRP"]):
            return "삼성IRP"
        if any(k in raw for k in ["삼성ISA", "ISA", "중개형ISA"]):
            return "삼성ISA"
        if any(k in raw for k in ["삼성연금", "연금저축", "삼성연금저축"]):
            return "삼성연금"
        if any(k in raw for k in ["삼성종합", "종합위탁", "해외주식", "일반위탁", "일반"]):
            return "삼성종합"
        return "삼성종합"

    # 계좌별 현황 매핑
    for acc in account_status.get("accounts", []):
        acc_key = match_account_key(acc["name"])
        if acc_key in account_map_data:
            account_map_data[acc_key]["info"] = acc

    # 계좌별 보유종목 매핑 (투자계좌현황 Relation 기반 정확한 소속 계좌 배속)
    account_holdings_map = portfolio_dataset.get("account_holdings", {})
    for stock_name, details in account_holdings_map.items():
        for d in details:
            acc_name = d.get("account_name") or d.get("portfolio") or d.get("full_name", "")
            acc_key = match_account_key(acc_name)
            if acc_key in account_map_data:
                account_map_data[acc_key]["holdings"].append({
                    "stock_name": stock_name,
                    "full_name": d.get("full_name", stock_name),
                    "eval_amt": d.get("eval_amt", 0.0),
                    "qty": d.get("qty", 0.0),
                    "price": d.get("price", 0.0),
                    "buy_price": d.get("buy_price", 0.0),
                    "cum_ret": d.get("cum_ret"),
                    "guide": d.get("guide", ""),
                })

    # 8. [신규] 연간 세액공제(입출금 현황 DB의 당해연도 입금 합산 기준) 실시간 트래커 산출
    pension_target = TAX_DEDUCTION_CONFIG.get("PENSION_TARGET", 6_000_000.0)
    irp_target = TAX_DEDUCTION_CONFIG.get("IRP_TARGET", 3_000_000.0)
    total_tax_target = TAX_DEDUCTION_CONFIG.get("TOTAL_TARGET", pension_target + irp_target)
    ref_min_rate = TAX_DEDUCTION_CONFIG.get("REFUND_RATE_MIN", 0.132)
    ref_max_rate = TAX_DEDUCTION_CONFIG.get("REFUND_RATE_MAX", 0.165)

    # 당해연도 입금액
    pension_dep = yearly_deposits.get("삼성연금", 0.0)
    irp_dep = yearly_deposits.get("삼성IRP", 0.0)
    
    pension_pct = min(100.0, (pension_dep / pension_target * 100.0)) if pension_target > 0 else 0.0
    irp_pct = min(100.0, (irp_dep / irp_target * 100.0)) if irp_target > 0 else 0.0
    
    pension_rem = max(0.0, pension_target - pension_dep)
    irp_rem = max(0.0, irp_target - irp_dep)
    
    total_tax_dep = pension_dep + irp_dep
    total_tax_pct = min(100.0, (total_tax_dep / total_tax_target * 100.0)) if total_tax_target > 0 else 0.0
    total_tax_rem = max(0.0, total_tax_target - total_tax_dep)
    
    # 예상 세액공제 환급액
    refund_min = min(total_tax_dep, total_tax_target) * ref_min_rate
    refund_max = min(total_tax_dep, total_tax_target) * ref_max_rate

    tax_deduction_tracker_text = (
        f"* **🏛️ 삼성연금 (연금저축 - 연 {pension_target/10000:,.0f}만원 세액공제 목표)**: {current_year}년 누적 입금 **{pension_dep:,.0f} 원** "
        f"(달성률: **{pension_pct:.1f}%**, 연말 잔여 납입 필요액: **{pension_rem:,.0f} 원**)\n"
        f"* **🛡️ 삼성IRP (개인형 IRP - 연 {irp_target/10000:,.0f}만원 세액공제 목표)**: {current_year}년 누적 입금 **{irp_dep:,.0f} 원** "
        f"(달성률: **{irp_pct:.1f}%**, 연말 잔여 납입 필요액: **{irp_rem:,.0f} 원**)\n"
        f"* **💰 [합계] {current_year}년 {total_tax_target/10000:,.0f}만원 세액공제 달성 현황**: 총 **{total_tax_dep:,.0f} 원** 입금 완료 "
        f"(전체 달성률: **{total_tax_pct:.1f}%**, 잔여 납입 필요액: **{total_tax_rem:,.0f} 원**)\n"
        f"  ➡️ *현재까지 확보된 예상 절세 환급액: 약 **{refund_min:,.0f} 원 ~ {refund_max:,.0f} 원** (한도 {total_tax_target/10000:,.0f}만원 완납 시 최대 {total_tax_target*ref_max_rate/10000:,.1f}만원 환급)*"
    )

    # 9. [개조식 & 탭 들여쓰기] 6대 계좌별 독립 상세 진단 마크다운 생성
    acc_cat_lines = []
    
    # 1) 코어 적립식 (삼성연금 & 미래연금)
    p_info = account_map_data["삼성연금"]["info"] or {}
    p_eval = p_info.get("asset_eval", 0.0) or sum(h["eval_amt"] for h in account_map_data["삼성연금"]["holdings"])
    m_info = account_map_data["미래연금"]["info"] or {}
    m_eval = m_info.get("asset_eval", 0.0) or sum(h["eval_amt"] for h in account_map_data["미래연금"]["holdings"])
    m_dep = yearly_deposits.get("미래연금", 0.0)

    acc_cat_lines.append("* **① [삼성연금 & 미래연금] 코어 적립식 계좌 (영구 복리 & 매도 절대 금지)**")
    acc_cat_lines.append(f"  - **삼성연금 (연금저축)**: {current_year}년 누적입금 {pension_dep:,.0f}원 (달성률 {pension_pct:.1f}%, 잔여 {pension_rem:,.0f}원) | 총평가 {p_eval:,.0f}원 | 주 10만원 적립 (`KODEX 미국S&P500`)")
    for h in account_map_data["삼성연금"]["holdings"]:
        ret_str = f", 수익률 {h['cum_ret']*100:+.1f}%" if isinstance(h.get("cum_ret"), (int, float)) else ""
        acc_cat_lines.append(f"    * `{h['stock_name']}`: 평가액 {h['eval_amt']:,.0f}원 ({h['qty']:,.1f}주{ret_str}) [적립유지/매도금지]")
    if not account_map_data["삼성연금"]["holdings"]:
        acc_cat_lines.append("    * (현재 적립 종목: KODEX 미국S&P500 매주 10만원 매수 지속 요망)")
    
    acc_cat_lines.append(f"  - **미래연금 (미래에셋)**: {current_year}년 누적입금 {m_dep:,.0f}원 (총평가 {m_eval:,.0f}원) | 주 20만원 적립 (`TIGER 미국S&P500` 10만 + `KODEX 미국나스닥100` 10만)")
    for h in account_map_data["미래연금"]["holdings"]:
        ret_str = f", 수익률 {h['cum_ret']*100:+.1f}%" if isinstance(h.get("cum_ret"), (int, float)) else ""
        acc_cat_lines.append(f"    * `{h['stock_name']}`: 평가액 {h['eval_amt']:,.0f}원 ({h['qty']:,.1f}주{ret_str}) [적립유지/매도금지]")
    if not account_map_data["미래연금"]["holdings"]:
        acc_cat_lines.append("  • (현재 적립 종목: TIGER 미국S&P500 10만 + KODEX 미국나스닥100 10만 매주 지속 매수 요망)")
    acc_cat_lines.append("")



    # 2) 삼성IRP (세액공제 300만 & 7:3 패키지)
    irp_info = account_map_data["삼성IRP"]["info"] or {}
    irp_eval = irp_info.get("asset_eval", 0.0) or sum(h["eval_amt"] for h in account_map_data["삼성IRP"]["holdings"])
    acc_cat_lines.append("* **② [삼성IRP] 세액공제(연 300만) & 고수익 7:3 패키지 매수 가이드**")
    acc_cat_lines.append(f"  - **계좌 현황**: {current_year}년 누적입금 {irp_dep:,.0f}원 (달성률 {irp_pct:.1f}%, 잔여 {irp_rem:,.0f}원) | 총평가 {irp_eval:,.0f}원")
    acc_cat_lines.append(f"  - **법정 의무 규정**: 안전자산 $\\ge 30\\%$, 위험자산 $\\le 70\\%$ 준수 필수")
    acc_cat_lines.append(f"  - **추천 패키지**: 위험 70% (`TIGER 미국테크TOP10`) + 안전 30% (`SOL 미국배당미국채혼합50` 또는 `ACE 미국30년국채액티브(H)`)")
    for h in account_map_data["삼성IRP"]["holdings"]:
        ret_str = f", 수익률 {h['cum_ret']*100:+.1f}%" if isinstance(h.get("cum_ret"), (int, float)) else ""
        acc_cat_lines.append(f"    * `{h['stock_name']}`: 평가액 {h['eval_amt']:,.0f}원 ({h['qty']:,.1f}주{ret_str})")
    acc_cat_lines.append("")

    # 3) 연금이전 (삼성이전 - 100% ETF 월배당 인컴 & 재투자 가이드)
    prev_p_info = account_map_data["삼성이전"]["info"] or {}
    prev_p_eval = prev_p_info.get("asset_eval", 0.0) or sum(h["eval_amt"] for h in account_map_data["삼성이전"]["holdings"])
    acc_cat_lines.append("* **③ [연금이전 (삼성이전)] 100% ETF 월배당 인컴 복리 & 재투자 가이드**")
    acc_cat_lines.append(f"  - **계좌 현황**: 총 평가자산 {prev_p_eval:,.0f} 원 (기소득공제 연금 - 과세이연 복리 계좌)")
    acc_cat_lines.append(f"  - **운용 제약**: **개별주식 매수 불가 (100% ETF 전용 계좌)**")
    acc_cat_lines.append(f"  - **인컴 엔진**: 월배당 배당성장(40%) + 월배당 안전국채(30%) + 국내 테마 알파(30%)")
    for h in account_map_data["삼성이전"]["holdings"]:
        ret_str = f", 수익률 {h['cum_ret']*100:+.1f}%" if isinstance(h.get("cum_ret"), (int, float)) else ""
        guide_str = f" [{h['guide']}]" if h.get("guide") else ""
        acc_cat_lines.append(f"    * `{h['stock_name']}`: 평가액 {h['eval_amt']:,.0f}원 ({h['qty']:,.1f}주{ret_str}){guide_str}")
    acc_cat_lines.append(f"  - **재투자 추천**: 월 발생 분배금으로 저평가된 테마 ETF (`AI반도체`, `전력설비` 등) 수동 추가매수 추천 (자동매수 ❌)")
    acc_cat_lines.append("")

    # 4) 삼성ISA (국내 주식 & 테마 ETF 퀀트 스윙 알파)
    isa_info = account_map_data["삼성ISA"]["info"] or {}
    isa_eval = isa_info.get("asset_eval", 0.0) or sum(h["eval_amt"] for h in account_map_data["삼성ISA"]["holdings"])
    acc_cat_lines.append("* **④ [삼성ISA] 국내 주식 & 테마 ETF 퀀트 스윙 알파 (3년 비과세 극대화)**")
    acc_cat_lines.append(f"  - **계좌 현황**: 총 평가자산 {isa_eval:,.0f} 원 | 운용 대상: 삼성전자 + 국내 상장 테마/섹터 ETF")
    for h in account_map_data["삼성ISA"]["holdings"]:
        ret_str = f", 수익률 {h['cum_ret']*100:+.1f}%" if isinstance(h.get("cum_ret"), (int, float)) else ""
        guide_str = f" [{h['guide']}]" if h.get("guide") else ""
        acc_cat_lines.append(f"    * `{h['stock_name']}`: 평가액 {h['eval_amt']:,.0f}원 ({h['qty']:,.1f}주{ret_str}){guide_str}")
    acc_cat_lines.append(f"  - **리포트 조언**: 퀀트 신호(`▲ 추세탑승`, `▲ 분할매수`, `▼ 비중조절`) 기반 적극적 교체매매/수익실현 조언")
    acc_cat_lines.append("")

    # 5) 삼성종합 (해외 직투)
    glob_info = account_map_data["삼성종합"]["info"] or {}
    glob_eval = glob_info.get("asset_eval", 0.0) or sum(h["eval_amt"] for h in account_map_data["삼성종합"]["holdings"])
    acc_cat_lines.append("* **⑤ [삼성종합] 미국 빅테크 직투 대기 & 달러 환전 타이밍**")
    acc_cat_lines.append(f"  - **계좌 현황**: 총 평가자산 {glob_eval:,.0f} 원 | 운용 대상: 미국 빅테크 개별주 (`NVDA`, `AAPL`, `MSFT` 등)")
    acc_cat_lines.append(f"  - **운용 일정**: 2027년 본격 투입 대기. 환율 3M 동적 밴드 하단($Q_{25}$) 진입 시 달러 사전 환전 권고")
    for h in account_map_data["삼성종합"]["holdings"]:
        ret_str = f", 수익률 {h['cum_ret']*100:+.1f}%" if isinstance(h.get("cum_ret"), (int, float)) else ""
        acc_cat_lines.append(f"    * `{h['stock_name']}`: 평가액 {h['eval_amt']:,.0f}원 ({h['qty']:,.1f}주{ret_str})")
    
    account_categorized_text = "\n".join(acc_cat_lines)

    # 10. 테마별 요약 텍스트
    theme_lines = []
    for t_name, t_val in sorted(theme_distribution.items(), key=lambda x: x[1], reverse=True):
        t_pct = (t_val / total_eval_krw * 100.0) if total_eval_krw > 0 else 0.0
        theme_lines.append(f"- **{t_name}**: {t_val:,.0f}원 ({t_pct:.1f}%)")
    theme_summary_text = "\n".join(theme_lines)

    # 11. 자산군별 상세 보유 종목 텍스트 (52주위치, 안전마진, 투자가이드 결합)
    detail_lines = []
    for code in ASSET_ORDER:
        grp = asset_groups[code]
        active_items = [h for h in grp["holdings"] if h["eval_asset"] > 0]
        active_items.sort(key=lambda x: x["eval_asset"], reverse=True)
        watch_items = [h for h in grp["holdings"] if h["eval_asset"] <= 0]
        
        detail_lines.append(f"#### [{grp['name']}] 목표 {grp['target_pct']:.1f}% / 현재 {grp['actual_pct']:.1f}% (총 {grp['eval_krw']:,.0f}원)")
        
        if not active_items:
            detail_lines.append("  - (실제 보유 자산 없음: 목표 비중 도달을 위한 신규 편입 필요)")
        else:
            for item in active_items:
                weight_in_portfolio = (item["eval_asset"] / total_eval_krw * 100.0) if total_eval_krw > 0 else 0.0
                
                pos_str = f", 52주위치:{item['pos_52w']*100:.1f}%" if isinstance(item.get("pos_52w"), (int, float)) else ""
                margin_str = f", {item['margin_of_safety']}" if item.get("margin_of_safety") else ""
                guide_str = f", 가이드:{'/'.join(item['guides'])}" if item.get("guides") else ""
                div_str = f", 배당:{item['div_yield']*100:.1f}%" if isinstance(item.get("div_yield"), (int, float)) else ""
                
                detail_lines.append(
                    f"  - **{item['name']}** ({item['ticker']}): {item['eval_asset']:,.0f}원 ({weight_in_portfolio:.1f}%) [{item.get('theme', '')}{pos_str}{margin_str}{guide_str}{div_str}]"
                )
        
        if watch_items:
            watch_names = [f"{w['name']}({w['ticker']})" for w in watch_items[:8]]
            more_str = f" 외 {len(watch_items) - 8}개" if len(watch_items) > 8 else ""
            detail_lines.append(f"  * 관심/모니터링 대상 (평가액 0원): {', '.join(watch_names)}{more_str}")

        detail_lines.append("")
    holdings_detail_text = "\n".join(detail_lines)

    # 12. 전주 대비(WoW) 주간 자산 추적 및 델타 지표 산출
    prev_report = portfolio_dataset.get("prev_report")
    if prev_report and prev_report.get("total_eval_krw", 0) > 0:
        prev_date = prev_report["date"]
        prev_total = prev_report["total_eval_krw"]
        prev_cash = prev_report["cash_pct"]
        prev_fitness = prev_report["fitness"]
        prev_actions = ", ".join(prev_report["actions"]) if prev_report["actions"] else "없음"

        diff_total_krw = total_eval_krw - prev_total
        diff_total_pct = (diff_total_krw / prev_total * 100.0) if prev_total > 0 else 0.0
        diff_cash_pct = cash_pct - prev_cash

        sign_total = "+" if diff_total_krw >= 0 else ""
        sign_cash = "+" if diff_cash_pct >= 0 else ""

        prev_report_summary_text = (
            f"- **비교 기준 (직전/전주 리포트)**: {prev_date} ({prev_report['title']})\n"
            f"- **총 평가자산**: {prev_total:,.0f} 원 ➡️ {total_eval_krw:,.0f} 원 "
            f"(**주간 증감: {sign_total}{diff_total_krw:,.0f} 원, {sign_total}{diff_total_pct:.2f}%**)\n"
            f"- **현금 비중**: {prev_cash:.1f}% ➡️ {cash_pct:.1f}% (**주간 변화: {sign_cash}{diff_cash_pct:.1f}%p**)\n"
            f"- **올라운드 적합도**: {prev_fitness}\n"
            f"- **직전 권고 핵심 조치**: {prev_actions}\n"
            f"- **직전 요약**: {prev_report.get('summary', '')}"
        )
    else:
        prev_report_summary_text = "- **비교 기준**: 직전 리포트 없음 (금주 리포트를 기준점(Baseline)으로 최초 생성)"

    # 13. 스마트 밸류 에버리징(Value Averaging) 적립금 분배 계산 (주간 100만원 기준)
    asset_quant_map = {item["code"]: item for item in macro_snapshot.get("asset_quant_metrics", [])}
    weekly_budget = 1_000_000.0  # 기본 100만원 기준
    
    alloc_scores: Dict[str, float] = {}
    for code in ASSET_ORDER:
        grp = asset_groups[code]
        q_meta = asset_quant_map.get(code, {})
        
        target_pct = grp["target_pct"]
        drift_pct = grp["drift_pct"]
        is_bull = q_meta.get("is_bull", True)
        drawdown = q_meta.get("drawdown_52w", 0.0)
        
        deficit_factor = max(0.0, -drift_pct) * 2.0
        base_score = max(0.5, target_pct + deficit_factor)
        
        trend_mult = 1.2 if is_bull else 0.85
        dd_mult = 1.15 if drawdown <= -10.0 else 1.0
        
        alloc_scores[code] = base_score * trend_mult * dd_mult

    total_score = sum(alloc_scores.values()) or 1.0
    va_lines = [
        "| 추천 계좌 | 목표 자산군 | 200MA 추세 | 52주 낙폭 | 스마트 배분 비중 | 주간 추천 매수금액 (100만원 기준) | 매수 집행 방식 |",
        "|:---|:---|:---:|:---:|:---:|:---:|:---|",
    ]
    # 계좌별 추천 매핑 헬퍼
    account_proxy_map = {
        "US_CORE_INDEX": "삼성연금/미래연금 (KODEX미국S&P500, 나스닥100)",
        "DIVIDEND_GROWTH": "연금이전 (TIGER 미국배당다우존스)",
        "KR_EQUITY": "삼성ISA/연금이전 (삼성전자, AI반도체, 밸류업 ETF)",
        "US_LONG_BOND": "연금이전/삼성IRP (ACE 미국30년국채액티브(H))",
        "KR_BOND_SHORT": "삼성IRP (SOL 미국배당미국채혼합50 / 단기채)",
        "GOLD": "삼성ISA/삼성IRP (ACE KRX금현물)",
        "COMMODITY_CASH": "삼성종합/일반 (달러 예수금 / 원자재)",
    }
    for code in ASSET_ORDER:
        grp = asset_groups[code]
        q_meta = asset_quant_map.get(code, {})
        alloc_pct = (alloc_scores[code] / total_score) * 100.0
        alloc_amt = weekly_budget * (alloc_pct / 100.0)
        
        trend_str = q_meta.get("trend", "판정대기")
        dd_str = f"{q_meta.get('drawdown_52w', 0.0):+.1f}%" if q_meta else "-"
        target_acc = account_proxy_map.get(code, grp["name"])
        va_lines.append(
            f"| **{target_acc}** | {grp['name']} | {trend_str} | `{dd_str}` | **{alloc_pct:.1f}%** | `{alloc_amt:,.0f} 원` | 수동 매수 권고 |"
        )
    value_averaging_table = "\n".join(va_lines)

    # 14. 포트폴리오 95% 1주일 최대 예상 변동성 (VaR) 추정
    port_weighted_vol = 0.0
    for code in ASSET_ORDER:
        grp = asset_groups[code]
        q_meta = asset_quant_map.get(code, {})
        w = grp["actual_pct"] / 100.0
        vol = q_meta.get("volatility_60d", 12.0)
        port_weighted_vol += w * vol

    portfolio_var_pct = 1.65 * (port_weighted_vol / (52 ** 0.5))
    portfolio_var_krw = total_eval_krw * (portfolio_var_pct / 100.0)

    active_holdings_count = sum(1 for h in holdings if h["eval_asset"] > 0)
    
    return {
        "analysis_date": get_kst_str("%Y-%m-%d %H:%M:%S (KST)"),
        "macro_as_of_date": macro_snapshot.get("as_of_date", get_kst_str("%Y-%m-%d")),
        "macro_table_markdown": macro_snapshot.get("macro_table_markdown", ""),
        "fx_rule_status": macro_snapshot.get("fx_rule_status", ""),
        "fx_rate": macro_snapshot.get("fx_rate", 1400.0),
        "total_eval_krw": total_eval_krw,
        "stock_total_krw": stock_total_krw,
        "cash_total_krw": cash_total_krw,
        "cash_pct": cash_pct,
        "tax_deduction_tracker_text": tax_deduction_tracker_text,
        "current_year": current_year,
        "total_tax_limit": total_tax_target,
        "pension_tax_limit": pension_target,
        "irp_tax_limit": irp_target,
        "account_categorized_text": account_categorized_text,
        "portfolio_weighted_vol": port_weighted_vol,
        "portfolio_var_pct": portfolio_var_pct,
        "portfolio_var_krw": portfolio_var_krw,
        "total_positions_count": active_holdings_count,
        "monitoring_count": len(holdings) - active_holdings_count,
        "asset_groups": asset_groups,
        "asset_summary_table": asset_summary_table,
        "value_averaging_table": value_averaging_table,
        "theme_summary_text": theme_summary_text,
        "holdings_detail_text": holdings_detail_text,
        "prev_report_summary_text": prev_report_summary_text,
    }


# ==============================================================================
# 4. 리포트 저장 및 노션 DB 적재
# ==============================================================================
def save_report_locally(report_markdown: str, date_str: str) -> str:
    """리포트를 로컬 reports/ 디렉토리에 백업 파일로 저장합니다."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filename = f"portfolio_report_{get_kst_str('%Y%m%d_%H%M%S')}.md"
    file_path = os.path.join(REPORTS_DIR, filename)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(report_markdown)
    print(f"💾 [Local Backup] 리포트 파일 저장 완료: {file_path}")
    return file_path


def upload_report_to_notion(
    client: Any,
    database_id: str,
    report_markdown: str,
    summary: Dict[str, Any]
) -> bool:
    """
    생성된 마크다운 진단 리포트와 퀀트 지표(총 평가자산, 현금비중, 적합도, 핵심조치, 요약)를
    Notion Report DB의 각 열에 맞추어 신규 페이지로 적재합니다.
    """
    if not database_id:
        print("ℹ️ NOTION_REPORT_DB_ID가 설정되지 않아 노션 적재를 건너뜁니다.")
        return False

    title_str = f"{get_kst_str('%y%m%d')}/자산리포트"
    print(f"📤 [Notion DB] 리포트 페이지 생성 중: '{title_str}'...")

    try:
        db_info = client.databases.retrieve(database_id=database_id)
        db_props = db_info.get("properties", {})
    except Exception as e:
        print(f"❌ [Notion DB] 리포트 DB 메타데이터 조회 실패: {e}")
        print("💡 [안내] 노션 페이지 상단 우측 '...' > '연결 추가'에서 '노션_가격_재무 업데이트' 봇을 연결해 주세요.")
        return False

    total_eval_krw = summary.get("total_eval_krw", 0.0)
    cash_total_krw = summary.get("cash_total_krw", 0.0)
    cash_pct = (cash_total_krw / total_eval_krw * 100.0) if total_eval_krw > 0 else 0.0
    fx_rate = summary.get("fx_rate", 1400.0)

    # 1. K-올라운드 적합도 등급 산출 (총 절대 괴리율 기반)
    asset_groups = summary.get("asset_groups", {})
    total_abs_drift = sum(abs(grp.get("drift_pct", 0.0)) for grp in asset_groups.values())
    if total_abs_drift <= 15.0:
        fitness_grade = "🟢 최적 (85점 이상)"
    elif total_abs_drift <= 35.0:
        fitness_grade = "🟡 주의 (70~84점)"
    else:
        fitness_grade = "🔴 리밸런싱 시급 (70점 미만)"

    # 2. 핵심 조치 키워드 다중선택 목록 산출 (임계치 ±3.0%p 기준)
    action_keywords = []
    if asset_groups.get("US_CORE_INDEX", {}).get("drift_pct", 0.0) <= -3.0:
        action_keywords.append("미국대표지수 매수")
    if asset_groups.get("DIVIDEND_GROWTH", {}).get("drift_pct", 0.0) <= -3.0:
        action_keywords.append("배당성장 편입")
    if asset_groups.get("KR_EQUITY", {}).get("drift_pct", 0.0) >= 3.0:
        action_keywords.append("국내주식 비중축소")
    elif asset_groups.get("KR_EQUITY", {}).get("drift_pct", 0.0) <= -3.0:
        action_keywords.append("국내밸류업 매수")
    if asset_groups.get("US_LONG_BOND", {}).get("drift_pct", 0.0) <= -3.0:
        action_keywords.append("미국장기채 매수")
    if asset_groups.get("GOLD", {}).get("drift_pct", 0.0) <= -3.0:
        action_keywords.append("금 편입")
    if asset_groups.get("COMMODITY_CASH", {}).get("drift_pct", 0.0) <= -3.0:
        action_keywords.append("원자재/달러 확보")
    if asset_groups.get("KR_BOND_SHORT", {}).get("drift_pct", 0.0) <= -3.0:
        action_keywords.append("단기채/현금 확보")
    if not action_keywords:
        action_keywords.append("비중 유지 (적정)")

    # 3. 한 줄 요약 텍스트
    actions_summary_str = ", ".join(action_keywords[:2])
    summary_text = (
        f"총자산 {total_eval_krw:,.0f}원 (현금 {cash_total_krw:,.0f}원, {cash_pct:.1f}%) | "
        f"{fitness_grade} | 환율 {fx_rate:,.1f}원 | 핵심: {actions_summary_str}"
    )

    # 노션 '포트폴리오 분석' DB 컬럼 1:1 동적 매칭
    title_col = next((k for k in db_props if db_props[k].get("type") == "title"), "이름")
    date_col = next((k for k in db_props if "날짜" in k or "일자" in k or db_props[k].get("type") == "date"), "날짜")
    summary_col = next((k for k in db_props if "요약" in k or db_props[k].get("type") == "rich_text"), "요약")
    asset_col = next((k for k in db_props if "총 평가자산" in k or "총평가자산" in k or "자산" in k), "총 평가자산")
    cash_col = next((k for k in db_props if "현금 비중" in k or "현금비중" in k or "현금" in k), "현금 비중")
    fitness_col = next((k for k in db_props if "적합도" in k or "올웨더" in k or "올라운드" in k or db_props[k].get("type") == "select"), "올웨더 적합도")
    actions_col = next((k for k in db_props if "조치" in k or "액션" in k or db_props[k].get("type") == "multi_select"), "핵심 조치")

    page_properties: Dict[str, Any] = {
        title_col: {
            "title": [{"type": "text", "text": {"content": title_str}}]
        },
        date_col: {
            "date": {"start": kst_isoformat()}
        },
        summary_col: {
            "rich_text": [{"type": "text", "text": {"content": summary_text}}]
        },
        asset_col: {
            "number": round(total_eval_krw)
        },
        cash_col: {
            "number": round(cash_pct, 1)
        },
        fitness_col: {
            "select": {"name": fitness_grade}
        },
        actions_col: {
            "multi_select": [{"name": kw} for kw in action_keywords]
        },
    }

    print(f"📋 [Notion DB] 주입될 페이지 속성 ({len(page_properties)}개 매핑됨): {list(page_properties.keys())}")
    for k, v in page_properties.items():
        print(f"   • {k}: {v}")

    blocks = markdown_to_notion_blocks(report_markdown)
    print(f"🧩 [Notion DB] 변환된 노션 블록 수: {len(blocks)}개")

    created_page = safe_create_page(
        client=client,
        database_id=database_id,
        properties=page_properties,
        children=blocks,
        max_retries=3,
        retry_delay=2.0
    )

    if created_page and created_page.get("id"):
        page_url = created_page.get("url", f"https://notion.so/{created_page.get('id').replace('-', '')}")
        print(f"✅ [Notion DB] 리포트 페이지 적재 완료! (URL: {page_url})")
        return True
    else:
        print("❌ [Notion DB] 리포트 페이지 적재 실패.")
        return False


# ==============================================================================
# 5. 메인 실행 함수
# ==============================================================================
def main() -> None:
    """K-올라운드 마스터 포트폴리오 진단 및 리포트 자동 생성 파이프라인 메인"""
    print("=" * 80)
    print("🚀 [K-올라운드 마스터] 실시간 매크로 분석 & 4대 DB 통합 자산배분 진단 시작")
    print("=" * 80)

    notion = build_notion_client(NOTION_TOKEN)
    macro_service = MacroService()
    ai_service = AIService()

    # 1. 실시간 매크로 정량 지표 수집
    macro_snapshot = macro_service.get_macro_snapshot()

    # 2. 4대 노션 DB 통합 데이터 수집 (숨김/계산 퀀트 열 포함)
    portfolio_dataset = collect_all_portfolio_data(notion)
    holdings = portfolio_dataset["holdings"]
    if not holdings and portfolio_dataset["account_status"]["total_asset_val"] <= 0:
        print("⚠️ 분석할 유효 보유 종목 또는 계좌 데이터가 없습니다.")
        return

    # 3. K-올라운드 마스터 7대 자산배분 통계 분석
    summary = analyze_integrated_portfolio(portfolio_dataset, macro_snapshot)
    print("\n" + "=" * 80)
    print(f"📊 [통합 포트폴리오 요약] 총 자산: {summary['total_eval_krw']:,.0f} 원 (주식 {summary['stock_total_krw']:,.0f}원 + 현금 {summary['cash_total_krw']:,.0f}원)")
    print(f"🌐 [실시간 환율 지표] USD/KRW: {summary['fx_rate']:,.1f}원 | {summary['fx_rule_status']}")
    print("=" * 80)
    for code in ASSET_ORDER:
        g = summary["asset_groups"][code]
        print(f"  • {g['name']:<16} | 목표: {g['target_pct']:>4.1f}% | 현재: {g['actual_pct']:>4.1f}% ({g['eval_krw']:>13,.0f}원) | 괴리율: {g['drift_pct']:>+5.1f}%p")
    print("=" * 80 + "\n")

    # 4. Google Gemini API (Google Search Grounding) 진단 리포트 생성
    if not ai_service.is_available():
        print("⚠️ GEMINI_API_KEY가 설정되지 않아 AI 리포트 생성을 건너뛰고 기본 통계 리포트만 생성합니다.")
        report_markdown = f"# [통합 분석 리포트] K-올라운드 마스터 계좌 분리형 자산배분 진단\n\n"
        report_markdown += f"- **분석 일시**: {summary['analysis_date']}\n"
        report_markdown += f"- **총 평가 자산**: {summary['total_eval_krw']:,.0f} 원 (주식 {summary['stock_total_krw']:,.0f}원 + 현금 {summary['cash_total_krw']:,.0f}원)\n\n"
        report_markdown += f"## 💰 1. 연간 세액공제 900만원 실시간 진척도\n{summary['tax_deduction_tracker_text']}\n\n"
        report_markdown += f"## 🌐 2. 실시간 글로벌 매크로 지표\n{summary['macro_table_markdown']}\n\n"
        report_markdown += f"## 🏛️ 3. 6대 계좌별 세부 운용 현황\n{summary['account_categorized_text']}\n\n"
        report_markdown += f"## 📊 4. K-올라운드 7대 자산군 비중 vs 목표\n{summary['asset_summary_table']}\n\n"
        report_markdown += f"## 🎯 5. 스마트 밸류 에버리징 매수 추천표\n{summary['value_averaging_table']}\n\n"
        report_markdown += f"## 🔍 6. 상세 보유 종목 및 퀀트 지표\n{summary['holdings_detail_text']}"
    else:
        try:
            report_markdown = ai_service.generate_portfolio_diagnosis(summary)
        except Exception as e:
            print(f"❌ [AI Service] Gemini 리포트 생성 중 예외 발생: {e}")
            report_markdown = f"# [자산배분 진단] K-올라운드 마스터 요약 리포트 (AI 생성 오류)\n\n"
            report_markdown += f"- **분석 일시**: {summary['analysis_date']}\n"
            report_markdown += f"- **총 평가액**: {summary['total_eval_krw']:,.0f} 원\n\n"
            report_markdown += f"## 💰 1. 연간 세액공제 900만원 진척도\n{summary['tax_deduction_tracker_text']}\n\n"
            report_markdown += f"## 🌐 2. 실시간 매크로 지표\n{summary['macro_table_markdown']}\n\n"
            report_markdown += f"## 🏛️ 3. 6대 계좌별 세부 운용 현황\n{summary['account_categorized_text']}\n\n"
            report_markdown += f"## 📊 4. 자산군별 비중 현황\n{summary['asset_summary_table']}\n\n"
            report_markdown += f"## 🔍 5. 상세 보유 종목\n{summary['holdings_detail_text']}"

    # 5. 로컬 백업 저장
    save_report_locally(report_markdown, summary["analysis_date"])

    # 6. 노션 Report DB 적재
    if NOTION_REPORT_DB_ID:
        upload_report_to_notion(
            client=notion,
            database_id=NOTION_REPORT_DB_ID,
            report_markdown=report_markdown,
            summary=summary
        )

    print("\n✨ [K-올라운드 마스터] 모든 작업이 성공적으로 완료되었습니다.\n")


if __name__ == "__main__":
    main()
