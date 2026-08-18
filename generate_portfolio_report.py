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
)
from config_portfolio import (
    K_ALL_ROUND_MASTER_CONFIG,
    TARGET_ALLOCATION,
    ASSET_ORDER,
    REBALANCING_DRIFT_THRESHOLD_PCT,
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
def collect_account_status(client: Any, db_id: str) -> Dict[str, Any]:
    """
    [투자계좌현황 DB]를 조회하여 계좌별(ISA, 연금, IRP 등) 총자산, 현금 예수금, 투자원금,
    수익률, 현금비중, 확정손익, 누적 배당금을 수집합니다.
    """
    if not db_id:
        print("ℹ️ [Notion] 1. ACCOUNT_STATUS_DB_ID가 설정되지 않아 계좌현황 조회를 건너뜁니다.")
        return {"accounts": [], "total_asset_val": 0.0, "total_cash_val": 0.0, "total_invest_val": 0.0}

    print("🏦 [Notion] 1. 투자계좌현황 DB 스캔 시작...")
    accounts: List[Dict[str, Any]] = []
    total_asset_val = 0.0
    total_cash_val = 0.0
    total_invest_val = 0.0
    total_realized_profit = 0.0
    total_dividend = 0.0

    try:
        for page in paginate_database(client, db_id, page_size=100, retry_delay=0.2):
            props = page.get("properties", {})
            name = str(get_prop_value(props, ["이름", "Name", "계좌명"]) or "").strip()
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
    }


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


def collect_account_holdings_detail(client: Any, db_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    [계좌별 보유종목 DB]를 조회하여 계좌별 보유 종목, 실시간 투자가이드(50%익절/물타기/추세추종),
    매수단가, 익절가, 평가비중, 누적수익률 등을 수집합니다.
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

            if name:
                base_name = name.split("#")[0].strip()
                if base_name not in account_holdings_map:
                    account_holdings_map[base_name] = []
                account_holdings_map[base_name].append({
                    "full_name": name,
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
    4대 데이터베이스 및 직전 리포트 DB를 통합 조회하여 퀀트 지표(안전마진, 52주 위치, 배당수익률, 투자가이드, 전주 대비 변화)를 결합합니다.
    """
    # 0. 직전(전주) 리포트 스냅샷 수집
    prev_report = fetch_latest_previous_report(client, NOTION_REPORT_DB_ID)

    # 1. 계좌 현황 수집
    account_status = collect_account_status(client, ACCOUNT_STATUS_DB_ID)
    
    # 2. 종목별 보유현황 메타 수집
    stock_meta = collect_stock_holdings_meta(client, STOCK_HOLDINGS_DB_ID)

    # 3. 계좌별 보유종목 상세 수집
    account_holdings = collect_account_holdings_detail(client, ACCOUNT_HOLDINGS_DB_ID)

    # 4. 투자주 DB(마스터) 전수 스캔 (숨김 퀀트 지표 포함)
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

        # 숨김/계산 퀀트 열 추출
        pos_52w = get_prop_value(props, ["52주 위치"])
        high_52w = safe_float(get_prop_value(props, ["52주 최고가"])) or 0.0
        low_52w = safe_float(get_prop_value(props, ["52주 최저가"])) or 0.0
        margin_of_safety = str(get_prop_value(props, ["안전마진"]) or "").strip()
        target_price = safe_float(get_prop_value(props, ["목표주가"])) or 0.0
        target_range = str(get_prop_value(props, ["목표가 범위"]) or "").strip()
        div_yield = get_prop_value(props, ["배당수익률"])
        per = safe_float(get_prop_value(props, ["PER", "추정PER"]))
        pbr = safe_float(get_prop_value(props, ["PBR"]))
        day_change = get_prop_value(props, ["전일대비"])

        # 종목 메타 결합
        meta_info = stock_meta.get(name.upper()) or stock_meta.get(ticker.upper()) or {}
        country = meta_info.get("country", "")
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
    }


# ==============================================================================
# 3. K-올라운드 마스터 7대 자산배분 통계 분석부
# ==============================================================================
def analyze_integrated_portfolio(
    portfolio_dataset: Dict[str, Any],
    macro_snapshot: Dict[str, Any]
) -> Dict[str, Any]:
    """
    통합 포트폴리오 데이터를 「K-올라운드 마스터」 7대 자산군, 테마별, 퀀트 지표별로 분석하고
    실시간 매크로 스냅샷을 결합하여 분석 지표를 산출합니다.
    """
    holdings = portfolio_dataset["holdings"]
    account_status = portfolio_dataset["account_status"]
    
    # 1. 총 평가금액 계산 (종목 평가액 + 현금 예수금)
    stock_total_krw = sum(h["eval_asset"] for h in holdings)
    cash_total_krw = account_status.get("total_cash_val", 0.0)
    total_eval_krw = stock_total_krw + cash_total_krw
    cash_pct = (cash_total_krw / total_eval_krw * 100.0) if total_eval_krw > 0 else 0.0

    # 2. 7대 자산군 구조 초기화
    asset_groups: Dict[str, Dict[str, Any]] = {
        code: {
            "code": code,
            "name": cfg["name"],
            "target_pct": cfg["target_pct"],
            "currency_exposure": cfg["currency_exposure"],
            "role": cfg["role"],
            "eval_krw": 0.0,
            "actual_pct": 0.0,
            "drift_pct": 0.0,
            "rebalance_krw": 0.0,
            "holdings": [],
        }
        for code, cfg in K_ALL_ROUND_MASTER_CONFIG.items()
    }

    # 3. 현금 예수금을 '국내 채권 & 단기자금'에 안전자산/단기자금으로 자동 편입
    if cash_total_krw > 0:
        asset_groups["KR_BOND_SHORT"]["eval_krw"] += cash_total_krw
        asset_groups["KR_BOND_SHORT"]["holdings"].append({
            "ticker": "CASH_KRW",
            "name": "원화 현금 / 계좌 예수금",
            "eval_asset": cash_total_krw,
            "current_price": 1.0,
            "asset_code": "KR_BOND_SHORT",
            "asset_name": "국내 채권 & 단기자금",
            "theme": "Safe/Cash",
            "pos_52w": None,
            "margin_of_safety": "",
            "guides": ["유동성 보유"],
        })

    # 4. 종목별 자산군 분류 및 금액 합산
    theme_distribution: Dict[str, float] = {}

    for h in holdings:
        code, cname = classify_asset(
            name=h["name"],
            ticker=h["ticker"],
            market=h["market"],
            country=h["country"],
            custom_portfolio=h["portfolio_theme"],
            custom_selection=h["selection"],
        )
        if code not in asset_groups:
            code = "US_CORE_INDEX"

        h_info = {
            "ticker": h["ticker"],
            "name": h["name"],
            "eval_asset": h["eval_asset"],
            "current_price": h["current_price"],
            "asset_code": code,
            "asset_name": cname,
            "theme": h["portfolio_theme"] or h["selection"] or "General",
            "pos_52w": h.get("pos_52w"),
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

    # 7. 계좌별 요약 텍스트 (숨김 퀀트 열 포함: 자산수익률, 현금/자산 비중 등)
    account_lines = []
    for acc in account_status.get("accounts", []):
        checked_mark = "✅" if acc["checked"] else "⬜"
        acc_yield_str = f", 자산수익률 {acc['asset_yield']*100:+.1f}%" if isinstance(acc.get("asset_yield"), (int, float)) else ""
        cash_ratio_str = f", 현금비중 {acc['cash_asset_ratio']*100:.1f}%" if isinstance(acc.get("cash_asset_ratio"), (int, float)) else ""
        account_lines.append(
            f"- {checked_mark} **{acc['name']}**: 총자산 {acc['asset_eval']:,.0f}원 (현금 {acc['cash']:,.0f}원, 투자액 {acc['invest_eval']:,.0f}원{acc_yield_str}{cash_ratio_str})"
        )
    account_summary_text = "\n".join(account_lines) if account_lines else "계좌 정보 수신 대기 중"

    # 8. 테마별 요약 텍스트
    theme_lines = []
    for t_name, t_val in sorted(theme_distribution.items(), key=lambda x: x[1], reverse=True):
        t_pct = (t_val / total_eval_krw * 100.0) if total_eval_krw > 0 else 0.0
        theme_lines.append(f"- **{t_name}**: {t_val:,.0f}원 ({t_pct:.1f}%)")
    theme_summary_text = "\n".join(theme_lines)

    # 9. 자산군별 상세 보유 종목 텍스트 (52주위치, 안전마진, 투자가이드 결합)
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
                
                # 퀀트 지표 포맷팅
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

    # 10. 전주 대비(WoW) 주간 자산 추적 및 델타 지표 산출
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
        "total_positions_count": active_holdings_count,
        "monitoring_count": len(holdings) - active_holdings_count,
        "asset_groups": asset_groups,
        "asset_summary_table": asset_summary_table,
        "account_summary_text": account_summary_text,
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
        report_markdown = f"# [통합 분석 리포트] K-올라운드 마스터 자산배분 및 다차원 퀀트 진단\n\n"
        report_markdown += f"- **분석 일시**: {summary['analysis_date']}\n"
        report_markdown += f"- **총 평가 자산**: {summary['total_eval_krw']:,.0f} 원 (주식 {summary['stock_total_krw']:,.0f}원 + 현금 {summary['cash_total_krw']:,.0f}원)\n\n"
        report_markdown += f"## 🌐 1. 실시간 글로벌 매크로 지표\n{summary['macro_table_markdown']}\n\n"
        report_markdown += f"## 🏦 2. 투자 계좌별 자산 및 현금 현황\n{summary['account_summary_text']}\n\n"
        report_markdown += f"## 🏷️ 3. 포트폴리오 테마별 비중\n{summary['theme_summary_text']}\n\n"
        report_markdown += f"## 📊 4. K-올라운드 7대 자산군 비중 vs 목표\n{summary['asset_summary_table']}\n\n"
        report_markdown += f"## 🔍 5. 상세 보유 종목 및 퀀트 지표\n{summary['holdings_detail_text']}"
    else:
        try:
            report_markdown = ai_service.generate_portfolio_diagnosis(summary)
        except Exception as e:
            print(f"❌ [AI Service] Gemini 리포트 생성 중 예외 발생: {e}")
            report_markdown = f"# [자산배분 진단] K-올라운드 마스터 요약 리포트 (AI 생성 오류)\n\n"
            report_markdown += f"- **분석 일시**: {summary['analysis_date']}\n"
            report_markdown += f"- **총 평가액**: {summary['total_eval_krw']:,.0f} 원\n\n"
            report_markdown += f"## 🌐 1. 실시간 매크로 지표\n{summary['macro_table_markdown']}\n\n"
            report_markdown += f"## 📊 2. 자산군별 비중 현황\n{summary['asset_summary_table']}\n\n"
            report_markdown += f"## 🔍 3. 상세 보유 종목\n{summary['holdings_detail_text']}"

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
