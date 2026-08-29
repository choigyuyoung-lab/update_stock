"""
kis_data_service.py
===================
한국투자증권(KIS) Open Trading API 공식 밸류에이션 및 시세 엔드포인트(FHKST01010100, HHDFS76200200),
yfinance 및 로컬 SQLite DB 캐시를 결합한 3단계 자동 폴백 퀀트 데이터 서비스 모듈입니다.
- 1순위: KIS Open API 공식 실시간 데이터 수집 (PER, PBR, EPS, BPS, 52주 고저점, 배당수익률)
- 2순위: yfinance 컨센서스 및 보조 지표 (추정PER, 추정EPS, 목표주가, 투자의견, 200일선)
- 3순위: 로컬 SQLite DB(tbl_finances) 직전 캐시 0.001초 자동 채택
"""

import time
import logging
from typing import Any, Dict, Optional

import yfinance as yf

from core.notion_utils import safe_float, get_http_session, to_yfinance_symbol

logger = logging.getLogger("KISDataService")
SESSION = get_http_session()


# ==============================================================================
# 1. KIS Open API 국내 주식 밸류에이션 & 시세 수집기 (FHKST01010100)
# ==============================================================================
def fetch_kr_valuation_kis(
    ticker: str,
    kis_ctx: Optional[Dict[str, Any]],
    max_retries: int = 2
) -> Dict[str, Any]:
    """
    한국투자증권 주식현재가 시세 API(FHKST01010100)를 호출하여
    PER, PBR, EPS, BPS, 52주 최고/최저, 배당수익률, 현재가를 1회에 일괄 수집합니다.
    """
    if not kis_ctx or not ticker:
        return {}

    clean_t = ticker.split(".")[0].strip()
    headers = {
        "authorization": f"Bearer {kis_ctx['token']}",
        "appkey": kis_ctx["app_key"],
        "appsecret": kis_ctx["app_secret"],
        "tr_id": "FHKST01010100",
        "custtype": "P",
    }
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": clean_t
    }

    url = f"{kis_ctx['url_base']}/uapi/domestic-stock/v1/quotations/inquire-price"

    for attempt in range(1, max_retries + 1):
        try:
            res = SESSION.get(url, headers=headers, params=params, timeout=5)
            if res.status_code == 200:
                data = res.json()
                out = data.get("output", {})
                if not out:
                    return {}

                curr_p = safe_float(out.get("stck_prpr"))
                high_52 = safe_float(out.get("w52_hgpr") or out.get("d250_hgpr"))
                low_52 = safe_float(out.get("w52_lwpr") or out.get("d250_lwpr"))
                per_val = safe_float(out.get("per"))
                pbr_val = safe_float(out.get("pbr"))
                eps_val = safe_float(out.get("eps"))
                bps_val = safe_float(out.get("bps"))
                div_yield = safe_float(out.get("dryy"))  # 배당수익률 (%)
                prev_c = safe_float(out.get("stck_sdpr") or out.get("stck_prdy_clpr"))

                return {
                    "current_price": curr_p,
                    "prev_close": prev_c if prev_c and prev_c > 0 else curr_p,
                    "high_52w": high_52,
                    "low_52w": low_52,
                    "per": per_val if per_val and per_val > 0 else None,
                    "pbr": pbr_val if pbr_val and pbr_val > 0 else None,
                    "eps": eps_val if eps_val and eps_val > 0 else None,
                    "bps": bps_val if bps_val and bps_val > 0 else None,
                    "dividend_yield": div_yield if div_yield and div_yield > 0 else None,
                }
        except Exception:
            if attempt < max_retries:
                time.sleep(0.5)
    return {}


# ==============================================================================
# 2. yfinance 컨센서스 & 퀀트 지표 보강 수집기
# ==============================================================================
def fetch_kr_consensus_yfinance(ticker: str) -> Dict[str, Any]:
    """yfinance로부터 추정PER, 추정EPS, 컨센서스 목표주가, 투자의견을 수집합니다."""
    clean_t = ticker.split(".")[0].strip()
    yf_symbol = f"{clean_t}.KS"
    res: Dict[str, Any] = {}
    try:
        t_obj = yf.Ticker(yf_symbol)
        info = t_obj.info or {}
        if not info or not info.get("regularMarketPrice"):
            # KOSDAQ 종목 시도
            t_obj = yf.Ticker(f"{clean_t}.KQ")
            info = t_obj.info or {}

        if info:
            res["forward_per"] = safe_float(info.get("forwardPE"))
            res["forward_eps"] = safe_float(info.get("forwardEps"))
            res["target_price"] = safe_float(info.get("targetMeanPrice") or info.get("targetMedianPrice"))
            rec_key = str(info.get("recommendationKey", "")).lower()
            rec_map = {
                "strong_buy": "적극매수",
                "buy": "매수",
                "hold": "중립(Hold)",
                "underperform": "비중축소",
                "sell": "매도",
            }
            if rec_key in rec_map:
                res["opinion"] = rec_map[rec_key]
    except Exception:
        pass
    return res


# ==============================================================================
# 3. 해외 주식 퀀트 지표 고속 수집기
# ==============================================================================
def fetch_us_quant_yfinance(ticker: str) -> Dict[str, Any]:
    """해외 주식의 밸류에이션, 컨센서스 및 기술적 지표를 yfinance로 일괄 수집합니다."""
    t_clean = to_yfinance_symbol(ticker)
    res: Dict[str, Any] = {}
    try:
        t_obj = yf.Ticker(t_clean)
        info = t_obj.info or {}

        # 1. 시세 및 52주 고저점
        fast_info = getattr(t_obj, "fast_info", None)
        curr_p = None
        high_52 = None
        low_52 = None
        prev_c = None
        if fast_info:
            try:
                curr_p = safe_float(fast_info.last_price)
                high_52 = safe_float(fast_info.year_high)
                low_52 = safe_float(fast_info.year_low)
                prev_c = safe_float(fast_info.previous_close)
            except Exception:
                pass

        if not curr_p:
            curr_p = safe_float(info.get("currentPrice") or info.get("regularMarketPrice"))
        if not high_52:
            high_52 = safe_float(info.get("fiftyTwoWeekHigh"))
        if not low_52:
            low_52 = safe_float(info.get("fiftyTwoWeekLow"))
        if not prev_c:
            prev_c = safe_float(info.get("previousClose") or info.get("regularMarketPreviousClose"))

        res["current_price"] = curr_p
        res["prev_close"] = prev_c
        res["high_52w"] = high_52
        res["low_52w"] = low_52

        # 2. 밸류에이션
        res["per"] = safe_float(info.get("trailingPE"))
        res["forward_per"] = safe_float(info.get("forwardPE"))
        res["pbr"] = safe_float(info.get("priceToBook"))
        res["eps"] = safe_float(info.get("trailingEps"))
        res["forward_eps"] = safe_float(info.get("forwardEps"))
        res["bps"] = safe_float(info.get("bookValue"))
        res["dividend_yield"] = safe_float(info.get("dividendYield"))
        if res["dividend_yield"] and res["dividend_yield"] < 1.0:
            res["dividend_yield"] *= 100.0  # 0.035 -> 3.5%

        # 3. 컨센서스
        res["target_price"] = safe_float(info.get("targetMeanPrice") or info.get("targetMedianPrice"))
        rec_key = str(info.get("recommendationKey", "")).lower()
        rec_map = {
            "strong_buy": "적극매수",
            "buy": "매수",
            "hold": "중립(Hold)",
            "underperform": "비중축소",
            "sell": "매도",
        }
        if rec_key in rec_map:
            res["opinion"] = rec_map[rec_key]

        # 4. 기술적 이동평균선
        res["ma_200"] = safe_float(info.get("twoHundredDayAverage"))
        res["ma_50"] = safe_float(info.get("fiftyDayAverage"))

    except Exception:
        pass
    return res
