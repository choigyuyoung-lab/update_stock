# -*- coding: utf-8 -*-
"""
macro_service.py
================
FinanceDataReader를 활용하여 실시간 글로벌 거시경제(Macro) 핵심 지표
(USD/KRW 환율, 미국 10년/2년 국채금리, 장단기 금리차, S&P 500, KOSPI, WTI 유가, 금 선물)의
최신값 및 주간 변동률(WoW, 1주 전 대비 %)을 수집하고 정량 지표 스냅샷을 생성하는 서비스 모듈입니다.
"""

import sys
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import FinanceDataReader as fdr

# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

logger = logging.getLogger("MacroService")

# config_portfolio의 단일 진실 공급원(Single Source of Truth) 프록시 매핑 사용
from config_portfolio import ASSET_CLASS_PROXIES


class MacroService:
    """FinanceDataReader 기반 실시간 글로벌 매크로 정량 지표 수집기"""

    def __init__(self, lookback_days: int = 120):
        self.lookback_days = lookback_days

    def _get_kst_now(self) -> datetime:
        """KST 기준 현재 시각을 반환합니다."""
        return datetime.now(ZoneInfo("Asia/Seoul"))

    def _fetch_series(self, symbol: str, start_date_str: str) -> Optional[Any]:
        """지정된 심볼의 시계열 데이터를 FinanceDataReader로 안전하게 조회합니다."""
        try:
            df = fdr.DataReader(symbol, start_date_str)
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            logger.warning(f"⚠️ [MacroService] 심볼 '{symbol}' 데이터 조회 실패: {exc}")
        return None

    def _extract_latest_and_prev_week(
        self,
        df: Any,
        val_col: Optional[str] = None
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        데이터프레임에서 최신값과 약 1주 전(5~7영업일 전) 값을 추출하고 주간 변동률(%)을 계산합니다.
        반환: (최신값, 1주 전 값, 주간 변동률(%))
        """
        if df is None or df.empty:
            return None, None, None

        try:
            if val_col and val_col in df.columns:
                series = df[val_col].dropna()
            elif "Close" in df.columns:
                series = df["Close"].dropna()
            elif "Adj Close" in df.columns:
                series = df["Adj Close"].dropna()
            elif len(df.columns) == 1:
                series = df.iloc[:, 0].dropna()
            else:
                series = df.iloc[:, -1].dropna()

            if series.empty:
                return None, None, None

            latest_val = float(series.iloc[-1])

            # 약 5~7 영업일 전 값 추출
            if len(series) >= 6:
                prev_val = float(series.iloc[-6])
            elif len(series) >= 2:
                prev_val = float(series.iloc[0])
            else:
                prev_val = latest_val

            if prev_val != 0:
                wow_change_pct = ((latest_val - prev_val) / prev_val) * 100.0
            else:
                wow_change_pct = 0.0

            return latest_val, prev_val, wow_change_pct
        except Exception as exc:
            logger.warning(f"⚠️ [MacroService] 시계열 값 추출 중 오류: {exc}")
            return None, None, None

    def _calculate_dynamic_band(
        self,
        df: Any,
        val_col: Optional[str] = None,
        window: int = 60
    ) -> Dict[str, Any]:
        """
        최근 N영업일(기본 60영업일, 약 3개월) 시계열 데이터를 롤링 분석하여
        동적 백분위수 밴드(하위 25%, 50%, 상위 25%) 및 현재 백분위 순위를 산출합니다.
        """
        if df is None or df.empty:
            return {}

        try:
            if val_col and val_col in df.columns:
                series = df[val_col].dropna()
            elif "Close" in df.columns:
                series = df["Close"].dropna()
            elif "Adj Close" in df.columns:
                series = df["Adj Close"].dropna()
            elif len(df.columns) == 1:
                series = df.iloc[:, 0].dropna()
            else:
                series = df.iloc[:, -1].dropna()

            if series.empty:
                return {}

            rolling_data = series.tail(window)
            current_val = float(rolling_data.iloc[-1])
            q25 = float(rolling_data.quantile(0.25))
            q50 = float(rolling_data.median())
            q75 = float(rolling_data.quantile(0.75))
            min_val = float(rolling_data.min())
            max_val = float(rolling_data.max())
            
            # 백분위 순위 (0~100%)
            pct_rank = float((rolling_data < current_val).mean() * 100.0)

            if current_val <= q25:
                regime = "LOW"
            elif current_val >= q75:
                regime = "HIGH"
            else:
                regime = "NEUTRAL"

            return {
                "current": current_val,
                "q25": q25,
                "q50": q50,
                "q75": q75,
                "min": min_val,
                "max": max_val,
                "pct_rank": pct_rank,
                "regime": regime,
                "sample_count": len(rolling_data),
            }
        except Exception as exc:
            logger.warning(f"⚠️ [MacroService] 동적 밴드 계산 중 오류: {exc}")
            return {}

    def get_7_asset_quant_metrics(self) -> List[Dict[str, Any]]:
        """
        K-올라운드 마스터 7대 자산군 대표 ETF의 1년 시계열 데이터를 분석하여
        200일선, 추세(상승/하락), 12개월 모멘텀, 52주 낙폭, 60일 변동성을 산출하고
        12개월 모멘텀 기준 순위를 매겨 반환합니다.
        """
        start_date = (self._get_kst_now() - timedelta(days=400)).strftime("%Y-%m-%d")
        results: List[Dict[str, Any]] = []

        for code, meta in ASSET_CLASS_PROXIES.items():
            ticker = meta["ticker"]
            name = meta["name"]
            unit = meta.get("unit", "")
            target_pct = meta["target_pct"]
            try:
                df = fdr.DataReader(ticker, start_date)
                if df is not None and not df.empty:
                    c = df["Close"].dropna() if "Close" in df.columns else df.iloc[:, 0].dropna()
                    curr_price = float(c.iloc[-1])
                    ma200 = float(c.rolling(200).mean().iloc[-1]) if len(c) >= 200 else float(c.mean())
                    trend = "🟢 상승추세" if curr_price >= ma200 else "🔴 하락추세"
                    is_bull = curr_price >= ma200

                    # 12개월 모멘텀
                    mom_12m = ((curr_price - float(c.iloc[0])) / float(c.iloc[0])) * 100.0 if len(c) > 0 else 0.0
                    
                    # 52주 최고가 대비 낙폭 (Drawdown %)
                    peak_52w = float(c.tail(252).max()) if len(c) > 0 else curr_price
                    dd_52w = ((curr_price - peak_52w) / peak_52w) * 100.0 if peak_52w > 0 else 0.0

                    # 60영업일 연환산 변동성 (1년 252일 기준)
                    returns_60 = c.pct_change().tail(60).dropna()
                    vol_60d = float(returns_60.std() * np.sqrt(252) * 100.0) if len(returns_60) > 5 else 0.0

                    # 주간 변동률(WoW)
                    prev_week_val = float(c.iloc[-6]) if len(c) >= 6 else (float(c.iloc[0]) if len(c) >= 2 else curr_price)
                    wow_pct = ((curr_price - prev_week_val) / prev_week_val) * 100.0 if prev_week_val > 0 else 0.0

                    results.append({
                        "code": code,
                        "name": name,
                        "ticker": ticker,
                        "unit": unit,
                        "target_pct": target_pct,
                        "current_price": curr_price,
                        "ma200": ma200,
                        "trend": trend,
                        "is_bull": is_bull,
                        "momentum_12m": mom_12m,
                        "drawdown_52w": dd_52w,
                        "volatility_60d": vol_60d,
                        "wow_pct": wow_pct,
                    })
                else:
                    results.append({
                        "code": code,
                        "name": name,
                        "ticker": ticker,
                        "unit": unit,
                        "target_pct": target_pct,
                        "current_price": None,
                        "ma200": None,
                        "trend": "판정대기",
                        "is_bull": False,
                        "momentum_12m": 0.0,
                        "drawdown_52w": 0.0,
                        "volatility_60d": 0.0,
                        "wow_pct": 0.0,
                    })
            except Exception as e:
                logger.warning(f"⚠️ [MacroService] 자산군 '{name}({ticker})' 퀀트 지표 산출 실패: {e}")

        # 12개월 모멘텀 내림차순 정렬
        results.sort(key=lambda x: x.get("momentum_12m", -999.0), reverse=True)
        for idx, item in enumerate(results, 1):
            item["rank"] = idx

        return results

    def get_macro_snapshot(self) -> Dict[str, Any]:
        """
        글로벌 & 국내 거시경제 핵심 지표, 3개월 롤링 동적 밴드 및
        K-올라운드 7대 자산군 퀀트 모멘텀 지표를 종합 수집합니다.
        """
        # 기존 글로벌/국내 지표 수집
        snapshot = self._get_macro_snapshot_indicators()
        
        # 7대 자산군 퀀트 팩터(200일선, 모멘텀, 낙폭, 변동성) 수집
        asset_quant_list = self.get_7_asset_quant_metrics()
        snapshot["asset_quant_metrics"] = asset_quant_list

        # 정량 지표 마크다운 표 재빌드 (7대 자산 퀀트 표 포함)
        macro_table = self._build_macro_table(
            snapshot["indicators"],
            snapshot.get("fx_band"),
            snapshot.get("us10y_band"),
            asset_quant_list
        )
        snapshot["macro_table_markdown"] = macro_table

        return snapshot

    def _get_macro_snapshot_indicators(self) -> Dict[str, Any]:
        now_kst = self._get_kst_now()
        start_date_str = (now_kst - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")

        print(f"🌐 [MacroService] 실시간 글로벌 & 국내 매크로 지표 및 동적 밴드 수집 시작 (기준일: {now_kst.strftime('%Y-%m-%d')})...")

        # 1. USD/KRW 환율
        df_usdkrw = self._fetch_series("USD/KRW", start_date_str)
        usdkrw_now, usdkrw_prev, usdkrw_wow = self._extract_latest_and_prev_week(df_usdkrw)
        fx_band = self._calculate_dynamic_band(df_usdkrw)

        # 2. 미국 10년물 국채금리 (FRED: DGS10)
        df_us10y = self._fetch_series("FRED:DGS10", start_date_str)
        us10y_now, us10y_prev, us10y_wow = self._extract_latest_and_prev_week(df_us10y)
        us10y_band = self._calculate_dynamic_band(df_us10y)

        # 3. 미국 2년물 국채금리 (FRED: DGS2)
        df_us2y = self._fetch_series("FRED:DGS2", start_date_str)
        us2y_now, us2y_prev, us2y_wow = self._extract_latest_and_prev_week(df_us2y)
        us2y_band = self._calculate_dynamic_band(df_us2y)

        # 4. 미국 장단기 금리차
        term_spread_now, term_spread_prev, term_spread_delta_bp = None, None, None
        if us10y_now is not None and us2y_now is not None:
            term_spread_now = us10y_now - us2y_now
            if us10y_prev is not None and us2y_prev is not None:
                term_spread_prev = us10y_prev - us2y_prev
                term_spread_delta_bp = (term_spread_now - term_spread_prev) * 100.0

        # 5. S&P 500
        df_sp500 = self._fetch_series("US500", start_date_str)
        if df_sp500 is None:
            df_sp500 = self._fetch_series("SPY", start_date_str)
        sp500_now, sp500_prev, sp500_wow = self._extract_latest_and_prev_week(df_sp500)

        # 6. WTI 원유 선물
        df_wti = self._fetch_series("CL", start_date_str)
        wti_now, wti_prev, wti_wow = self._extract_latest_and_prev_week(df_wti)

        # 7. 금 (GLD 또는 GC)
        df_gold = self._fetch_series("GLD", start_date_str)
        if df_gold is None or df_gold.empty:
            df_gold = self._fetch_series("GC", start_date_str)
        gold_now, gold_prev, gold_wow = self._extract_latest_and_prev_week(df_gold)

        # 8. KOSPI 지수
        df_kospi = self._fetch_series("KS11", start_date_str)
        kospi_now, kospi_prev, kospi_wow = self._extract_latest_and_prev_week(df_kospi)
        kospi_band = self._calculate_dynamic_band(df_kospi)

        # 9. KOSDAQ 지수
        df_kosdaq = self._fetch_series("KQ11", start_date_str)
        kosdaq_now, kosdaq_prev, kosdaq_wow = self._extract_latest_and_prev_week(df_kosdaq)

        # 10. 한국 국고채 10년물
        df_kr10y = self._fetch_series("FRED:IRLTLT01KRM156N", start_date_str)
        if df_kr10y is None or df_kr10y.empty:
            df_kr10y = self._fetch_series("114820", start_date_str)
        kr10y_now, kr10y_prev, kr10y_wow = self._extract_latest_and_prev_week(df_kr10y)
        kr10y_band = self._calculate_dynamic_band(df_kr10y)

        # 11. 한-미 10년물 금리차
        kr_us_spread_now, kr_us_spread_prev, kr_us_spread_delta_bp = None, None, None
        if us10y_now is not None and kr10y_now is not None:
            kr_us_spread_now = us10y_now - kr10y_now
            if us10y_prev is not None and kr10y_prev is not None:
                kr_us_spread_prev = us10y_prev - kr10y_prev
                kr_us_spread_delta_bp = (kr_us_spread_now - kr_us_spread_prev) * 100.0

        # 12. 국내 단기채 ETF
        df_kr_short = self._fetch_series("153130", start_date_str)
        kr_short_now, kr_short_prev, kr_short_wow = self._extract_latest_and_prev_week(df_kr_short)

        # 환율 상태 평가
        if fx_band and fx_band.get("regime") == "LOW":
            fx_badge = f"🟢 저환율 (하위 {fx_band['pct_rank']:.0f}%)"
            fx_status = "LOW_FX"
        elif fx_band and fx_band.get("regime") == "HIGH":
            fx_badge = f"🔴 고환율 (상위 {100 - fx_band['pct_rank']:.0f}%)"
            fx_status = "HIGH_FX"
        else:
            pct_str = f" ({fx_band['pct_rank']:.0f}% 위치)" if fx_band else ""
            fx_badge = f"⚖️ 적정 중립 환율{pct_str}"
            fx_status = "NEUTRAL_FX"

        # 미국 10년물 금리 상태
        if us10y_band and us10y_band.get("regime") == "HIGH":
            us10y_status_str = f"고금리 매수기회 (상위 {100 - us10y_band['pct_rank']:.0f}%, {us10y_band['q75']:.2f}% 이상) -> 미국 장기채 분할 매수 최적기 (채권 가격 저평가 + 자본차익 기대)"
        elif us10y_band and us10y_band.get("regime") == "LOW":
            us10y_status_str = f"저금리 구간 (하위 {us10y_band['pct_rank']:.0f}%) -> 장기채 신규 매수 신중, 단기채/배당성장 중심 운용"
        elif us10y_now is not None:
            us10y_status_str = f"중립 금리 레인지 ({us10y_now:.2f}%) -> 7대 자산군 목표 비중(20%) 유지"
        else:
            us10y_status_str = "글로벌 벤치마크 금리"

        # 수익률 곡선 상태
        if term_spread_now is not None:
            if term_spread_now < 0:
                yield_curve_status = f"역전 상태 ({term_spread_now:+.2f}%p) -> 경기 둔화 경계"
            else:
                yield_curve_status = "정상 우상향 수익률 곡선 (Steepener)"
        else:
            yield_curve_status = "산출 대기"

        # 한-미 금리차 상태
        if kr_us_spread_now is not None:
            if kr_us_spread_now > 1.5:
                spread_status = f"미국 우위 대폭 확대 ({kr_us_spread_now:+.2f}%p) -> 원화 약세/달러 선호 지속"
            elif kr_us_spread_now < 0:
                spread_status = f"한국 금리 우위 ({kr_us_spread_now:+.2f}%p) -> 원화 자산 매력도 상승"
            else:
                spread_status = f"한-미 금리차 ({kr_us_spread_now:+.2f}%p) -> 안정적 자금 흐름 유지"
        else:
            spread_status = "산출 대기"

        # 코스피 상태
        if kospi_band:
            pct_k = kospi_band["pct_rank"]
            if kospi_band["regime"] == "LOW":
                kospi_status_str = f"3개월 저평가 (하위 {pct_k:.0f}%) -> 국내 밸류업 ETF 저가 분할매수 적기"
            elif kospi_band["regime"] == "HIGH":
                kospi_status_str = f"3개월 단기 과열 (상위 {100 - pct_k:.0f}%) -> 차익실현 및 비중 유지"
            else:
                kospi_status_str = f"3개월 적정 밸런스 ({pct_k:.0f}% 위치)"
        else:
            kospi_status_str = "국내 대형주 & 밸류업 지표"

        indicators: Dict[str, Any] = {
            "usdkrw": {
                "name": "USD/KRW 환율",
                "value": usdkrw_now,
                "prev_week": usdkrw_prev,
                "wow_pct": usdkrw_wow,
                "unit": "원",
                "formatted": f"{usdkrw_now:,.1f}원" if usdkrw_now else "N/A",
                "status": fx_badge,
                "band": fx_band,
            },
            "us10y": {
                "name": "미국 10년물 국채금리",
                "value": us10y_now,
                "prev_week": us10y_prev,
                "wow_pct": us10y_wow,
                "unit": "%",
                "formatted": f"{us10y_now:.2f}%" if us10y_now is not None else "N/A",
                "status": us10y_status_str,
                "band": us10y_band,
            },
            "us2y": {
                "name": "미국 2년물 국채금리",
                "value": us2y_now,
                "prev_week": us2y_prev,
                "wow_pct": us2y_wow,
                "unit": "%",
                "formatted": f"{us2y_now:.2f}%" if us2y_now is not None else "N/A",
                "status": "연준 정책금리 민감 지표",
                "band": us2y_band,
            },
            "term_spread_10y_2y": {
                "name": "미국 장단기 금리차 (10Y-2Y)",
                "value": term_spread_now,
                "prev_week": term_spread_prev,
                "delta_bp": term_spread_delta_bp,
                "unit": "%p",
                "formatted": f"{term_spread_now:+.2f}%p" if term_spread_now is not None else "N/A",
                "status": yield_curve_status,
            },
            "sp500": {
                "name": "S&P 500 지수",
                "value": sp500_now,
                "prev_week": sp500_prev,
                "wow_pct": sp500_wow,
                "unit": "pt",
                "formatted": f"{sp500_now:,.1f}pt" if sp500_now else "N/A",
                "status": "미국 대형주 대표 성장 지표",
            },
            "wti": {
                "name": "WTI 원유 선물",
                "value": wti_now,
                "prev_week": wti_prev,
                "wow_pct": wti_wow,
                "unit": "달러/배럴",
                "formatted": f"${wti_now:,.2f}" if wti_now else "N/A",
                "status": "에너지 공급 인플레이션 지표",
            },
            "gold": {
                "name": "국제 금 선물",
                "value": gold_now,
                "prev_week": gold_prev,
                "wow_pct": gold_wow,
                "unit": "달러/온스",
                "formatted": f"${gold_now:,.1f}" if gold_now else "N/A",
                "status": "실물 안전자산 & 통화가치 헤지",
            },
            "kospi": {
                "name": "KOSPI 지수",
                "value": kospi_now,
                "prev_week": kospi_prev,
                "wow_pct": kospi_wow,
                "unit": "pt",
                "formatted": f"{kospi_now:,.1f}pt" if kospi_now else "N/A",
                "status": kospi_status_str,
                "band": kospi_band,
            },
            "kosdaq": {
                "name": "KOSDAQ 지수",
                "value": kosdaq_now,
                "prev_week": kosdaq_prev,
                "wow_pct": kosdaq_wow,
                "unit": "pt",
                "formatted": f"{kosdaq_now:,.1f}pt" if kosdaq_now else "N/A",
                "status": "국내 중소형 성장주 지표",
            },
            "kr10y": {
                "name": "한국 국고채 10년물 금리",
                "value": kr10y_now,
                "prev_week": kr10y_prev,
                "wow_pct": kr10y_wow,
                "unit": "%",
                "formatted": f"{kr10y_now:.2f}%" if kr10y_now is not None else "N/A",
                "status": "국내 장기금리 및 채권 벤치마크",
                "band": kr10y_band,
            },
            "kr_us_spread": {
                "name": "한-미 10년물 금리차 (US 10Y - KR 10Y)",
                "value": kr_us_spread_now,
                "prev_week": kr_us_spread_prev,
                "delta_bp": kr_us_spread_delta_bp,
                "unit": "%p",
                "formatted": f"{kr_us_spread_now:+.2f}%p" if kr_us_spread_now is not None else "N/A",
                "status": spread_status,
            },
            "kr_short": {
                "name": "국내 단기채/단기자금 ETF (153130)",
                "value": kr_short_now,
                "prev_week": kr_short_prev,
                "wow_pct": kr_short_wow,
                "unit": "원",
                "formatted": f"{kr_short_now:,.0f}원" if kr_short_now else "N/A",
                "status": "IRP 안전자산 및 단기 파킹 지표",
            },
        }

        return {
            "as_of_date": now_kst.strftime("%Y-%m-%d"),
            "indicators": indicators,
            "fx_rule_status": fx_status,
            "fx_rate": usdkrw_now or 1400.0,
            "fx_band": fx_band,
            "us10y_band": us10y_band,
        }

    def _build_macro_table(
        self,
        indicators: Dict[str, Any],
        fx_band: Optional[Dict[str, Any]] = None,
        us10y_band: Optional[Dict[str, Any]] = None,
        asset_quant_list: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """글로벌 및 국내 지표와 3개월 롤링 동적 밴드, 7대 자산 퀀트 순위표를 Markdown 대시보드로 포맷팅합니다."""
        lines = [
            "### 🌐 [글로벌 거시경제 & 원자재 지표]",
            "| 글로벌 지표 | 현재값 | 1주 전 대비 (WoW) | 상태 및 시장 시사점 |",
            "|:---|:---:|:---:|:---|",
        ]

        # 1. 환율
        fx = indicators["usdkrw"]
        wow_fx_str = f"{fx['wow_pct']:+.2f}%" if fx["wow_pct"] is not None else "-"
        lines.append(f"| 💵 **{fx['name']}** | `{fx['formatted']}` | `{wow_fx_str}` | {fx['status']} |")

        # 2. 미국 10년물
        u10 = indicators["us10y"]
        wow_u10_str = f"{u10['wow_pct']:+.2f}%" if u10["wow_pct"] is not None else "-"
        lines.append(f"| 📈 **{u10['name']}** | `{u10['formatted']}` | `{wow_u10_str}` | {u10['status']} |")

        # 3. 미국 2년물
        u2 = indicators["us2y"]
        wow_u2_str = f"{u2['wow_pct']:+.2f}%" if u2["wow_pct"] is not None else "-"
        lines.append(f"| 📉 **{u2['name']}** | `{u2['formatted']}` | `{wow_u2_str}` | {u2['status']} |")

        # 4. 미국 장단기 금리차
        ts = indicators["term_spread_10y_2y"]
        bp_str = f"{ts['delta_bp']:+.1f}bp" if ts.get("delta_bp") is not None else "-"
        lines.append(f"| ⚖️ **{ts['name']}** | `{ts['formatted']}` | `{bp_str}` | {ts['status']} |")

        # 5. S&P 500
        sp = indicators["sp500"]
        wow_sp_str = f"{sp['wow_pct']:+.2f}%" if sp["wow_pct"] is not None else "-"
        lines.append(f"| 🇺🇸 **{sp['name']}** | `{sp['formatted']}` | `{wow_sp_str}` | {sp['status']} |")

        # 6. WTI 유가
        wti = indicators["wti"]
        wow_wti_str = f"{wti['wow_pct']:+.2f}%" if wti["wow_pct"] is not None else "-"
        lines.append(f"| 🛢️ **{wti['name']}** | `{wti['formatted']}` | `{wow_wti_str}` | {wti['status']} |")

        # 7. 금
        gold = indicators["gold"]
        wow_gold_str = f"{gold['wow_pct']:+.2f}%" if gold["wow_pct"] is not None else "-"
        lines.append(f"| 🥇 **{gold['name']}** | `{gold['formatted']}` | `{wow_gold_str}` | {gold['status']} |")

        # 국내 시장 지표 테이블
        lines.append("")
        lines.append("### 🇰🇷 [국내(한국) 시장 & 금리·채권 지표]")
        lines.append("| 국내 지표 | 현재값 | 1주 전 대비 (WoW) | 상태 및 시장 시사점 |")
        lines.append("|:---|:---:|:---:|:---|")

        # 8. KOSPI
        ks = indicators["kospi"]
        wow_ks_str = f"{ks['wow_pct']:+.2f}%" if ks["wow_pct"] is not None else "-"
        lines.append(f"| 🇰🇷 **{ks['name']}** | `{ks['formatted']}` | `{wow_ks_str}` | {ks['status']} |")

        # 9. KOSDAQ
        kq = indicators["kosdaq"]
        wow_kq_str = f"{kq['wow_pct']:+.2f}%" if kq["wow_pct"] is not None else "-"
        lines.append(f"| 🇰🇷 **{kq['name']}** | `{kq['formatted']}` | `{wow_kq_str}` | {kq['status']} |")

        # 10. 한국 국고채 10년물
        kr10 = indicators["kr10y"]
        wow_kr10_str = f"{kr10['wow_pct']:+.2f}%" if kr10["wow_pct"] is not None else "-"
        lines.append(f"| 📊 **{kr10['name']}** | `{kr10['formatted']}` | `{wow_kr10_str}` | {kr10['status']} |")

        # 11. 한-미 금리차
        ku = indicators["kr_us_spread"]
        ku_bp_str = f"{ku['delta_bp']:+.1f}bp" if ku.get("delta_bp") is not None else "-"
        lines.append(f"| 🌐 **{ku['name']}** | `{ku['formatted']}` | `{ku_bp_str}` | {ku['status']} |")

        # 12. 단기채권 ETF
        ks_short = indicators["kr_short"]
        wow_kshort_str = f"{ks_short['wow_pct']:+.2f}%" if ks_short["wow_pct"] is not None else "-"
        lines.append(f"| 🛡️ **{ks_short['name']}** | `{ks_short['formatted']}` | `{wow_kshort_str}` | {ks_short['status']} |")

        # 동적 3개월 롤링 퀀타일 밴드 요약
        if fx_band or us10y_band:
            lines.append("")
            lines.append("### 📐 [최근 3개월(60영업일) 롤링 동적 통계 밴드]")
            if fx_band:
                lines.append(
                    f"- 💵 **USD/KRW 환율**: 현재 `{fx_band['current']:,.1f}원` (**3개월 백분위: 하위 {fx_band['pct_rank']:.1f}%**) | "
                    f"저환율(Q25) `{fx_band['q25']:,.1f}원` / 중앙값(Q50) `{fx_band['q50']:,.1f}원` / 고환율(Q75) `{fx_band['q75']:,.1f}원`"
                )
            if us10y_band:
                lines.append(
                    f"- 📈 **미국 10년물 국채금리**: 현재 `{us10y_band['current']:.2f}%` (**3개월 백분위: 하위 {us10y_band['pct_rank']:.1f}%**) | "
                    f"저금리(Q25) `{us10y_band['q25']:.2f}%` / 중앙값(Q50) `{us10y_band['q50']:.2f}%` / 고금리(Q75) `{us10y_band['q75']:.2f}%`"
                )

        # 7대 자산군 듀얼 모멘텀 & 200일선 추세 순위표
        if asset_quant_list:
            lines.append("")
            lines.append("### 💎 [K-올라운드 7대 자산군 듀얼 모멘텀 & 200일선 추세 순위표]")
            lines.append("| 순위 | 7대 자산군 | 대표 ETF | 현재가 | 200일선 | 200MA 추세 | 12M 모멘텀 | 52주 낙폭 | 60일 변동성 |")
            lines.append("|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|")
            for item in asset_quant_list:
                rank_str = f"**{item['rank']}위**" if item['rank'] <= 3 else f"{item['rank']}위"
                unit = item.get("unit", "")
                if unit == "원":
                    p_str = f"{item['current_price']:,.0f}원" if item['current_price'] else "N/A"
                    ma_str = f"{item['ma200']:,.0f}원" if item['ma200'] else "N/A"
                else:
                    p_str = f"${item['current_price']:,.2f}" if item['current_price'] else "N/A"
                    ma_str = f"${item['ma200']:,.2f}" if item['ma200'] else "N/A"
                
                mom_str = f"{item['momentum_12m']:+.1f}%"
                dd_str = f"{item['drawdown_52w']:+.1f}%"
                vol_str = f"{item['volatility_60d']:.1f}%"
                lines.append(f"| {rank_str} | **{item['name']}** | `{item['ticker']}` | `{p_str}` | `{ma_str}` | {item['trend']} | `{mom_str}` | `{dd_str}` | `{vol_str}` |")

        return "\n".join(lines)


if __name__ == "__main__":
    service = MacroService()
    res = service.get_macro_snapshot()
    print("\n" + res["macro_table_markdown"])



