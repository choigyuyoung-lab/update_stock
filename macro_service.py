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
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

logger = logging.getLogger("MacroService")


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
            import FinanceDataReader as fdr
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

    def get_macro_snapshot(self) -> Dict[str, Any]:
        """
        글로벌 & 국내 핵심 매크로 지표를 실시간 수집하고
        최근 3개월(60영업일) 롤링 백분위수 기반 '동적 통계 밴드'를 자동 계산하여 반환합니다.
        """
        now_kst = self._get_kst_now()
        start_date = (now_kst - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
        print(f"🌐 [MacroService] 실시간 글로벌 & 국내 매크로 지표 및 동적 밴드 수집 시작 (기준일: {now_kst.strftime('%Y-%m-%d')})...")

        # 1. USD/KRW 환율
        usdkrw_df = self._fetch_series("USD/KRW", start_date)
        usdkrw_now, usdkrw_prev, usdkrw_wow = self._extract_latest_and_prev_week(usdkrw_df)
        fx_band = self._calculate_dynamic_band(usdkrw_df, window=60)
        if usdkrw_now is None and fx_band:
            usdkrw_now = fx_band["current"]

        # 2. 미국 10년물 국채 금리 (FRED:DGS10)
        us10y_df = self._fetch_series("FRED:DGS10", start_date)
        us10y_now, us10y_prev, us10y_wow = self._extract_latest_and_prev_week(us10y_df, "DGS10")
        us10y_band = self._calculate_dynamic_band(us10y_df, val_col="DGS10", window=60)
        if us10y_now is None:
            us10y_df = self._fetch_series("US10YT", start_date)
            us10y_now, us10y_prev, us10y_wow = self._extract_latest_and_prev_week(us10y_df)
            us10y_band = self._calculate_dynamic_band(us10y_df, window=60)

        # 3. 미국 2년물 국채 금리 (FRED:DGS2)
        us2y_df = self._fetch_series("FRED:DGS2", start_date)
        us2y_now, us2y_prev, us2y_wow = self._extract_latest_and_prev_week(us2y_df, "DGS2")
        us2y_band = self._calculate_dynamic_band(us2y_df, val_col="DGS2", window=60)

        # 4. 장단기 금리차 (10Y - 2Y)
        term_spread_now = None
        term_spread_prev = None
        term_spread_delta_bp = None
        if us10y_now is not None and us2y_now is not None:
            term_spread_now = us10y_now - us2y_now
            if us10y_prev is not None and us2y_prev is not None:
                term_spread_prev = us10y_prev - us2y_prev
                term_spread_delta_bp = (term_spread_now - term_spread_prev) * 100.0  # bp
            else:
                term_spread_delta_bp = 0.0

        # 5. S&P 500
        sp500_df = self._fetch_series("US500", start_date)
        if sp500_df is None or sp500_df.empty:
            sp500_df = self._fetch_series("SPY", start_date)
        sp500_now, sp500_prev, sp500_wow = self._extract_latest_and_prev_week(sp500_df)

        # 6. KOSPI
        kospi_df = self._fetch_series("KS11", start_date)
        kospi_now, kospi_prev, kospi_wow = self._extract_latest_and_prev_week(kospi_df)
        kospi_band = self._calculate_dynamic_band(kospi_df, window=60)

        # 7. WTI 원유 (CL=F / FRED:DCOILWTICO)
        wti_df = self._fetch_series("CL=F", start_date)
        if wti_df is None or wti_df.empty:
            wti_df = self._fetch_series("FRED:DCOILWTICO", start_date)
        wti_now, wti_prev, wti_wow = self._extract_latest_and_prev_week(wti_df)

        # 8. 금 선물 (GC=F)
        gold_df = self._fetch_series("GC=F", start_date)
        gold_now, gold_prev, gold_wow = self._extract_latest_and_prev_week(gold_df)

        # 9. KOSDAQ 지수 (KQ11)
        kosdaq_df = self._fetch_series("KQ11", start_date)
        kosdaq_now, kosdaq_prev, kosdaq_wow = self._extract_latest_and_prev_week(kosdaq_df)

        # 10. 한국 국고채 10년물 금리 (FRED:IRLTLT01KRM156N 또는 KODEX 국고채10년 ETF 114820)
        kr10y_df = self._fetch_series("FRED:IRLTLT01KRM156N", start_date)
        kr10y_now, kr10y_prev, kr10y_wow = self._extract_latest_and_prev_week(kr10y_df, "IRLTLT01KRM156N")
        kr10y_band = self._calculate_dynamic_band(kr10y_df, val_col="IRLTLT01KRM156N", window=60)
        if kr10y_now is None:
            kr10y_df = self._fetch_series("114820", start_date)
            _, _, kr10y_wow = self._extract_latest_and_prev_week(kr10y_df)
            kr10y_band = self._calculate_dynamic_band(kr10y_df, window=60)
            kr10y_now = 4.18  # 표준 기준치

        # 11. 한-미 10년물 금리차 (미국 10Y - 한국 10Y)
        kr_us_spread_now = None
        kr_us_spread_prev = None
        kr_us_spread_delta_bp = None
        if us10y_now is not None and kr10y_now is not None:
            kr_us_spread_now = us10y_now - kr10y_now
            if us10y_prev is not None and kr10y_prev is not None:
                kr_us_spread_prev = us10y_prev - kr10y_prev
                kr_us_spread_delta_bp = (kr_us_spread_now - kr_us_spread_prev) * 100.0
            else:
                kr_us_spread_delta_bp = 0.0

        # 12. 국내 단기자금/채권 지표 (KODEX 단기채권PLUS 153130)
        kr_short_df = self._fetch_series("153130", start_date)
        kr_short_now, kr_short_prev, kr_short_wow = self._extract_latest_and_prev_week(kr_short_df)

        # ----------------------------------------------------------------------
        # 💡 [동적 통계 밴드 기반 전술 가이드 자동 산출]
        # ----------------------------------------------------------------------
        # 1) 환율 3개월 롤링 퀀타일 전술 가이드
        if fx_band:
            q25_f = fx_band["q25"]
            q75_f = fx_band["q75"]
            pct_f = fx_band["pct_rank"]
            if fx_band["regime"] == "LOW":
                fx_status = f"저환율 기회 구간 (3개월 하위 {pct_f:.0f}%, {q25_f:,.1f}원 이하) -> 미국 환노출(S&P500/나스닥/미국장기채) 분할 매수 적극 확대"
                fx_badge = f"🟢 저환율 (하위 {pct_f:.0f}%)"
            elif fx_band["regime"] == "HIGH":
                fx_status = f"고환율 경계 구간 (3개월 상위 {100-pct_f:.0f}%, {q75_f:,.1f}원 이상) -> 미국 환노출 매수 자제, 국내 주식/원화채권/금(KRX) 우선 매수"
                fx_badge = f"🔴 고환율 (상위 {100-pct_f:.0f}%)"
            else:
                fx_status = f"중립 적정 구간 ({q25_f:,.1f}~{q75_f:,.1f}원, 3개월 {pct_f:.0f}% 위치) -> 7대 자산군 목표 괴리율에 따른 정석 분할 매수"
                fx_badge = f"🟡 중립 ({pct_f:.0f}% 위치)"
        else:
            # 안전 폴백
            fx_status = "환율 중립 구간 -> 목표 비중 괴리율에 따른 정석 분할 매수"
            fx_badge = "🟡 중립 구간"

        # 2) 미국 10년물 금리 3개월 롤링 퀀타일 상태 분석
        if us10y_band:
            q25_u = us10y_band["q25"]
            q75_u = us10y_band["q75"]
            pct_u = us10y_band["pct_rank"]
            if us10y_band["regime"] == "HIGH":
                us10y_status_str = f"고금리 매수기회 (상위 {100-pct_u:.0f}%, {q75_u:.2f}% 이상) -> 미국 장기채 분할 매수 최적기 (채권 가격 저평가 + 자본차익 기대)"
            elif us10y_band["regime"] == "LOW":
                us10y_status_str = f"저금리 구간 (하위 {pct_u:.0f}%, {q25_u:.2f}% 이하) -> 채권 신규매수 감속 및 주식 비중 확대"
            else:
                us10y_status_str = f"중립 금리 밴드 ({q25_u:.2f}~{q75_u:.2f}%, 3개월 {pct_u:.0f}% 위치)"
        else:
            us10y_status_str = "글로벌 무위험 기준금리"

        # 3) 미국 장단기 금리차 상태 분석
        if term_spread_now is not None:
            if term_spread_now < 0:
                yield_curve_status = "장단기 금리 역전 (경기 침체 선행 경계)"
            elif term_spread_now < 0.2:
                yield_curve_status = "수익률 곡선 평탄화 (Flattener)"
            else:
                yield_curve_status = "정상 우상향 수익률 곡선 (Steepener)"
        else:
            yield_curve_status = "산출 대기"

        # 4) 한-미 금리차 상태 분석
        if kr_us_spread_now is not None:
            if kr_us_spread_now > 1.0:
                spread_status = f"한-미 금리 역전 심화 (+{kr_us_spread_now:.2f}%p) -> 원화 약세/외인 수급 변동성 주의"
            else:
                spread_status = f"한-미 금리차 ({kr_us_spread_now:+.2f}%p) -> 안정적 자금 흐름 유지"
        else:
            spread_status = "산출 대기"

        # 5) 코스피 3개월 퀀타일 위치 분석
        if kospi_band:
            pct_k = kospi_band["pct_rank"]
            if kospi_band["regime"] == "LOW":
                kospi_status_str = f"3개월 저평가 (하위 {pct_k:.0f}%) -> 국내 밸류업 ETF 저가 분할매수 적기"
            elif kospi_band["regime"] == "HIGH":
                kospi_status_str = f"3개월 단기 과열 (상위 {100-pct_k:.0f}%) -> 차익실현 및 비중 유지"
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

        # 정량 지표 마크다운 표 빌드
        macro_table = self._build_macro_table(indicators, fx_band, us10y_band)

        print(f"   ✅ [MacroService] 글로벌 & 국내 핵심 매크로 지표 수집 완료 (환율: {indicators['usdkrw']['formatted']}, 미10Y: {indicators['us10y']['formatted']}, 코스피: {indicators['kospi']['formatted']})")

        return {
            "as_of_date": now_kst.strftime("%Y-%m-%d"),
            "indicators": indicators,
            "fx_rule_status": fx_status,
            "fx_rate": usdkrw_now or 1400.0,
            "fx_band": fx_band,
            "us10y_band": us10y_band,
            "macro_table_markdown": macro_table,
        }

    def _build_macro_table(
        self,
        indicators: Dict[str, Any],
        fx_band: Optional[Dict[str, Any]] = None,
        us10y_band: Optional[Dict[str, Any]] = None
    ) -> str:
        """글로벌 및 국내 지표와 3개월 롤링 동적 밴드를 Markdown 대시보드로 포맷팅합니다."""
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

        return "\n".join(lines)


if __name__ == "__main__":
    service = MacroService()
    res = service.get_macro_snapshot()
    print("\n" + res["macro_table_markdown"])


