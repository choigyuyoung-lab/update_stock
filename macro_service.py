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

    def __init__(self, lookback_days: int = 40):
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
            # 컬럼 결정
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

            # 약 5~7 영업일 전 값 추출 (데이터 길이에 따라 인덱스 선택)
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

    def get_macro_snapshot(self) -> Dict[str, Any]:
        """
        글로벌 8대 핵심 매크로 지표를 실시간 수집하여 구조화된 딕셔너리와 Markdown 요약표를 반환합니다.
        
        수집 대상:
        1. USD/KRW 환율 (원/달러)
        2. 미국 국채 10년물 금리 (US 10Y Yield, FRED:DGS10)
        3. 미국 국채 2년물 금리 (US 2Y Yield, FRED:DGS2)
        4. 장단기 금리차 (10Y - 2Y Term Spread)
        5. S&P 500 지수 (US500 / ^GSPC)
        6. KOSPI 지수 (KS11 / 코스피)
        7. WTI 원유 선물 (CL=F / FRED:DCOILWTICO)
        8. 금 선물 (GC=F / Gold)
        """
        now_kst = self._get_kst_now()
        start_date = (now_kst - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
        print(f"🌐 [MacroService] 실시간 글로벌 매크로 지표 수집 시작 (기준일: {now_kst.strftime('%Y-%m-%d')})...")

        # 1. USD/KRW 환율
        usdkrw_df = self._fetch_series("USD/KRW", start_date)
        usdkrw_now, usdkrw_prev, usdkrw_wow = self._extract_latest_and_prev_week(usdkrw_df)
        if usdkrw_now is None:
            # 폴백
            usdkrw_now, usdkrw_prev, usdkrw_wow = 1410.0, 1405.0, 0.35

        # 2. 미국 10년물 국채 금리 (FRED:DGS10)
        us10y_df = self._fetch_series("FRED:DGS10", start_date)
        us10y_now, us10y_prev, us10y_wow = self._extract_latest_and_prev_week(us10y_df, "DGS10")
        if us10y_now is None:
            us10y_df = self._fetch_series("US10YT", start_date)
            us10y_now, us10y_prev, us10y_wow = self._extract_latest_and_prev_week(us10y_df)

        # 3. 미국 2년물 국채 금리 (FRED:DGS2)
        us2y_df = self._fetch_series("FRED:DGS2", start_date)
        us2y_now, us2y_prev, us2y_wow = self._extract_latest_and_prev_week(us2y_df, "DGS2")

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

        # 7. WTI 원유 (CL=F / FRED:DCOILWTICO)
        wti_df = self._fetch_series("CL=F", start_date)
        if wti_df is None or wti_df.empty:
            wti_df = self._fetch_series("FRED:DCOILWTICO", start_date)
        wti_now, wti_prev, wti_wow = self._extract_latest_and_prev_week(wti_df)

        # 8. 금 선물 (GC=F)
        gold_df = self._fetch_series("GC=F", start_date)
        gold_now, gold_prev, gold_wow = self._extract_latest_and_prev_week(gold_df)

        # 환율 구간 분석
        if usdkrw_now >= 1380.0:
            fx_status = "고환율 (1,380원 이상) -> 미국 환노출 매수 자제, 국내 주식/원화채권/금 우선"
            fx_badge = "🔴 고환율 경계"
        elif usdkrw_now <= 1300.0:
            fx_status = "저환율 (1,300원 이하) -> 미국 대표지수 및 미국 장기채(환노출) 분할 매수 확대"
            fx_badge = "🟢 저환율 기회"
        else:
            fx_status = "중립환율 (1,300~1,380원) -> 목표 비중 괴리율에 따른 정석 분할 매수"
            fx_badge = "🟡 중립 구간"

        # 장단기 금리차 상태 분석
        if term_spread_now is not None:
            if term_spread_now < 0:
                yield_curve_status = "장단기 금리 역전 상태 (경기 침체 선행 지표 경계)"
            elif term_spread_now < 0.2:
                yield_curve_status = "금리차 축소 상태 (수익률 곡선 평탄화 Flattener)"
            else:
                yield_curve_status = "정상 우상향 수익률 곡선 (Steepener)"
        else:
            yield_curve_status = "금리차 데이터 산출 중"

        indicators: Dict[str, Any] = {
            "usdkrw": {
                "name": "USD/KRW 환율",
                "value": usdkrw_now,
                "prev_week": usdkrw_prev,
                "wow_pct": usdkrw_wow,
                "unit": "원",
                "formatted": f"{usdkrw_now:,.1f}원" if usdkrw_now else "N/A",
                "status": fx_badge,
            },
            "us10y": {
                "name": "미국 10년물 국채금리",
                "value": us10y_now,
                "prev_week": us10y_prev,
                "wow_pct": us10y_wow,
                "unit": "%",
                "formatted": f"{us10y_now:.2f}%" if us10y_now is not None else "N/A",
                "status": "글로벌 무위험 기준금리",
            },
            "us2y": {
                "name": "미국 2년물 국채금리",
                "value": us2y_now,
                "prev_week": us2y_prev,
                "wow_pct": us2y_wow,
                "unit": "%",
                "formatted": f"{us2y_now:.2f}%" if us2y_now is not None else "N/A",
                "status": "연준 정책금리 민감 지표",
            },
            "term_spread_10y_2y": {
                "name": "장단기 금리차 (10Y-2Y)",
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
            "kospi": {
                "name": "KOSPI 지수",
                "value": kospi_now,
                "prev_week": kospi_prev,
                "wow_pct": kospi_wow,
                "unit": "pt",
                "formatted": f"{kospi_now:,.1f}pt" if kospi_now else "N/A",
                "status": "국내 주식 밸류업 시장 지표",
            },
            "wti": {
                "name": "WTI 원유",
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
                "status": "실물 안전자산 및 통화가치 헤지 지표",
            },
        }

        # 정량 지표 마크다운 표 빌드
        macro_table = self._build_macro_table(indicators)

        print(f"   ✅ [MacroService] 8대 매크로 수집 완료 (환율: {indicators['usdkrw']['formatted']}, 10년금리: {indicators['us10y']['formatted']}, 금리차: {indicators['term_spread_10y_2y']['formatted']})")

        return {
            "as_of_date": now_kst.strftime("%Y-%m-%d"),
            "indicators": indicators,
            "fx_rule_status": fx_status,
            "fx_rate": usdkrw_now or 1400.0,
            "macro_table_markdown": macro_table,
        }

    def _build_macro_table(self, indicators: Dict[str, Any]) -> str:
        """8대 지표를 보기 쉬운 Markdown 표로 포맷팅합니다."""
        lines = [
            "| 매크로 지표 | 현재값 | 1주 전 대비 (WoW) | 시장 상태 및 시사점 |",
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

        # 4. 장단기 금리차
        ts = indicators["term_spread_10y_2y"]
        bp_str = f"{ts['delta_bp']:+.1f}bp" if ts.get("delta_bp") is not None else "-"
        lines.append(f"| ⚖️ **{ts['name']}** | `{ts['formatted']}` | `{bp_str}` | {ts['status']} |")

        # 5. S&P 500
        sp = indicators["sp500"]
        wow_sp_str = f"{sp['wow_pct']:+.2f}%" if sp["wow_pct"] is not None else "-"
        lines.append(f"| 🇺🇸 **{sp['name']}** | `{sp['formatted']}` | `{wow_sp_str}` | {sp['status']} |")

        # 6. KOSPI
        ks = indicators["kospi"]
        wow_ks_str = f"{ks['wow_pct']:+.2f}%" if ks["wow_pct"] is not None else "-"
        lines.append(f"| 🇰🇷 **{ks['name']}** | `{ks['formatted']}` | `{wow_ks_str}` | {ks['status']} |")

        # 7. WTI 유가
        wti = indicators["wti"]
        wow_wti_str = f"{wti['wow_pct']:+.2f}%" if wti["wow_pct"] is not None else "-"
        lines.append(f"| 🛢️ **{wti['name']}** | `{wti['formatted']}` | `{wow_wti_str}` | {wti['status']} |")

        # 8. 금
        gold = indicators["gold"]
        wow_gold_str = f"{gold['wow_pct']:+.2f}%" if gold["wow_pct"] is not None else "-"
        lines.append(f"| 🥇 **{gold['name']}** | `{gold['formatted']}` | `{wow_gold_str}` | {gold['status']} |")

        return "\n".join(lines)


if __name__ == "__main__":
    service = MacroService()
    res = service.get_macro_snapshot()
    print("\n" + res["macro_table_markdown"])
