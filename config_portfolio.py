# -*- coding: utf-8 -*-
"""
config_portfolio.py
===================
한국 거주자 맞춤형 한-미 듀얼 올웨더(Dual All-Weather) 자산배분 기준,
계좌별 절세 특성 규칙(삼성이전 소득공제 기적용 vs 미래연금 미적용 등),
종목/ETF 자산군 자동 분류 규칙(Aliases), 및 Google Gemini 2.5 Flash 기반 진단 프롬프트를 정의합니다.
"""

from typing import Any, Dict, List, Tuple
from notion_utils import is_kr_ticker

# ==============================================================================
# 1. 한국 거주자 맞춤형 한-미 듀얼 올웨더 목표 비중 정의
# ==============================================================================
# - 주식(30%): 미국주식 20%(환노출) + 한국주식 10%
# - 채권(45%): 미국장기채 30%(환노출) + 국내중기채/단기/현금 15%
# - 실물/인플레(25%): 금 12.5% + 원자재/달러 12.5%
# - 총합: 100.0%
DUAL_ALL_WEATHER_CONFIG: Dict[str, Dict[str, Any]] = {
    "US_EQUITY": {
        "code": "US_EQUITY",
        "name": "미국주식",
        "target_pct": 20.0,
        "currency_exposure": "USD (환노출)",
        "role": "글로벌 혁신 성장 및 자본 이득 주도 (달러 자산)",
        "color": "blue",
    },
    "KR_EQUITY": {
        "code": "KR_EQUITY",
        "name": "한국주식",
        "target_pct": 10.0,
        "currency_exposure": "KRW (원화)",
        "role": "국내 주력 산업(반도체, AI인프라, 조선, 방산 등) 알파 추구",
        "color": "green",
    },
    "US_LONG_BOND": {
        "code": "US_LONG_BOND",
        "name": "미국장기채",
        "target_pct": 30.0,
        "currency_exposure": "USD (환노출)",
        "role": "경제 침체/디플레이션 방어 및 위기 시 달러 급등 헤지 효과",
        "color": "purple",
    },
    "KR_MED_SHORT_BOND_CASH": {
        "code": "KR_MED_SHORT_BOND_CASH",
        "name": "국내중기채/단기/현금",
        "target_pct": 15.0,
        "currency_exposure": "KRW (원화)",
        "role": "원화 유동성 확보, 변동성 완충 및 리밸런싱 실탄 역할",
        "color": "yellow",
    },
    "GOLD": {
        "code": "GOLD",
        "name": "금",
        "target_pct": 12.5,
        "currency_exposure": "Gold / USD",
        "role": "화폐 가치 하락(스태그플레이션) 및 지정학적 위기 헤지",
        "color": "orange",
    },
    "COMMODITY_USD": {
        "code": "COMMODITY_USD",
        "name": "원자재/달러",
        "target_pct": 12.5,
        "currency_exposure": "USD / Real Assets",
        "role": "공급망 충격 및 인플레이션 가속화 방어",
        "color": "red",
    },
}

# 자산군 순서
ASSET_ORDER: List[str] = [
    "US_EQUITY",
    "KR_EQUITY",
    "US_LONG_BOND",
    "KR_MED_SHORT_BOND_CASH",
    "GOLD",
    "COMMODITY_USD",
]


# ==============================================================================
# 2. 종목 및 ETF 자산군 지능형 분류(Aliases) 엔진
# ==============================================================================
# 금(GOLD) 관련 키워드
GOLD_KEYWORDS = [
    "KRX금", "금현물", "골드", "GOLD", "IAU", "GLD", "SGOL", "BAR",
    "금선물", "골드선물", "금은", "SILVER", "은선물", "ACE KRX금"
]

# 미국장기채(US_LONG_BOND) 관련 키워드
US_LONG_BOND_KEYWORDS = [
    "미국30년", "미국20년", "미국채30년", "미국채20년", "미국채 30년", "미국채 20년",
    "TLT", "TMF", "VGLT", "EDV", "SPTL", "ZROZ", "미국장기채", "미국채장기",
    "미국국채30년", "미국30년국채", "미국채30년액티브"
]

# 국내채권/단기채/현금(KR_MED_SHORT_BOND_CASH) 관련 키워드
KR_BOND_CASH_KEYWORDS = [
    "CD금리", "KOFR", "머니마켓", "MMF", "단기채", "국고채", "종합채권", "단기사채",
    "중기채", "단기자금", "원화RP", "외화RP", "예치금", "예탁금", "SOFR", "CASH",
    "현금", "초단기", "국채10년", "국채3년", "국채5년", "통안채", "회사채",
    "KODEX CD금리", "KODEX KOFR", "TIGER CD금리", "TIGER KOFR", "PLUS 단기", "ACE 단기"
]

# 원자재/달러(COMMODITY_USD) 관련 키워드
COMMODITY_USD_KEYWORDS = [
    "원자재", "구리", "원유", "WTI", "BRENT", "천연가스", "농산물", "곡물", "광물",
    "달러선물", "달러레버리지", "달러인덱스", "DBC", "GSG", "USO", "UNG", "UUP",
    "CPER", "DBA", "DBB", "COMEX", "블룸버그원자재", "미국달러선물"
]

# 미국/글로벌 주식 식별 키워드
US_GLOBAL_EQUITY_KEYWORDS = [
    "미국", "나스닥", "S&P", "S&P500", "다우", "필라델피아", "빅테크", "글로벌",
    "FANG", "TOP7", "QQQ", "SPY", "VOO", "IVV", "SCHD", "VTI", "SOXX", "SMH",
    "엔비디아", "애플", "마이크로소프트", "구글", "알파벳", "메타", "테슬라",
    "브로드컴", "아마존", "팔란티어", "AMD", "TSMC", "ASML", "퀄컴", "마이크론",
    "글로벌AI", "글로벌HBM", "미국AI", "미국우주항공"
]

# 한국/국내 명시 키워드
KR_EXPLICIT_KEYWORDS = [
    "코리아", "KOREA", "K-", "KOSPI", "KOSDAQ", "코스피", "코스닥", "KRX", "국내"
]


def classify_asset(
    name: str,
    ticker: str = "",
    market: str = "",
    country: str = "",
    custom_portfolio: str = "",
    custom_selection: str = ""
) -> Tuple[str, str]:
    """
    종목명, 티커, 마켓, 국가, 사용자정의 포트폴리오/선택 태그를 결합하여
    한-미 듀얼 올웨더 6대 자산군으로 정밀 분류합니다.
    (자산군 코드, 자산군 한글명) 튜플을 반환합니다.
    """
    n = (name or "").strip()
    n_upper = n.upper()
    t_upper = (ticker or "").strip().upper()
    m_upper = (market or "").strip().upper()
    country_upper = (country or "").strip().upper()
    port_upper = (custom_portfolio or "").strip().upper()
    sel_upper = (custom_selection or "").strip().upper()
    combined = f"{n_upper} {t_upper} {m_upper} {country_upper} {port_upper} {sel_upper}"

    # 1. 금 (GOLD) 판별 (태그: '금' 또는 금 관련 키워드)
    if sel_upper == "금" or any(kw.upper() in combined for kw in GOLD_KEYWORDS):
        return "GOLD", DUAL_ALL_WEATHER_CONFIG["GOLD"]["name"]

    # 2. 채권군 판별
    # 2-1. 미국장기채 (태그: '채권' & (미국/30년/TLT) 또는 미국장기채 키워드)
    if (sel_upper == "채권" and ("미국" in combined or "30" in combined or "TLT" in combined)) or any(kw.upper() in combined for kw in US_LONG_BOND_KEYWORDS):
        return "US_LONG_BOND", DUAL_ALL_WEATHER_CONFIG["US_LONG_BOND"]["name"]

    # 2-2. 국내채권 / 단기자금 / 현금 (태그: '금리', '단기', '현금' 또는 국내채권 키워드)
    if sel_upper in ("금리", "단기", "현금", "CASH") or any(kw.upper() in combined for kw in KR_BOND_CASH_KEYWORDS):
        return "KR_MED_SHORT_BOND_CASH", DUAL_ALL_WEATHER_CONFIG["KR_MED_SHORT_BOND_CASH"]["name"]

    # 3. 원자재 / 달러 (COMMODITY_USD) 판별
    if sel_upper in ("원자재", "원유", "달러", "달러선물") or any(kw.upper() in combined for kw in COMMODITY_USD_KEYWORDS):
        return "COMMODITY_USD", DUAL_ALL_WEATHER_CONFIG["COMMODITY_USD"]["name"]

    # 4. 주식군 판별 (미국/해외 vs 한국)
    # 4-1. 노션 '국가' 속성이 명시된 경우 최우선 적용
    if country_upper in ("미국", "글로벌", "US", "GLOBAL"):
        return "US_EQUITY", DUAL_ALL_WEATHER_CONFIG["US_EQUITY"]["name"]
    elif country_upper in ("한국", "KR", "KOREA"):
        return "KR_EQUITY", DUAL_ALL_WEATHER_CONFIG["KR_EQUITY"]["name"]

    # 4-2. 해외 거래소 직접 상장 티커 (해외 주식)
    if t_upper and not is_kr_ticker(t_upper):
        return "US_EQUITY", DUAL_ALL_WEATHER_CONFIG["US_EQUITY"]["name"]

    # 4-3. 마켓(Market) 속성에 따른 분류
    if m_upper in ("US", "NASDAQ", "NYSE", "AMEX"):
        return "US_EQUITY", DUAL_ALL_WEATHER_CONFIG["US_EQUITY"]["name"]
    elif m_upper in ("KRX", "KOSPI", "KOSDAQ", "KONEX"):
        # 국내 상장 ETF 중 해외 지수 추종 여부 판별
        if any(kw.upper() in n_upper for kw in US_GLOBAL_EQUITY_KEYWORDS):
            return "US_EQUITY", DUAL_ALL_WEATHER_CONFIG["US_EQUITY"]["name"]
        return "KR_EQUITY", DUAL_ALL_WEATHER_CONFIG["KR_EQUITY"]["name"]

    # 4-4. 종목명 키워드 기반 분류
    if any(kw.upper() in n_upper for kw in US_GLOBAL_EQUITY_KEYWORDS):
        return "US_EQUITY", DUAL_ALL_WEATHER_CONFIG["US_EQUITY"]["name"]
    if any(kw.upper() in n_upper for kw in KR_EXPLICIT_KEYWORDS):
        return "KR_EQUITY", DUAL_ALL_WEATHER_CONFIG["KR_EQUITY"]["name"]

    # 기본값: 미국/글로벌 주식
    return "US_EQUITY", DUAL_ALL_WEATHER_CONFIG["US_EQUITY"]["name"]


# ==============================================================================
# 3. Gemini 2.5 Flash 자산배분 진단 프롬프트 시스템
# ==============================================================================
GEMINI_MODEL_NAME = "gemini-2.5-flash"
GEMINI_FALLBACK_MODEL = "gemini-2.0-flash"

SYSTEM_PROMPT = """당신은 세계적인 헤지펀드(Bridgewater Associates 스타일)의 수석 포트폴리오 매니저이자 자산배분 전문가입니다.
한국 거주자 개인 투자자를 위한 [한-미 듀얼 올웨더(Dual All-Weather) 포트폴리오] 자산배분 진단 및 리밸런싱 리포트를 작성하는 것이 당신의 임무입니다.

### [한-미 듀얼 올웨더 포트폴리오 기준 비중]
1. 미국주식 (US Stocks, 환노출): 20.0%
2. 한국주식 (KR Stocks, 원화): 10.0%
3. 미국장기채 (US Long-Term Bonds, 환노출): 30.0%
4. 국내중기채/단기/현금 (KR Intermediate & Short Bonds / Cash): 15.0%
5. 금 (Gold / KRX금현물): 12.5%
6. 원자재/달러 (Commodities & USD): 12.5%
(합계: 100.0%)

### [계좌별 세제 특성 & 절세 가이드라인 (반드시 인지 및 적용)]
1. **삼성이전 (연금저축보험 계약이전 계좌 - 과거 소득공제/세액공제 기적용 원금)**:
   - **과세 특성**: 과거 소득공제/세액공제 혜택을 받은 원금이므로, 중도 인출 시 **16.5% 기타소득세** 부과. 만 55세 이후 연금 수령(3.3%~5.5% 저율과세)까지 장기 유지 필수.
   - **올웨더 운용 전략**: 매매차익 및 분배금의 **15.4% 배당소득세 과세이연 복리 효과 극대화**. 분배금이 지속 발생하는 **미국장기채(ACE 미국30년국채액티브 등), 배당/인컴 ETF(TIGER 미국배당다우존스), CD금리/단기채** 편입에 최적.
2. **미래연금 (`미래에셋`, 소득공제/세액공제 미적용 연금계좌)**:
   - **과세 특성**: 세액공제를 받지 않은 비과세 원금으로, **언제든지 16.5% 세금 없이 자유롭게 원금 인출 가능**한 뛰어난 유동성 보유.
   - **올웨더 운용 전략**: 비과세 원금의 인출 유동성을 유지하면서, 과세이연 혜택을 누릴 수 있는 고수익/고배당 자산 또는 유연한 리밸런싱 완충 계좌로 활용.
3. **삼성ISA (중개형 ISA)**:
   - **과세 특성**: 손익통산 후 200만원(서민형 400만원)까지 비과세, 초과분 9.9% 분리과세. 3년 의무납입 후 비과세 혜택 실현.
   - **올웨더 운용 전략**: 국내 상장 해외주식형 ETF(S&P500, 나스닥100, 반도체) 및 국내 성장주 매매에 최적.
4. **삼성IRP**:
   - **과세 특성**: 연간 세액공제 혜택(최대 900만원 한도 합산), 55세 이후 연금 수령 시 저율과세(3.3~5.5%).
   - **올웨더 운용 전략**: 안전자산 30% 의무 편입 규정을 고려하여 **미국장기채 / 단기채 / 금현물 ETF** 편입.
5. **일반 위탁계좌**:
   - 해외 직투 주식(양도세 연 250만 공제 후 22% 분리과세) 또는 국내 개별주(매매차익 비과세) 운용.

### [보고서 작성 핵심 원칙 & 문체 가이드라인]
1. **철저한 보고서 개조식 문체 준수 (필수)**:
   - 모든 문장은 반드시 **명사형 종결어미(`~함`, `~임`, `~필요`, `~권고`, `~유지`, `~확보`, `~축소`, `~상태`)**로 간결하게 끝맺으세요.
   - **절대 금지 어미**: `~합니다`, `~있습니다`, `~해야 합니다`, `~보입니다`, `~바랍니다`, `~머물러 있습니다` 등 설명조/대화체/문어체 서술을 일체 금지합니다.
2. **HTML 태그 사용 절대 금지**:
   - 마크다운 표(Table) 셀 내부를 포함하여 리포트 어디에도 `<br>`, `<br/>`, `<b>`, `<p>` 등의 HTML 태그를 쓰지 마세요.
   - 표 셀 안에서 구분은 슬래시(`/`), 쉼표(`, `), 또는 간결한 단문으로 표현하세요.
3. **5대 핵심 표(Table) 중심 요약**:
   - 각 표의 상/하단에는 **핵심 시사점 2~3개 불릿(`* `)**만 명사형 종결로 압축 요약하세요.
4. **완결성 보장**:
   - 모든 섹션과 표를 빠짐없이 끝까지 완전하게 작성하여 문장이 도중에 잘리는 일이 없도록 하세요.
"""

USER_PROMPT_TEMPLATE = """아래 제공된 [실시간 포트폴리오 자산배분 및 다차원 퀀트 데이터]를 바탕으로, 한-미 듀얼 올웨더 기준 정밀 진단 리포트를 작성해 주세요.

## 📊 현재 포트폴리오 요약 데이터
- **분석 기준 일시**: {analysis_date}
- **총 평가 자산**: {total_eval_krw:,.0f} 원 (주식/ETF: {stock_total_krw:,.0f}원 + 현금 예수금: {cash_total_krw:,.0f}원)
- **보유 종목 수**: {total_positions_count} 개 종목 (관심/모니터링: {monitoring_count}개)

### ⏱️ [0. 전주 대비(WoW) 주간 자산 추적 데이터]
{prev_report_summary_text}

### 🏦 [1. 투자 계좌별 자산 및 현금 현황]
{account_summary_text}

### 🏷️ [2. 포트폴리오 테마/섹터별 비중]
{theme_summary_text}

### 📊 [3. 올웨더 6대 자산군 목표 대비 괴리율]
{asset_summary_table}

### 🔍 [4. 자산군별 상세 보유 종목 및 퀀트 지표 (52주위치, 안전마진, 투자가이드)]
{holdings_detail_text}

---

## 📝 리포트 작성 요구사항 (반드시 아래 5대 표 중심 구조 준수, 모든 문장은 명사형 종결어미(~함/~임/~필요/~권고)로 작성)

### 1. 📈 [표 0] 전주 대비 자산 현황 & 리밸런싱 이행 점검표
| 구분 | 전주 | 금주 | 주간 증감 (Delta) | 주간 평가 및 시사점 |
|:---|:---:|:---:|:---:|:---|
*(총 평가자산, 현금 비중, 올웨더 적합도, 지난주 권고 조치 이행 여부 비교)*
* (표 하단 핵심 시사점 2개 불릿: 명사형 종결)

### 2. 🌟 포트폴리오 총평 & 자산배분 건전성 요약
- **올웨더 적합도 점수**: **XX점** / 100점 (판정: 🔴 리밸런싱 시급 / 🟡 주의 / 🟢 최적)
- **핵심 진단**: (한 줄 명사형 개조식 요약)
- **핵심 강점**:
  1. (강점 1: 명사형 종결)
  2. (강점 2: 명사형 종결)
- **핵심 취약점**:
  1. (취약점 1: 명사형 종결)
  2. (취약점 2: 명사형 종결)
  3. (취약점 3: 명사형 종결)

### 3. 📊 [표 1] 한-미 듀얼 올웨더 목표 비중 vs 현재 비중 정밀 분석표
| 자산군 | 목표비중(%) | 현재평가액(원) | 현재비중(%) | 괴리율(%p) | 상태 | 리밸런싱 필요금액(원) |
|:---|:---:|:---:|:---:|:---|:---:|:---:|
*(6대 자산군 전수 비교)*
* (표 하단 핵심 시사점 2개 불릿: 명사형 종결)

### 4. 🔍 [표 2] 주요 보유 종목 퀀트 진단 & 리스크 평가표
| 종목명 | 자산군 | 포트폴리오 테마 | 평가금액(원) | 비중(%) | 52주위치 | 안전마진 / 투자가이드 | 진단 및 권고 |
|:---|:---:|:---:|:---:|:---:|:---:|:---|:---|
*(주요 종목별 52주 위치, 가이드 기반 진단의견 요약)*
* (표 하단 핵심 시사점 2개 불릿: 명사형 종결)

### 5. 🎯 [표 3] 계좌별 절세 & 리밸런싱 실행 액션 플랜표
| 계좌명 | 목표 자산군 | 추천 ETF / 편입 종목 | 목표 조치 | 실행 우선순위 | 절세 혜택 활용 팁 (삼성이전: 과세이연 극대화 / 미래연금: 비과세 인출 유동성 등) |
|:---|:---:|:---|:---:|:---:|:---|
*(삼성이전, 미래연금, 삼성ISA, 삼성IRP, 일반계좌별 추천 ETF 및 긴급/점진적 분할 매매 플랜 - HTML 태그 없이 슬래시(/) 사용)*
* (표 하단 핵심 실행 요약 2~3개 불릿: 명사형 종결)

### 6. 💡 [표 4] 거시경제(Macro) 환경 연계 대응표
| 경제 변수 | 현재 상태 | 포트폴리오 영향 | 올웨더 전술적 조치 |
|:---|:---|:---|:---|
*(한미 금리차, 환율, 인플레이션/지정학 변수 대응 가이드 - HTML 태그 없이 슬래시(/) 사용)*
* (표 하단 거시경제 총평 1~2개 불릿: 완전한 문장으로 명사형 종결)
"""
