# -*- coding: utf-8 -*-
"""
config_portfolio.py
===================
한국 거주자 맞춤형 「K-올라운드 마스터 (K-All-Round Master)」 자산배분 기준,
환율 구간별 전술적 매크로 룰, 계좌별 절세 특성 규칙(삼성이전 vs 미래연금 vs ISA vs IRP),
종목/ETF 7대 자산군 지능형 분류 규칙(Aliases), 및 Google Gemini 기반 진단 프롬프트를 정의합니다.
"""

from typing import Any, Dict, List, Tuple
from notion_utils import is_kr_ticker


# ==============================================================================
# 1. 단일 통합 전략: 「K-올라운드 마스터 (K-All-Round Master)」 7대 자산군 목표 비중
# ==============================================================================
# - 주식(50.0%): 미국 대표지수 25.0% + 한·미 배당성장 15.0% + 국내 주식&밸류업 10.0%
# - 채권(30.0%): 미국 장기채 20.0% + 국내 채권&단기자금 10.0%
# - 실물/현금(20.0%): 금 10.0% + 원자재 & 달러/현금 10.0%
# - 총합: 100.0% (통화 노출: USD 65.0% : KRW 35.0%)
# - 리밸런싱 임계치: 목표 비중 대비 ±3.0%p 이상 괴리 시 조치 권고

K_ALL_ROUND_MASTER_CONFIG: Dict[str, Dict[str, Any]] = {
    "US_CORE_INDEX": {
        "code": "US_CORE_INDEX",
        "name": "미국 대표지수",
        "target_pct": 25.0,
        "currency_exposure": "USD (환노출)",
        "role": "S&P500, 나스닥 등 글로벌 1등 기업 성장 견인 및 자본 이득",
        "preferred_accounts": ["연금저축", "IRP", "일반위탁"],
        "color": "blue",
    },
    "DIVIDEND_GROWTH": {
        "code": "DIVIDEND_GROWTH",
        "name": "한·미 배당성장",
        "target_pct": 15.0,
        "currency_exposure": "USD / KRW",
        "role": "SCHD, 미국배당다우존스, K-배당 등 지속적 현금 인컴 창출 및 복리 재투자",
        "preferred_accounts": ["ISA", "연금저축(삼성이전)"],
        "color": "teal",
    },
    "KR_EQUITY": {
        "code": "KR_EQUITY",
        "name": "국내 주식 & 밸류업",
        "target_pct": 10.0,
        "currency_exposure": "KRW (원화)",
        "role": "코스피200, 반도체/AI인프라, 저PBR 밸류업 등 저평가 사이클 알파 매매",
        "preferred_accounts": ["일반위탁", "ISA"],
        "color": "green",
    },
    "US_LONG_BOND": {
        "code": "US_LONG_BOND",
        "name": "미국 장기채",
        "target_pct": 20.0,
        "currency_exposure": "USD (환노출)",
        "role": "미국 20~30년 국채, 경제 위기/금리인하 시 자본차익 및 환율 급등 버퍼",
        "preferred_accounts": ["연금저축", "IRP"],
        "color": "purple",
    },
    "KR_BOND_SHORT": {
        "code": "KR_BOND_SHORT",
        "name": "국내 채권 & 단기자금",
        "target_pct": 10.0,
        "currency_exposure": "KRW (원화)",
        "role": "국고채, 단기채, KOFR, CD금리, IRP 안전자산 30% 충족 및 변동성 완충",
        "preferred_accounts": ["IRP", "일반위탁"],
        "color": "yellow",
    },
    "GOLD": {
        "code": "GOLD",
        "name": "금 (Gold)",
        "target_pct": 10.0,
        "currency_exposure": "Gold / USD",
        "role": "KRX 금현물, 글로벌 금, 화폐 가치 하락 및 지정학적 리스크 절대 헤지",
        "preferred_accounts": ["일반위탁(KRX금)", "연금저축", "IRP"],
        "color": "orange",
    },
    "COMMODITY_CASH": {
        "code": "COMMODITY_CASH",
        "name": "원자재 & 달러/현금",
        "target_pct": 10.0,
        "currency_exposure": "USD / KRW",
        "role": "원자재(원유, 구리), 달러 예수금, 공급발 인플레 방어 및 저가매수 실탄",
        "preferred_accounts": ["일반위탁", "연금저축"],
        "color": "red",
    },
}

# 하위 호환성용 별칭
TARGET_ALLOCATION = K_ALL_ROUND_MASTER_CONFIG
DUAL_ALL_WEATHER_CONFIG = K_ALL_ROUND_MASTER_CONFIG

# 7대 자산군 표준 정렬 순서
ASSET_ORDER: List[str] = [
    "US_CORE_INDEX",
    "DIVIDEND_GROWTH",
    "KR_EQUITY",
    "US_LONG_BOND",
    "KR_BOND_SHORT",
    "GOLD",
    "COMMODITY_CASH",
]

# 리밸런싱 임계치 (±3.0%p)
REBALANCING_DRIFT_THRESHOLD_PCT = 3.0


# ==============================================================================
# 2. 환율 및 금리 동적 통계 밴드 룰 (3개월 롤링 퀀타일 기반 자율 적응)
# ==============================================================================
FX_MACRO_RULES = {
    "DYNAMIC_METHOD": "Rolling 60-Trading-Days Quantile Band (Q25 / Q50 / Q75)",
    "LOW_FX": {
        "regime": "LOW (하위 25% 이하)",
        "guideline": "환율이 최근 3개월 중 통계적 저평가 바닥권(Q25 이하)에 위치하므로 미국 대표지수(S&P500, 나스닥) 및 미국 장기채(환노출) 분할 매수를 적극 확대하여 달러 자산 비중 축적.",
        "action_tag": "저환율-달러자산확대",
    },
    "NEUTRAL_FX": {
        "regime": "NEUTRAL (하위 25% ~ 상위 25%)",
        "guideline": "환율이 최근 3개월의 정상 밸런스 밴드(Q25~Q75)에 위치하므로, K-올라운드 마스터 7대 자산군 목표 비중 괴리율(±3.0%p)에 따라 기계적 정석 분할 매수 실행.",
        "action_tag": "중립환율-괴리율기반매매",
    },
    "HIGH_FX": {
        "regime": "HIGH (상위 25% 이상)",
        "guideline": "환율이 최근 3개월 중 통계적 고평가 상단(Q75 이상)에 위치하므로 미국 환노출(USD) 자산의 추가 매수를 자제하고, 저평가된 국내 주식/밸류업, 원화 채권, 금(KRX금현물)을 우선 매수하여 환차손 위험 방어.",
        "action_tag": "고환율-원화/실물우선",
    },
}


# ==============================================================================
# 3. 7대 자산군 지능형 분류(Aliases) 엔진
# ==============================================================================
# 1. 금 (GOLD)
GOLD_KEYWORDS = [
    "KRX금", "금현물", "골드", "GOLD", "IAU", "GLD", "SGOL", "BAR",
    "금선물", "골드선물", "금은", "SILVER", "은선물", "ACE KRX금", "ACE KRX금현물"
]

# 2. 국내 채권 & 단기자금 (KR_BOND_SHORT)
KR_BOND_SHORT_KEYWORDS = [
    "단기채", "국고채3년", "국고채1년", "KOFR", "CD금리", "머니마켓", "MMF",
    "단기자금", "단기채권", "종합채권", "국채선물3년", "KIS국고채3년",
    "KODEX 단기채권", "TIGER 단기채권", "KBSTAR 단기채권", "ACE 단기채권",
    "SOL 단기채권", "KODEX KOFR금리액티브", "TIGER KOFR금리액티브",
    "KODEX CD금리액티브", "TIGER CD금리액티브", "153130"
]

# 3. 미국 장기채 (US_LONG_BOND)
US_LONG_BOND_KEYWORDS = [
    "미국채30년", "미국채20년", "미국채10년", "미국장기국채", "미국30년국채",
    "미국채10년액티브", "TLT", "TMF", "SPTLL", "TLH", "EDV", "ZROZ",
    "ACE 미국30년국채액티브", "TIGER 미국30년국채프리미엄", "KODEX 미국30년국채액티브",
    "SOL 미국30년국채액티브", "KBSTAR 미국30년국채액티브", "미국채 30년",
]

# 4. 국내 주식 & 밸류업 (KR_EQUITY)
KR_EQUITY_KEYWORDS = [
    "KODEX 200", "TIGER 200", "KBSTAR 200", "코스피200", "KOSPI200", "코스닥150",
    "KODEX 코스닥150", "TIGER 코스닥150", "밸류업", "K-밸류업", "기업밸류업",
    "저PBR", "KODEX 은행", "TIGER 은행", "KBSTAR 금융채", "현대차", "삼성전자",
    "SK하이닉스", "LG에너지솔루션", "삼성바이오로직스", "기아", "KB금융", "신한지주",
    "삼성물산", "POSCO홀딩스", "NAVER", "카카오", "셀트리온", "코스피", "코스닥"
]

# 5. 한·미 배당성장 (DIVIDEND_GROWTH)
DIVIDEND_GROWTH_KEYWORDS = [
    "SCHD", "미국배당다우존스", "TIGER 미국배당다우존스", "ACE 미국배당다우존스",
    "SOL 미국배당다우존스", "KODEX 미국배당다우존스", "KBSTAR 미국배당다우존스",
    "DGRO", "VIG", "NOBL", "DVY", "배당성장", "미국배당", "K-배당", "고배당",
    "ARIRANG 고배당주", "KBSTAR 고배당", "TIGER 미국배당+7%프리미엄다우존스"
]

# 6. 미국 대표지수 (US_CORE_INDEX)
US_CORE_INDEX_KEYWORDS = [
    "S&P500", "S&P 500", "SPY", "VOO", "IVV", "SPLG", "나스닥100", "NASDAQ100",
    "QQQ", "QQQM", "TIGER 미국S&P500", "ACE 미국S&P500", "KODEX 미국S&P500TR",
    "SOL 미국S&P500", "KBSTAR 미국S&P500", "TIGER 미국나스닥100", "ACE 미국나스닥100",
    "KODEX 미국나스닥100TR", "SOL 미국나스닥100", "다우존스", "DIA", "VTI",
    "미국S&P500", "미국나스닥", "미국주식"
]

# 7. 원자재 & 달러/현금 (COMMODITY_CASH)
COMMODITY_CASH_KEYWORDS = [
    "원자재", "구리", "원유", "WTI", "달러", "달러선물", "USD", "DBC", "GSG", "USO",
    "BNO", "CPER", "TIGER 원유선물", "KODEX WTI원유선물", "KODEX 구리선물",
    "TIGER 구리선물", "KODEX 미국달러선물", "달러예수금", "현금", "예수금"
]

# 국가 식별 키워드
KR_EXPLICIT_KEYWORDS = ["한국", "국내", "KOREA", "KOSPI", "KOSDAQ", "KRX", "코스피", "코스닥"]


def classify_asset(
    name: str,
    ticker: str = "",
    market: str = "",
    country: str = "",
    custom_portfolio: str = "",
    custom_selection: str = ""
) -> Tuple[str, str]:
    """
    종목명, 티커, 마켓, 국가, 노션 커스텀 속성을 다각도로 검사하여
    K-올라운드 마스터 7대 자산군 코드(Code)와 표시 이름(Name)을 반환합니다.
    """
    n_upper = name.upper()
    t_upper = ticker.upper()
    m_upper = market.upper()
    c_upper = country.upper()
    port_upper = custom_portfolio.upper()
    sel_upper = custom_selection.upper()

    # 1. 노션 '포트폴리오' / '선택' 커스텀 속성 최우선 반영
    if any(kw in port_upper or kw in sel_upper for kw in ["미국대표", "US_CORE", "CORE", "S&P", "나스닥"]):
        return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]
    if any(kw in port_upper or kw in sel_upper for kw in ["배당", "DIVIDEND", "SCHD"]):
        return "DIVIDEND_GROWTH", K_ALL_ROUND_MASTER_CONFIG["DIVIDEND_GROWTH"]["name"]
    if any(kw in port_upper or kw in sel_upper for kw in ["국내주식", "밸류업", "KR_EQUITY", "VALUE"]):
        return "KR_EQUITY", K_ALL_ROUND_MASTER_CONFIG["KR_EQUITY"]["name"]
    if any(kw in port_upper or kw in sel_upper for kw in ["미국채", "장기채", "US_BOND", "LONG_BOND"]):
        return "US_LONG_BOND", K_ALL_ROUND_MASTER_CONFIG["US_LONG_BOND"]["name"]
    if any(kw in port_upper or kw in sel_upper for kw in ["단기채", "단기자금", "KR_BOND", "CASH_KRW"]):
        return "KR_BOND_SHORT", K_ALL_ROUND_MASTER_CONFIG["KR_BOND_SHORT"]["name"]
    if any(kw in port_upper or kw in sel_upper for kw in ["금", "GOLD", "금현물"]):
        return "GOLD", K_ALL_ROUND_MASTER_CONFIG["GOLD"]["name"]
    if any(kw in port_upper or kw in sel_upper for kw in ["원자재", "COMMODITY", "달러", "USD"]):
        return "COMMODITY_CASH", K_ALL_ROUND_MASTER_CONFIG["COMMODITY_CASH"]["name"]

    # 2. 금 (Gold)
    if any(kw.upper() in n_upper or kw.upper() == t_upper for kw in GOLD_KEYWORDS):
        return "GOLD", K_ALL_ROUND_MASTER_CONFIG["GOLD"]["name"]

    # 3. 미국 장기채 (US Long Bond)
    if any(kw.upper() in n_upper or kw.upper() == t_upper for kw in US_LONG_BOND_KEYWORDS):
        return "US_LONG_BOND", K_ALL_ROUND_MASTER_CONFIG["US_LONG_BOND"]["name"]

    # 4. 국내 채권 및 단기자금 (KR Bond / Cash)
    if any(kw.upper() in n_upper or kw.upper() == t_upper for kw in KR_BOND_SHORT_KEYWORDS):
        return "KR_BOND_SHORT", K_ALL_ROUND_MASTER_CONFIG["KR_BOND_SHORT"]["name"]

    # 5. 한·미 배당성장 (Dividend Growth)
    if any(kw.upper() in n_upper or kw.upper() == t_upper for kw in DIVIDEND_GROWTH_KEYWORDS):
        return "DIVIDEND_GROWTH", K_ALL_ROUND_MASTER_CONFIG["DIVIDEND_GROWTH"]["name"]

    # 6. 원자재 & 달러/현금 (Commodity & Cash)
    if any(kw.upper() in n_upper or kw.upper() == t_upper for kw in COMMODITY_CASH_KEYWORDS):
        return "COMMODITY_CASH", K_ALL_ROUND_MASTER_CONFIG["COMMODITY_CASH"]["name"]

    # 7. 국가/마켓 기반 국내 주식 vs 미국 주식 분류
    # 7-1. 국가 속성 기준
    if c_upper in ("미국", "USA", "US"):
        return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]
    if c_upper in ("한국", "KOR", "KR", "KOREA"):
        if any(kw.upper() in n_upper for kw in US_CORE_INDEX_KEYWORDS):
            return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]
        return "KR_EQUITY", K_ALL_ROUND_MASTER_CONFIG["KR_EQUITY"]["name"]

    # 7-2. 티커 기준 (숫자 6자리 -> 한국 ETF/주식)
    if ticker.isdigit() and len(ticker) == 6:
        if any(kw.upper() in n_upper for kw in US_CORE_INDEX_KEYWORDS):
            return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]
        if any(kw.upper() in n_upper for kw in KR_EQUITY_KEYWORDS):
            return "KR_EQUITY", K_ALL_ROUND_MASTER_CONFIG["KR_EQUITY"]["name"]
        return "KR_EQUITY", K_ALL_ROUND_MASTER_CONFIG["KR_EQUITY"]["name"]

    # 7-3. 마켓 속성 기준
    if m_upper in ("US", "NASDAQ", "NYSE", "AMEX"):
        return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]
    if m_upper in ("KRX", "KOSPI", "KOSDAQ", "KONEX"):
        if any(kw.upper() in n_upper for kw in US_CORE_INDEX_KEYWORDS):
            return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]
        return "KR_EQUITY", K_ALL_ROUND_MASTER_CONFIG["KR_EQUITY"]["name"]

    # 7-4. 키워드 기반 분류
    if any(kw.upper() in n_upper for kw in US_CORE_INDEX_KEYWORDS):
        return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]
    if any(kw.upper() in n_upper for kw in KR_EXPLICIT_KEYWORDS):
        return "KR_EQUITY", K_ALL_ROUND_MASTER_CONFIG["KR_EQUITY"]["name"]

    # 기본값: 미국 대표지수
    return "US_CORE_INDEX", K_ALL_ROUND_MASTER_CONFIG["US_CORE_INDEX"]["name"]


# ==============================================================================
# 4. Google Gemini 4단계 지능형 모델 풀 (Model Pool) 및 프롬프트 시스템
# ==============================================================================
GEMINI_MODEL_POOL: List[str] = [
    "gemini-3.6-flash",
    "gemini-2.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-2.5-flash-lite",
]

GEMINI_MODEL_NAME = GEMINI_MODEL_POOL[0]
GEMINI_FALLBACK_MODEL = GEMINI_MODEL_POOL[1]


SYSTEM_PROMPT = """당신은 세계적인 헤지펀드(Bridgewater Associates & AQR 스타일)의 수석 포트폴리오 매니저이자 자산배분 전문가입니다.
한국 거주자 개인 투자자를 위한 [K-올라운드 마스터(K-All-Round Master)] 주간 자산배분 진단, 한·미 듀얼 실시간 매크로 분석, 및 리밸런싱 리포트를 작성하는 것이 당신의 임무입니다.

### [K-올라운드 마스터 단일 100% 자산배분 기준]
1. 미국 대표지수 (US Core Index, USD 환노출): 25.0% (S&P500, 나스닥 등 글로벌 성장 견인)
2. 한·미 배당성장 (Dividend Growth, USD/KRW): 15.0% (SCHD, 미국배당다우존스, K-배당 등 현금 인컴 창출 및 복리 재투자)
3. 국내 주식 & 밸류업 (KR Equity & Value-Up, KRW): 10.0% (코스피200, 반도체, 저PBR 등 저평가 사이클 알파 매매)
4. 미국 장기채 (US Long Bonds, USD 환노출): 20.0% (미국 20~30년 국채, 경제 위기/금리인하 시 완충 및 환율 버퍼)
5. 국내 채권 & 단기자금 (KR Bond & Cash, KRW): 10.0% (국고채, 단기채, KOFR, CD금리, IRP 안전자산 30% 충족)
6. 금 (Gold / KRX금현물, Real Asset): 10.0% (화폐가치 하락 및 지정학적 위기 절대 헤지)
7. 원자재 & 달러/현금 (Commodities & USD/Cash): 10.0% (공급망 충격 인플레 방어 및 저가매수 실탄)
(합계: 100.0% / 통화 비중: USD 65% : KRW 35% / 리밸런싱 임계치: 목표 대비 ±3.0%p 이상 괴리 시 조치)

### [환율 및 금리 동적 통계 밴드(3개월 롤링 퀀타일) 기반 전술 가이드라인]
1. **저환율 기회 구간 (최근 3개월 하위 25% Q25 이하)**:
   - 환율이 통계적 저평가 바닥권에 위치하므로 미국 대표지수(S&P500, 나스닥) 및 미국 장기채(환노출) 분할 매수 적극 확대.
2. **중립 적정 구간 (최근 3개월 Q25 ~ Q75, 중간 50%)**:
   - 환율이 정상 밸런스 밴드에 위치하므로 7대 자산군 목표 비중 괴리율(±3.0%p)에 따라 정석 분할 매수.
3. **고환율 경계 구간 (최근 3개월 상위 25% Q75 이상)**:
   - 환율이 통계적 고평가 상단에 위치하므로 미국 환노출 자산 추가 매수를 자제하고, 저평가된 국내 주식/밸류업, 원화 채권, 금(KRX금현물) 우선 매수.
4. **미국 10년물 금리 고금리 구간 (Q75 이상)**:
   - 금리 고점권(채권 가격 저평가)이므로 미국 장기채 분할 매수를 공격적으로 확대하여 향후 금리 인하 시 자본차익 및 이자수익 극대화.

### [한국 시장 & 글로벌 매크로 주간 점검 프레임워크 (필수 적용)]
1. **금주 동향 복기 (This Week Review)**:
   - **글로벌**: 미국 연준(Fed) 금리/통화정책 기조, 미국 10Y/2Y 국채금리 및 장단기 스프레드, 달러 인덱스, 국제유가 및 금 시세 주간 변동 원인.
   - **국내(한국)**: 한국은행 기준금리 및 금통위 스탠스, 한-미 금리차, 관세청 수출입 속보치(반도체/자동차 수출 증감률), 외국인/기관 주간 수급 동향, 코스피 밸류에이션(PBR/PER) 및 기업 밸류업 프로그램 동향.
2. **차주 전망 & 주요 경제 캘린더 (Next Week Outlook & Calendar - Google Search Grounding 활용)**:
   - **글로벌 핵심 일정**: 차주 예정된 FOMC 회의/의사록, 미국 CPI/PPI/PCE 물가지표, 고용보고서, 주요 빅테크 실적 발표 등.
   - **국내 핵심 일정**: 차주 한국은행 금통위, 한국 수출입 통계 발표, 코스피 대형주 실적 및 밸류업 공시, 선물옵션 만기 등.
3. **차주 전술적 자산배분 대응 전략**:
   - 차주 매크로 이벤트와 7대 자산군 괴리율을 결합하여, 다음 1주일간의 구체적인 분할매수/리밸런싱 우선순위 제시.

### [계좌별 세제 특성 & 절세 가이드라인 (반드시 인지 및 적용)]
1. **삼성이전 (연금저축보험 계약이전 계좌 - 소득공제 기적용 원금)**:
   - 중도 인출 시 16.5% 기타소득세 부과. 배당소득세(15.4%) 과세이연 복리 효과를 극대화해야 하므로 **배당성장 ETF(TIGER 미국배당다우존스 등), 미국장기채, 단기채** 편입에 최적.
2. **미래연금 (소득공제 미적용 연금계좌)**:
   - 비과세 원금으로 언제든지 16.5% 세금 없이 자유롭게 인출 가능. 과세이연을 누리면서도 유연한 리밸런싱 및 인출 완충 계좌로 활용.
3. **삼성ISA (중개형 ISA)**:
   - 200만~400만원 비과세 + 초과분 9.9% 분리과세. 국내 상장 해외주식형 ETF 및 배당성장주, 국내 성장주 운용에 최적.
4. **삼성IRP**:
   - 연간 900만 한도 세액공제 + 안전자산 30% 의무 규정. 미국장기채, 단기채, 금현물 ETF 편입 필수.
5. **일반 위탁계좌**:
   - 해외 직투 주식(연 250만 공제 후 22% 양도세) 및 KRX 금현물(매매차익 비과세) 운용.

### [보고서 작성 핵심 원칙 & 문체 가이드라인]
1. **철저한 보고서 개조식 문체 준수 (필수)**:
   - 모든 문장은 반드시 **명사형 종결어미(`~함`, `~임`, `~필요`, `~권고`, `~유지`, `~확보`, `~축소`, `~상태`)**로 간결하게 끝맺으세요.
   - **절대 금지 어미**: `~합니다`, `~있습니다`, `~해야 합니다`, `~보입니다`, `~바랍니다` 등 설명조/대화체 일체 금지.
2. **HTML 태그 사용 절대 금지**:
   - 마크다운 표(Table) 셀 내부를 포함하여 `<br>`, `<b>`, `<p>` 등의 HTML 태그를 일체 쓰지 마세요. 셀 내 구분은 슬래시(`/`), 쉼표(`, `)를 사용하세요.
3. **실시간 검색 기반 팩트체크**:
   - Google Search Grounding 도구를 활용하여 차주 글로벌 및 한국 시장 핵심 경제 캘린더/이벤트를 실시간 검색하여 반영하세요.
4. **완결성 보장**:
   - 모든 섹션과 표를 빠짐없이 끝까지 완전하게 작성하세요.
"""


USER_PROMPT_TEMPLATE = """아래 제공된 [실시간 글로벌 & 국내 매크로 지표]와 [포트폴리오 자산배분 데이터]를 바탕으로, 「K-올라운드 마스터」 기준 주간 정밀 진단 리포트를 작성해 주세요.

## 🌐 1. 실시간 글로벌 & 🇰🇷 국내 매크로 정량 지표 스냅샷 (기준일: {as_of_date})
{macro_table_markdown}
- **환율 전술 가이드**: {fx_rule_status} (현재 환율: {fx_rate:,.1f}원)

## 📊 2. 현재 포트폴리오 요약 데이터
- **분석 기준 일시**: {analysis_date}
- **총 평가 자산**: {total_eval_krw:,.0f} 원 (주식/ETF: {stock_total_krw:,.0f}원 + 현금 예수금: {cash_total_krw:,.0f}원)
- **보유 종목 수**: {total_positions_count} 개 종목 (관심/모니터링: {monitoring_count}개)

### ⏱️ [전주 대비 주간 자산 추적]
{prev_report_summary_text}

### 🏦 [투자 계좌별 현황]
{account_summary_text}

### 🏷️ [포트폴리오 테마/섹터별 비중]
{theme_summary_text}

### 📊 [K-올라운드 7대 자산군 목표 대비 괴리율 (임계치: ±3.0%p)]
{asset_summary_table}

### 🔍 [자산군별 상세 보유 종목 및 퀀트 지표 (52주위치, 안전마진, 투자가이드)]
{holdings_detail_text}

---

## 📝 리포트 작성 요구사항 (반드시 아래 구조 준수, 모든 문장은 명사형 종결어미(~함/~임/~필요/~권고)로 작성)

### 1. 🌐 글로벌 & 🇰🇷 국내 매크로 주간 브리핑 (Google Search 기반)

#### 📌 1) 금주 매크로 & 시장 복기 (This Week in Review)
- **글로벌 (연준 금리/환율/원자재)**: (미국 국채금리, 연준 정책, 달러 및 유가/금 동향 요약: 명사형 종결)
- **국내 (수출/한은/수급/밸류업)**: (한국은행 금리, 수출입 속보치, 외인/기관 수급, 코스피 밸류에이션 요약: 명사형 종결)

#### 🔮 2) 차주 주요 경제 캘린더 & 시장 전망 (Next Week Outlook)
- **글로벌 핵심 일정 & 영향**: (차주 예정된 FOMC/CPI/실적 등 주요 일정 및 시장 파급효과: 명사형 종결)
- **국내 핵심 일정 & 관전 포인트**: (차주 예정된 한은 금통위/수출통계/밸류업 공시 등 주요 일정: 명사형 종결)
- **차주 전술적 매크로 대응 가이드**: (차주 이벤트 및 환율({fx_rate:,.1f}원) 대비 주간 행동 지침: 명사형 종결)

### 2. 🌟 K-올라운드 포트폴리오 총평 & 자산배분 건전성
- **올라운드 적합도 점수**: **XX점** / 100점 (판정: 🔴 리밸런싱 시급 / 🟡 주의 / 🟢 최적)
- **핵심 진단**: (한 줄 명사형 요약)
- **핵심 강점 2가지**:
  1. (강점 1: 명사형 종결)
  2. (강점 2: 명사형 종결)
- **핵심 취약점 2가지**:
  1. (취약점 1: 명사형 종결)
  2. (취약점 2: 명사형 종결)

### 3. 📊 [표 1] K-올라운드 7대 자산군 비중 vs 목표 괴리율 점검표
| 자산군 | 목표비중(%) | 현재평가액(원) | 현재비중(%) | 괴리율(%p) | 상태 (적정/과다/부족) | 리밸런싱 필요금액(원) |
|:---|:---:|:---:|:---:|:---|:---:|:---:|
*(7대 자산군 전수 비교, ±3.0%p 초과 시 과다/부족 판정)*
* (표 하단 핵심 시사점 2개 불릿: 명사형 종결)

### 4. 🔍 [표 2] 주요 보유 종목 퀀트 진단 & 리스크 평가표
| 종목명 | 자산군 | 포트폴리오 테마 | 평가금액(원) | 비중(%) | 52주위치 | 안전마진 / 투자가이드 | 진단 및 권고 |
|:---|:---:|:---:|:---:|:---|:---:|:---|:---|
*(주요 종목별 52주 위치, 투자가이드 기반 진단의견 요약 - HTML 태그 없이 작성)*
* (표 하단 종목 리스크 요약 2개 불릿: 명사형 종결)

### 5. 🎯 [표 3] 계좌별 절세 & 리밸런싱 실행 액션 플랜표
| 계좌명 | 목표 자산군 | 추천 ETF / 편입 종목 | 목표 조치 | 실행 우선순위 | 계좌별 절세 팁 |
|:---|:---:|:---|:---:|:---|:---|
*(삼성이전, 미래연금, 삼성ISA, 삼성IRP, 일반계좌별 추천 ETF 및 긴급/점진적 분할 매매 플랜)*
* (표 하단 실행 가이드 2개 불릿: 명사형 종결)

### 6. ✅ 차주 실행 체크리스트 (3대 핵심 액션)
1. **신규 적립금 투입 우선순위 (환율 {fx_rate:,.1f}원 연계)**: (현재 환율 구간 및 차주 일정에 맞추어 어느 계좌의 어떤 자산군에 신규 자금을 우선 투입할지 구체적 명시: 명사형 종결)
2. **비중 과다 자산 리밸런싱/차익실현**: (괴리율 +3.0%p 초과 자산 조치 가이드: 명사형 종결)
3. **취약 자산군 보강**: (괴리율 -3.0%p 미달 자산 보강 플랜: 명사형 종결)
"""
