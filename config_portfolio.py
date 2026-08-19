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


# 별칭 호환성 제공
classify_asset_group = classify_asset


# 6대 계좌별 고유 성격 및 정책
ACCOUNT_POLICIES: Dict[str, Dict[str, Any]] = {
    "삼성연금": {
        "name": "삼성연금 (연금저축)",
        "type": "연금저축펀드",
        "tax_deduction_target": 6_000_000,
        "tax_type": "세액공제 대상",
        "description": "KODEX 미국S&P500 단일 종목 주 10만원 적립 + 연말 일시금 완납. 매도 리밸런싱 금지.",
    },
    "미래연금": {
        "name": "미래연금 (미래에셋)",
        "type": "연금저축펀드",
        "tax_deduction_target": 0,
        "tax_type": "소득공제 미적용 (자유 인출 가능)",
        "description": "TIGER 미국S&P500(10만) + KODEX 미국나스닥100(10만) 주 20만원 규칙 적립. 매도 리밸런싱 금지.",
    },
    "삼성IRP": {
        "name": "삼성IRP (개인형 IRP)",
        "type": "IRP",
        "tax_deduction_target": 3_000_000,
        "tax_type": "세액공제 대상 (안전자산 30% 의무)",
        "description": "위험자산 70%(테크TOP10) + 안전자산 30%(배당국채혼합/30년국채) 7:3 패키지 운용.",
    },
    "삼성이전": {
        "name": "연금이전 (삼성이전)",
        "type": "연금저축펀드",
        "tax_deduction_target": 0,
        "tax_type": "소득공제 기적용 원금",
        "description": "개별주식 매수 불가(100% ETF). 월배당 배당성장(40%) + 월배당 국채(30%) + 국내 테마 알파 ETF(30%). 분배금으로 저평가 ETF 수동 매수.",
    },
    "삼성ISA": {
        "name": "삼성ISA (중개형 ISA)",
        "type": "중개형 ISA",
        "tax_deduction_target": 0,
        "tax_type": "3년 비과세/분리과세",
        "description": "삼성전자 + 국내 상장 테마/섹터 ETF. 유일하게 적극적 교체매매(알파 스윙) 수행.",
    },
    "삼성종합": {
        "name": "삼성종합 (해외 직투)",
        "type": "일반위탁",
        "tax_deduction_target": 0,
        "tax_type": "해외주식 양도세/KRX금 비과세",
        "description": "미국 빅테크 개별주 직접투자 대기 (2027년 투입). 환율 저점 시 달러 환전.",
    },
}


# ==============================================================================
# 4. Google Gemini 4단계 지능형 모델 풀 (Model Pool) 및 프롬프트 시스템
# ==============================================================================
GEMINI_MODEL_POOL: List[str] = [
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
]

GEMINI_MODEL_NAME = GEMINI_MODEL_POOL[0]
GEMINI_FALLBACK_MODEL = GEMINI_MODEL_POOL[1]


SYSTEM_PROMPT = """당신은 대한민국 최고의 공인재무설계사(CFP)이자 정량 퀀트(Quant) 포트폴리오 매니저입니다.
당신의 임무는 사용자의 6대 개별 계좌의 고유 성격(세액공제 한도, 법적 제약, 적립 룰, 투자 목적)을 명확히 분리하여,
실행 불가능한 통합 리밸런싱이 아닌 **계좌별 핀포인트 맞춤 조언 및 주간 매수 추천 가이드**를 작성하는 것입니다.

### 🏛️ [6대 계좌별 고유 성격 및 조언 원칙 (절대 위반 금지)]
1. **🅰️ 삼성연금 (연금저축 - 연 600만원 세액공제 목표)**:
   - 종목: `KODEX 미국S&P500` 단일 종목 주 10만원 적립 + 연말 일시금 완납.
   - **조언 원칙**: **매도 리밸런싱 절대 금지**. 1년 600만원 세액공제 달성률 점검 및 주간 적립 지속성 확인.
2. **🅰️ 미래연금 (미래에셋 - 규칙 적립식)**:
   - 종목: `TIGER 미국S&P500` (10만) + `KODEX 미국나스닥100` (10만) 주간 적립.
   - **조언 원칙**: **매도 리밸런싱 절대 금지**. 2대 지수 수량 축적 및 평단가 관리 상태 점검.
3. **🛡️ 삼성IRP (개인형 IRP - 연 300만원 세액공제 목표)**:
   - 법적 규정: **안전자산 30% 이상 의무**, 위험자산 최대 70% 제한.
   - **조언 원칙**: 연 300만원 일시금 납입 시 2종목 7:3 패키지 매수 추천:
     - 위험 70%: `TIGER 미국테크TOP10` 또는 `KODEX 미국나스닥100TR`
     - 안전 30%: `SOL 미국배당미국채혼합50` (실질 주식비중 85% 효과) 또는 `ACE 미국30년국채액티브(H)`
4. **🔄 연금이전 (삼성이전 - 기소득공제 연금저축펀드)**:
   - 법적 제약: **개별주식 매수 절대 불가 (100% ETF 전용)**.
   - 전략: 월배당 배당성장(40%) + 월배당 국채(30%) + 국내 테마 알파 ETF(30%).
   - **조언 원칙**: 월 발생 분배금으로 매수할 저평가 테마 ETF(AI반도체, 밸류업 등) **수동 매수 추천 가이드** 제시 (자동 매수 ❌).
5. **⚡ 삼성ISA (중개형 ISA - 3년 비과세 한도 극대화)**:
   - 대상: `삼성전자` + `국내 상장 테마/섹터 ETF` (AI반도체, 밸류업 등).
   - **조언 원칙**: 유일하게 적극적 교체매매를 수행하는 계좌. 퀀트 신호(`▲ 분할매수`, `▲ 추세탑승`, `▼ 비중조절`) 기반 알파 스윙 리밸런싱 조언.
6. **🌐 삼성종합 (해외 직접투자)**:
   - 대상: 미국 빅테크 개별주 (`NVDA`, `AAPL`, `MSFT` 등).
   - **조언 원칙**: 2027년 본격 투입 대기. 환율 3M 동적 밴드 하단(Q25) 시 달러 사전 환전 가이드 제공.

### ✍️ [보고서 작성 문체 및 형식 규칙]
1. 모든 분석 문장은 반드시 **명사형 종결어미(`~함`, `~임`, `~필요`, `~권고`, `~유지`, `~상태`)**로 간결하게 작성하세요. (`~합니다`, `~바랍니다` 등 대화체 일체 금지)
2. HTML 태그(`<br>`, `<b>`, `<p>` 등) 사용을 일체 금지하고 표준 마크다운을 사용하세요.
3. Google Search Grounding을 통해 차주 글로벌 및 한국 시장 핵심 일정을 실시간 검색하여 반영하세요.
"""


USER_PROMPT_TEMPLATE = """아래 제공된 [실시간 매크로 지표], [세액공제 트래커], [6대 계좌별 현황 데이터]를 바탕으로 「K-올라운드 마스터」 계좌 분리형 맞춤 진단 리포트를 작성해 주세요.

## 🌐 1. 실시간 글로벌 & 🇰🇷 국내 매크로 스냅샷 (기준일: {as_of_date})
{macro_table_markdown}
- **환율 3M 동적 밴드 진단**: {fx_rule_status} (현재 환율: {fx_rate:,.1f}원)

## 📊 2. 전체 자산 및 세액공제(900만원) 실시간 진척도
- **분석 기준 일시**: {analysis_date}
- **총 평가 자산**: {total_eval_krw:,.0f} 원 (주식/ETF: {stock_total_krw:,.0f}원 + 현금 예수금: {cash_total_krw:,.0f}원)
- **포트폴리오 위험도(VaR)**: 95% 1주일 최대 예상 변동성 약 **-{portfolio_var_krw:,.0f}원 (-{portfolio_var_pct:.2f}%)**
- **세액공제 트래커**:
{tax_deduction_tracker_text}

### ⏱️ [전주 대비 주간 자산 추적]
{prev_report_summary_text}

### 🏛️ [6대 계좌별 세부 운용 현황 및 퀀트 지표]
{account_categorized_text}

### 🎯 [스마트 밸류 에버리징 (Value Averaging) 주간 추천 매수 가이드 (100만원 기준)]
{value_averaging_table}

---

## 📝 리포트 작성 요구사항 (반드시 아래 목차 구조 준수, 모든 문장 명사형 종결어미 필수)

### 1. 🌐 글로벌 & 국내 매크로 주간 브리핑 (Google Search 기반)
- **금주 시장 복기**: (글로벌 연준 금리/달러/유가 및 국내 수출/한은 수급 동향 요약: 명사형 종결)
- **차주 핵심 일정 & 전망**: (차주 예정된 FOMC/물가/실적 및 국내 캘린더 요약: 명사형 종결)
- **환율 3M 밴드 대응 가이드**: (현재 환율 {fx_rate:,.1f}원 기준 달러 환전 및 자산배분 대응 전략: 명사형 종결)

### 2. 🏛️ 6대 계좌별 핀포인트 맞춤 진단 & 실행 가이드
#### ① [삼성연금 & 미래연금] 코어 적립식 계좌
- 주간 적립(삼성연금 10만, 미래연금 20만) 정상 실행 여부 및 평단가 관리 상태 점검 (매도 금지 확인)
- 연 600만원 세액공제 달성률 및 연말 추가 납입 계획 점검 (명사형 종결)

#### ② [삼성IRP] 세액공제(연 300만) & 7:3 패키지 매수 가이드
- 법정 안전자산 30% 충족 여부 및 위험 70%(테크TOP10) + 안전 30%(배당국채혼합/30년국채) 2종목 패키지 추천 (명사형 종결)

#### ③ [연금이전 (삼성이전)] 100% ETF 월배당 인컴 & 분배금 재투자 가이드
- 개별주 배제 확인(100% ETF), 월배당 인컴(미국배당다우존스 40% + 30년국채 30%) 예상 수령액 진단
- 발생 분배금으로 수동 매수 추천할 저평가 국내 테마 ETF(AI반도체, 밸류업 등) 제시 (명사형 종결)

#### ④ [삼성ISA] 국내 주식 & 테마 ETF 퀀트 스윙 알파 가이드
- 삼성전자 및 보유 테마 ETF의 퀀트 지표(`▲ 분할매수`, `▲ 추세탑승`, `▼ 비중조절`) 기반 교체매매/수익실현 조언 (명사형 종결)

#### ⑤ [삼성종합] 미국 빅테크 직투 대기 & 달러 환전 타이밍
- 2027년 본격 투입 대기 상태 점검 및 환율 밴드 하단 시 달러 환전 권고 (명사형 종결)

### 3. 🎯 [표] 주간 스마트 밸류 에버리징 매수 추천 가이드 (수동 집행용)
| 계좌 | 추천 종목 | 목표 자산군 | 200MA 추세 | 52주 낙폭 | 밸류에버리징 추천금액 | 매수 우선순위 |
|:---|:---|:---:|:---:|:---:|:---|:---|
*(계좌별 정책에 부합하는 종목들로 100만원 기준 추천금액 및 퀀트 근거 제시)*

### 4. ✅ 차주 핵심 실행 체크리스트 (3대 액션)
1. **코어 적립식 주간 자동 매수 확인**: (삼성연금/미래연금 매수 집행 점검: 명사형 종결)
2. **삼성ISA / 연금이전 퀀트 신호 대응**: (스마트가이드 기반 전술적 매수 추천 종목 확인: 명사형 종결)
3. **세액공제 납입 스케줄 점검**: (연간 900만원 목표 달성을 위한 잔여액 관리: 명사형 종결)
"""
