<!--
시스템 수석 감사관(Principal QA Architect) 관점에서 @[d:\Github IDE\AUDIT_CHECKLIST.md] 의 6대 영역 검수 기준을 바탕으로 현재 워크스페이스(k_all_round_portfolio 및 update_stock) 전체를 전수 감사하고 종합 감사 보고서를 제출해줘.
-->

# 🛡️ [K-All-Round Master] 6대 영역 시스템 종합 품질 검수 계획서 (AUDIT_CHECKLIST.md)

<!--
# 🛡️ [K-올라운드 마스터] 6대 영역 시스템 종합 품질 검수 계획서 (AUDIT_CHECKLIST.md)
-->

> **목적**: 금융 데이터 수집/가공 ETL 허브(`update_stock`) 및 7대 자산배분 퀀트 리포트 엔진(`k_all_round_portfolio`)의 무결성, 안정성, 단일 진실 공급원(SSOT), 코드 슬림화 및 품질을 전수 검사하기 위한 최고 수준의 엔터프라이즈 감사 표준 명세서입니다.

---

## 1️⃣ [영역 1: 데이터 무결성 및 중복 방지 (Data Integrity & Deduplication)]

- [ ] **1.1. 티커 표준화 및 마켓 식별자 보존 (`clean_ticker_key`)**:
  - `clean_ticker_key` 함수가 마켓 접미사(`.T`, `.KS`, `.KQ`, `.DE`, `.AS` 등), 하이픈, 언더스코어를 훼손 없이 완벽히 보존하는지 검증.
- [ ] **1.2. 3중 교차 검증 게이트웨이 (`StockRegistryGateway`)**:
  - 노션 `pages.create` 호출 전 로컬 SQLite DB(0.001s) 및 노션 인메모리 캐시를 통해 (1차: 정규화 티커 $\rightarrow$ 2차: 종목명/브랜드 $\rightarrow$ 3차: 온톨로지 사전) 3단계 교차 조회를 거쳐 기존 레코드 ID를 100% 재사용하는가?
- [ ] **1.3. 배치 내 즉시 영구 적재 (Zero-Duplicate Loop)**:
  - 신규 생성된 종목이 즉시 SQLite `tbl_stocks`와 게이트웨이 인메모리 맵에 적재되어, 동일 배치 내 후속 루프에서 중복 생성이 원천 차단되는가?

---

## 2️⃣ [영역 2: 아키텍처 결합도 및 단일 진실 공급원 (SSOT & Decoupling)]

- [ ] **2.1. 정적 데이터와 비즈니스 로직의 완전 분리 (Data Decoupling)**:
  - GICS 23개 산업 분류, 글로벌 대표 ETF 18개, 해외 특수 종목 26개 등 정적 룰셋이 파이썬 코드 내 하드코딩되지 않고 `data/seed_dictionary.json` 또는 `data/*.csv`로 분리되었는가?
- [ ] **2.2. 모듈 파편화 제로 (Zero-Fragmentation)**:
  - 종목 레지스트리 검증(`core/stock_registry.py`), 로컬 DB CRUD 및 CSV 자동 복원(`core/local_db_manager.py`), 프롬프트 관리(`services/prompt_manager.py`) 등 공통 책임이 단 1개의 파일로 일원화되었는가?
- [ ] **2.3. 위임 패턴 준수 (Delegation Pattern)**:
  - `notion_utils.py`의 `load_local_*` 함수 및 `safe_page_create` 등이 독자 로직을 중복 구현하지 않고 핵심 모듈로 안전하게 위임하고 있는가?

---

## 3️⃣ [영역 3: 외부 API 장애 격리 및 셀프 힐링 (Fault-Tolerance & Self-Healing)]

- [ ] **3.1. 글로벌 티커 탐색 우선순위 규칙 (`search_foreign_ticker`)**:
  - Yahoo Finance 탐색 시 미국 OTC/Pink Sheet보다 주요 정규 거래소(도쿄 `.T`, 한국 `.KS`/`.KQ`, 홍콩 `.HK`, 대만 `.TW`)를 최우선으로 매칭하는가?
- [ ] **3.2. 노션 페이로드 및 스키마 방어 (`Defensive Guard`)**:
  - Date 프로퍼티 초기화 시 `{"date": None}` 표준 규격을 준수하고, 모든 프로퍼티 접근 전 `if prop in properties` 검증을 선행하여 400 Validation Error를 원천 방어하는가?
- [ ] **3.3. 네트워크 장애 격리 및 로컬 DB 폴백**:
  - KIS API, WiseReport, yfinance 장애 시 전체 배치가 비정상 중단되지 않고 로컬 SQLite DB 캐시로 안전하게 격리/폴백되는가?

---

## 4️⃣ [영역 4: 퀀트 수식 및 보고서 서식 불변성 (Quant Math & Schema Guardrails)]

- [ ] **4.1. 5대 핵심 퀀트 공식 수학적 무결성**:
  - 12M 모멘텀 $\frac{P_t - P_{t-252}}{P_{t-252}}$, 52주 낙폭 $\frac{P_t - High_{52W}}{High_{52W}}$, 60일 연환산 변동성 $\sigma \times \sqrt{252}$, 200일 이동평균선 공식이 `core/guardrails.py` 표준과 수학적으로 100% 일치하는가?
- [ ] **4.2. 가드레일 단위 테스트 전수 통과**:
  - `python -m unittest tests/test_guardrails.py` 및 `discover tests`가 0.001초 만에 오류 없이 통과하는가?
- [ ] **4.3. 기관형 명사형 종결어미 엄격 준수**:
  - AI 리포트 프롬프트 및 분석 문장이 `~함`, `~임`, `~필요`, `~권고` 규칙을 강제하고 있는가?

---

## 5️⃣ [영역 5: Twin-Pair 저장소 동기화 및 0.01초 자동 복원 (Workspace Sync)]

- [ ] **5.1. Twin-Pair 단일 진실 공급원 동기화**:
  - `k_all_round_portfolio/core`와 `update_stock/core`의 공통 인프라 파일(`stock_registry.py`, `local_db_manager.py`, `notion_utils.py`, `guardrails.py`)이 상호 일치하는가?
- [ ] **5.2. GitHub Actions 0.01초 무중단 자가 복구**:
  - GitHub Actions 러너 환경에서 Git으로 추적 중인 `data/*.csv` 파일들로부터 SQLite DB(`stock_master.db`)가 0.01초 만에 자동 복원(`auto_restore_from_csv_if_needed`)되는가?
- [ ] **5.3. 자동 Git 커밋/푸시 금지 및 안전 종료**:
  - AI 에이전트가 `git commit/push`를 자동 실행하지 않고 `3_작업종료_동기화.bat`를 통해 사용자가 최종 점검 후 수동 커밋하도록 안내하는가?

---

## 6️⃣ [영역 6: 코드 슬림화 및 안티 블로트 검수 (Code Diet & Anti-Bloat)]

- [ ] **6.1. 공통 인프라 위임 및 중복 로직 박멸 (Infrastructure Delegation)**:
  - 자체 티커 정규화(`normalize_ticker`), 수동 마스터 DB 쿼리 루프, 하드코딩된 DB ID가 제거되고 `StockRegistryGateway` 및 `core/` 표준 모듈에 100% 위임되었는가?
- [ ] **6.2. 무거운 웹 스크래핑 배제 및 인메모리 해석기 우선 (In-Memory Pure Resolver)**:
  - 불필요한 HTML 네트워크 스크래핑 없이 정규식 및 인메모리 파이프라인으로 채널/재생목록/영상을 0.001초 만에 자동 분류하는가?
- [ ] **6.3. 노션 블록 및 페이로드 데이터 주도형 생성 (Data-Driven Payload)**:
  - 수백 줄에 달하던 절차형 딕셔너리 `append` 코드가 List Comprehension 및 간결한 데이터 주도형 딕셔너리 빌더로 슬림화(50% 이상 압축)되었는가?
- [ ] **6.4. 통합 캐시/대기열 I/O 단일화 (Consolidated Cache I/O)**:
  - 프로젝트 루트 및 로컬 디렉터리 캐시 탐색 시 중복 `try-except` 블록 없이 통합 경로 리스트 순회로 일원화되었는가?
- [ ] **6.5. 독립 실행 모듈 경로 선제 보장 (`sys.path` Root Injection)**:
  - `jobs/` 하위 스크립트가 단독 CLI 실행(`python jobs/.../job_*.py`) 시에도 `ModuleNotFoundError` 없이 완벽히 동작하도록 `PROJECT_ROOT`가 `sys.path`에 선제 등록되었는가?
