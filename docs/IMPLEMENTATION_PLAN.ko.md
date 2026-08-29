# 🏗️ [update_stock] 전체 프로젝트 시스템 리빌딩 상세 실행 계획서 (IMPLEMENTATION_PLAN.ko.md)

<!--
# 🏗️ [update_stock] Total System Rebuilding Implementation Plan (Korean Edition)
-->

> **문서 버전**: 3.0.0 (엔터프라이즈 아키텍처 전면 개편 표준)  
> **대상 서브시스템**: `update_stock` (금융 데이터 수집/정제, 종목 마스터 및 시황 인텔리전스 서브 엔진)  
> **트윈 시스템**: [`k_all_round_portfolio`](file:///d:/Github%20IDE/k_all_round_portfolio) (패밀리오피스 퀀트 CIO 및 포트폴리오 BI)  
> **기준 설계 명세서**: [`REBUILDING_SPEC.ko.md`](file:///d:/Github%20IDE/update_stock/REBUILDING_SPEC.ko.md) / [`REBUILDING_SPEC.md`](file:///d:/Github%20IDE/update_stock/REBUILDING_SPEC.md)

---

## 👥 사용자 검토 및 사전 승인 필요 사항 (User Review Required)

> [!IMPORTANT]
> **핵심 아키텍처 거버넌스 및 사용자 승인 항목**:
> 1. **비파괴적 병렬 테스트 파일 프로토콜 (Non-Destructive `[test]` Protocol)**: 모든 신규 및 개편 도메인 배치는 독립된 병렬 테스트 파일(`[test]_*.py` 또는 `*_test.py`)로 개발함. 기존 운영 스크립트(`jobs/*/*.py`)는 단위 테스트 및 검증이 100% 완료되고 사용자의 최종 승인이 있을 때까지 절대 수정하지 않고 원본을 보존함.
> 2. **Tailscale 모바일 안티봇 메시 연동**: 유튜브 시황 수집 파이프라인에 `TAILSCALE_PROXY_URL`(예: `http://100.x.y.z:1080`) 환경변수 옵션을 지원하여, 클라우드 러너 IP 차단(HTTP 429) 시 안드로이드 Termux / iOS a-Shell의 LTE/5G 통신사 IP 프록시로 투명하게 자동 우회하도록 구현함.
> 3. **SQLite 스키마 확장**: `core/local_db_manager.py`에 비디오 ID, 채널, 발행일, 감성, 리스크 스탠스에 대한 고속 B-Tree 인덱스가 적용된 6번째 테이블(`tbl_youtube_insights`)을 증설함.

---

## 📋 단계별 변경 계획 (Proposed Changes by Phase)

### Phase 1: 공통 모델, FastMCP 서버 및 데이터베이스 엔진 현대화

#### [NEW] [`services/pydantic_models.py`](file:///d:/Github%20IDE/update_stock/services/pydantic_models.py)
- AI 시황 추출 및 재무 데이터 검증을 위한 Pydantic v2 결정론적 구조화 모델 정의:
  - `AssetImpact`: 수혜/피해 자산 티커, 예상 방향(`UP`/`DOWN`/`NEUTRAL`), 기관형 명사형 종결어미(`~함`, `~임`) 촉매 원인.
  - `YouTubeMarketInsight`: 영상 메타데이터, 거시 스탠스(`Bullish`/`Bearish`/`Neutral`), 위험 선호도(`Risk-On`/`Risk-Off`/`Defensive`), 핵심 시황 요약 리스트, 자산별 영향, 구체적 대응 전략(`~필요`, `~권고`).
  - `StockValuationItem`: KIS/Yahoo 밸류에이션 페이로드 검증 모델.

#### [MODIFY] [`core/local_db_manager.py`](file:///d:/Github%20IDE/update_stock/core/local_db_manager.py)
- `init_database()`에 고속 B-Tree 인덱스가 포함된 `tbl_youtube_insights` DDL 추가.
- CRUD 헬퍼 함수 구현: `upsert_youtube_insight()`, `get_youtube_insights_by_date()`, `get_latest_market_sentiment()`.
- `auto_restore_from_csv_if_needed()` 및 `export_all_tables_to_csv()`가 증설된 6개 테이블을 유연하게 처리하도록 보강.

#### [NEW] [`tools/mcp_server.py`](file:///d:/Github%20IDE/update_stock/tools/mcp_server.py)
- AI 코딩 에이전트(Antigravity IDE, Claude Code)에 4대 핵심 금융 도구를 제공하는 FastMCP 표준 서버 구축:
  1. `get_stock_quote(ticker: str)`: 실시간 현재가, 52주 고저점, PER/PBR, 5대 퀀트 팩터 조회.
  2. `search_ontology_keyword(keyword: str)`: 521개 온톨로지 사전 룰셋 및 매핑 조회.
  3. `get_macro_benchmark(ticker: str)`: 글로벌 벤치마크, 환율, 국채금리 시계열 조회.
  4. `get_latest_youtube_insights(limit: int)`: 최신 유튜브 AI 시황 요약 및 감성 조회.

---

### Phase 2: 현대화된 유튜브 & 모바일 안티봇 서브시스템

#### [NEW] [`jobs/youtube/job_sync_youtube_insights_test.py`](file:///d:/Github%20IDE/update_stock/jobs/youtube/job_sync_youtube_insights_test.py)
- 유튜브 수집 배치의 격리된 최신 테스트 모듈 개발:
  - 이중화 인그레스: 클라우드 IP 직접 수집 $\rightarrow$ HTTP 429 감지 시 Android Tailscale LTE/5G 프록시로 자동 폴백.
  - 정적 시스템 프롬프트에 대한 Google Gemini Context Caching 적용.
  - `services/pydantic_models.py` 기반의 무결점 구조화 데이터 추출.
  - 이중 영구 저장: 로컬 SQLite `tbl_youtube_insights`($<1\text{ms}$) + 노션 AI 시황 DB 블록 자동 적재.

#### [NEW] [`tests/test_pydantic_schemas.py`](file:///d:/Github%20IDE/update_stock/tests/test_pydantic_schemas.py)
- Pydantic 모델의 유효하지 않은 JSON 거부, 한국어 명사형 종결어미 검증, 노션 블록 페이로드 직렬화를 전수 검증하는 단위 테스트 작성.

---

### Phase 3: 도메인 배치 공통 모듈 단순화 및 성능 최적화

#### [NEW] [`jobs/price/[test]_job_sync_price_kr.py`](file:///d:/Github%20IDE/update_stock/jobs/price/[test]_job_sync_price_kr.py)
- `StockRegistryGateway` 및 벡터화 Dirty Checking을 적용하여 10배 빠른 배치 업데이트를 제공하는 테스트 모듈.

#### [NEW] [`jobs/finance/[test]_job_sync_finance_kr.py`](file:///d:/Github%20IDE/update_stock/jobs/finance/[test]_job_sync_finance_kr.py)
- 3단계 밸류에이션 폴백(`KIS` $\rightarrow$ `yfinance` $\rightarrow$ `SQLite Cache`)이 완비된 테스트 모듈.

#### [NEW] [`jobs/master/[test]_job_sync_master_kr.py`](file:///d:/Github%20IDE/update_stock/jobs/master/[test]_job_sync_master_kr.py)
- 외부 스크래핑 의존성을 완전히 제거하고 인메모리 온톨로지 토크나이저를 적용한 마스터 동기화 테스트 모듈.

#### [NEW] [`tests/test_local_db_perf.py`](file:///d:/Github%20IDE/update_stock/tests/test_local_db_perf.py)
- 다음 항목을 측정하는 성능 벤치마크 테스트:
  - SQLite WAL 모드 쿼리 응답 속도 ($<1\text{ms}$).
  - 5개 CSV 파일로부터의 DB 자가 복원 속도 ($<0.01\text{s}$).
  - 다중 프로세스 동시 읽기/쓰기 환경에서의 동시성 및 무경합 검증.

---

### Phase 4: 전수 감사, 가드레일 TDD 및 프로덕션 컷오버

#### [MODIFY] [`tests/test_guardrails.py`](file:///d:/Github%20IDE/update_stock/tests/test_guardrails.py)
- `tbl_youtube_insights` 스키마 락, FastMCP 도구 시그니처, Pydantic 모델 불변성 검증 단언(Assert) 추가.

#### 동기화 및 프로덕션 컷오버:
- 전체 테스트 스위트 실행: `python -m unittest discover tests`.
- [`AUDIT_CHECKLIST.md`](file:///d:/Github%20IDE/update_stock/AUDIT_CHECKLIST.md) 6대 영역 20개 감사 항목 전수 검수 완료.
- 검증 완료된 `[test]` 코드를 운영 스크립트로 안전하게 컷오버하고 `3_작업종료_동기화.bat`를 통해 안전하게 종료함.

---

## 🧪 검증 계획 (Verification Plan)

### 자동화 테스트 스위트
```powershell
# 1. 5대 핵심 퀀트 공식 및 수학적 불변성 검증
python -m unittest tests/test_guardrails.py

# 2. Pydantic v2 구조화 출력 스키마 검증
python -m unittest tests/test_pydantic_schemas.py

# 3. SQLite 서브 밀리초 쿼리 성능 및 CSV 자가 복구 검증
python -m unittest tests/test_local_db_perf.py

# 4. 전체 단위 테스트 탐색 및 실행
python -m unittest discover tests
```

### 수동 검증 단계
1. **FastMCP 서버 도구 검증**:
   - `python tools/mcp_server.py` 구동 후 `get_stock_quote("005930")` 및 `search_ontology_keyword("반도체")` 응답 검증.
2. **유튜브 비파괴 테스트 배치 실행**:
   - `python jobs/youtube/job_sync_youtube_insights_test.py`를 실행하여 기존 파이프라인에 영향 없이 `tbl_youtube_insights` 및 노션 DB에 정상 적재되는지 검증.
3. **워크스페이스 무결성 점검**:
   - `1_작업시작_동기화.bat` 및 `3_작업종료_동기화.bat`를 실행하여 충돌 없이 안전한 커밋 준비 상태를 확인.
