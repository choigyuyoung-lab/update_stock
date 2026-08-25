# 🤖 [update_stock] Market Data ETL Sub-Engine Guide (GEMINI.md)

<!--
# 🤖 [update_stock] 데이터 수집 서브 엔진 가이드 (GEMINI.md)
-->

This document defines the **financial data collection/ETL architecture, 11-workflow specification, and domain rules (WHAT)** for `update_stock`.
For 7-asset quantitative allocation models and portfolio diagnostics, refer to [`k_all_round_portfolio/GEMINI.md`](file:///d:/Github%20IDE/k_all_round_portfolio/GEMINI.md).

<!--
이 문서는 본 저장소(update_stock)의 금융 데이터 수집/정제 아키텍처, 11대 워크플로우 명세 및 도메인 규칙(WHAT)을 정의하는 프로젝트 명세서입니다.
포트폴리오 분석 및 7대 자산배분 퀀트 수식은 메인 저장소인 k_all_round_portfolio/GEMINI.md를 참조하십시오.
-->

> 📌 **Engineering Standards & Behavioral Code**: All code formatting, early returns, TDD bug fixes, absolute paths, and the 7 Git commit rules (HOW AI writes code) strictly follow [AGENT.md](file:///d:/Github%20IDE/AGENT.md).

<!--
> 📌 **엔지니어링 표준 & 코딩 행동 수칙**: 클린 코드, 조기 반환, TDD, 절대 경로, Git 7대 커밋 규칙 등 AI가 코드를 작성하는 방법(HOW)은 AGENT.md를 100% 준수합니다.
-->

---

## 🏛️ 1. Repository Layer Hierarchy & 1:1 Symmetric Workflows

### 📂 Layer Hierarchy
- `core/`: Shared Infrastructure (`notion_utils.py`, `local_db_manager.py`, `guardrails.py`)
- `services/`: Shared Adapters across multiple domains (`stock_fallback_resolver.py`, `prompt_manager.py`)
- `jobs/`: ⚙️ Autonomous Batches & Co-located Dependencies (Job-Centric Architecture)
  - `price/`: `job_sync_price_kr.py`, `job_sync_price_us.py`
  - `finance/`: `job_sync_finance_kr.py`, `job_sync_finance_us.py`, `kis_data_service.py`
  - `master/`: `job_sync_master_kr.py`, `job_sync_master_us.py`, `kis_master_loader.py`
  - `etf/`: `job_sync_etf_holdings.py`
  - `macro/`: `job_sync_benchmark.py`
  - `local_db/`: `job_sync_local_db.py`, `job_sync_unorganized_stocks.py`
  - `youtube/`: `job_sync_youtube_insights.py`, `ai_service.py`, `system_fia_youtube.en.md`
- `data/`: Local Permanent Cache (`stock_master.db`, 5 CSV Dumps)
- `tools/`: `sync_manager.py`, `tool_apply_tech_radar_patch.py`
- `tests/`: `test_guardrails.py`

<!--
### 📂 계층 구조
- core/: 공통 엔진 (notion_utils.py, local_db_manager.py, guardrails.py)
- services/: 다중 도메인 공통 어댑터 (stock_fallback_resolver.py, prompt_manager.py)
- jobs/: 도메인별 실행 잡 및 종속 파일 (Job-Centric Co-location)
- data/: 로컬 영구 캐시 (stock_master.db, CSV 5종)
- tools/: sync_manager.py, tool_apply_tech_radar_patch.py
- tests/: test_guardrails.py
-->

### 🤖 11 Symmetric Workflows & Python Execution Scripts:
1. `sync_price_kr.yml` $\leftrightarrow$ `jobs/price/job_sync_price_kr.py`: Korean Stocks/ETFs real-time price batch
2. `sync_price_us.yml` $\leftrightarrow$ `jobs/price/job_sync_price_us.py`: US Stocks/ETFs closing price
3. `sync_finance_kr.yml` $\leftrightarrow$ `jobs/finance/job_sync_finance_kr.py`: Korean financial metrics & 5 quant factors
4. `sync_finance_us.yml` $\leftrightarrow$ `jobs/finance/job_sync_finance_us.py`: US financial metrics & 5 quant factors
5. `sync_master_kr.yml` $\leftrightarrow$ `jobs/master/job_sync_master_kr.py`: KRX listed stocks master sync
6. `sync_master_us.yml` $\leftrightarrow$ `jobs/master/job_sync_master_us.py`: US listed stocks master sync
7. `sync_benchmark.yml` $\leftrightarrow$ `jobs/macro/job_sync_benchmark.py`: Global benchmarks, FX, interest rates
8. `sync_etf_holdings.yml` $\leftrightarrow$ `jobs/etf/job_sync_etf_holdings.py`: ETF portfolio constituents (PDF) incremental upsert
9. `sync_youtube_insights.yml` $\leftrightarrow$ `jobs/youtube/job_sync_youtube_insights.py`: YouTube RSS transcripts AI analysis
10. `sync_unorganized_stocks.yml` $\leftrightarrow$ `jobs/local_db/job_sync_unorganized_stocks.py`: Unorganized stocks FX update $\rightarrow$ master matching
11. `sync_local_db.yml` $\leftrightarrow$ `jobs/local_db/job_sync_local_db.py`: SQLite DB (`stock_master.db`) & 5 CSV dumps compilation

<!--
### 🤖 11대 대칭 워크플로우 & 파이썬 스크립트:
1. sync_price_kr.yml <-> jobs/price/job_sync_price_kr.py: 국내 주식/ETF 실시간 시세
2. sync_price_us.yml <-> jobs/price/job_sync_price_us.py: 미국 주식/ETF 종가 시세
3. sync_finance_kr.yml <-> jobs/finance/job_sync_finance_kr.py: 국내 재무비율 & 5대 퀀트팩터
4. sync_finance_us.yml <-> jobs/finance/job_sync_finance_us.py: 미국 재무비율 & 5대 퀀트팩터
5. sync_master_kr.yml <-> jobs/master/job_sync_master_kr.py: 국내 상장주식 마스터 DB 동기화
6. sync_master_us.yml <-> jobs/master/job_sync_master_us.py: 미국 상장주식 마스터 DB 동기화
7. sync_benchmark.yml <-> jobs/macro/job_sync_benchmark.py: 글로벌 벤치마크/환율/금리 동기화
8. sync_etf_holdings.yml <-> jobs/etf/job_sync_etf_holdings.py: ETF 구성종목(PDF) 증분 Upsert 동기화
9. sync_youtube_insights.yml <-> jobs/youtube/job_sync_youtube_insights.py: 유튜브 RSS 자막 AI 구조화 분석
10. sync_unorganized_stocks.yml <-> jobs/local_db/job_sync_unorganized_stocks.py: 미정리 종목 환율 갱신 -> 마스터 매칭 -> 특이사항 이관
11. sync_local_db.yml <-> jobs/local_db/job_sync_local_db.py: 통합 로컬 SQLite DB 및 CSV 5종 덤프 갱신
-->

---

## 🚨 2. Development & Operational Domain Invariants

1. **Twin-Pair Single Source of Truth (SSOT)**:
   - `k_all_round_portfolio` and `update_stock` are tightly coupled. Common utilities, asset allocation rules, and schema changes MUST be mirrored consistently across both repositories.
2. **Smart Work Start Sync (`1_작업시작_동기화.bat`)**:
   - Automatically detects environment switching (Office $\leftrightarrow$ Home), alerts uncommitted changes, briefs previous commits, and prompts periodic (7-Day / 30-Day) strategic checklists.
3. **Automated Git Commit/Push Prohibition**:
   - AI agents MUST stop after code verification. Final commits and pushes MUST be manually executed by the user via `3_작업종료_동기화.bat`.
4. **Pre-Execution Korean Terminal Command Briefing**:
   - Before executing terminal commands, ALWAYS explain in Korean what command is being executed and why.
5. **Notion Schema Auto-Provisioning & Defensive Guard**:
   - Auto-create missing timestamp columns (e.g., `업데이트 일자`) via API, and ALWAYS guard property access with `if field in props`.
6. **Report Linguistic Termination Standard**:
   - All diagnostic statements MUST use Korean institutional noun-ending terminations (`~함`, `~임`, `~필요`, `~권고`).
7. **iOS & Mobile Anti-Bot Automation Standard**:
   - When running YouTube synchronization in cloud environments with anti-bot IP blocks (429/empty transcripts), utilize mobile carrier IP execution via iOS terminal apps (`a-Shell`, `iSH Shell`, `Pythonista`) triggered by iOS Shortcuts Automation (scheduled times or charger connection) to guarantee 100% transcript extraction without bot challenges.

<!--
## 🚨 4. 프로젝트 불변 도메인 원칙
1. 2대 프로젝트 쌍(Twin Pair) 단일 진실 공급원(SSOT) 원칙: 공통 유틸, 자산 분류 룰, 스키마 수정 시 양쪽 프로젝트에 일관되게 반영함.
2. 스마트 시작 동기화: 환경 전환 감지, 미동기화 파일 경고, 직전 작업 요약, 주기별 전략 점검 질문 팝업 제공.
3. 자동 Git 커밋/푸시 금지: 코드 수정 및 검증까지만 완료하고 최종 커밋/푸시는 사용자가 수동 실행하도록 안내.
4. 터미널 명령어 사전 한국어 안내 원칙: 실행 전 무엇을 위해 어떤 명령어를 실행하는지 한국어로 명확히 설명.
5. 노션 DB 스키마 자동 프로비저닝 & 방어 로직: 누락 열 자동 생성 및 if field in props 방어 로직 적용.
6. 리포트 문체 규칙: 모든 분석 문장은 명사형 종결어미(~함, ~임, ~필요, ~권고)로 작성.
7. iOS & 모바일 안티봇 자동화 표준: 클라우드 IP 차단(429/자막 0자) 우회를 위해 아이폰 터미널 앱(a-Shell, iSH Shell, Pythonista)과 iOS 단축어 자동화(특정 시간/충전기 연결 시 무인 실행)를 활용한 모바일 통신사 IP 기반 100% 무차단 자막 수집 방식을 지원함.
-->
