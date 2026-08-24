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

1. **Twin-Pair SSOT Mirroring**:
   - `update_stock` and `k_all_round_portfolio` are a Single Source of Truth pair. Modifications to data models or ontology MUST be reflected across both.
2. **Smart Work Start Sync (`1_작업시작_동기화.bat`)**:
   - Detects environment transitions, warns about uncommitted changes, briefs previous commits, and prompts periodic checklists.
3. **Automated Git Commit/Push Prohibition**:
   - Final commits and pushes must be executed manually by the user via `3_작업종료_동기화.bat`.
4. **Notion Database Schema Auto-Provisioning**:
   - Missing columns (e.g., `업데이트 일자`) are automatically created via API.
5. **Pydantic Structured Outputs Enforcement**:
   - Gemini AI calls MUST use structured schemas (`response_schema=YouTubeAnalysisResult`) to eliminate markdown regex parsing errors.

<!--
## 🚨 2. 개발 및 운영 절대 준수 원칙
1. 2대 프로젝트 쌍(Twin Pair) 동기화 원칙: 한쪽의 데이터 구조, 온톨로지, 스키마 수정 시 양쪽 프로젝트에 동시에 영향 반영.
2. 스마트 시작 동기화: 환경 전환 감지, 미동기화 파일 경고, 직전 작업 요약, 주기별 전략 점검 체크리스트 제공.
3. 자동 Git 커밋/푸시 금지: 최종 커밋/푸시는 사용자가 3_작업종료_동기화.bat를 통해 실행하도록 안내.
4. 노션 DB 스키마 자동 프로비저닝: 필수 열 자동 생성.
5. Pydantic Structured Outputs 준수: Gemini AI 분석 시 response_schema=YouTubeAnalysisResult 사용하여 정규식 오류 차단.
-->
