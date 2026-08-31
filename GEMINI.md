# 🤖 [K-All-Round Master] Unified Workspace AI Domain Master Guide (GEMINI.md)

<!--
# 🤖 [K-올라운드 마스터] 통합 워크스페이스 AI 도메인 마스터 가이드 (GEMINI.md)
-->

This document serves as the supreme domain specification defining the **business architecture, quantitative financial formulas, Notion database schemas, and critical domain invariants (WHAT)** across `k_all_round_portfolio` (Main Private) and `update_stock` (Sub Public).

<!--
이 문서는 k_all_round_portfolio (메인 Private 저장소)와 update_stock (서브 Public 데이터 수집 엔진)의 비즈니스 아키텍처, 퀀트 분석 수식, 노션 DB 스키마 및 도메인 핵심 규칙(WHAT)을 정의하는 최상위 프로젝트 명세서입니다.
-->

> 📌 **Engineering Standards & Behavioral Code**: All code formatting, early returns, TDD bug fixes, absolute paths, and the 7 Git commit rules (HOW AI writes code) strictly follow [AGENT.md](file:///d:/Github%20IDE/AGENT.md).

<!--
> 📌 **엔지니어링 표준 & 코딩 행동 수칙**: 클린 코드, 조기 반환, TDD, 절대 경로, Git 7대 커밋 규칙 등 AI가 코드를 작성하는 방법(HOW)은 AGENT.md를 100% 준수합니다.
-->

---

## 🏛️ 1. Workspace Architecture & 3-Repository Division of Roles

```text
d:\Github IDE/
│
├── 📂 k_all_round_portfolio/            👑 [Main Private: 7-Asset Allocation BI & AI Reports]
│   ├── .github/workflows/               # AI Weekly Portfolio Report Workflow (generate_portfolio_report.yml)
│   ├── core/                            # config_portfolio.py, notion_utils.py (0.001s Local Cache), guardrails.py
│   ├── services/                        # prompt_manager.py
│   ├── jobs/                            # ⚙️ Autonomous Batches & Co-located Dependencies (Job-Centric Architecture)
│   │   ├── quant_report/                # job_generate_portfolio_report.py, macro_service.py, ai_service.py, system_portfolio_quant.en.md, user_portfolio_template.en.md
│   │   └── tech_radar/                  # job_sync_tech_radar.py, tech_radar_gemini.md
│   ├── reports/                         # Weekly Asset Allocation AI Diagnostic Reports Local Permanent Backup
│   ├── tools/                           # sync_manager.py, tool_generate_gemini_prompt.py, tool_apply_tech_radar_patch.py
│   ├── tests/                           # test_guardrails.py, test_prompts.py
│   ├── 1_작업시작_동기화.bat              # Smart Work Start (Environment Detection / Summary / Periodic Strategy Checklist)
│   ├── 3_작업종료_동기화.bat              # Smart Work Finish (Syntax Validation & Git Commit/Push & Mobile GDrive Sync)
│   └── 4_테크레이더_패치적용.bat          # AI Tech Radar One-Click Patch Applier
│
├── 📂 update_stock/                     ⚙️ [Sub Public: Financial Market Data ETL Hub]
│   ├── .github/workflows/ (11 YMLs)     # Price, Finance, Master, ETF, YouTube, Unorganized, Local DB Workflows
│   ├── core/                            # notion_utils.py, local_db_manager.py, guardrails.py
│   ├── services/                        # stock_fallback_resolver.py, prompt_manager.py
│   ├── jobs/                            # ⚙️ Domain-Specific Autonomous Execution Batches
│   │   ├── price/                       # job_sync_price_kr.py, job_sync_price_us.py
│   │   ├── finance/                     # job_sync_finance_kr.py, job_sync_finance_us.py, kis_data_service.py
│   │   ├── master/                      # job_sync_master_kr.py, job_sync_master_us.py, kis_master_loader.py
│   │   ├── etf/                         # job_sync_etf_holdings.py
│   │   ├── macro/                       # job_sync_benchmark.py
│   │   ├── local_db/                    # job_sync_local_db.py, job_sync_unorganized_stocks.py
│   │   └── youtube/                     # job_sync_youtube_insights.py, ai_service.py, system_fia_youtube.en.md
│   ├── data/                            # stock_master.db (5 Normalized Tables) + 5 CSV Dumps
│   ├── tools/                           # sync_manager.py, tool_apply_tech_radar_patch.py
│   ├── tests/                           # test_guardrails.py
│   ├── 1_작업시작_동기화.bat              # Smart Work Start
│   └── 3_작업종료_동기화.bat              # Smart Work Finish
│
└── 📂 workspace-vault/                  🔐 [Private Storage: Security Vault, Backups & Docs Hub]
    ├── configs/                         # Global IDE & AI Master Settings (GEMINI.md, AGENT.md, pyrightconfig, .vscode)
    ├── env_vault/                       # .env Backup & Cross-environment Synchronization (update_stock.env, k_all_round_portfolio.env)
    ├── backups/                         # KIS Token Cache (.kis_token_cache.json) & Snapshot Backups
    ├── docs/                            # Centralized Architecture Specs, Operation Guides & Quality Checklists
    ├── setup_environment.py             # All-in-one Environment Installation Wizard
    ├── link_master_db.py                # Notion Watchlist Linker & Helper Script
    └── *.bat (0~6)                      # Full Suite of Operations & Synchronization Batch Files
```

<!--
## 🏛️ 1. 워크스페이스 아키텍처 및 역할 분담 (3대 저장소: Private 메인 + Public 서브 + Private 보안금고)
- k_all_round_portfolio: 7대 자산 퀀트 분석, VaR 산출, 6대 계좌 분리 진단, Gemini AI 주간 리포트 발행
- update_stock: 국내/미국 실시간 시세, 재무제표 밸류에이션, 전수 마스터, ETF 구성종목, 거시 지표, 유튜브 시황 수집
- workspace-vault: 환경설정(.env) 중앙 금고, 토큰 캐시 백업, 아키텍처/가이드 통합 문서, 전체 실행 배치 스크립트 허브
-->

---

## 📊 2. 5 Core Quant Factors & Mathematical Calculation Formulas

1. **`200-Day Moving Average` (Number)**:
   $$\text{MA}_{200} = \frac{1}{200}\sum_{i=0}^{199} \text{Close}_{t-i}$$
2. **`Institutional Supply-Demand Line` (Number)**:
   - Korean Stocks / ETFs: 60-Day Moving Average ($\text{MA}_{60}$)
   - US Stocks / ETFs: 50-Day Moving Average ($\text{MA}_{50}$)
3. **`12M Dual Momentum` (Number - %)**:
   $$\text{Momentum}_{12M} = \frac{\text{Close}_t - \text{Close}_{t-252}}{\text{Close}_{t-252}} \times 100$$
4. **`52-Week Maximum Drawdown` (Number - %)**:
   $$\text{Drawdown}_{52W} = \frac{\text{Close}_t - \text{High}_{52W}}{\text{High}_{52W}} \times 100 \quad (\le 0\%)$$
5. **`60-Day Annualized Volatility` (Number - %)**:
   $$\sigma_{\text{annual}} = \text{std}(R_{t-59 \dots t}) \times \sqrt{252} \times 100$$
6. **`Portfolio 95% 1-Week VaR (Value at Risk)` (Currency)**:
   $$\text{VaR}_{95\%, 1W} = \text{Total Asset} \times 1.65 \times \frac{\sum (w_i \times \sigma_i)}{\sqrt{52}}$$
7. **`Smart Value Averaging Score`**:
   $$\text{Score} = \left( \text{Target Weight} + \max(0, -\text{Disparity} \times 2) \right) \times W_{\text{Trend}} \times W_{\text{Drawdown}}$$

<!--
## 📊 2. 5대 핵심 퀀트 팩터 & 수학적 산출 수식
1. 200일 이동평균선: 최근 200영업일 종가 평균
2. 수급선: 한국 60일선 / 미국 50일선
3. 12M 듀얼 모멘텀: (현재가 - 252영업일전가격) / 252영업일전가격 * 100
4. 52주 낙폭: (현재가 - 52주최고가) / 52주최고가 * 100 (음수 %)
5. 60일 연환산 변동성: 60일 일일수익률 표준편차 * sqrt(252) * 100
6. 포트폴리오 95% 1-Week VaR: 총평가자산 * 1.65 * (가중변동성합 / sqrt(52))
7. 스마트 밸류 에버리징 점수: (목표비중 + max(0, -괴리율 * 2)) * 추세가중치 * 낙폭가중치
-->

---

## 🗄️ 3. Notion Core Databases Schema & Interconnections

- **Account Status DB (`tbl_account_status`)**: Total assets, cash deposits, principal, return rates, realized P&L across 6 distinct accounts (ISA, Pension Savings, IRP, CMA, etc.).
- **Total Stock Holdings DB (`tbl_stock_holdings`)**: Unified holdings across accounts, evaluation amount, valuation P&L, portfolio weight, dividend yield.
- **Account Holdings Mapping DB (`tbl_account_holdings`)**: Account-Stock N:M relation, purchase price per account, quantity, evaluation profit/loss.
- **Stocks & Investment Assets DB (`tbl_stocks`)**: Real-time price, 200-day MA, 52W drawdown, 12M momentum, trend signal, valuation metrics (PER/PBR/ROE).
- **Macro Benchmark DB (`tbl_benchmark`)**: 54 global macroeconomic indicators (Dollar Index, Yield Curve Spread, WTI Crude Oil, Gold, FX rates).
- **Ontology Dictionary DB (`tbl_dictionary`)**: 521 theme/sector keywords mapping and global GICS categorization rules.

<!--
## 🗄️ 3. 노션(Notion) 핵심 데이터베이스 스키마 및 연동
- 투자계좌현황 DB: 계좌별(ISA, 연금저축, IRP, CMA 등) 총자산, 예수금, 원금, 수익률, 확정손익
- 종목별 보유현황 DB: 전 계좌 통합 종목별 평가금액, 평가손익, 포트폴리오 비중, 배당수익률
- 계좌별 보유종목 DB: 계좌-종목 N:M 매핑, 계좌별 매수단가, 수량, 평가손익
- 상장주식/투자주 DB: 실시간 현재가, 200일선, 52주 낙폭, 12M 모멘텀, 추세 시그널, 밸류에이션(PER/PBR/ROE)
- 벤치마크/거시지표 DB: 54종 거시경제 지표(달러인덱스, 장단기금리차, WTI유가, 금, 환율)
- 온톨로지 사전 DB: 521개 테마/섹터 키워드 매핑 및 GICS 분류 규칙
-->

---

## 🚨 4. Project Domain Critical Invariants

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
