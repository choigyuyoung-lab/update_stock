# 🤖 [K-All-Round Master] AGENT.md: LLM Code Quality & Engineering Standards

<!--
# 🤖 [K-올라운드 마스터] AGENT.md: LLM 코드 품질 & 엔지니어링 표준
-->

> **Reference**: Fabien Sanglard's agent.md (https://fabiensanglard.net/agent.md/index.html) & K-All-Round Quant Engineering Standard  
> This specification defines the immutable engineering standards, coding conventions, and architectural constraints that all AI coding agents (Antigravity IDE, Claude Code, Gemini CLI, Cursor) MUST adhere to across the entire workspace (`k_all_round_portfolio` and `update_stock`).

<!--
> **참조**: Fabien Sanglard의 agent.md (https://fabiensanglard.net/agent.md/index.html) 및 K-올라운드 퀀트 엔지니어링 표준
> 본 가이드는 Antigravity IDE, Claude Code, Gemini CLI, Cursor 등 모든 AI 코딩 에이전트가 본 통합 워크스페이스(`k_all_round_portfolio`, `update_stock`)에서 코드를 생성, 리팩토링, 디버깅할 때 반드시 준수해야 하는 불변의 엔지니어링 품질 표준 및 코딩 컨벤션입니다.
-->

---

## 1. 💬 Communication & Output Clarity

- **Brevity & Precision (Less is More)**: Use as few words as possible. Pick every word meticulously. Minimize conversational padding.
- **No Hollow Praises or Superlatives**: Never output sycophantic phrases (e.g., "You are absolutely right", "Great question"). Provide the cold hard technical truth.
- **Strict Korean Institutional Noun Endings**: All analytical conclusions, summary briefings, and diagnostic statements MUST terminate in Korean noun-ending verbs (`~함`, `~임`, `~필요`, `~권고`, `~전망`). Strictly FORBID conversational endings (`~합니다`, `~해요`).
- **Pre-Execution Korean Command Explanation**: Before executing any terminal command, ALWAYS provide a concise explanation in Korean stating WHAT is being executed and WHY.

<!--
## 1. 💬 간결하고 명확한 커뮤니케이션
- **최소 단어 & 핵심 전달 (Less is More)**: 프롬프트 답변, 코드 주석, 리포트 생성 시 군더더기 없는 정밀한 어휘만을 선별하여 간결하게 전달함.
- **감정적 수식어 및 무의미한 칭찬 배제**: "당신의 말이 전적으로 맞습니다", "정말 훌륭한 질문입니다" 등의 무의미한 칭찬을 금지하고, 냉철하고 객관적인 기술적 팩트(Cold hard technical truth) 위주로 응답함.
- **명사형 종결어미 엄격 준수**: 모든 분석 문장, 변경 요약, 리포트 브리핑은 공적/기관형 명사형 종결어미(`~함`, `~임`, `~필요`, `~권고`, `~전망`)로 작성함. 일상 대화체(`~합니다`, `~해요`) 사용을 엄격히 금지함.
- **사전 한국어 설명 원칙**: 모든 터미널 명령어를 실행하기 전, 무엇을 위해 어떤 명령어를 실행하는지 반드시 한국어로 명확하게 사전 설명한 후 실행함.
-->

---

## 2. 🛡️ Code Quality & Formatting Guardrails (Python 3.10+)

- **Eliminate Magic Numbers & Magic Strings**:
  - Quant factor thresholds (e.g., 200-day MA, 252 trading days, 1.65 VaR multiplier, 52-week high), Notion property keys, and status codes MUST be defined as uppercase constants or `Enum`/`Literal` types.
- **Reduce Indentation & Arrow Anti-Pattern**:
  - Avoid deeply nested `if-else` blocks. Aggressively use early returns (`return`, `continue`, `break`) and guard clauses.
- **Short & Explicit Naming**:
  - Keep function and variable names intention-revealing yet concise (< 30 characters).
- **Enums and Literals over Boolean Flags**:
  - Use `Enum` or `Literal["start", "finish"]` instead of ambiguous boolean flags for multi-state function arguments.
- **Visual Breathing Room**:
  - Insert a single blank line between distinct logical blocks inside functions to enhance readability.
- **Contextual Comments (WHAT & WHY, not HOW)**:
  - Explain *WHAT* the code accomplishes and *WHY* the design was chosen, not syntax details. Use ASCII diagrams for complex multi-database architectures.
- **Strict Python 3.10+ Type Hints**:
  - All function signatures MUST have explicit type hints (e.g., `int | None`, `tuple[...]`, `dict[str, Any]`, `list[Path]`).

<!--
## 2. 🛡️ 코드 품질 & 포맷팅 가드레일 (Python 3.10+)
- **매직 넘버 & 매직 스트링 박멸 (No Magic Numbers/Strings)**:
  - 퀀트 계수(200일선, 252영업일, 1.65 VaR 승수, 52주 등), 노션 속성명, 상태 코드 등은 반드시 전역 상수(`CONSTANTS`) 또는 `Enum` / `Literal`로 정의하여 재사용함.
- **들여쓰기 축소 & 조기 반환 (Reduce Indentation & Early Return)**:
  - 깊은 `if-else` 중첩(화살표 안티패턴)을 지양하고, 가드 절(Guard Clause)과 조기 반환(`return`, `continue`, `break`)을 적극 활용함.
- **간결하고 명시적인 네이밍 (Short & Explicit Naming)**:
  - 함수명 및 변수명은 의도를 명확히 드러내되 30자 이내로 간결하게 명명함.
- **불리언 플래그 대신 Enum/Literal 활용 (Enums over Booleans)**:
  - 다중 상태 파라미터에는 모호한 `bool` 플래그 대신 `Enum` 또는 `Literal["start", "finish"]` 타입을 사용함.
- **시각적 호흡 공간 (Visual Breathing Room)**:
  - 함수 내 서로 다른 논리적 블록 사이에는 단일 빈 줄(1줄)을 삽입하여 시각적 가독성을 확보함.
- **맥락 중심 주석 (Concise Contextual Comments)**:
  - 코드가 '어떻게(HOW)' 돌아가는지가 아니라 '무엇(WHAT)'을 '왜(WHY)' 하는지 설명함. 복잡한 다중 DB 아키텍처는 ASCII 다이어그램을 활용함.
- **엄격한 Python 3.10+ 타입 힌팅 (Strict Typing)**:
  - 모든 함수 시그니처에 명시적 타입 힌트(`int | None`, `tuple[...]`, `dict[str, Any]`, `list[Path]`)를 필수로 적용함.
-->

---

## 3. 🏛️ Architecture, Layering & Path Safety

- **Strict Layer Hierarchy**:
  - `UI / CLI / Tools` $\rightarrow$ `Jobs (Autonomous Batches)` $\rightarrow$ `Services (External APIs & Quant Domain)` $\rightarrow$ `Core / Data (Notion Engine & SQLite DB)`.
  - Layer-piercing is strictly FORBIDDEN (e.g., UI tools must never mutate raw Notion schemas or bypass service layers).
- **Job-Centric Dependency Co-location**:
  - Files exclusively dedicated to a specific Job (e.g., specialized AI service, system prompt `*.en.md`, specialized loader) MUST be co-located inside `jobs/<domain>/`, NOT scattered across global folders.
  - Global folders (`core/`, `data/`, `tools/`) MUST only contain infrastructure shared across the twin repositories.
- **100% Robust Absolute Paths**:
  - Bare relative paths (e.g., `open('file.txt')`) are strictly BANNED. ALWAYS resolve paths relative to `pathlib.Path(__file__).resolve()` to eliminate working directory mismatches (`FileNotFoundError`).
- **Batch File Directory Lock**:
  - Every `*.bat` script MUST begin with `cd /d "%~dp0"`.
- **Notion Schema Defensive Guard**:
  - Always guard Notion database properties with `if prop_name in page["properties"]` before accessing values.

<!--
## 3. 🏛️ 계층화 & 경로 안전성
- **엄격한 계층 구조 준수 (Strict Layer Hierarchy)**:
  - `UI / CLI / Tools` -> `Jobs (독립 실행 배치)` -> `Services (외부 연동 & 도메인 로직)` -> `Core / Data (노션 엔진 & SQLite DB)`.
  - 계층 관통 금지: UI/프롬프트 도구가 하위 Core 스키마나 SQLite 데이터를 임의로 직접 변조하지 않음.
- **종속성 중심 폴더 응집도 (Job-Centric Dependency Co-location)**:
  - 특정 Job에만 종속된 파일(전용 AI 서비스, 전용 시스템 프롬프트 `*.en.md`, 전용 로더 등)은 글로벌 폴더로 분산하지 않고 해당 `jobs/<domain>/` 폴더 내에 밀집 배치함.
  - 전역 공통 폴더(`core/`, `data/`, `tools/`)에는 2대 프로젝트 전체가 공유하는 핵심 인프라만 유지함.
- **100% 견고한 절대 경로 참조 (100% Robust Absolute Paths)**:
  - 실행 위치(CWD)에 따른 `FileNotFoundError`를 원천 방지하기 위해, 모든 파일 I/O 및 모듈 로드는 반드시 `pathlib.Path(__file__).resolve()` 기반 절대 경로를 사용함.
- **배치 파일 작업 디렉토리 고정 (Batch File Directory Lock)**:
  - 모든 `*.bat` 파일은 최상단에 `cd /d "%~dp0"`를 명시하여 스크립트 위치로 작업 디렉토리를 고정함.
- **노션 속성 스키마 방어 로직 (Notion Schema Defensive Guard)**:
  - 노션 DB 속성에 접근할 때 항상 `if prop_name in page["properties"]` 체크를 선행하여 API 예외를 방어함.
-->

---

## 4. 🧪 Scoping, Modification & TDD Bug-Fixing

- **Minimal Blast Radius**:
  - Do NOT touch, reformat, or re-comment lines unrelated to the requested change. Minimize modified lines per commit.
- **Test-First Bug Fixing Protocol**:
  1. **Fail First**: Write a failing unit test or reproduction script demonstrating the bug.
  2. **Verify Red**: Confirm test failure.
  3. **Minimal Fix**: Apply the minimal code change to fix the issue.
  4. **Verify Green**: Ensure all tests and guardrails (`python -m tests.test_guardrails`) pass.

<!--
## 4. 🧪 최소 변경 원칙 & TDD 버그 수정
- **최소 변경 원칙 (Minimal Blast Radius)**:
  - 요청받은 작업 범위와 무관한 파일, 코드 라인, 주석을 임의로 재포맷팅하거나 수정하지 않음. 커밋당 변경 라인 수를 최소화함.
- **버그 수정 TDD 프로토콜 (Test-First Bug Fixing)**:
  1. **Fail First**: 버그 수정 요청 시 실패하는 단위 테스트 또는 재현 코드를 먼저 작성함.
  2. **Verify Red**: 테스트가 의도대로 실패함을 확인함.
  3. **Minimal Fix**: 최소한의 코드 수정으로 버그를 해결함.
  4. **Verify Green**: 테스트 통과 및 불변 가드레일(`python -m tests.test_guardrails`) 전수 검증을 완료함.
-->

---

## 5. 📦 Git Commit Protocol (7 Classical Rules)

- **Rule 1**: Separate subject from body with a single blank line.
- **Rule 2**: Limit the subject line to 50 characters (72 max).
- **Rule 3**: Capitalize the subject line.
- **Rule 4**: Do not end the subject line with a period.
- **Rule 5**: Use the imperative mood (e.g., "Fix", "Add", "Refactor").
- **Rule 6**: Wrap the body text manually at 72 characters.
- **Rule 7**: Use the body to explain *WHAT* and *WHY* vs. *HOW*.
- 🚨 **Safety Constraint**: AI agents MUST NEVER automatically run `git commit` or `git push`. Always instruct the user to run `3_작업종료_동기화.bat` or provide manual commands.

<!--
## 5. 📦 Git 7대 표준 커밋 규칙
- **Rule 1**: 제목과 본문은 빈 줄(1줄)로 분리함.
- **Rule 2**: 제목은 50자 이내(최대 72자)로 간결하게 작성함.
- **Rule 3**: 제목의 첫 글자는 대문자로 시작함.
- **Rule 4**: 제목 끝에 마침표(`.`)를 붙이지 않음.
- **Rule 5**: 명령조(Imperative Mood) 동사(Fix, Add, Refactor, Docs 등)를 사용함.
- **Rule 6**: 본문 줄바꿈은 72자마다 수동 적용함.
- **Rule 7**: 본문에는 '어떻게(HOW)'가 아니라 '무엇(WHAT)'을 '왜(WHY)' 수정했는지 명시함.
- 🚨 **안전 제약 (Safety Constraint)**: AI 에이전트는 `git commit` 및 `git push`를 절대 자동 실행하지 않음. 사용자에게 `3_작업종료_동기화.bat` 실행 또는 수동 커밋 명령어를 안내함.
-->

---

## 6. 🧠 Context Dilution Countermeasures

- Explicitly reload `AGENT.md` whenever session length causes rule drift or degraded compliance.
- Keep each iteration strictly scoped to 1 feature or bugfix.

<!--
## 6. 🧠 컨텍스트 희석 방지 대책
- 긴 세션 진행으로 코드 품질 저하나 불변 규칙 위반 조짐이 보일 경우, 즉시 `AGENT.md`를 재로드하여 원칙을 환기함.
- 모든 작업은 1회 반복당 1개의 모듈/기능 단위로 집중하여 컨텍스트 희석을 방지함.
-->

---

## 📋 7. LLM Handoff Protocol (Antigravity IDE Prompt Template)

When concluding architectural changes or code refactoring, append this handoff prompt to ensure seamless continuation across IDE sessions:

```text
### 📋 [Antigravity IDE 작업 지시 프롬프트]
- 대상 프로젝트/파일: [e.g., k_all_round_portfolio/core/config_portfolio.py]
- 변경 목적 및 핵심 로직: [Specific formula, algorithm, or parameters]
- 준수 규칙: [notion_utils defensive guard, strict typing, noun-ending verbs]
- 요청 사항: 위 변경 사항을 실제 코드베이스에 정밀 반영하고 문법 검증을 완료해줘.
```

<!--
## 📋 7. LLM 인계 프로토콜 (작업 지시 프롬프트 표준 양식)
주요 설계 변경 및 코드 리팩토링 완료 시 다음 세션 또는 IDE 에이전트 연동을 위해 답변 하단에 표준 작업 지시서 형식을 첨부함.
-->

---

## 8. 🔍 Architectural Proactivity & System Audit Protocol

- **Zero-Patchwork Rule**: When the user reports an anomaly, bug, or requests a feature, AI agents MUST NOT apply superficial patches. Agents MUST inspect the root architectural cause, check for potential code fragmentation across both repositories, and enforce Single Source of Truth (SSOT).
- **Proactive Decoupling**: Static rule tables, dictionaries, and domain constants MUST be decoupled into structured files (`data/*.json` or `data/*.csv`), keeping Python scripts dedicated to pure ETL/analytical logic.
- **Deduplication First**: Any operation writing to Notion or SQLite MUST verify existing records across normalized in-memory indexes (0.001s) before issuing create commands.

<!--
## 8. 🔍 아키텍처 감사 및 선제적 리팩토링 수칙 (System Audit Protocol)
- **임시 땜질식 코딩 금지 (Zero-Patchwork Rule)**: 사용자가 오류를 제보하거나 수정을 요청할 때, 단순 증상 치료형 땜질을 금지하고 근본적인 아키텍처 원인, 양대 저장소 간 코드 파편화 여부 및 단일 진실 공급원(SSOT) 위반 여부를 선제적으로 전수 감사하여 제안함.
- **데이터와 로직의 분리 (Proactive Decoupling)**: 정적 룰셋, 사전 테이블, 도메인 분류 기준은 파이썬 코드 내 하드코딩을 지양하고 data/*.json 또는 data/*.csv로 분리하여 파이썬 코드는 순수 로직만 유지함.
- **중복 방지 우선 원칙 (Deduplication First)**: 노션 및 로컬 DB에 데이터를 생성/등록하는 모든 로직은 반드시 인메모리 3중 교차 검증(0.001s)을 선행하여 기존 레코드 재사용을 원천 강제함.
-->
