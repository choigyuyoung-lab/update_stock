"""
generate_gemini_prompt.py
=========================
현재 [K-올라운드 마스터] 프로젝트의 최신 아키텍처, 퀀트 엔진 수식, 노션 DB 스키마, 
Git 상태를 종합하여 Gemini 웹/모바일 앱에 즉시 붙여넣을 수 있는 완성형 컨텍스트 프롬프트를 자동 생성하고
Windows 클립보드에 복사합니다.
"""

import os
import sys
import subprocess
import datetime
import socket
from typing import Optional, List

# 터미널 UTF-8 설정
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# 색상 출력용 ANSI 코드
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
GRAY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"

def get_git_info():
    """현재 Git 상태 및 최근 커밋 내역 수집"""
    try:
        status_res = subprocess.run(["git", "status", "-s"], capture_output=True, text=True, encoding="utf-8", errors="replace")
        status_out = status_res.stdout.strip()
        if not status_out:
            status_summary = "• 모든 파일이 원격 저장소와 완벽히 동기화된 상태임 (Clean Working Tree)"
        else:
            status_summary = "• 로컬 수정/작업 중인 파일 목록:\n" + "\n".join([f"  - {line}" for line in status_out.splitlines()[:10]])

        log_res = subprocess.run(["git", "log", "-n", "3", "--oneline"], capture_output=True, text=True, encoding="utf-8", errors="replace")
        log_out = log_res.stdout.strip()
        recent_commits = log_out if log_out else "최근 커밋 기록 없음"
    except Exception as e:
        status_summary = f"Git 상태 조회 불가: {e}"
        recent_commits = "조회 불가"
    
    return status_summary, recent_commits

def copy_to_clipboard(text: str) -> bool:
    """Windows 클립보드에 텍스트 복사"""
    try:
        process = subprocess.Popen(["clip"], stdin=subprocess.PIPE, shell=True)
        process.communicate(input=text.encode("utf-16le"))
        return process.returncode == 0
    except Exception:
        try:
            process = subprocess.Popen(["clip"], stdin=subprocess.PIPE, shell=True)
            process.communicate(input=text.encode("utf-8"))
            return process.returncode == 0
        except Exception:
            return False

def generate_prompt() -> str:
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hostname = socket.gethostname().upper()
    location = f"🏢 회사 PC ({hostname})" if hostname == "CHOIGYUYOUNG" else f"🏠 집 PC ({hostname})"
    git_status, recent_commits = get_git_info()

    prompt_content = f"""# 🚀 [Context & Memory] K-올라운드 마스터 (update_stock) 프로젝트 AI 세션 연속성 프롬프트

- **생성 일시**: {now_str}
- **작업 환경**: {location}

---

## 1. [Role & Mission]
- **역할 (Role)**: 당신은 Python 3.10 기반 노션(Notion) 연동 퀀트 주식/ETF 자동화 시스템인 **[K-올라운드 마스터 (update_stock)]** 프로젝트를 완벽히 숙지하고 있는 **시니어 퀀트 소프트웨어 엔지니어**입니다.
- **핵심 미션 (Mission)**: 
  - 국내(KRX, KOSPI, KOSDAQ) 및 미국(S&P500, NASDAQ) 주식/ETF의 시세·재무·퀀트 지표를 수집하고 노션 데이터베이스에 무결성 있게 적재합니다.
  - 7대 자산군 듀얼 모멘텀, 스마트 밸류 에버리징(Value Averaging), 95% 1-Week VaR(Value at Risk) 등 정량적 리스크 관리 엔진과 Gemini AI 기반 자산배분 주간 리포트 파이프라인을 안정적으로 유지보수하고 발전시킵니다.
  - 사용자가 모바일(Gemini App)이나 웹(gemini.google.com)에서 어떤 질문이나 코드 수정을 요청하더라도, 기존 아키텍처와 규칙을 100% 준수하여 즉시 실전에 적용 가능한 완성형 코드를 제시합니다.

---

## 2. [System Architecture & Tech Stack]

### 🛠️ 기술 스택 (Tech Stack)
- **Core Environment**: Python 3.10+, Windows 11 (로컬) & Ubuntu-latest (GitHub Actions CI/CD)
- **Notion Integration**: `notion-client` (v2.x), Custom Paginated Engine (`notion_utils.py`), Multi-threading (`ThreadPoolExecutor`), Safe Property Mapping
- **데이터 소스 (Data Ingestion Engine)**:
  - 🇰🇷 국내 주식/ETF: `FinanceDataReader (fdr)` (KRX-DESC, ETF/KR 초고속 시계열 캐싱), 한국투자증권(KIS) Open API (`FHKST01010100` 시세/투자지표, 토큰 자동 갱신 및 `.kis_token_cache.json` 캐싱)
  - 🇺🇸 미국 주식/ETF: `yfinance` (1년치 일봉 데이터 및 기본 재무 지표 일괄 추출)
  - 🌐 거시경제/매크로: `FinanceDataReader` + `yfinance` (환율 USD/KRW, 미국 10년물 국채금리, WTI, 금선물, S&P500, KOSPI 등)
- **AI & Grounding Engine**: Google Gemini API (`ai_service.py` - Google Search Grounding 연동, 4단계 모델 Fallback)
- **스케줄링 & CI/CD**: GitHub Actions (9개 독립 워크플로), cronjob.org 웹훅, 로컬 스마트 동기화 매니저 (`sync_manager.py`)

### ⚡ GitHub Actions 독립 워크플로 목록
1. `kr_price_update.yml` / `us_price_update.yml`: 국내/미국 장중 실시간 현재가·등락률 동기화
2. `kr_finance_update.yml` / `us_finance_update.yml`: 장 마감 후 재무제표, 5대 퀀트 팩터 일괄 계산 및 노션 적재
3. `kr_master_db_sync.yml` / `us_master_db_sync.yml`: 신규 상장/섹터/산업/벤치마크 릴레이션 마스터 동기화
4. `update_etf_holdings.yml`: 주요 ETF의 구성종목 및 비중 자동 갱신
5. `benchmark_db_sync.yml`: 주요 지수, 환율, 금리 벤치마크 DB 동기화
6. `generate_report.yml`: 주간 AI 포트폴리오 진단 및 밸류 에버리징 리포트 생성

---

## 3. [Database Schema & Key Functions Snapshot]

### 📊 노션 데이터베이스 스키마 매핑 (개별 종목 Master DB)
- **기본 식별/시세**: `티커`, `종목명`, `마켓`(KOSPI/KOSDAQ/NYSE/NASDAQ/ETF), `현재가`, `전일 종가`, `마지막 업데이트`
- **재무 팩터**: `PER`, `PBR`, `EPS`, `BPS`, `배당수익률`, `업종PER`
- **5대 핵심 퀀트 팩터 (파이썬 100% 자체 계산)**:
  - `200일선` (Number): 최근 200영업일 종가 이동평균 (`Close.rolling(200).mean()`)
  - `수급선` (Number): 미국 50일선 / 한국 60일선
  - `추세` (Select): `▲ 기관주도` / `▲ 수급유입` / `━ 눌림조정` / `━ 박스권세` / `▼ 하락추세`
  - `12M 모멘텀` (Number-%): (현재가 - 252일전가격) / 252일전가격
  - `52주 낙폭` (Number-%): (현재가 - 52주최고가) / 52주최고가 (음수 백분율)
  - `60일 변동성` (Number-%): std(일일수익률_60) * sqrt(252) (연환산 변동성)
- **진단 등급**: `위험도 등급`, `스마트 가이드`, `모멘텀 진단`
- **릴레이션**: `시장BM`, `K산업BM`, `G산업BM`

### ⚙️ 핵심 모듈 및 함수 스냅샷
- `notion_utils.py`: `paginate_database`, `safe_page_update`, `batch_update_pages`, `get_kis_auth_context`
- `macro_service.py`: `_calculate_dynamic_band` (60영업일 롤링 백분위수 Q25/Q50/Q75), `get_7_asset_quant_metrics`
- `generate_portfolio_report.py`: `calculate_smart_value_averaging` (100만원 적립금 배분), `calculate_portfolio_var` (95% 1-Week VaR)
- `sync_manager.py`: 회사/집 PC 자동 인식, `git pull/push`, `.env` 보안 파일 점검

---

## 4. [Current Session Progress & Decisions]

### 📌 최근 작업 및 설계 결정 사항
1. **GitHub Actions 파이프라인 성능 최적화 (2026-08 완료)**:
   - 9개 워크플로우 전체에 `actions/setup-python@v5` (cache: 'pip'), `fetch-depth: 1`, `timeout-minutes: 5` 적용. 패키지 설치 시간 45초 ➡️ 3초 단축 및 무료 분 소모 50% 절감.
2. **노션 무거운 수식 열 제거 및 파이썬 자체 계산 전환 (DB 경량화)**:
   - `52주 위치`, `안전마진`, `전일대비`, `목표주가`, `52주 최고/최저가` 등을 제거하고 파이썬에서 전량 계산.
3. **동적 거시경제 밴드 & 7대 자산 퀀트 엔진 탑재**:
   - 환율/금리의 3개월(60영업일) 동적 백분위수($Q_{25}, Q_{50}, Q_{75}$) 밴드 산출.
   - 7대 자산군(069500, DBC, GLD, SCHD, SPY, 153130, TLT) 듀얼 모멘텀 순위표 및 스마트 밸류 에버리징 배분 계산기 탑재.
4. **재택/회사 원클릭 동기화 및 프롬프트 연동**:
   - `작업종료_동기화.bat` 실행 시 `gemini_context_prompt.md` 자동 갱신 및 Git Push 통합.

### 📋 현재 Git 작업 상태
{git_status}

- **최근 커밋 로그**:
{recent_commits}

---

## 5. [Instruction to LLM]
1. **맥락 유지**: 사용자가 모바일/웹 Gemini에서 질문이나 요청을 할 때, 위 아키텍처, 퀀트 공식, 노션 스키마, 파일 구조를 완벽히 인지한 상태에서 답변하십시오.
2. **코드 작성 원칙**:
   - Python 3.10 문법을 준수하고, 타입 힌팅(`typing`)과 예외 처리(`try-except`)를 철저히 작성하십시오.
   - 노션 속성을 수정하는 코드는 반드시 `notion_utils.py`의 함수와 스키마 방어 로직(`if field in props`)을 적용하십시오.
   - 불필요한 KIS 투자의견 API나 무거운 노션 수식에 의존하지 말고 자체 계산 로직을 사용하십시오.
3. **리포트 문체 규칙**: 리포트 분석 문장은 반드시 **명사형 종결어미 (`~함`, `~임`, `~필요`, `~권고`)**로 작성하십시오.
4. **즉시 실행성**: 설명만 장황하게 늘어놓지 말고, 사용자가 바로 복사하여 교체할 수 있는 완성형 코드 블록과 수정 위치를 명확히 제시하십시오.
5. **🌟 [중요: IDE 이전용 작업 지시서 자동 생성 (Handoff Protocol)]**:
   - 대화 중 **새로운 퀀트 공식 적용, 코드 수정, 노션 DB 스키마 변경, 버그 수정 등 실제 PC 코드에 반영해야 할 중요한 결론이나 아이디어가 도출되었을 때**, 답변 맨 마지막에 항상 **[Antigravity IDE 작업 지시 프롬프트]** 코드 블록을 자동으로 첨부해 주십시오.
   - 사용자가 해당 블록만 복사하여 PC의 Antigravity IDE에 붙여넣으면, IDE가 즉시 오차 없이 실제 파일들을 찾아 수정할 수 있도록 아래 형식으로 작성하십시오:
   ```text
   ### 📋 [Antigravity IDE 작업 지시 프롬프트]
   - 수정 대상 파일: [예: update_finance_kr.py, config_portfolio.py 등]
   - 변경 목적 및 핵심 로직: [구체적인 수식, 변경할 알고리즘, 파라미터]
   - 준수 규칙: [notion_utils 방어 로직 유지, 타입 힌팅, 명사형 종결어미 등]
   - 요청 사항: 위 변경 사항을 실제 코드베이스에 정밀 반영하고 문법 검증을 완료해줘.
   ```
"""
    return prompt_content

import shutil

def find_google_drive_dir() -> Optional[str]:
    """PC 내 구글 드라이브 데스크톱 폴더 자동 탐색"""
    possible_roots = [
        r"G:\내 드라이브",
        r"G:\My Drive",
        os.path.expanduser(r"~\Google Drive"),
        os.path.expanduser(r"~\내 드라이브"),
        r"C:\Google Drive",
        r"D:\Google Drive",
    ]
    
    # 1. 기존에 생성된 update_stock 관련 폴더 우선 탐색
    for root in possible_roots:
        if os.path.exists(root):
            for sub in ["update_stock_core", "update_stock", "update_stock_prompt"]:
                candidate = os.path.join(root, sub)
                if os.path.exists(candidate):
                    return candidate

    # 2. 루트 드라이브가 존재하면 update_stock_core 폴더 생성 후 반환
    for root in possible_roots:
        if os.path.exists(root):
            target = os.path.join(root, "update_stock_core")
            try:
                os.makedirs(target, exist_ok=True)
                return target
            except Exception:
                pass
    return None

def get_sync_target_files(workspace_dir: str) -> List[str]:
    """프로젝트 내 유의미한 핵심 소스파일(.py, .md, .bat, .yml) 수집 (외부 라이브러리/샘플/캐시 제외)"""
    targets = []
    exclude_dirs = {
        ".git", ".venv", "venv", "env", ".gemini", ".vscode", 
        ".devcontainer", ".agents", "__pycache__", "backup", 
        "open-trading-api", "reports"
    }
    
    for root, dirs, files in os.walk(workspace_dir):
        # 제외 폴더 필터링
        dirs[:] = [d for d in dirs if d not in exclude_dirs and not d.startswith(".")]
        
        for f in files:
            if f.endswith((".py", ".md", ".bat", ".yml")):
                rel_path = os.path.relpath(os.path.join(root, f), workspace_dir)
                targets.append(rel_path)
    return sorted(targets)

def sync_to_google_drive(prompt_path: str):
    """구글 드라이브로 프롬프트 및 모든 신규/기존 코드 파일 자동 동기화"""
    gdrive_dir = find_google_drive_dir()
    if not gdrive_dir:
        print(f"  {YELLOW}ℹ️ 구글 드라이브 데스크톱 폴더(G:\\내 드라이브 등)를 찾지 못해 로컬 저장만 완료했습니다.{RESET}")
        return

    workspace_dir = os.path.dirname(__file__)
    target_files = get_sync_target_files(workspace_dir)
    copied_count = 0

    try:
        for rel_file in target_files:
            src = os.path.join(workspace_dir, rel_file)
            dst = os.path.join(gdrive_dir, rel_file)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            copied_count += 1

        print(f"☁️ {GREEN}{BOLD}구글 드라이브 전체 동기화 완료! (신규 파일 포함 {copied_count}개 파일){RESET}")
        print(f"   📂 동기화 경로: {CYAN}{gdrive_dir}{RESET}")
    except Exception as e:
        print(f"  {YELLOW}⚠️ 구글 드라이브 복사 중 오류: {e}{RESET}")

def main():
    print(f"{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  [K-올라운드] Gemini 모바일/웹 세션 프롬프트 자동 생성기{RESET}")
    print(f"{CYAN}{'='*60}{RESET}")

    prompt = generate_prompt()
    
    # 1. 로컬 파일 저장
    output_filename = "gemini_context_prompt.md"
    output_path = os.path.join(os.path.dirname(__file__), output_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(prompt)
    print(f"📁 로컬 파일 저장 완료: {YELLOW}{output_filename}{RESET}")

    # 2. 구글 드라이브 자동 동기화
    sync_to_google_drive(output_path)

    # 3. 클립보드 복사
    copied = copy_to_clipboard(prompt)
    if copied:
        print(f"📋 {GREEN}{BOLD}클립보드 자동 복사 성공!{RESET}")
    else:
        print(f"⚠️ {YELLOW}클립보드 복사 실패 (위 파일 {output_filename}을 열어 복사하세요){RESET}")

    print(f"{CYAN}{'-'*60}{RESET}")
    print(f"💡 {BOLD}사용 방법:{RESET}")
    print(f"  1. 스마트폰의 {GREEN}Gemini 앱{RESET} 또는 {GREEN}gemini.google.com{RESET} 접속")
    print(f"  2. Gems 또는 대화창에서 {YELLOW}@Google Drive{RESET}로 실시간 조회하며 작업 진행!")
    print(f"{CYAN}{'='*60}{RESET}\n")

if __name__ == "__main__":
    main()
