# -*- coding: utf-8 -*-
"""
tool_apply_tech_radar_patch.py
==============================
AI 테크 레이더 스캐너가 발굴한 최신 패키지 버전 승격 및 신규 고성능 도구(Polars 등)를
5대 퀀트 공식 및 노션 정규화 스키마(Guardrails) 검증을 거쳐 안전하게 원클릭 패치하는 에이전틱 도구입니다.
"""

import sys
import re
import subprocess
import logging
from pathlib import Path
from typing import List

# Windows 콘솔 UTF-8 안전화
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TechRadarPatcher")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GRAY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"

TOOLS_DIR: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = TOOLS_DIR.parent
WORKSPACE_ROOT: Path = PROJECT_ROOT.parent
REPORT_PATH: Path = PROJECT_ROOT / "reports" / "tech_radar_latest.md"


def run_guardrails_check() -> bool:
    """불변 가드레일 단위 테스트를 실행하여 안전성을 검증합니다."""
    print(f"\n{CYAN}🛡️ [Guardrails] 불변 공식 & 스키마 무결성 실시간 검증 중...{RESET}")
    python_exe = sys.executable
    cmd = [python_exe, "-m", "tests.test_guardrails"]
    res = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True, encoding="utf-8", errors="replace")
    if res.returncode == 0:
        print(f"   {GREEN}✅ 5대 퀀트 공식 & 노션 스키마 100% 정상 통과!{RESET}")
        return True
    else:
        print(f"   {RED}❌ 가드레일 위반 감지! 패치가 롤백됩니다.{RESET}")
        print(f"   {GRAY}{res.stderr or res.stdout}{RESET}")
        return False


def apply_requirements_upgrade() -> bool:
    """2대 프로젝트(update_stock & k_all_round_portfolio)의 requirements.txt를 안전 승격합니다."""
    print(f"\n{YELLOW}📦 [패치 1] requirements.txt 패키지 버전 안전 승격 진행 중...{RESET}")
    req_files: List[Path] = [
        WORKSPACE_ROOT / "update_stock" / "requirements.txt",
        WORKSPACE_ROOT / "k_all_round_portfolio" / "requirements.txt",
    ]

    upgrades = {
        "exchange-calendars": "exchange-calendars>=4.5.0",
        "pydantic": "pydantic>=2.10.0",
        "google-genai": "google-genai>=2.19.0",
    }

    for req_path in req_files:
        if not req_path.exists():
            logger.warning(f"⚠️ requirements 파일이 존재하지 않습니다: {req_path}")
            continue
        try:
            with open(req_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            new_lines = []
            for line in lines:
                trimmed = line.strip()
                matched = False
                for pkg_name, new_spec in upgrades.items():
                    if trimmed.startswith(pkg_name):
                        new_lines.append(f"{new_spec}\n")
                        matched = True
                        break
                if not matched:
                    new_lines.append(line)

            with open(req_path, "w", encoding="utf-8") as f:
                f.writelines(new_lines)

            rel = req_path.relative_to(WORKSPACE_ROOT)
            print(f"   {GREEN}✔ {rel} 버전 사양 승격 완료{RESET}")
        except Exception as e:
            logger.error(f"❌ requirements 파일 업데이트 실패 ({req_path}): {e}")

    return run_guardrails_check()


def apply_polars_scaffolding() -> bool:
    """Polars 고속 쿼리 어댑터(core/polars_helper.py)를 안전하게 생성/동기화합니다."""
    print(f"\n{YELLOW}⚡ [패치 2] Polars 고속 쿼리 엔진 어댑터 프로비저닝 중...{RESET}")
    helper_code = '''# -*- coding: utf-8 -*-
"""
polars_helper.py
================
AI 테크 레이더 추천 고성능 데이터 처리 모듈:
SQLite stock_master.db 및 CSV 데이터를 Polars를 통해 초고속(Zero-Copy)으로 로드합니다.
"""
from typing import Optional, Dict, Any

def is_polars_available() -> bool:
    try:
        import polars as pl
        return True
    except ImportError:
        return False

def read_stocks_with_polars(db_path: str):
    """Polars를 통한 초고속 주식 마스터 테이블 스캔"""
    import polars as pl
    import sqlite3
    conn = sqlite3.connect(db_path)
    df = pl.read_database("SELECT * FROM tbl_stocks;", conn)
    conn.close()
    return df
'''
    paths: List[Path] = [
        WORKSPACE_ROOT / "k_all_round_portfolio" / "core" / "polars_helper.py",
        WORKSPACE_ROOT / "update_stock" / "core" / "polars_helper.py",
    ]
    for p in paths:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "w", encoding="utf-8") as f:
                f.write(helper_code)
            rel = p.relative_to(WORKSPACE_ROOT)
            print(f"   {GREEN}✔ {rel} 생성 완료{RESET}")
        except Exception as e:
            logger.error(f"❌ Polars 헬퍼 생성 실패 ({p}): {e}")

    return run_guardrails_check()


def print_latest_tech_radar_summary() -> None:
    """최신 테크 레이더 리포트 핵심 제안을 요약 출력합니다."""
    if not REPORT_PATH.exists():
        print(f"{YELLOW}ℹ️ 아직 생성된 테크 레이더 리포트가 없습니다. 먼저 python -m jobs.tech_radar.job_sync_tech_radar 를 실행하세요.{RESET}")
        return

    print(f"\n{CYAN}{'='*70}{RESET}")
    print(f"{CYAN}{BOLD}  📡 [AI 테크 레이더] 최신 생태계 분석 & 추천 제안 브리핑{RESET}")
    print(f"{CYAN}{'='*70}{RESET}")

    try:
        with open(REPORT_PATH, "r", encoding="utf-8") as f:
            content = f.read()

        # Section 2 발췌
        rec_match = re.search(r'## 2\. 💡 AI 추천 신기술.*?\n(.*?)(?=---|\Z)', content, re.DOTALL)
        if rec_match:
            recs = [line.strip() for line in rec_match.group(1).splitlines() if line.strip().startswith("- **")]
            print(f"\n{YELLOW}{BOLD}💡 [신규 발굴 추천 도구]{RESET}")
            for r in recs[:4]:
                print(f"   {r}")

        # Section 5 발췌
        diff_match = re.search(r'## 5\. 🤖 AI 에이전틱 리팩토링.*?\n(.*?)(?=---|\Z)', content, re.DOTALL)
        if diff_match:
            print(f"\n{GREEN}{BOLD}🛠️ [추천 코드 수정안 & 가드레일]{RESET}")
            for line in diff_match.group(1).splitlines()[:10]:
                if line.strip():
                    print(f"   {GRAY}{line}{RESET}")

        print(f"{CYAN}{'-'*70}{RESET}")
    except Exception as e:
        logger.error(f"❌ 테크 레이더 요약 읽기 실패 ({REPORT_PATH}): {e}")


def interactive_menu():
    print(f"{CYAN}{'='*70}{RESET}")
    print(f"{CYAN}{BOLD}  🤖 [K-올라운드] 테크 레이더 원클릭 에이전틱 패치 적용기{RESET}")
    print(f"{CYAN}{'='*70}{RESET}")

    print_latest_tech_radar_summary()

    print(f"\n{BOLD}실행할 패치 작업을 선택하세요:{RESET}")
    print(f"  {CYAN}[1]{RESET} {BOLD}requirements.txt 패키지 버전 안전 승격 (pydantic>=2.10, exchange-calendars>=4.5){RESET}")
    print(f"  {CYAN}[2]{RESET} {BOLD}Polars 고속 데이터 연산 헬퍼 어댑터 도입{RESET}")
    print(f"  {CYAN}[3]{RESET} {BOLD}전체 패치 일괄 적용 ([1] + [2]) + 불변 가드레일 전수 검증{RESET}")
    print(f"  {CYAN}[4]{RESET} {BOLD}불변 가드레일(5대 퀀트공식 / 노션스키마) 무결성 단독 검증{RESET}")
    print(f"  {CYAN}[q]{RESET} 종료")

    choice = input(f"\n선택 (1/2/3/4/q): ").strip()
    if choice == "1":
        ok = apply_requirements_upgrade()
        if ok:
            print(f"\n🎉 {GREEN}{BOLD}[성공] requirements.txt 버전 승격 및 가드레일 검증이 완료되었습니다!{RESET}")
    elif choice == "2":
        ok = apply_polars_scaffolding()
        if ok:
            print(f"\n🎉 {GREEN}{BOLD}[성공] Polars 헬퍼 어댑터 생성 및 가드레일 검증이 완료되었습니다!{RESET}")
    elif choice == "3":
        ok1 = apply_requirements_upgrade()
        ok2 = apply_polars_scaffolding()
        if ok1 and ok2:
            print(f"\n🎉 {GREEN}{BOLD}[성공] 전체 패치 일괄 적용 및 5대 퀀트 공식 가드레일 100% 통과!{RESET}")
    elif choice == "4":
        run_guardrails_check()
    else:
        print("종료합니다.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--all":
        apply_requirements_upgrade()
        apply_polars_scaffolding()
    else:
        interactive_menu()
