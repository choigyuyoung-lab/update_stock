# -*- coding: utf-8 -*-
"""
sync_manager.py
===============
1. 회사 PC <-> 집 PC 간 작업 환경 전환 감지 및 미동기화 파일 경고
2. 직전 작업/커밋 이력 간략 요약 브리핑
3. 주간/월간 주기별 전략 고도화 & 코드 수정 영감 체크리스트 질문 팝업
4. 2대 저장소(update_stock & k_all_round_portfolio) 원클릭 양방향 Git 동기화
"""

import sys
import json
import re
import socket
import datetime
import subprocess
import logging
import shutil
import py_compile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Windows 콘솔 UTF-8 출력 안전화
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            getattr(sys.stdout, "reconfigure")(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SyncManager")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GRAY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"

TOOLS_DIR: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = TOOLS_DIR.parent if TOOLS_DIR.name == "tools" else TOOLS_DIR
WORKSPACE_ROOT: Path = PROJECT_ROOT.parent if PROJECT_ROOT.name in ["update_stock", "k_all_round_portfolio", "workspace-vault"] else PROJECT_ROOT
STATE_FILE: Path = WORKSPACE_ROOT / "update_stock" / "data" / ".sync_state.json"


def run_cmd(cmd_list: List[str], cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    """터미널 명령어를 실행하고 리턴코드, stdout, stderr를 반환합니다."""
    try:
        result = subprocess.run(
            cmd_list,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return 1, "", str(e)


def get_location() -> str:
    """호스트명(컴퓨터 이름)으로 회사/집 환경을 판별합니다 (호스트명 비노출)."""
    hostname = socket.gethostname().upper()
    company_host = "CHOIGYUYOUNG"
    if hostname == company_host:
        return "🏢 회사 환경"
    else:
        return "🏠 개인 환경"


def ensure_workspace_vault_cloned() -> None:
    """회사 PC나 신규 PC에 workspace-vault 저장소가 없는 경우 자동으로 GitHub에서 Clone합니다."""
    vault_path: Path = WORKSPACE_ROOT / "workspace-vault"
    if not (vault_path / ".git").exists():
        print(f"\n{CYAN}📦 [신규 저장소 감지] workspace-vault 저장소가 로컬에 없습니다. GitHub에서 자동 Clone 진행...{RESET}")
        clone_url = "https://github.com/choigyuyoung-lab/workspace-vault.git"
        code, stdout, stderr = run_cmd(["git", "clone", clone_url], cwd=WORKSPACE_ROOT)
        if code == 0:
            print(f"  {GREEN}✅ workspace-vault 자동 클론 성공!{RESET}")
            # .venv 정션 링크 자동 연결
            venv_src = WORKSPACE_ROOT / "update_stock" / ".venv"
            venv_dst = vault_path / ".venv"
            if venv_src.exists() and not venv_dst.exists():
                try:
                    if sys.platform == "win32":
                        subprocess.run(["cmd", "/c", "mklink", "/J", str(venv_dst), str(venv_src)], capture_output=True)
                except Exception:
                    pass
        else:
            print(f"  {RED}❌ workspace-vault 클론 실패: {stderr}{RESET}")


def get_all_target_repos() -> List[Dict[str, Any]]:
    """update_stock, k_all_round_portfolio, workspace-vault 3대 저장소 경로를 정확히 탐색합니다."""
    ensure_workspace_vault_cloned()

    target_defs = [
        ("update_stock", "update_stock (금융 데이터 허브)"),
        ("k_all_round_portfolio", "k_all_round_portfolio (자산배분 & AI 리포트)"),
        ("workspace-vault", "workspace-vault (보안 설정·백업·문서 금고)")
    ]

    repos: List[Dict[str, Any]] = []
    for folder_name, display_name in target_defs:
        repo_path: Path = WORKSPACE_ROOT / folder_name
        if (repo_path / ".git").exists():
            repos.append({"name": display_name, "path": repo_path})

    if not repos:
        repos.append({"name": PROJECT_ROOT.name, "path": PROJECT_ROOT})

    return repos



def sync_common_ssot_files() -> None:
    """
    3대 저장소(k_all_round, update_stock, workspace-vault/configs) 간 공통 핵심 파일(GEMINI.md, AGENT.md)의
    최신 수정 사항을 상호 비교하여 양방향 자동 전파(SSOT 무결성 일치화)합니다.
    GitHub 웹에서 직접 수정했거나 로컬 특정 프로젝트에서 수정한 변경사항도 3개 저장소에 즉시 동기화됩니다.
    """
    vault_repo = WORKSPACE_ROOT / "workspace-vault"
    if not (vault_repo / ".git").exists():
        return

    configs_dir = vault_repo / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)

    common_files = ["GEMINI.md", "AGENT.md"]
    synced_count = 0

    for fname in common_files:
        vault_file = configs_dir / fname
        k_file = WORKSPACE_ROOT / "k_all_round_portfolio" / fname
        u_file = WORKSPACE_ROOT / "update_stock" / fname

        candidates = [p for p in [vault_file, k_file, u_file] if p.exists()]
        if not candidates:
            continue

        # 가장 최근에 수정된 최신 파일 탐색
        latest_file = max(candidates, key=lambda p: p.stat().st_mtime)
        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                latest_content = f.read()

            for target in [vault_file, k_file, u_file]:
                needs_update = False
                if not target.exists():
                    needs_update = True
                else:
                    try:
                        with open(target, "r", encoding="utf-8") as tf:
                            if tf.read() != latest_content:
                                needs_update = True
                    except Exception:
                        needs_update = True

                if needs_update:
                    shutil.copy2(latest_file, target)
                    synced_count += 1
        except Exception as e:
            logger.debug(f"SSOT 파일 동기화 예외 ({fname}): {e}")

    # pyrightconfig.json 동기화 (configs/ -> 각 프로젝트)
    vault_pyright = configs_dir / "pyrightconfig.json"
    if vault_pyright.exists():
        for proj in ["k_all_round_portfolio", "update_stock"]:
            proj_pyright = WORKSPACE_ROOT / proj / "pyrightconfig.json"
            if not proj_pyright.exists():
                shutil.copy2(vault_pyright, proj_pyright)

    if synced_count > 0:
        print(f"  ✨ [SSOT 자동 동기화] 공통 마스터 파일({synced_count}건)을 3대 저장소에 최신 일치화했습니다.")


def sync_env_vault_backup() -> None:
    """작업 종료 시 프로젝트의 .env, 전역 설정(configs/) 및 중요 토큰 캐시를 workspace-vault로 안전 백업합니다."""
    vault_repo = WORKSPACE_ROOT / "workspace-vault"
    if not (vault_repo / ".git").exists():
        return

    env_vault_dir = vault_repo / "env_vault"
    backups_dir = vault_repo / "backups"
    docs_dir = vault_repo / "docs"
    configs_dir = vault_repo / "configs"

    env_vault_dir.mkdir(parents=True, exist_ok=True)
    backups_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)

    # 1. .env 파일 백업
    env_copied = 0
    for proj_name in ["update_stock", "k_all_round_portfolio"]:
        src_env = WORKSPACE_ROOT / proj_name / ".env"
        dst_env = env_vault_dir / f"{proj_name}.env"
        if src_env.exists():
            shutil.copy2(src_env, dst_env)
            env_copied += 1

    # 2. 3대 저장소 SSOT 동기화
    sync_common_ssot_files()

    # 3. Antigravity IDE 대화 기록(brain/) 자동 아카이빙 백업
    chat_count = backup_chat_history()
    if chat_count > 0:
        print(f"  💬 [대화기록 백업] Antigravity IDE 최근 대화 세션 {chat_count}개를 workspace-vault로 백업했습니다.")

    if env_copied > 0:
        print(f"  🔐 [보안 금고 백업] .env({env_copied}건) 및 전역 설정을 workspace-vault로 안전 백업했습니다.")


def get_antigravity_brain_dir() -> Optional[Path]:
    """현재 운영체제에 맞는 Antigravity IDE의 로컬 brain/ 디렉토리 경로를 반환합니다."""
    import os
    if sys.platform == "win32":
        user_prof = os.environ.get("USERPROFILE", "")
        if user_prof:
            b_path = Path(user_prof) / ".gemini" / "antigravity-ide" / "brain"
            if b_path.exists():
                return b_path
    else:
        b_path = Path.home() / ".gemini" / "antigravity-ide" / "brain"
        if b_path.exists():
            return b_path
    return None


def backup_chat_history(max_sessions: int = 30) -> int:
    """Antigravity IDE의 대화 기록(brain/ 디렉토리)을 workspace-vault로 자동 아카이빙 백업합니다."""
    brain_dir = get_antigravity_brain_dir()
    if not brain_dir or not brain_dir.exists():
        return 0

    vault_repo = WORKSPACE_ROOT / "workspace-vault"
    if not (vault_repo / ".git").exists():
        return 0

    vault_brain_dir = vault_repo / "backups" / "brain"
    vault_chat_doc_dir = vault_repo / "backups" / "chat_history"
    vault_brain_dir.mkdir(parents=True, exist_ok=True)
    vault_chat_doc_dir.mkdir(parents=True, exist_ok=True)

    session_dirs = [p for p in brain_dir.iterdir() if p.is_dir() and p.name != "tempmediaStorage"]
    session_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    backed_up_count = 0
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    index_lines = [
        "# 💬 Antigravity IDE 대화 기록 색인 목록 (Chat History Archive)\n",
        f"> **최근 동기화 시각**: {now_str}  ",
        f"> **보관 세션 수**: 최근 {min(len(session_dirs), max_sessions)}개 세션  \n",
        "| 번호 | 최근 일시 | 세션 ID | 초기 질문 / 주제 요약 | 계획서 |",
        "| :--- | :--- | :--- | :--- | :--- |"
    ]

    for idx, sdir in enumerate(session_dirs[:max_sessions], 1):
        cid = sdir.name
        dst_sdir = vault_brain_dir / cid
        dst_sdir.mkdir(parents=True, exist_ok=True)

        plan_src = sdir / "implementation_plan.md"
        walk_src = sdir / "walkthrough.md"
        trans_src = sdir / ".system_generated" / "logs" / "transcript.jsonl"

        if plan_src.exists():
            shutil.copy2(plan_src, dst_sdir / "implementation_plan.md")
        if walk_src.exists():
            shutil.copy2(walk_src, dst_sdir / "walkthrough.md")
        if trans_src.exists():
            dst_logs = dst_sdir / ".system_generated" / "logs"
            dst_logs.mkdir(parents=True, exist_ok=True)
            shutil.copy2(trans_src, dst_logs / "transcript.jsonl")

        first_q = "(대화 내용 없음)"
        time_str = datetime.datetime.fromtimestamp(sdir.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        if trans_src.exists():
            try:
                with open(trans_src, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        data = json.loads(line)
                        if data.get("type") == "USER_INPUT":
                            raw_content = data.get("content", "").strip()
                            clean_q = re.sub(r'<[^>]+>', '', raw_content).strip()
                            clean_q = clean_q.replace("\n", " ").replace("|", "/")
                            if clean_q:
                                first_q = clean_q[:60] + "..." if len(clean_q) > 60 else clean_q
                                break
            except Exception:
                pass

        plan_link = "✅ 유" if plan_src.exists() else "-"
        index_lines.append(f"| {idx} | {time_str} | `{cid[:8]}` | {first_q} | {plan_link} |")
        backed_up_count += 1

    index_path = vault_chat_doc_dir / "대화이력_색인.md"
    try:
        with open(index_path, "w", encoding="utf-8") as f:
            f.write("\n".join(index_lines) + "\n")
    except Exception:
        pass

    return backed_up_count


def restore_chat_history() -> int:
    """workspace-vault의 대화 기록(brain/ 디렉토리)을 현재 PC의 Antigravity 로컬 brain/ 디렉토리로 자동 복원합니다."""
    brain_dir = get_antigravity_brain_dir()
    if not brain_dir:
        import os
        if sys.platform == "win32":
            user_prof = os.environ.get("USERPROFILE", "")
            if user_prof:
                brain_dir = Path(user_prof) / ".gemini" / "antigravity-ide" / "brain"
        else:
            brain_dir = Path.home() / ".gemini" / "antigravity-ide" / "brain"

    if not brain_dir:
        return 0

    vault_repo = WORKSPACE_ROOT / "workspace-vault"
    vault_brain_dir = vault_repo / "backups" / "brain"
    if not vault_brain_dir.exists():
        return 0

    brain_dir.mkdir(parents=True, exist_ok=True)
    restored_count = 0

    for sdir in vault_brain_dir.iterdir():
        if not sdir.is_dir():
            continue
        dst_sdir = brain_dir / sdir.name
        if not dst_sdir.exists():
            try:
                shutil.copytree(sdir, dst_sdir)
                restored_count += 1
            except Exception:
                pass

    return restored_count


def restore_env_from_vault() -> None:
    """작업 시작 시 workspace-vault의 최신 .env, 전역 설정(configs/), 토큰 캐시 및 대화기록을 자동 동기화 복원합니다."""
    vault_repo = WORKSPACE_ROOT / "workspace-vault"
    if not (vault_repo / ".git").exists():
        return

    env_vault_dir = vault_repo / "env_vault"
    backups_dir = vault_repo / "backups"
    configs_dir = vault_repo / "configs"

    # 1. .env 양방향 최신 복원/동기화 (USB 불필요)
    if env_vault_dir.exists():
        for proj_name in ["update_stock", "k_all_round_portfolio"]:
            dst_env = WORKSPACE_ROOT / proj_name / ".env"
            src_env = env_vault_dir / f"{proj_name}.env"
            if src_env.exists():
                shutil.copy2(src_env, dst_env)
                print(f"  ✨ [자동 동기화] {proj_name}/.env 설정을 workspace-vault 금고와 최신 일치화했습니다.")

    # 2. 3대 저장소 SSOT 공통 파일 자동 동기화 (GEMINI.md, AGENT.md)
    sync_common_ssot_files()

    # 3. .venv 가상환경 정션 링크 점검 및 자동 연결
    venv_src = WORKSPACE_ROOT / "update_stock" / ".venv"
    if venv_src.exists():
        for link_target in [WORKSPACE_ROOT / ".venv", WORKSPACE_ROOT / "k_all_round_portfolio" / ".venv", vault_repo / ".venv"]:
            if not link_target.exists():
                try:
                    if sys.platform == "win32":
                        subprocess.run(["cmd", "/c", "mklink", "/J", str(link_target), str(venv_src)], capture_output=True)
                    else:
                        link_target.symlink_to(venv_src)
                except Exception:
                    pass

    # 4. .vscode IDE 환경 설정 최신 동기화 (workspace-vault/configs -> .vscode/)
    root_vscode = WORKSPACE_ROOT / ".vscode"
    root_vscode.mkdir(parents=True, exist_ok=True)
    if (configs_dir / "vscode_settings.json").exists():
        shutil.copy2(configs_dir / "vscode_settings.json", root_vscode / "settings.json")
    if (configs_dir / "vscode_tasks.json").exists():
        shutil.copy2(configs_dir / "vscode_tasks.json", root_vscode / "tasks.json")
    if (configs_dir / "vscode_extensions.json").exists():
        shutil.copy2(configs_dir / "vscode_extensions.json", root_vscode / "extensions.json")

    # 5. Antigravity IDE 대화 기록(brain/) 로컬 복원
    restored_chat = restore_chat_history()
    if restored_chat > 0:
        print(f"  💬 [대화기록 복원] workspace-vault에서 대화 세션 {restored_chat}개를 로컬 IDE로 복원했습니다.")






def load_sync_state() -> Dict[str, Any]:
    """주기 체크용 상태 파일을 로드합니다."""
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        if STATE_FILE.exists():
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.debug(f"동기화 상태 파일 읽기 생략 ({STATE_FILE}): {e}")
    return {
        "last_location": "",
        "last_weekly_check": "",
        "last_monthly_check": "",
        "last_sync_time": ""
    }


def save_sync_state(state: Dict[str, Any]) -> None:
    """주기 체크용 상태 파일을 저장합니다."""
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"⚠️ 상태 파일 저장 실패 ({STATE_FILE}): {e}")


def show_recent_work_summary(repo_path: Path, repo_name: str) -> None:
    """직전 커밋 이력을 2~3줄로 깔끔하게 요약 출력합니다."""
    code, log_out, _ = run_cmd(
        ["git", "log", "-n", "3", "--pretty=format:%cd | %s", "--date=format:%Y-%m-%d %H:%M"],
        cwd=repo_path
    )
    if code == 0 and log_out:
        print(f"\n  {CYAN}📦 [{repo_name} 직전 작업 이력]{RESET}")
        for line in log_out.splitlines():
            print(f"     • {GRAY}{line}{RESET}")


def check_periodic_strategy_questions(state: Dict[str, Any]) -> None:
    """주간(7일) 및 월간(30일) 단위 전략 고도화 & 코드 수정 영감 체크리스트를 출력합니다."""
    today_str = datetime.date.today().isoformat()
    today = datetime.date.today()

    last_weekly = state.get("last_weekly_check", "")
    last_monthly = state.get("last_monthly_check", "")

    show_weekly = False
    show_monthly = False

    if not last_weekly:
        show_weekly = True
    else:
        try:
            prev_d = datetime.date.fromisoformat(last_weekly)
            if (today - prev_d).days >= 7:
                show_weekly = True
        except Exception:
            show_weekly = True

    if not last_monthly:
        show_monthly = True
    else:
        try:
            prev_m = datetime.date.fromisoformat(last_monthly)
            if (today - prev_m).days >= 30:
                show_monthly = True
        except Exception:
            show_monthly = True

    if not show_weekly and not show_monthly:
        return

    print(f"\n{YELLOW}{'='*70}{RESET}")
    print(f"{YELLOW}{BOLD}  💡 [정기 점검] 전략 고도화 & 코드 수정 영감 체크리스트{RESET}")
    print(f"{YELLOW}{'='*70}{RESET}")

    if show_weekly:
        print(f"\n{CYAN}{BOLD}📅 [주간 전략 점검 (Weekly Checklist)]{RESET}")
        print(f"  🎬 1) {BOLD}[유튜브 인사이트]{RESET} 최근 새로 구독하거나 시황을 추적하고 싶은 신규 유튜브 채널 RSS가 있나요?")
        print(f"     ➔ {GRAY}수정 위치: update_stock/jobs/youtube/job_sync_youtube_insights.py (YOUTUBE_CHANNELS 목록){RESET}")
        print(f"  🤖 2) {BOLD}[AI 리포트 & 테크 레이더]{RESET} 프롬프트 최적화 또는 테크 레이더 추천 신기술 도입 검토가 필요한가요?")
        print(f"     ➔ {GRAY}수정 위치: k_all_round_portfolio/jobs/quant_report/system_portfolio_quant.en.md{RESET}")
        print(f"  🛡️ 3) {BOLD}[불변 가드레일]{RESET} 5대 퀀트 공식 및 노션 정규화 스키마 무결성 검증 (python -m tests.test_guardrails)")
        state["last_weekly_check"] = today_str

    if show_monthly:
        print(f"\n{GREEN}{BOLD}🏛️ [월간 전략 점검 (Monthly Checklist)]{RESET}")
        print(f"  📊 1) {BOLD}[자산배분 룰]{RESET} 7대 자산군 목표 비중(성장주 25%, 장기채 20% 등)이나 환율 롤링 밴드($Q_{{25}}, Q_{{75}}$) 스위칭 룰을 재검토하시겠습니까?")
        print(f"     ➔ {GRAY}수정 위치: k_all_round_portfolio/core/config_portfolio.py{RESET}")
        print(f"  🏷️ 2) {BOLD}[온톨로지 사전]{RESET} 새로 상장된 특이 ETF나 관심 종목 키워드를 노션 [사전 DB]에 보강할 필요가 있나요?")
        print(f"     ➔ {GRAY}수정 위치: 노션 온톨로지 사전 DB (tbl_dictionary){RESET}")
        print(f"  🔑 3) {BOLD}[KIS API Key]{RESET} 한국투자증권 실전 API Key의 1년 유효기간을 점검하셨나요?")
        state["last_monthly_check"] = today_str

    print(f"{YELLOW}{'-'*70}{RESET}")


def mode_start() -> None:
    """작업 시작 시 환경 판별, 미동기화 체크, Git Pull, 이전 작업 요약 및 주기별 질문 팝업을 수행합니다."""
    location = get_location()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    repos = get_all_target_repos()
    state = load_sync_state()

    print(f"{CYAN}{'='*70}{RESET}")
    print(f"{CYAN}{BOLD}  🚀 [K-올라운드 & update_stock] 스마트 작업 시작 동기화 매니저{RESET}")
    print(f"{CYAN}{'='*70}{RESET}")
    print(f"📍 현재 환경 : {YELLOW}{location}{RESET}")
    print(f"⏰ 실행 시각 : {GRAY}{now_str}{RESET}")

    # 환경 변경 감지
    last_loc = state.get("last_location", "")
    if last_loc and last_loc != location:
        print(f"🔄 {GREEN}{BOLD}[작업 환경 전환 감지]{RESET} 이전 작업 환경: {GRAY}{last_loc}{RESET} ➔ 현재: {YELLOW}{location}{RESET}")

    print(f"{CYAN}{'-'*70}{RESET}")

    # 1. 3대 저장소 상태 점검 & Pull
    for idx, repo in enumerate(repos, 1):
        print(f"\n{GREEN}▶ [{idx}/{len(repos)}] {repo['name']}{RESET}")

        # 로컬 미커밋/미동기화 파일 사전 검사
        _, status_before, _ = run_cmd(["git", "status", "--porcelain"], cwd=repo["path"])
        if status_before:
            uncommitted_count = len(status_before.splitlines())
            print(f"  {YELLOW}⚠️ [로컬 미동기화 파일 감지] 커밋되지 않은 변경사항 {uncommitted_count}건이 존재합니다.{RESET}")

        # Git Pull 실행
        code, stdout, stderr = run_cmd(["git", "pull", "origin", "main"], cwd=repo["path"])
        if code == 0:
            if "Already up to date" in stdout or "이미 최신" in stdout:
                print(f"  {CYAN}✅ GitHub 원격 저장소와 최신 동기화 상태입니다.{RESET}")
            else:
                print(f"  {GREEN}📥 [최신 코드 수신 완료] 이전 환경 작업 내용이 성공적으로 병합되었습니다!{RESET}")
                print(f"     {GRAY}{stdout.splitlines()[0] if stdout else ''}{RESET}")
        else:
            print(f"  {RED}❌ git pull 실패: {stderr}{RESET}")

    # 보안 금고에서 .env 누락 시 자동 복구 점검
    restore_env_from_vault()

    for repo in repos:
        # .env 존재 점검 (notion.Sync 제외)
        if repo["name"].startswith("update_stock") or repo["name"].startswith("k_all_round"):
            if not (repo["path"] / ".env").exists():
                print(f"  {RED}⚠️ [경고] {repo['name']}에 .env 파일이 없습니다! API 키/토큰을 확인해주세요.{RESET}")


    # 2. 직전 작업 이력 요약 브리핑
    for repo in repos:
        show_recent_work_summary(repo["path"], repo["name"].split(" ")[0])

    # 3. AI 테크 레이더 최신 제안 브리핑
    radar_report_path: Path = WORKSPACE_ROOT / "k_all_round_portfolio" / "reports" / "tech_radar_latest.md"
    if radar_report_path.exists():
        try:
            with open(radar_report_path, "r", encoding="utf-8") as f:
                content = f.read()
            tool_matches = re.findall(r'-\s+\*\*([^*]+)\*\*\s*\(([^)]+)\):\s*([^\n]+)', content)
            if tool_matches:
                print(f"\n{CYAN}{BOLD}📡 [AI 테크 레이더 최신 추천 제안 브리핑]{RESET}")
                for name, domain_link, summary in tool_matches[:3]:
                    clean_summary = summary.split(".")[0] if "." in summary else summary[:60]
                    print(f"  💡 {BOLD}{name}{RESET} ({domain_link.split('/')[0].strip()}): {GRAY}{clean_summary}{RESET}")
                print(f"  👉 {YELLOW}원클릭 패치 실행: 6_테크레이더_패치적용.bat (또는 python -m tools.tool_apply_tech_radar_patch){RESET}")
        except Exception as e:
            print(f"  ⚠️ 테크 레이더 브리핑 읽기 오류 ({radar_report_path}): {e}")

    # 4. 주기별 전략 고도화 & 코드 수정 영감 체크리스트
    check_periodic_strategy_questions(state)

    # 상태 업데이트
    state["last_location"] = location
    state["last_sync_time"] = now_str
    save_sync_state(state)

    print(f"\n{CYAN}{'='*70}{RESET}")
    print(f"{CYAN}{BOLD}  🎉 모든 준비가 완료되었습니다! 즐겁고 생산적인 작업을 시작하세요.{RESET}")
    print(f"{CYAN}{'='*70}{RESET}\n")


def validate_modified_python_files(repos: List[Dict[str, Any]]) -> bool:
    """
    당일 수정되거나 추가된 Python 파일(.py)에 대해 문법(Syntax/Indentation) 무결성 검사를 수행합니다.
    오류가 발견되면 즉시 False를 반환하여 Push를 차단합니다.
    """
    print(f"\n{CYAN}🔍 [1단계] 당일 수정된 Python 파일 문법 무결성 사전 검증...{RESET}")
    all_valid = True
    validated_files_count = 0

    for repo in repos:
        repo_path: Path = repo["path"]
        _, status_out, _ = run_cmd(["git", "status", "--porcelain"], cwd=repo_path)
        if not status_out:
            continue

        for line in status_out.splitlines():
            line_clean = line.strip()
            if not line_clean:
                continue
            parts = line_clean.split(maxsplit=1)
            if len(parts) < 2:
                continue
            _, rel_file_str = parts[0], parts[1]
            if "->" in rel_file_str:
                rel_file_str = rel_file_str.split("->")[-1].strip()

            rel_file_str = rel_file_str.strip('"\'')
            if not rel_file_str.endswith(".py"):
                continue

            file_path = repo_path / rel_file_str
            if not file_path.exists() or not file_path.is_file():
                continue

            try:
                py_compile.compile(str(file_path), doraise=True)
                validated_files_count += 1
            except py_compile.PyCompileError as e:
                all_valid = False
                print(f"  {RED}❌ [문법 오류 발견] {repo['name']}/{rel_file_str}:{RESET}")
                err_msg = getattr(e, 'msg', str(e))
                print(f"     {YELLOW}{err_msg}{RESET}")
            except Exception as e:
                all_valid = False
                print(f"  {RED}❌ [검증 실패] {repo['name']}/{rel_file_str}: {e}{RESET}")

    if not all_valid:
        print(f"\n{RED}{BOLD}⚠️ 문법 오류가 감지되어 원격 Push 및 동기화를 중단했습니다. 코드를 수정한 후 다시 실행해주세요.{RESET}\n")
        return False

    if validated_files_count > 0:
        print(f"  {GREEN}✅ 당일 수정/추가된 Python 파일 {validated_files_count}개 문법 검증 완료 (무결성 통과){RESET}")
    else:
        print(f"  {CYAN}ℹ️ 당일 수정된 Python 파일이 없어 문법 검사를 건너뜁니다.{RESET}")
    return True


def clean_local_pycache(workspace_path: Path) -> None:
    """워크스페이스 내 불필요한 __pycache__ 디렉토리를 정리합니다."""
    cleaned_count = 0
    try:
        for p in workspace_path.rglob("__pycache__"):
            if p.is_dir():
                try:
                    shutil.rmtree(p)
                    cleaned_count += 1
                except Exception:
                    pass
        if cleaned_count > 0:
            print(f"  🧹 로컬 임시 캐시(__pycache__) {cleaned_count}개 정리 완료")
    except Exception as e:
        logger.debug(f"캐시 정리 중 예외 발생: {e}")


def mode_finish() -> None:
    """
    스마트 작업 종료 및 통합 동기화:
    1단계: 당일 수정된 Python 파일 문법 무결성 사전 검증 (오류 시 Push 차단)
    2단계: 2대 저장소 Git Commit & Push (GitHub 반영)
    3단계: 최신 모바일 세션 프롬프트 생성 & Google Drive 동기화 & 클립보드 복사 (핸드폰/아이패드 점검용)
    4단계: 로컬 임시 캐시(__pycache__) 정리
    """
    location = get_location()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    repos = get_all_target_repos()
    state = load_sync_state()

    print(f"{CYAN}{'='*70}{RESET}")
    print(f"{CYAN}{BOLD}  🏁 [K-올라운드 & update_stock] 스마트 작업 종료/퇴근 통합 동기화 매니저{RESET}")
    print(f"{CYAN}{'='*70}{RESET}")
    print(f"📍 현재 환경 : {YELLOW}{location}{RESET}")
    print(f"⏰ 실행 시각 : {GRAY}{now_str}{RESET}")
    print(f"{CYAN}{'-'*70}{RESET}")

    # [1단계] 당일 수정된 Python 파일 문법 무결성 사전 검증
    if not validate_modified_python_files(repos):
        return

    # [2단계] 최신 모바일 세션 프롬프트 생성 & Google Drive 동기화 (프롬프트 파일 최신화)
    print(f"\n{CYAN}📱 [2단계] 최신 모바일 세션 프롬프트 생성 & Google Drive 동기화...{RESET}")
    prompt_tool_path = WORKSPACE_ROOT / "k_all_round_portfolio" / "tools" / "tool_generate_gemini_prompt.py"
    if prompt_tool_path.exists():
        _, p_out, _ = run_cmd(
            [sys.executable, "-m", "tools.tool_generate_gemini_prompt"],
            cwd=WORKSPACE_ROOT / "k_all_round_portfolio"
        )
        if p_out:
            for line in p_out.splitlines():
                if any(icon in line for icon in ["📁", "☁️", "📦", "📋", "🧹", "📂"]):
                    print(f"  {line}")
    else:
        print(f"  {YELLOW}⚠️ 프롬프트 생성 도구를 찾을 수 없습니다: {prompt_tool_path}{RESET}")

    # [3단계] 보안 금고 백업 및 3대 저장소 Git Commit & Push (GitHub 반영)
    print(f"\n{CYAN}🚀 [3단계] 보안 금고 백업 및 3대 저장소 Git Commit & Push (GitHub 반영)...{RESET}")
    sync_env_vault_backup()
    state["last_location"] = location
    state["last_sync_time"] = now_str
    save_sync_state(state)

    for idx, repo in enumerate(repos, 1):
        print(f"\n{GREEN}▶ [{idx}/{len(repos)}] {repo['name']} Git 상태 점검 및 Push...{RESET}")
        _, status_out, _ = run_cmd(["git", "status", "--porcelain"], cwd=repo["path"])
        _, unpushed_out, _ = run_cmd(["git", "log", "origin/main..HEAD", "--oneline"], cwd=repo["path"])

        if not status_out and not unpushed_out:
            print(f"  {CYAN}✨ 변경되거나 푸시할 내역이 없습니다. 원격 저장소와 100% 일치합니다.{RESET}")
            continue

        if status_out:
            run_cmd(["git", "add", "."], cwd=repo["path"])
            commit_msg = f"sync: automated project synchronization ({now_str})"
            c_code, _, _ = run_cmd(["git", "commit", "-m", commit_msg], cwd=repo["path"])
            if c_code == 0:
                print(f"  {CYAN}💾 커밋 완료: {commit_msg}{RESET}")
            else:
                print(f"  {YELLOW}커밋 건너뜀 (변경 없음){RESET}")

        p_code, _, p_err = run_cmd(["git", "push", "origin", "main"], cwd=repo["path"])
        if p_code == 0:
            print(f"  {GREEN}🚀 {repo['name']} GitHub 원격 저장소 Push 성공!{RESET}")
        else:
            print(f"  {RED}❌ git push 실패: {p_err}{RESET}")

    # [4단계] 로컬 임시 캐시 정리
    print(f"\n{CYAN}🧹 [4단계] 로컬 임시 캐시 정리...{RESET}")
    clean_local_pycache(WORKSPACE_ROOT)

    print(f"\n{GREEN}{'='*70}{RESET}")
    print(f"{GREEN}{BOLD}  🎉 모든 저장소 Push 및 구글 드라이브(모바일/아이패드) 동기화 완료!{RESET}")
    print(f"{CYAN}  📱 스마트폰/아이패드 Gemini 앱에서 최신 상태로 바로 상담하실 수 있습니다.{RESET}")
    print(f"{GREEN}{'='*70}{RESET}\n")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "start"
    if mode == "start":
        mode_start()
    elif mode in ["finish", "end"]:
        mode_finish()
    else:
        print(f"Unknown mode: {mode}. Use 'start' or 'finish'.")
