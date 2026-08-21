# -*- coding: utf-8 -*-
"""
sync_manager.py
===============
회사 PC 및 집 PC 간 Git 다중 저장소 동기화(Pull/Push)를 원클릭으로 수행하는 유틸리티입니다.
"""

import os
import sys
import socket
import datetime
import subprocess

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GRAY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)


def run_cmd(cmd_list, cwd=None):
    try:
        result = subprocess.run(cmd_list, cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return 1, "", str(e)


def get_location():
    hostname = socket.gethostname().upper()
    company_host = "CHOIGYUYOUNG"
    if hostname == company_host:
        return f"🏢 회사 PC ({hostname})"
    else:
        return f"🏠 집 PC ({hostname})"


def get_all_target_repos():
    repos = [{"name": "update_stock", "path": PROJECT_ROOT}]
    portfolio_path = os.path.join(WORKSPACE_ROOT, "k_all_round_portfolio")
    if os.path.exists(os.path.join(portfolio_path, ".git")):
        repos.append({"name": "k_all_round_portfolio", "path": portfolio_path})
    return repos


def mode_start():
    location = get_location()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    repos = get_all_target_repos()

    print(f"{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  [update_stock] 원클릭 스마트 작업 시작 동기화 매니저{RESET}")
    print(f"{CYAN}{'='*60}{RESET}")
    print(f"📍 현재 환경 : {YELLOW}{location}{RESET}")
    print(f"⏰ 실행 시각 : {GRAY}{now_str}{RESET}")
    print(f"{CYAN}{'-'*60}{RESET}")

    for idx, repo in enumerate(repos, 1):
        print(f"\n{GREEN}▶ [{idx}/{len(repos)}] {repo['name']} 최신 코드 내려받기 (git pull)...{RESET}")
        code, stdout, stderr = run_cmd(["git", "pull", "origin", "main"], cwd=repo["path"])
        if code == 0:
            print(f"  {stdout if stdout else '최신 상태가 유지되고 있습니다.'}")
        else:
            print(f"  {RED}git pull 실패: {stderr}{RESET}")

        if os.path.exists(os.path.join(repo["path"], ".env")):
            print(f"  {CYAN}✅ .env 파일이 정상적으로 존재합니다.{RESET}")
        else:
            print(f"  {RED}⚠️ [경고] .env 파일이 없습니다! API 키/토큰을 설정해주세요.{RESET}")

    print(f"\n{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  🎉 모든 프로젝트 동기화 완료! 즐겁게 작업을 시작하세요.{RESET}")
    print(f"{CYAN}{'='*60}{RESET}\n")


def mode_finish():
    location = get_location()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    repos = get_all_target_repos()

    print(f"{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  [update_stock] 원클릭 스마트 작업 종료/퇴근 동기화 매니저{RESET}")
    print(f"{CYAN}{'='*60}{RESET}")
    print(f"📍 현재 환경 : {YELLOW}{location}{RESET}")
    print(f"⏰ 실행 시각 : {GRAY}{now_str}{RESET}")
    print(f"{CYAN}{'-'*60}{RESET}")

    for idx, repo in enumerate(repos, 1):
        print(f"\n{GREEN}▶ [{idx}/{len(repos)}] {repo['name']} Git 상태 점검 및 Push...{RESET}")
        code, status_out, _ = run_cmd(["git", "status", "--porcelain"], cwd=repo["path"])
        code, unpushed_out, _ = run_cmd(["git", "log", "origin/main..HEAD", "--oneline"], cwd=repo["path"])

        if not status_out and not unpushed_out:
            print(f"  {CYAN}✨ 변경되거나 푸시할 내역이 없습니다. 원격 저장소와 일치합니다!{RESET}")
            continue

        if status_out:
            run_cmd(["git", "add", "."], cwd=repo["path"])
            commit_msg = f"sync: [{location}] {now_str} 작업 완료 동기화"
            c_code, c_out, c_err = run_cmd(["git", "commit", "-m", commit_msg], cwd=repo["path"])
            if c_code == 0:
                print(f"  {CYAN}커밋 완료: {commit_msg}{RESET}")
            else:
                print(f"  {YELLOW}커밋 건너뜀 (변경 없음){RESET}")

        p_code, p_out, p_err = run_cmd(["git", "push", "origin", "main"], cwd=repo["path"])
        if p_code == 0:
            print(f"  {GREEN}🚀 {repo['name']} GitHub 원격 저장소 Push 성공!{RESET}")
        else:
            print(f"  {RED}❌ git push 실패: {p_err}{RESET}")

    print(f"\n{GREEN}{'='*60}{RESET}")
    print(f"{GREEN}{BOLD}  🎉 모든 저장소 업로드 완료! 안심하고 이동/퇴근하세요.{RESET}")
    print(f"{GREEN}{'='*60}{RESET}\n")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "start"
    if mode == "start":
        mode_start()
    elif mode == "finish":
        mode_finish()
    else:
        print(f"Unknown mode: {mode}. Use 'start' or 'finish'.")
