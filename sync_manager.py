import os
import sys
import socket
import datetime
import subprocess

# 터미널 UTF-8 인코딩 설정
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# 색상 출력용 ANSI 코드
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GRAY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"

def run_cmd(cmd_list):
    """터미널 명령어 실행 및 결과 반환"""
    try:
        result = subprocess.run(cmd_list, capture_output=True, text=True, encoding="utf-8", errors="replace")
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return 1, "", str(e)

def get_location():
    """호스트명(컴퓨터 이름)으로 회사/집 판별"""
    hostname = socket.gethostname().upper()
    company_host = "CHOIGYUYOUNG"
    if hostname == company_host:
        return f"🏢 회사 PC ({hostname})"
    else:
        return f"🏠 집 PC ({hostname})"

def mode_start():
    location = get_location()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  [K-올라운드] 원클릭 스마트 작업 시작 동기화 매니저{RESET}")
    print(f"{CYAN}{'='*60}{RESET}")
    print(f"📍 현재 환경 : {YELLOW}{location}{RESET}")
    print(f"⏰ 실행 시각 : {GRAY}{now_str}{RESET}")
    print(f"{CYAN}{'-'*60}{RESET}")

    # 1. git pull
    print(f"\n{GREEN}▶ [1/3] 최신 코드 내려받기 (git pull origin main)...{RESET}")
    code, stdout, stderr = run_cmd(["git", "pull", "origin", "main"])
    if code == 0:
        print(f"  {stdout if stdout else '최신 상태가 유지되고 있습니다.'}")
    else:
        print(f"  {RED}git pull 실패: {stderr}{RESET}")

    # 2. .env 점검
    print(f"\n{GREEN}▶ [2/3] 보안 환경변수(.env) 파일 점검...{RESET}")
    if os.path.exists(".env"):
        print(f"  {CYAN}✅ .env 파일이 정상적으로 존재합니다.{RESET}")
    else:
        print(f"  {RED}⚠️ [경고] .env 파일이 없습니다! API 키/토큰을 설정해주세요.{RESET}")

    # 3. 안내
    print(f"\n{GREEN}▶ [3/3] 패키지/가상환경 점검...{RESET}")
    print(f"  {GRAY}💡 신규 패키지 설치 필요 시: pip install -r requirements.txt{RESET}")

    print(f"\n{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  🎉 동기화 완료! 즐겁게 작업을 시작하세요.{RESET}")
    print(f"{CYAN}{'='*60}{RESET}\n")

def mode_finish():
    location = get_location()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"{CYAN}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  [K-올라운드] 원클릭 스마트 작업 종료/퇴근 동기화 매니저{RESET}")
    print(f"{CYAN}{'='*60}{RESET}")
    print(f"📍 현재 환경 : {YELLOW}{location}{RESET}")
    print(f"⏰ 실행 시각 : {GRAY}{now_str}{RESET}")
    print(f"{CYAN}{'-'*60}{RESET}")

    # 1. 상태 확인
    print(f"\n{GREEN}▶ [1/3] 로컬 변경 사항 확인 (git status)...{RESET}")
    code, status_out, _ = run_cmd(["git", "status", "--porcelain"])
    code, unpushed_out, _ = run_cmd(["git", "log", "origin/main..HEAD", "--oneline"])

    if not status_out and not unpushed_out:
        print(f"  {CYAN}✨ 변경되거나 푸시할 내역이 없습니다. 이미 원격 저장소와 완벽히 일치합니다!{RESET}")
        print(f"\n{GREEN}{'='*60}{RESET}")
        print(f"{GREEN}{BOLD}  안심하고 이동/퇴근하셔도 됩니다.{RESET}")
        print(f"{GREEN}{'='*60}{RESET}\n")
        return

    # 2. git add & commit
    if status_out:
        print(f"\n{GREEN}▶ [2/3] 변경 파일 스테이징 및 커밋 생성 (git add & commit)...{RESET}")
        run_cmd(["git", "add", "."])
        commit_msg = f"sync: [{location}] {now_str} 작업 완료 동기화"
        c_code, c_out, c_err = run_cmd(["git", "commit", "-m", commit_msg])
        if c_code == 0:
            print(f"  {CYAN}커밋 완료: {commit_msg}{RESET}")
        else:
            print(f"  {YELLOW}커밋 건너뜀 (이미 커밋됨 또는 변경 없음){RESET}")

    # 3. git push
    print(f"\n{GREEN}▶ [3/3] GitHub 원격 저장소로 업로드 (git push origin main)...{RESET}")
    p_code, p_out, p_err = run_cmd(["git", "push", "origin", "main"])
    if p_code == 0:
        print(f"\n{GREEN}{'='*60}{RESET}")
        print(f"{GREEN}{BOLD}  🚀 GitHub 원격 저장소 업로드 성공! 안심하고 이동/퇴근하셔도 됩니다.{RESET}")
        print(f"{GREEN}{'='*60}{RESET}\n")
    else:
        print(f"\n  {RED}❌ git push 실패: {p_err}{RESET}")
        print(f"  {YELLOW}네트워크 연결 또는 충돌 여부를 확인하세요.{RESET}\n")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "start"
    if mode == "start":
        mode_start()
    elif mode == "finish":
        mode_finish()
    else:
        print(f"Unknown mode: {mode}. Use 'start' or 'finish'.")
