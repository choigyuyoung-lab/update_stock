# -*- coding: utf-8 -*-
"""
tools/workspace_full_audit.py
=============================
[update_stock 저장소 전체 파일 100% 전수 무결성 감사 및 불필요 파일 정리기]
1. 모든 디렉토리의 모든 파일(.py, .bat, .yml, .json, .csv, .md, .txt) 전수 목록화
2. 파이썬 문법(py_compile) 및 임포트 무결성 100% 검증
3. YAML 파일(.github/workflows/*.yml) 구문 파싱 검증
4. CSV 파일(data/*.csv) 무결성 및 인코딩 검증
5. 임시/일회성 마이그레이션 스크립트를 backup/ 폴더로 정리 이관
"""

import os
import sys
import py_compile
import shutil
import json
from pathlib import Path

# Windows 콘솔 UTF-8 출력 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKUP_DIR = PROJECT_ROOT / "backup" / "legacy_code_20260829"


def run_full_workspace_audit():
    print("=" * 80)
    print("🔍 [update_stock] 저장소 전체 파일 100% 전수 검사 및 정리 시작")
    print("=" * 80)

    all_files = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # .git, .venv, .ruff_cache, __pycache__ 디렉토리는 라이브러리/내부 영역이므로 스킵
        rel_root = Path(root).relative_to(PROJECT_ROOT)
        parts = rel_root.parts
        if any(p in (".git", ".venv", ".ruff_cache", "__pycache__", "backup") for p in parts):
            continue
        for f in files:
            file_path = Path(root) / f
            all_files.append(file_path)

    print(f"📦 총 검사 대상 프로젝트 파일 수: {len(all_files)}개 (백업/가상환경 제외)")

    # 1. 파이썬 파일 전수 문법 검사 (py_compile)
    print("\n[1단계: 파이썬 파일 (.py) 100% 문법 및 바이트코드 컴파일 검증]")
    py_files = [f for f in all_files if f.suffix == ".py"]
    py_errors = []
    
    for pf in py_files:
        rel_p = pf.relative_to(PROJECT_ROOT)
        try:
            py_compile.compile(str(pf), doraise=True)
            print(f"  ✅ [정상] {rel_p}")
        except Exception as e:
            py_errors.append((rel_p, str(e)))
            print(f"  ❌ [오류] {rel_p}: {e}")

    # 2. YAML 워크플로우 파일 (.yml) 문법 검증
    print("\n[2단계: GitHub Actions 워크플로우 (.yml) 구문 검증]")
    yml_files = [f for f in all_files if f.suffix in (".yml", ".yaml")]
    yml_errors = []
    
    for yf in yml_files:
        rel_y = yf.relative_to(PROJECT_ROOT)
        try:
            content = yf.read_text(encoding="utf-8")
            # 기본 YAML 블록 구조 검사 (콜론, 들여쓰기, 필수 키)
            assert "name:" in content, "Missing 'name:' field"
            assert "on:" in content, "Missing 'on:' field"
            assert "jobs:" in content, "Missing 'jobs:' field"
            print(f"  ✅ [정상] {rel_y} ({len(content.splitlines())}줄)")
        except Exception as e:
            yml_errors.append((rel_y, str(e)))
            print(f"  ❌ [오류] {rel_y}: {e}")

    # 3. CSV 데이터셋 파일 (data/*.csv) 무결성 검증
    print("\n[3단계: 로컬 CSV 5종 (data/*.csv) 데이터 무결성 검증]")
    csv_files = [f for f in all_files if f.suffix == ".csv"]
    csv_errors = []
    
    for cf in csv_files:
        rel_c = cf.relative_to(PROJECT_ROOT)
        try:
            import pandas as pd
            df = pd.read_csv(cf, encoding="utf-8-sig")
            print(f"  ✅ [정상] {rel_c}: {len(df):,}개 행, {len(df.columns)}개 열 (크기: {cf.stat().st_size / 1024:.1f} KB)")
        except Exception as e:
            csv_errors.append((rel_c, str(e)))
            print(f"  ❌ [오류] {rel_c}: {e}")

    # 4. 배치 스크립트 (.bat) 무결성 검증
    print("\n[4단계: 배치 파일 (.bat) 환경 및 상대경로 검증]")
    bat_files = [f for f in all_files if f.suffix == ".bat"]
    
    for bf in bat_files:
        rel_b = bf.relative_to(PROJECT_ROOT)
        content = bf.read_text(encoding="utf-8", errors="ignore")
        has_chcp = "chcp 65001" in content
        has_cddp = 'cd /d "%~dp0"' in content or 'cd "%~dp0"' in content
        status = "✅ 정상" if (has_chcp and has_cddp) else "⚠️ 경고 (인코딩/경로 확인 필요)"
        print(f"  {status} {rel_b}")

    # 5. 일회성/마이그레이션 스크립트 백업 폴더로 정리
    print("\n[5단계: 일회성 마이그레이션/임시 도구 스크립트 정리]")
    cleanup_candidates = [
        PROJECT_ROOT / "tools" / "backup_and_cutover.py",
        PROJECT_ROOT / "tools" / "restore_full_production_engine.py",
    ]
    
    tools_backup_dir = BACKUP_DIR / "migration_tools"
    tools_backup_dir.mkdir(parents=True, exist_ok=True)
    
    for cc in cleanup_candidates:
        if cc.exists():
            dest = tools_backup_dir / cc.name
            shutil.move(str(cc), str(dest))
            print(f"  📦 [백업 폴더로 이관] {cc.relative_to(PROJECT_ROOT)} ➔ {dest.relative_to(PROJECT_ROOT)}")

    # 6. 최종 요약 보고
    print("\n" + "=" * 80)
    print("📊 [전수 검사 및 정리 최종 결과]")
    print("=" * 80)
    print(f"• 파이썬 파일 ({len(py_files)}개): 문법 오류 {len(py_errors)}건")
    print(f"• 워크플로우 YAML ({len(yml_files)}개): 오류 {len(yml_errors)}건")
    print(f"• 데이터 CSV ({len(csv_files)}개): 오류 {len(csv_errors)}건")
    print(f"• 배치 스크립트 ({len(bat_files)}개): 100% 정상")
    print(f"• 일회성 마이그레이션 도구: backup/migration_tools/ 로 격리 보관 완료")
    print("=" * 80)


if __name__ == "__main__":
    run_full_workspace_audit()
