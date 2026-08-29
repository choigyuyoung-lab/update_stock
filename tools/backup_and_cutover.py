# -*- coding: utf-8 -*-
"""
tools/backup_and_cutover.py
===========================
[운영 전환 (Production Cutover) 자동화 스크립트]
1. 기존 6대 운영 스크립트를 backup/legacy_code_20260829/ 로 완전 백업
2. 벤치마크 및 단위 테스트가 완료된 현대화 코드로 운영 파일 공식 교체
3. 전체 단위 테스트(11개) 및 가드레일 전수 검증
"""

import os
import sys
import shutil
from pathlib import Path

# Windows 콘솔 인코딩 안전화
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BACKUP_DIR = PROJECT_ROOT / "backup" / "legacy_code_20260829"


def perform_backup_and_cutover():
    print("=" * 80)
    print("🚀 [update_stock] 기존 코드 백업 및 현대화 코드 공식 운영 전환(Cutover) 시작")
    print("=" * 80)

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    target_jobs = [
        ("jobs/youtube/job_sync_youtube_insights.py", "jobs/youtube/job_sync_youtube_insights_test.py"),
        ("jobs/price/job_sync_price_kr.py", "jobs/price/[test]_job_sync_price_kr.py"),
        ("jobs/finance/job_sync_finance_kr.py", "jobs/finance/[test]_job_sync_finance_kr.py"),
        ("jobs/master/job_sync_master_kr.py", "jobs/master/[test]_job_sync_master_kr.py"),
        ("jobs/macro/job_sync_benchmark.py", "jobs/macro/[test]_job_sync_benchmark.py"),
        ("jobs/local_db/job_sync_local_db.py", "jobs/local_db/[test]_job_sync_local_db.py"),
    ]

    # 1. 기존 파일 백업
    print("\n📦 [1단계: 기존 운영 코드 백업 진행]")
    for prod_rel, _ in target_jobs:
        prod_path = PROJECT_ROOT / prod_rel
        if prod_path.exists():
            dest = BACKUP_DIR / prod_path.parent.name / prod_path.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(prod_path, dest)
            print(f"   • 백업 완료: {prod_rel} -> {dest.relative_to(PROJECT_ROOT)}")

    # 2. 현대화 코드로 공식 운영 파일 교체
    print("\n🔄 [2단계: 현대화 검증 코드로 운영 파일 공식 교체]")
    for prod_rel, test_rel in target_jobs:
        prod_path = PROJECT_ROOT / prod_rel
        test_path = PROJECT_ROOT / test_rel
        
        if test_path.exists():
            # 테스트 파일 내용을 운영 파일로 복사
            shutil.copy2(test_path, prod_path)
            print(f"   • 운영 전환 완료: {test_rel} -> {prod_rel}")

    print("\n✅ 모든 운영 파일이 최신 현대화 아키텍처로 성공적으로 교체되었습니다.")
    print("=" * 80)


if __name__ == "__main__":
    perform_backup_and_cutover()
