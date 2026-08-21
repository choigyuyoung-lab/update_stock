# 🤖 [update_stock] 데이터 수집 서브 엔진 가이드 (GEMINI.md)

이 문서는 본 저장소(`update_stock`)의 아키텍처 및 코딩 규칙을 정의합니다.
포트폴리오 분석 및 마스터 가이드는 메인 Private 저장소인 [`k_all_round_portfolio`](file:///d:/Github%20IDE/k_all_round_portfolio/GEMINI.md)를 참조하십시오.

---

## 🏛️ 1. 저장소 역할 및 핵심 미션 (Sub-Engine: Data Ingestion)
- **역할**: 국내/미국 주식 및 ETF의 실시간 시세, 재무제표 5대 팩터, 마스터 DB 동기화
- **독립 GitHub Actions 워크플로 (8개)**:
  - `kr_price_update.yml` / `us_price_update.yml`: 장중 실시간 현재가 동기화
  - `kr_finance_update.yml` / `us_finance_update.yml`: 장 마감 후 재무 및 5대 퀀트 팩터 적재
  - `kr_master_db_sync.yml` / `us_master_db_sync.yml`: 마스터 DB 동기화
  - `update_etf_holdings.yml`: ETF 구성종목 및 비중 갱신
  - `benchmark_db_sync.yml`: 벤치마크 지수/환율/금리 동기화

---

## 🚨 2. 절대 준수 원칙 (Critical Constraints)
1. **자동 Git 커밋/푸시 금지**:
   - **어떠한 경우에도 `git commit` 또는 `git push` 명령어를 자동으로 실행하지 마십시오.**
   - 최종 커밋/푸시는 사용자가 상위의 `작업종료_전체동기화.bat`를 통해 실행하도록 안내합니다.
2. **터미널 명령어 사전 한국어 안내 원칙**:
   - 터미널 명령어 실행 전 **목적과 내용을 한국어로 사전 설명**합니다.
3. **노션 DB 스키마 방어 로직**:
   - 노션 속성 업데이트 시 반드시 `if field in props`로 열 존재 여부를 확인합니다.
4. **5대 퀀트 팩터 산출**:
   - `200일선`, `수급선`(미국 50일선 / 한국 60일선), `추세`, `12M 모멘텀`, `52주 낙폭`, `60일 변동성`은 파이썬에서 100% 자체 계산하여 적재합니다.
