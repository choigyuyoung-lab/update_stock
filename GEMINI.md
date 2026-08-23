# 🤖 [update_stock] 데이터 수집 서브 엔진 가이드 (GEMINI.md)

이 문서는 본 저장소(`update_stock`)의 아키텍처 및 코딩 규칙을 정의하는 개발 가이드입니다.
포트폴리오 분석 및 퀀트 마스터 가이드는 메인 저장소인 [`k_all_round_portfolio`](file:///d:/Github%20IDE/k_all_round_portfolio/GEMINI.md)를 참조하십시오.

---

## 🏛️ 1. 저장소 계층 구조 및 1:1 대칭 워크플로우

### 📂 계층 구조
- `core/`: 공통 엔진 (`notion_utils.py`, `local_db_manager.py`)
- `services/`: 외부 어댑터 (`kis_data_service.py`, `kis_master_loader.py`, `stock_fallback_resolver.py`, `ai_service.py`, `prompt_manager.py`)
- `data/`: 로컬 영구 캐시 (`stock_master.db`, CSV 5종)

### 🤖 11대 대칭 워크플로우 & 파이썬 스크립트:
1. `sync_price_kr.yml` $\leftrightarrow$ `sync_price_kr.py`: 국내 주식/ETF 실시간 시세 (30종목 묶음)
2. `sync_price_us.yml` $\leftrightarrow$ `sync_price_us.py`: 미국 주식/ETF 종가 시세
3. `sync_finance_kr.yml` $\leftrightarrow$ `sync_finance_kr.py`: 국내 재무비율 & 5대 퀀트팩터
4. `sync_finance_us.yml` $\leftrightarrow$ `sync_finance_us.py`: 미국 재무비율 & 5대 퀀트팩터
5. `sync_master_kr.yml` $\leftrightarrow$ `sync_master_kr.py`: 국내 상장주식 마스터 DB 동기화
6. `sync_master_us.yml` $\leftrightarrow$ `sync_master_us.py`: 미국 상장주식 마스터 DB 동기화
7. `sync_benchmark.yml` $\leftrightarrow$ `sync_benchmark.py`: 글로벌 벤치마크/환율/금리 동기화
8. `sync_etf_holdings.yml` $\leftrightarrow$ `sync_etf_holdings.py`: ETF 구성종목(PDF) 증분 Upsert 동기화
9. `sync_youtube_insights.yml` $\leftrightarrow$ `sync_youtube_insights.py`: 유튜브 RSS 자막 AI 구조화 분석
10. `sync_unorganized_stocks.yml` $\leftrightarrow$ `sync_unorganized_stocks.py`: 미정리 종목 환율 갱신 $\rightarrow$ 마스터 매칭 $\rightarrow$ 특이사항 이관
11. `sync_local_db.yml` $\leftrightarrow$ `sync_local_db.py`: 통합 로컬 SQLite DB (`stock_master.db`) 및 CSV 5종 덤프 갱신

---

## 🚨 2. 개발 및 운영 절대 준수 원칙

1. **2대 프로젝트 쌍(Twin Pair) 동기화 원칙**:
   - `update_stock`과 `k_all_round_portfolio`는 단일 진실 공급원(SSOT) 쌍입니다. 한쪽의 데이터 구조, 온톨로지, 스키마 수정 시 양쪽 프로젝트에 동시에 영향을 반영합니다.
2. **스마트 시작 동기화 (`1_작업시작_동기화.bat`)**:
   - 환경 전환(회사 PC $\leftrightarrow$ 집 PC) 감지, 미동기화 파일 경고, 직전 작업 요약, 주간/월간 전략 점검 체크리스트를 자동 제공합니다.
3. **자동 Git 커밋/푸시 금지**:
   - 코드 수정 및 단위 테스트까지만 수행하고, 최종 커밋/푸시는 사용자가 `3_작업종료_동기화.bat`를 통해 실행하도록 안내합니다.
4. **노션 DB 스키마 자동 프로비저닝 (`ensure_database_properties`)**:
   - 노션 DB에 `업데이트 일자` 등의 필수 열이 없을 경우 자동으로 생성합니다.
5. **Pydantic Structured Outputs 준수**:
   - Gemini AI 분석 시 `response_schema=YouTubeAnalysisResult`를 사용하여 마크다운 정규식 파싱 오류를 원천 차단합니다.
