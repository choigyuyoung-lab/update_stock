# 🤖 [update_stock] 데이터 수집 서브 엔진 가이드 (GEMINI.md)

이 문서는 본 저장소(`update_stock`)의 아키텍처 및 코딩 규칙을 정의하는 개발 가이드입니다.
포트폴리오 분석 및 퀀트 마스터 가이드는 메인 저장소인 [`k_all_round_portfolio`](file:///d:/Github%20IDE/k_all_round_portfolio/GEMINI.md)를 참조하십시오.

---

## 🏛️ 1. 저장소 역할 및 1:1 대칭 워크플로우

- **역할**: 국내/미국 주식 및 ETF의 실시간 시세, 재무제표 5대 팩터, 마스터 DB, 거시 지표, 유튜브 AI 시황 동기화
- **10대 대칭 워크플로우 & 파이썬 스크립트**:
  1. `sync_price_kr.yml` $\leftrightarrow$ `sync_price_kr.py`: 국내 주식/ETF 실시간 시세
  2. `sync_price_us.yml` $\leftrightarrow$ `sync_price_us.py`: 미국 주식/ETF 종가 시세
  3. `sync_finance_kr.yml` $\leftrightarrow$ `sync_finance_kr.py`: 국내 재무비율 & 5대 퀀트팩터
  4. `sync_finance_us.yml` $\leftrightarrow$ `sync_finance_us.py`: 미국 재무비율 & 5대 퀀트팩터
  5. `sync_master_kr.yml` $\leftrightarrow$ `sync_master_kr.py`: 국내 상장주식 마스터 DB 동기화
  6. `sync_master_us.yml` $\leftrightarrow$ `sync_master_us.py`: 미국 상장주식 마스터 DB 동기화
  7. `sync_benchmark.yml` $\leftrightarrow$ `sync_benchmark.py`: 글로벌 벤치마크/환율/금리 동기화
  8. `sync_etf_holdings.yml` $\leftrightarrow$ `sync_etf_holdings.py`: ETF 구성종목(PDF) 데이터 동기화
  9. `sync_youtube_insights.yml` $\leftrightarrow$ `sync_youtube_insights.py`: 유튜브 RSS 자막 AI 구조화 분석
  10. `sync_unorganized_stocks.yml` $\leftrightarrow$ `sync_unorganized_stocks.py`: 미정리 종목 환율 갱신 $\rightarrow$ 마스터 매칭 $\rightarrow$ 특이사항 이관

---

## 🚨 2. 개발 및 운영 절대 준수 원칙

1. **자동 Git 커밋/푸시 금지**:
   - 코드 수정 및 단위 테스트까지만 수행하고, 최종 커밋/푸시는 사용자가 `3_작업종료_동기화.bat`를 통해 실행하도록 안내합니다.
2. **터미널 명령어 사전 한국어 안내 원칙**:
   - 로컬 터미널 명령어 실행 전 **목적과 내용을 한국어로 사전 설명**한 후 실행합니다.
3. **노션 DB 스키마 방어 로직 (Graceful Property Check)**:
   - 노션 속성 업데이트 시 반드시 `if field in props`로 열 존재 여부를 확인하여 예외 발생을 방지합니다.
4. **Pydantic Structured Outputs 준수**:
   - Gemini AI 분석 시 `response_schema=YouTubeAnalysisResult`를 사용하여 마크다운 정규식 파싱 오류를 원천 차단합니다.
5. **프롬프트 중앙 관리**:
   - 프롬프트는 `prompts/*.en.md` 및 `prompt_manager.py`를 통해 관리합니다.
