import os
import sys
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Any, List, Dict, Optional

# Windows 콘솔 한글 및 이모지 출력 인코딩 설정
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from notion_utils import (
    build_notion_client,
    get_env_var,
    paginate_database,
    safe_page_update,
    RETRY_STATUS_CODES,
)

# ==========================================
# 1. 환경 변수 및 설정 (실전투자 PROD / 모의투자 VTS 이중 지원)
# ==========================================
NOTION_TOKEN = get_env_var("NOTION_TOKEN")
MASTER_DB_ID = os.environ.get("MASTER_DATABASE_ID") or os.environ.get("MASTER_DB_ID") or get_env_var("MASTER_DATABASE_ID")
ETF_DB_ID = get_env_var("ETF_DB_ID")

# KIS 환경 선택: 'prod' (실전투자) 또는 'vts' (모의투자)
KIS_PAPER_TRADING = os.environ.get("KIS_PAPER_TRADING", "vts").strip().lower()
IS_PROD = KIS_PAPER_TRADING == "prod"

KIS_DOMAIN = "https://openapi.koreainvestment.com:9443" if IS_PROD else "https://openapivts.koreainvestment.com:29443"

if IS_PROD:
    KIS_APP_KEY = os.environ.get("KIS_PROD_APP_KEY") or os.environ.get("KIS_APP_KEY") or os.environ.get("KIS_APPKEY")
    KIS_APP_SECRET = os.environ.get("KIS_PROD_APP_SECRET") or os.environ.get("KIS_APP_SECRET") or os.environ.get("KIS_SECRET")
else:
    KIS_APP_KEY = os.environ.get("KIS_VTS_APP_KEY") or os.environ.get("KIS_APP_KEY") or os.environ.get("KIS_APPKEY")
    KIS_APP_SECRET = os.environ.get("KIS_VTS_APP_SECRET") or os.environ.get("KIS_APP_SECRET") or os.environ.get("KIS_SECRET")

SESSION = requests.Session()
SESSION.headers.update({
    "Content-Type": "application/json",
    "Connection": "close"
})
retries = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))


def sf(value: Any) -> float:
    """안전하게 float으로 변환합니다."""
    try:
        if value is None:
            return 0.0
        val = float(value)
        return val if not (val != val) else 0.0
    except (TypeError, ValueError):
        return 0.0


# ==========================================
# 2. 한국투자증권 API 통신부
# ==========================================
def get_kis_token(max_retries: int = 3, base_delay: float = 2.0) -> Optional[str]:
    """KIS 접속 토큰 발급 (지수 백오프 적용)"""
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        print(f"❌ KIS [{'실전투자(PROD)' if IS_PROD else '모의투자(VTS)'}] API Key 또는 Secret이 설정되지 않았습니다.")
        if IS_PROD:
            print("   👉 .env 파일의 `KIS_PROD_APP_KEY`와 `KIS_PROD_APP_SECRET`에 실전 API 키를 입력해주세요.")
        return None

    url = f"{KIS_DOMAIN}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET
    }
    
    attempt = 1
    while attempt <= max_retries:
        try:
            res = SESSION.post(url, json=body, timeout=10)
            status = res.status_code
            if status in RETRY_STATUS_CODES and attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ KIS 토큰 재시도 {attempt}/{max_retries} - status={status}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            
            res.raise_for_status()
            token = res.json().get("access_token")
            if token:
                return token
            else:
                print("❌ KIS 토큰 응답에서 access_token을 찾을 수 없습니다.")
                return None
        except Exception as exc:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                print(f"   ⚠️ KIS 토큰 발급 재시도 {attempt}/{max_retries}: {exc}, {delay}초 대기")
                time.sleep(delay)
                attempt += 1
                continue
            print(f"❌ KIS 토큰 발급 실패: {exc}")
            return None
    return None


def get_etf_composition(token: str, etf_ticker: str) -> Optional[List[Dict[str, Any]]]:
    """KIS API: 특정 ETF의 편입 종목 및 비중 수집"""
    clean_ticker = etf_ticker.split(".")[0]
    url = f"{KIS_DOMAIN}/uapi/domestic-stock/v1/quotations/inquire-etf-composition-item"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHPKA43600000",
        "custtype": "P"
    }
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": clean_ticker
    }
    
    try:
        res = SESSION.get(url, headers=headers, params=params, timeout=10)
        
        # 모의투자(VTS) 도메인에서는 한투 정책상 ETF 구성종목 엔드포인트가 404로 미지원됨 안내
        if res.status_code == 404 and "openapivts" in KIS_DOMAIN:
            print(f"⚠️ [주의] 모의투자(VTS) 서버에서는 KIS API 정책상 ETF 구성종목(PDF) 조회가 제공되지 않습니다 (404 Not Found).")
            print(f"   👉 실전투자(PROD) API 키 설정 후 `.env`에서 `KIS_PAPER_TRADING=prod`로 변경 시 정상 동작합니다.")
            return None

        res.raise_for_status()
        data = res.json()
        
        if data.get("rt_cd") != "0":
            print(f"❌ KIS API 조회 실패 ({clean_ticker}): {data.get('msg1')}")
            return None
            
        holdings: List[Dict[str, Any]] = []
        for item in data.get("output", []):
            raw_ticker = str(item.get("stck_shrn_iscd", "")).strip()
            if not raw_ticker:
                continue
            # 티커 6자리 zfill 포맷팅
            formatted_ticker = raw_ticker.zfill(6) if raw_ticker.isdigit() else raw_ticker
            name = item.get("hts_kor_isnm", "").strip()
            weight = sf(item.get("stck_prpr_vl", 0))
            
            holdings.append({
                "ticker": formatted_ticker,
                "name": name,
                "weight": weight
            })
        return holdings
    except Exception as exc:
        print(f"❌ ETF({clean_ticker}) 구성종목 조회 실패: {exc}")
        return None


# ==========================================
# 3. 노션 API 통신부
# ==========================================
def archive_existing_etf_holdings(client: Any, etf_page_id: str) -> None:
    """[핵심 1] 기존 ETF DB에서 해당 ETF로 연결된 과거 데이터 일괄 삭제(Archive) - 페이징 완벽 대응"""
    start_cursor = None
    total_archived = 0
    
    while True:
        try:
            params = {
                "database_id": ETF_DB_ID,
                "filter": {
                    "property": "ETF(투자DB)",
                    "relation": {"contains": etf_page_id}
                },
                "page_size": 100
            }
            if start_cursor:
                params["start_cursor"] = start_cursor
                
            res = client.databases.query(**params)
            results = res.get("results", [])
            
            for page in results:
                page_id = page["id"]
                success = safe_page_update(client, page_id, {"archived": True})
                if success:
                    total_archived += 1
                    
            if not res.get("has_more"):
                break
            start_cursor = res.get("next_cursor")
            time.sleep(0.2)
        except Exception as exc:
            print(f"❌ 과거 데이터 삭제 중 에러 발생: {exc}")
            break
            
    print(f"🧹 과거 데이터 {total_archived}건 정리(Archive) 완료.")


def get_or_create_master_stock(client: Any, ticker: str, name: str) -> Optional[str]:
    """[핵심 2] 마스터 DB에 종목이 없으면 검색 후 신규 생성하여 ID 반환"""
    clean_ticker = ticker.split(".")[0]
    try:
        res = client.databases.query(
            database_id=MASTER_DB_ID,
            filter={
                "or": [
                    {"property": "티커", "title": {"equals": clean_ticker}},
                    {"property": "티커", "title": {"equals": f"{clean_ticker}.KS"}},
                    {"property": "티커", "title": {"equals": f"{clean_ticker}.KQ"}},
                    {"property": "티커", "title": {"equals": ticker}},
                    {"property": "티커", "title": {"contains": clean_ticker}}
                ]
            }
        )
        results = res.get("results", [])
        if results:
            return results[0]["id"]
            
        # 신규 종목 생성 (마스터 DB 스키마: '티커'가 title, '종목명'이 rich_text)
        new_payload = {
            "parent": {"database_id": MASTER_DB_ID},
            "properties": {
                "티커": {"title": [{"text": {"content": clean_ticker}}]},
                "종목명": {"rich_text": [{"text": {"content": name or clean_ticker}}]}
            }
        }
        created = client.pages.create(**new_payload)
        print(f"✨ 마스터 DB 신규 종목 자동 추가: {name} ({clean_ticker})")
        return created["id"]
    except Exception as exc:
        print(f"❌ 마스터 DB 조회/생성 실패 ({ticker}): {exc}")
        return None


def insert_etf_db_holding(client: Any, etf_page_id: str, stock_page_id: str, name: str, ticker: str, weight: float) -> bool:
    """[핵심 3] ETF DB에 최신 종목 정보 및 릴레이션 Insert"""
    clean_ticker = ticker.split(".")[0]
    payload = {
        "parent": {"database_id": ETF_DB_ID},
        "properties": {
            "이름": {"title": [{"text": {"content": name or clean_ticker}}]},
            "티커": {"rich_text": [{"text": {"content": clean_ticker}}]},
            "비중": {"number": weight},
            "종목(투자DB)": {"relation": [{"id": stock_page_id}]},
            "ETF(투자DB)": {"relation": [{"id": etf_page_id}]}
        }
    }
    
    try:
        client.pages.create(**payload)
        return True
    except Exception as exc:
        print(f"❌ ETF DB 삽입 실패 ({name}): {exc}")
        return False


# ==========================================
# 4. 메인 파이프라인
# ==========================================
def main() -> None:
    print(f"📡 한투 API 접속 설정: [{'실전투자 (PROD)' if IS_PROD else '모의투자 (VTS)'}] ({KIS_DOMAIN})")
    
    notion_client = build_notion_client(NOTION_TOKEN)
    
    kis_token = get_kis_token()
    if not kis_token:
        print("❌ KIS 토큰을 발급받지 못해 작업을 중단합니다.")
        return

    # 관리할 대상 ETF 목록 정의 (etf_page_id가 없거나 미입력된 경우 마스터 DB에서 티커로 자동 검색/생성)
    target_etfs = [
        {"etf_page_id": None, "ticker": "457780"}  # 예: ACE K휴머노이드...
    ]
    
    for target in target_etfs:
        etf_ticker = target["ticker"]
        etf_page_id = target.get("etf_page_id")
        
        # page_id가 지정되지 않은 경우 마스터 DB에서 검색/자동 생성
        if not etf_page_id or "여기에_" in str(etf_page_id):
            print(f"🔍 마스터 DB에서 ETF({etf_ticker}) 부모 페이지 조회/생성 중...")
            etf_page_id = get_or_create_master_stock(notion_client, etf_ticker, f"ETF_{etf_ticker}")
            if not etf_page_id:
                print(f"⚠️ 마스터 DB에서 ETF({etf_ticker}) 페이지를 처리하지 못해 건너뜁니다.")
                continue
            print(f"   └ Target ETF Page ID: {etf_page_id}")
            
        print(f"\n🔄 [시작] ETF({etf_ticker}) 구성종목 갱신 작업")
        
        # 1. KIS 최신 데이터 조회
        holdings = get_etf_composition(kis_token, etf_ticker)
        if not holdings:
            print(f"⚠️ {etf_ticker} 구성종목을 불러오지 못해 건너뜁니다.")
            continue
            
        # 2. 기존 노션 데이터 정리 (Clear)
        print("▶ 기존 과거 데이터를 정리합니다...")
        archive_existing_etf_holdings(notion_client, etf_page_id)
        
        # 3. 새로운 데이터 입력 및 연결 (Rewrite)
        print(f"▶ 최신 구성종목 {len(holdings)}건을 삽입합니다...")
        inserted_count = 0
        for holding in holdings:
            time.sleep(0.1)  # Notion API Rate Limit 방지
            
            stock_id = get_or_create_master_stock(notion_client, holding["ticker"], holding["name"])
            if stock_id:
                ok = insert_etf_db_holding(notion_client, etf_page_id, stock_id, holding["name"], holding["ticker"], holding["weight"])
                if ok:
                    inserted_count += 1
                
        print(f"✅ ETF({etf_ticker}) 갱신 완료! ({inserted_count}/{len(holdings)}건 입력)")


if __name__ == "__main__":
    main()