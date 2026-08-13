import os
import time
import requests
from typing import Any

# ==========================================
# 1. 환경 변수 및 설정
# ==========================================
NOTION_TOKEN: str | None = os.getenv("NOTION_TOKEN")
MASTER_DB_ID: str | None = os.getenv("MASTER_DB_ID")  # 마스터(투자주) DB ID
ETF_DB_ID: str | None = os.getenv("ETF_DB_ID")        # 구성종목(ETF) DB ID

KIS_APPKEY: str | None = os.getenv("KIS_APPKEY")
KIS_SECRET: str | None = os.getenv("KIS_SECRET")
# 회사/기존 코드와 동일한 실전투자/모의투자 분기
KIS_DOMAIN: str = "https://openapi.koreainvestment.com:9443" if os.getenv("KIS_PAPER_TRADING") == "prod" else "https://openapivts.koreainvestment.com:29443"

headers_notion: dict[str, str] = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ==========================================
# 2. 한국투자증권 API 통신부
# ==========================================
def get_kis_token() -> str | None:
    """KIS 접속 토큰 발급"""
    url = f"{KIS_DOMAIN}/oauth2/tokenP"
    body: dict[str, str] = {
        "grant_type": "client_credentials",
        "appkey": KIS_APPKEY or "",
        "appsecret": KIS_SECRET or ""
    }
    try:
        res = requests.post(url, json=body, timeout=10)
        res.raise_for_status()
        return res.json().get("access_token")
    except Exception as e:
        print(f"❌ KIS 토큰 발급 실패: {e}")
        return None

def get_etf_composition(token: str, etf_ticker: str) -> list[dict[str, Any]] | None:
    """KIS API: 특정 ETF의 편입 종목 및 비중 수집"""
    url = f"{KIS_DOMAIN}/uapi/domestic-stock/v1/quotations/inquire-etf-composition-item"
    headers: dict[str, str] = {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APPKEY or "",
        "appsecret": KIS_SECRET or "",
        "tr_id": "FHPKA43600000",
        "custtype": "P"
    }
    params: dict[str, str] = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": etf_ticker
    }
    
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        
        holdings: list[dict[str, Any]] = []
        # KIS API 응답에서 구성 종목 추출
        for item in data.get("output", []):
            holdings.append({
                "ticker": item.get("stck_shrn_iscd"),          # 편입 종목 티커
                "name": item.get("hts_kor_isnm"),              # 편입 종목명
                "weight": float(item.get("stck_prpr_vl", 0))   # 편입 비중
            })
        return holdings
    except Exception as e:
        print(f"❌ ETF({etf_ticker}) 구성종목 조회 실패: {e}")
        return None

# ==========================================
# 3. 노션 API 통신부 (핵심 로직)
# ==========================================
def archive_existing_etf_holdings(etf_page_id: str) -> None:
    """[핵심 1] 기존 ETF DB에서 해당 ETF로 연결된 과거 데이터 일괄 삭제(보관함 이동)"""
    search_url = f"https://api.notion.com/v1/databases/{ETF_DB_ID}/query"
    
    # ETF(투자DB) 릴레이션이 대상 ETF와 일치하는 행만 검색
    payload: dict[str, Any] = {
        "filter": {
            "property": "ETF(투자DB)",
            "relation": {"contains": etf_page_id}
        }
    }
    
    try:
        res = requests.post(search_url, headers=headers_notion, json=payload, timeout=10)
        results = res.json().get("results", [])
        
        for page in results:
            page_id = page["id"]
            # 페이지 Archive(삭제) 처리
            archive_url = f"https://api.notion.com/v1/pages/{page_id}"
            requests.patch(archive_url, headers=headers_notion, json={"archived": True})
            
        print(f"🧹 과거 데이터 {len(results)}건 정리(Archive) 완료.")
    except Exception as e:
        print(f"❌ 과거 데이터 삭제 중 에러 발생: {e}")

def get_or_create_master_stock(ticker: str, name: str) -> str | None:
    """[핵심 2] 마스터 DB에 종목이 없으면 신규 생성하고 ID 반환"""
    search_url = f"https://api.notion.com/v1/databases/{MASTER_DB_ID}/query"
    payload: dict[str, Any] = {
        "filter": {
            "property": "티커",
            "rich_text": {"equals": ticker}
        }
    }
    
    try:
        res = requests.post(search_url, headers=headers_notion, json=payload, timeout=10)
        results = res.json().get("results", [])
        
        if results:
            return results[0]["id"]
            
        # 신규 발견 종목 생성
        create_url = "https://api.notion.com/v1/pages"
        new_payload: dict[str, Any] = {
            "parent": {"database_id": MASTER_DB_ID},
            "properties": {
                "이름": {"title": [{"text": {"content": name}}]},
                "티커": {"rich_text": [{"text": {"content": ticker}}]}
            }
        }
        create_res = requests.post(create_url, headers=headers_notion, json=new_payload, timeout=10)
        create_res.raise_for_status()
        print(f"✨ 마스터 DB 신규 종목 자동 추가: {name} ({ticker})")
        return create_res.json()["id"]
        
    except Exception as e:
        print(f"❌ 마스터 DB 조회/생성 실패 ({ticker}): {e}")
        return None

def insert_etf_db_holding(etf_page_id: str, stock_page_id: str, name: str, ticker: str, weight: float) -> None:
    """[핵심 3] ETF DB에 최신 종목 정보 및 양방향 릴레이션 Insert"""
    create_url = "https://api.notion.com/v1/pages"
    payload: dict[str, Any] = {
        "parent": {"database_id": ETF_DB_ID},
        "properties": {
            "이름": {"title": [{"text": {"content": name}}]},
            "티커": {"rich_text": [{"text": {"content": ticker}}]},
            "비중": {"number": weight},
            "종목(투자DB)": {"relation": [{"id": stock_page_id}]},
            "ETF(투자DB)": {"relation": [{"id": etf_page_id}]}
        }
    }
    
    try:
        requests.post(create_url, headers=headers_notion, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ ETF DB 삽입 실패 ({name}): {e}")

# ==========================================
# 4. 메인 파이프라인
# ==========================================
def main() -> None:
    kis_token = get_kis_token()
    if not kis_token:
        return

    # 관리할 대상 ETF 목록 정의 (마스터 DB의 ETF 페이지 ID와 해당 ETF의 티커)
    target_etfs = [
        # 아래 정보는 회원님의 마스터 DB에서 직접 복사하여 세팅해주시면 됩니다.
        {"etf_page_id": "여기에_노션_부모ETF_페이지_ID입력", "ticker": "457780"} # 예: ACE K휴머노이드...
    ]
    
    for target in target_etfs:
        etf_page_id = target["etf_page_id"]
        etf_ticker = target["ticker"]
        
        print(f"\n🔄 [시작] ETF({etf_ticker}) 구성종목 갱신 작업")
        
        # 1. KIS 최신 데이터 조회
        holdings = get_etf_composition(kis_token, etf_ticker)
        if not holdings:
            print(f"⚠️ {etf_ticker} 구성종목을 불러오지 못해 건너뜁니다.")
            continue
            
        # 2. 기존 노션 데이터 정리 (Clear)
        print("▶ 기존 과거 데이터를 정리합니다...")
        archive_existing_etf_holdings(etf_page_id)
        
        # 3. 새로운 데이터 입력 및 연결 (Rewrite)
        print(f"▶ 최신 구성종목 {len(holdings)}건을 삽입합니다...")
        for holding in holdings:
            # API 제한(Rate Limit)을 피하기 위한 짧은 휴식
            time.sleep(0.1) 
            
            stock_id = get_or_create_master_stock(holding["ticker"], holding["name"])
            if stock_id:
                insert_etf_db_holding(etf_page_id, stock_id, holding["name"], holding["ticker"], holding["weight"])
                
        print(f"✅ ETF({etf_ticker}) 갱신이 완벽하게 완료되었습니다.")

if __name__ == "__main__":
    main()