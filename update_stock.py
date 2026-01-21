def main():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    now_iso = now.isoformat() 
    print(f"🚀 업데이트 시작 (KST: {now.strftime('%Y-%m-%d %H:%M:%S')})")
    
    has_more, next_cursor = True, None
    total_count = 0
    success_count = 0
    fail_count = 0

    while has_more:
        try:
            # 1. 노션 데이터 쿼리
            response = notion.databases.query(database_id=DATABASE_ID, start_cursor=next_cursor)
            pages = response.get("results", [])
            
            for page in pages:
                total_count += 1
                try:
                    props = page["properties"]
                    
                    # 속성 안전하게 가져오기
                    market_obj = props.get("Market", {}).get("select")
                    market = market_obj.get("name", "") if market_obj else ""
                    
                    ticker_data = props.get("티커", {}).get("title", [])
                    ticker = ticker_data[0].get("plain_text", "").strip() if ticker_data else ""
                    
                    # 마켓이나 티커가 없으면 건너뜀
                    if not market or not ticker:
                        print(f"⏩ {total_count}번 항목: 마켓 또는 티커 정보 없음 (건너뜀)")
                        fail_count += 1
                        continue

                    # 2. 주식 정보 가져오기
                    stock = get_stock_info(ticker, market)
                    
                    if stock and stock["price"] is not None:
                        upd = {
                            "현재가": {"number": stock["price"]},
                            "마지막 업데이트": {"date": {"start": now_iso}}
                        }
                        # 지표 추가 (is not None 체크)
                        if stock["per"] is not None: upd["PER"] = {"number": stock["per"]}
                        if stock["pbr"] is not None: upd["PBR"] = {"number": stock["pbr"]}
                        if stock["eps"] is not None: upd["EPS"] = {"number": stock["eps"]}
                        if stock["high52w"] is not None: upd["52주 최고가"] = {"number": stock["high52w"]}
                        if stock["low52w"] is not None: upd["52주 최저가"] = {"number": stock["low52w"]}

                        notion.pages.update(page_id=page["id"], properties=upd)
                        success_count += 1
                        
                        if success_count % 10 == 0:
                            print(f"✅ 진행 중... {success_count}개 성공 / {total_count}개 시도")
                    else:
                        print(f"⚠️ {total_count}번 항목 ({ticker}): 가격 정보를 가져오지 못함")
                        fail_count += 1
                    
                    time.sleep(0.4) # API 속도 제한 준수

                except Exception as page_err:
                    # 개별 페이지 처리 중 에러가 나도 전체 루프는 유지
                    print(f"❌ {total_count}번 페이지 처리 중 개별 오류: {page_err}")
                    fail_count += 1
                    continue
            
            # 다음 페이지 확인
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")

        except Exception as query_err:
            # 노션 API 쿼리 자체가 실패한 경우
            print(f"🚨 노션 데이터베이스 쿼리 중 치명적 오류: {query_err}")
            break

    print("-" * 30)
    print(f"✨ 작업 완료 보고서")
    print(f"  - 전체 항목 수: {total_count}")
    print(f"  - 업데이트 성공: {success_count}")
    print(f"  - 실패/건너뜀: {fail_count}")
    print("-" * 30)

if __name__ == "__main__":
    main()
