# -*- coding: utf-8 -*-
"""
kis_master_loader.py
====================
한국투자증권(KIS) 다운로드 서버에서 마스터 압축 파일(ZIP)을 다운로드하여
디스크 임시 파일 없이 메모리 상에서 초고속으로 파싱하는 통합 마스터 엔진입니다.
- 지원 마스터:
  1. 국내 주식: KOSPI(kospi_code.mst), KOSDAQ(kosdaq_code.mst), KONEX(konex_code.mst)
  2. 해외 주식: 나스닥(nasmst.cod), 뉴욕(nysmst.cod), 아멕스(amsmst.cod)
  3. 해외 지수: 해외 주요 지수 및 우량주 편입 정보(frgn_code.mst)
  4. 테마/업종: 한투 공식 테마 마스터(theme_code.mst), 업종 마스터(idxcode.mst)
"""

import io
import ssl
import zipfile
import logging
import urllib.request
from typing import Dict, Any, Optional
import pandas as pd

logger = logging.getLogger("KISMasterLoader")


def _download_and_extract_zip(url: str) -> Optional[zipfile.ZipFile]:
    """HTTPS URL에서 ZIP 파일을 메모리로 다운로드하여 ZipFile 객체를 반환합니다."""
    try:
        ctx = ssl._create_unverified_context()
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
            zip_bytes = io.BytesIO(response.read())
            return zipfile.ZipFile(zip_bytes)
    except Exception as exc:
        logger.warning(f"⚠️ 마스터 파일 다운로드 실패 ({url}): {exc}")
        return None


# ==============================================================================
# 1. 국내 주식 마스터 로더 (KOSPI, KOSDAQ, KONEX)
# ==============================================================================
def get_kr_master_dataframe() -> pd.DataFrame:
    """
    KOSPI, KOSDAQ, KONEX 마스터 파일을 다운로드하여 통합 DataFrame을 반환합니다.
    - 주요 컬럼: Code, Name, StandardCode, Market, GroupCode, BlueChips, RiskTags, RawCapSize
    """
    records = []

    # 1. KOSPI
    url_kospi = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
    z_kospi = _download_and_extract_zip(url_kospi)
    if z_kospi:
        try:
            target_name = z_kospi.namelist()[0]
            with z_kospi.open(target_name) as f:
                content = f.read().decode("cp949", errors="replace")
                for line in content.splitlines():
                    if len(line) < 230:
                        continue
                    rf1 = line[0:len(line) - 228]
                    code = rf1[0:9].strip()
                    std_code = rf1[9:21].strip()
                    name = rf1[21:].strip()

                    rf2 = line[-228:]
                    grp_code = rf2[0:2].strip()      # 증권그룹구분코드 (ST:주권, EF:ETF 등)
                    market_cap_size = rf2[2:3].strip() # 시총규모 (1:대, 2:중, 3:소)
                    
                    # 지수/우량주 플래그
                    k200_sector = rf2[11:12].strip()  # KOSPI200 섹터
                    k100 = rf2[12:13].strip()        # KOSPI100
                    k50 = rf2[13:14].strip()         # KOSPI50
                    etp_type = rf2[15:16].strip()    # ETP 상품구분 (1:투자회사형, 2:수익증권형, 3:ETN 등)
                    krx300 = rf2[64:65].strip()      # KRX300

                    # 리스크/거래상태 플래그
                    tr_stop = rf2[34:35].strip()     # 거래정지 (Y/N)
                    liquidation = rf2[35:36].strip() # 정리매매 (Y/N)
                    managed = rf2[36:37].strip()     # 관리종목 (Y/N)
                    warning = rf2[37:39].strip()     # 시장경고 (00:없음, 01:주의, 02:경고, 03:위험)
                    short_hot = rf2[62:63].strip()   # 공매도과열 (Y/N)
                    short_over = rf2[22:23].strip()  # 단기과열 (0:없음, 1:예고, 2:지정, 3:연장)

                    blue_chips = []
                    if k200_sector and k200_sector != "0":
                        blue_chips.append("KOSPI 200")
                    if k100 == "Y" or k100 == "1":
                        blue_chips.append("KOSPI 100")
                    if k50 == "Y" or k50 == "1":
                        blue_chips.append("KOSPI 50")
                    if krx300 == "Y" or krx300 == "1":
                        blue_chips.append("KRX 300")

                    risk_tags = []
                    if tr_stop == "Y":
                        risk_tags.append("거래정지")
                    if managed == "Y":
                        risk_tags.append("관리종목")
                    if liquidation == "Y":
                        risk_tags.append("정리매매")
                    if warning == "01":
                        risk_tags.append("투자주의")
                    elif warning == "02":
                        risk_tags.append("투자경고")
                    elif warning == "03":
                        risk_tags.append("투자위험")
                    if short_over in ["1", "2", "3"]:
                        risk_tags.append("단기과열")
                    if short_hot == "Y":
                        risk_tags.append("공매도과열")

                    records.append({
                        "Code": code,
                        "StandardCode": std_code,
                        "Name": name,
                        "Market": "KOSPI",
                        "GroupCode": grp_code,
                        "ETPType": etp_type,
                        "BlueChips": blue_chips,
                        "RiskTags": risk_tags,
                        "RawCapSize": market_cap_size,
                    })
        except Exception as exc:
            logger.warning(f"⚠️ KOSPI 마스터 파싱 중 오류: {exc}")

    # 2. KOSDAQ
    url_kosdaq = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"
    z_kosdaq = _download_and_extract_zip(url_kosdaq)
    if z_kosdaq:
        try:
            target_name = z_kosdaq.namelist()[0]
            with z_kosdaq.open(target_name) as f:
                content = f.read().decode("cp949", errors="replace")
                for line in content.splitlines():
                    if len(line) < 224:
                        continue
                    rf1 = line[0:len(line) - 222]
                    code = rf1[0:9].strip()
                    std_code = rf1[9:21].strip()
                    name = rf1[21:].strip()

                    rf2 = line[-222:]
                    grp_code = rf2[0:2].strip()
                    k150 = rf2[27:28].strip()        # KOSDAQ150 여부 (Y/N)
                    krx300 = rf2[60:61].strip()      # KRX300 여부
                    caut_alert = rf2[21:22].strip()  # (코스닥)투자주의환기종목 (Y/N)

                    tr_stop = rf2[31:32].strip()     # 거래정지
                    managed = rf2[33:34].strip()     # 관리종목
                    warning = rf2[34:36].strip()     # 시장경고
                    short_hot = rf2[58:59].strip()   # 공매도과열
                    short_over = rf2[17:18].strip()  # 단기과열

                    blue_chips = []
                    if k150 == "Y" or k150 == "1":
                        blue_chips.append("KOSDAQ 150")
                    if krx300 == "Y" or krx300 == "1":
                        blue_chips.append("KRX 300")

                    risk_tags = []
                    if tr_stop == "Y":
                        risk_tags.append("거래정지")
                    if managed == "Y":
                        risk_tags.append("관리종목")
                    if caut_alert == "Y":
                        risk_tags.append("투자주의환기종목")
                    if warning == "01":
                        risk_tags.append("투자주의")
                    elif warning == "02":
                        risk_tags.append("투자경고")
                    elif warning == "03":
                        risk_tags.append("투자위험")
                    if short_over in ["1", "2", "3"]:
                        risk_tags.append("단기과열")
                    if short_hot == "Y":
                        risk_tags.append("공매도과열")

                    records.append({
                        "Code": code,
                        "StandardCode": std_code,
                        "Name": name,
                        "Market": "KOSDAQ",
                        "GroupCode": grp_code,
                        "ETPType": "",
                        "BlueChips": blue_chips,
                        "RiskTags": risk_tags,
                        "RawCapSize": "",
                    })
        except Exception as exc:
            logger.warning(f"⚠️ KOSDAQ 마스터 파싱 중 오류: {exc}")

    # 3. KONEX
    url_konex = "https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip"
    z_konex = _download_and_extract_zip(url_konex)
    if z_konex:
        try:
            target_name = z_konex.namelist()[0]
            with z_konex.open(target_name) as f:
                content = f.read().decode("cp949", errors="replace")
                for line in content.splitlines():
                    if len(line) < 185:
                        continue
                    code = line[0:9].strip()
                    std_code = line[9:21].strip()
                    name = line[21:-184].strip()
                    grp_code = line[-184:-182].strip()

                    records.append({
                        "Code": code,
                        "StandardCode": std_code,
                        "Name": name,
                        "Market": "KONEX",
                        "GroupCode": grp_code,
                        "ETPType": "",
                        "BlueChips": [],
                        "RiskTags": [],
                        "RawCapSize": "",
                    })
        except Exception as exc:
            logger.warning(f"⚠️ KONEX 마스터 파싱 중 오류: {exc}")

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.drop_duplicates(subset=["Code"]).set_index("Code")
    return df


# ==============================================================================
# 2. 미국 및 해외 주식 마스터 로더 (NASDAQ, NYSE, AMEX, FRGN INDEX)
# ==============================================================================
def get_us_master_dataframe() -> pd.DataFrame:
    """
    미국 3대 시장(나스닥, 뉴욕, 아멕스) 및 해외지수 마스터를 파싱하여 통합 DataFrame을 반환합니다.
    - 주요 컬럼: Symbol, KoreaName, EnglishName, Exchange, S&P500, Nasdaq100, Dow30, IsADR
    """
    records = []

    # 1. 해외지수 및 우량주 플래그 마스터 (frgn_code.mst)
    frgn_flags: Dict[str, Dict[str, Any]] = {}
    url_frgn = "https://new.real.download.dws.co.kr/common/master/frgn_code.mst.zip"
    z_frgn = _download_and_extract_zip(url_frgn)
    if z_frgn:
        try:
            target_name = z_frgn.namelist()[0]
            with z_frgn.open(target_name) as f:
                content = f.read().decode("cp949", errors="replace")
                for line in content.splitlines():
                    if len(line) < 16:
                        continue
                    sym = line[1:11].strip().upper()
                    if not sym:
                        continue
                    rf2 = line[-15:]
                    dow = rf2[4:5].strip() == "1"
                    nas100 = rf2[5:6].strip() == "1"
                    sp500 = rf2[6:7].strip() == "1"
                    frgn_flags[sym] = {
                        "sp500": sp500,
                        "nas100": nas100,
                        "dow30": dow
                    }
        except Exception as exc:
            logger.warning(f"⚠️ 해외지수 마스터 파싱 중 오류: {exc}")

    # 2. 글로벌 해외 시장별 마스터 (미국 nas/nys/ams, 일본 tse, 홍콩 hks, 중국 shs/szs, 베트남 hsx/hnx)
    markets = [
        ("nas", "NASDAQ"),
        ("nys", "NYSE"),
        ("ams", "AMEX"),
        ("tse", "TSE"),
        ("hks", "HKEX"),
        ("shs", "SSE"),
        ("szs", "SZSE"),
        ("hsx", "HOSE"),
        ("hnx", "HNX"),
    ]
    for m_code, m_name in markets:
        url_m = f"https://new.real.download.dws.co.kr/common/master/{m_code}mst.cod.zip"
        z_m = _download_and_extract_zip(url_m)
        if not z_m:
            continue
        try:
            target_name = z_m.namelist()[0]
            with z_m.open(target_name) as f:
                content = f.read().decode("cp949", errors="replace")
                for line in content.splitlines():
                    parts = line.split("\t")
                    if len(parts) < 8:
                        continue
                    symbol = parts[4].strip().upper()
                    if not symbol:
                        continue
                    kor_name = parts[6].strip()
                    eng_name = parts[7].strip()
                    sec_type = parts[8].strip() if len(parts) > 8 else ""  # 1:지수, 2:주식, 3:ETF, 4:워런트
                    is_dr = parts[17].strip() == "Y" if len(parts) > 17 else False

                    flags = frgn_flags.get(symbol, {})
                    blue_chips = []
                    if flags.get("sp500"):
                        blue_chips.append("S&P 500")
                    if flags.get("nas100"):
                        blue_chips.append("나스닥 100")
                    if flags.get("dow30"):
                        blue_chips.append("다우 30")

                    rec = {
                        "Symbol": symbol,
                        "KoreaName": kor_name,
                        "EnglishName": eng_name,
                        "Exchange": m_name,
                        "SecurityType": sec_type,
                        "IsADR": is_dr,
                        "BlueChips": blue_chips,
                    }
                    records.append(rec)

                    # 일본 도쿄 증시 (.T 접미사 인덱스도 동시 등록)
                    if m_name == "TSE":
                        records.append({
                            "Symbol": f"{symbol}.T",
                            "KoreaName": kor_name,
                            "EnglishName": eng_name,
                            "Exchange": m_name,
                            "SecurityType": sec_type,
                            "IsADR": is_dr,
                            "BlueChips": blue_chips,
                        })
        except Exception as exc:
            logger.warning(f"⚠️ {m_name} 마스터 파싱 중 오류: {exc}")

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.drop_duplicates(subset=["Symbol"]).set_index("Symbol")
    return df


# ==============================================================================
# 3. 테마 마스터 로더 (theme_code.mst)
# ==============================================================================
def get_theme_master_dataframe() -> pd.DataFrame:
    """
    증권사 공식 테마 마스터(theme_code.mst)를 바이트 단위로 정밀 파싱하여 종목별 테마 리스트를 매핑합니다.
    - 반환: Code -> [테마명1, 테마명2, ...] 딕셔너리
    """
    url_theme = "https://new.real.download.dws.co.kr/common/master/theme_code.mst.zip"
    z_theme = _download_and_extract_zip(url_theme)
    records = []
    if z_theme:
        try:
            target_name = z_theme.namelist()[0]
            with z_theme.open(target_name) as f:
                raw_bytes = f.read()
                for line in raw_bytes.split(b"\n"):
                    line = line.strip(b"\r")
                    if len(line) < 14:
                        continue
                    tcode = line[0:3].decode("cp949", errors="ignore").strip()
                    jcode = line[-9:].decode("cp949", errors="ignore").strip()
                    tname = line[3:-9].decode("cp949", errors="ignore").strip()
                    if jcode and tname:
                        records.append({"Code": jcode, "ThemeCode": tcode, "ThemeName": tname})
        except Exception as exc:
            logger.warning(f"⚠️ 테마 마스터 파싱 중 오류: {exc}")

    df = pd.DataFrame(records)
    return df
