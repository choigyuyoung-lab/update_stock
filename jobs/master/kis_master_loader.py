# -*- coding: utf-8 -*-
"""
kis_master_loader.py
====================
한국투자증권(KIS) 공식 CDN 다운로드 서버에서 마스터 압축 파일(ZIP)을 다운로드하여
디스크 임시 파일 없이 메모리 상에서 초고속으로 파싱하는 통합 선언형 마스터 엔진입니다.
- 지원 마스터:
  1. 국내 주식: KOSPI(kospi_code.mst), KOSDAQ(kosdaq_code.mst), KONEX(konex_code.mst)
  2. 해외 주식: 나스닥(nasmst.cod), 뉴욕(nysmst.cod), 아멕스(amsmst.cod), 일본/홍콩/중국/베트남
  3. 해외 지수: 해외 주요 지수 및 우량주 편입 정보(frgn_code.mst)
  4. 테마 마스터: 한투 공식 테마 마스터(theme_code.mst)
"""

import io
import ssl
import zipfile
import logging
import urllib.request
from typing import Dict, Any, List, Optional
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
# 1. 국내 주식 선언형 스키마 및 마스터 로더 (KOSPI, KOSDAQ, KONEX)
# ==============================================================================
KR_MARKET_CONFIGS: Dict[str, Dict[str, Any]] = {
    "KOSPI": {
        "url": "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
        "tail_len": 228,
        "grp_slice": (0, 2),
        "cap_slice": (2, 3),
        "etp_slice": (15, 16),
        "blue_chips": [
            {"slice": (11, 12), "cond": lambda v: v and v != "0", "tag": "KOSPI 200"},
            {"slice": (12, 13), "cond": lambda v: v in ("Y", "1"), "tag": "KOSPI 100"},
            {"slice": (13, 14), "cond": lambda v: v in ("Y", "1"), "tag": "KOSPI 50"},
            {"slice": (64, 65), "cond": lambda v: v in ("Y", "1"), "tag": "KRX 300"},
        ],
        "risk_flags": [
            {"slice": (34, 35), "cond": lambda v: v == "Y", "tag": "거래정지"},
            {"slice": (35, 36), "cond": lambda v: v == "Y", "tag": "정리매매"},
            {"slice": (36, 37), "cond": lambda v: v == "Y", "tag": "관리종목"},
            {"slice": (37, 39), "cond": lambda v: v == "01", "tag": "투자주의"},
            {"slice": (37, 39), "cond": lambda v: v == "02", "tag": "투자경고"},
            {"slice": (37, 39), "cond": lambda v: v == "03", "tag": "투자위험"},
            {"slice": (22, 23), "cond": lambda v: v in ("1", "2", "3"), "tag": "단기과열"},
            {"slice": (62, 63), "cond": lambda v: v == "Y", "tag": "공매도과열"},
        ],
    },
    "KOSDAQ": {
        "url": "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
        "tail_len": 222,
        "grp_slice": (0, 2),
        "cap_slice": None,
        "etp_slice": None,
        "blue_chips": [
            {"slice": (27, 28), "cond": lambda v: v in ("Y", "1"), "tag": "KOSDAQ 150"},
            {"slice": (60, 61), "cond": lambda v: v in ("Y", "1"), "tag": "KRX 300"},
        ],
        "risk_flags": [
            {"slice": (31, 32), "cond": lambda v: v == "Y", "tag": "거래정지"},
            {"slice": (33, 34), "cond": lambda v: v == "Y", "tag": "관리종목"},
            {"slice": (21, 22), "cond": lambda v: v == "Y", "tag": "투자주의환기종목"},
            {"slice": (34, 36), "cond": lambda v: v == "01", "tag": "투자주의"},
            {"slice": (34, 36), "cond": lambda v: v == "02", "tag": "투자경고"},
            {"slice": (34, 36), "cond": lambda v: v == "03", "tag": "투자위험"},
            {"slice": (17, 18), "cond": lambda v: v in ("1", "2", "3"), "tag": "단기과열"},
            {"slice": (58, 59), "cond": lambda v: v == "Y", "tag": "공매도과열"},
        ],
    },
    "KONEX": {
        "url": "https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip",
        "tail_len": 184,
        "grp_slice": (0, 2),
        "cap_slice": None,
        "etp_slice": None,
        "blue_chips": [],
        "risk_flags": [],
    }
}


def _parse_kr_market(market: str, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """선언적 스펙 테이블에 따라 개별 국내 시장 마스터 파일을 안전하게 파싱합니다."""
    z = _download_and_extract_zip(cfg["url"])
    if not z:
        return []

    records = []
    tail_len = cfg["tail_len"]
    min_len = tail_len + 21

    try:
        with z.open(z.namelist()[0]) as f:
            content = f.read().decode("cp949", errors="replace")
            for line in content.splitlines():
                if len(line) < min_len:
                    continue

                code = line[0:9].strip()
                std_code = line[9:21].strip()
                name = line[21:-tail_len].strip()
                rf = line[-tail_len:]

                grp = rf[cfg["grp_slice"][0]:cfg["grp_slice"][1]].strip() if cfg.get("grp_slice") else ""
                cap = rf[cfg["cap_slice"][0]:cfg["cap_slice"][1]].strip() if cfg.get("cap_slice") else ""
                etp = rf[cfg["etp_slice"][0]:cfg["etp_slice"][1]].strip() if cfg.get("etp_slice") else ""

                blue = [bc["tag"] for bc in cfg["blue_chips"] if bc["cond"](rf[bc["slice"][0]:bc["slice"][1]].strip())]
                risk = [rf_rule["tag"] for rf_rule in cfg["risk_flags"] if rf_rule["cond"](rf[rf_rule["slice"][0]:rf_rule["slice"][1]].strip())]

                records.append({
                    "Code": code,
                    "StandardCode": std_code,
                    "Name": name,
                    "Market": market,
                    "GroupCode": grp,
                    "ETPType": etp,
                    "BlueChips": blue,
                    "RiskTags": risk,
                    "RawCapSize": cap,
                })
    except Exception as exc:
        logger.warning(f"⚠️ {market} 마스터 파싱 중 오류: {exc}")

    return records


def get_kr_master_dataframe() -> pd.DataFrame:
    """
    KOSPI, KOSDAQ, KONEX 마스터 파일을 다운로드하여 통합 DataFrame을 반환합니다.
    - 주요 컬럼: Code, Name, StandardCode, Market, GroupCode, BlueChips, RiskTags, RawCapSize
    """
    records = []
    for market, cfg in KR_MARKET_CONFIGS.items():
        records.extend(_parse_kr_market(market, cfg))

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.drop_duplicates(subset=["Code"]).set_index("Code")
    return df


# ==============================================================================
# 2. 미국 및 글로벌 해외 주식 마스터 로더 (NASDAQ, NYSE, AMEX, 글로벌 거래소)
# ==============================================================================
GLOBAL_MARKETS = [
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


def _get_foreign_index_flags() -> Dict[str, Dict[str, bool]]:
    """해외 주요 지수(S&P 500, 나스닥 100, 다우 30) 편입 플래그 맵을 로드합니다."""
    flags_map: Dict[str, Dict[str, bool]] = {}
    url = "https://new.real.download.dws.co.kr/common/master/frgn_code.mst.zip"
    z = _download_and_extract_zip(url)
    if not z:
        return flags_map

    try:
        with z.open(z.namelist()[0]) as f:
            content = f.read().decode("cp949", errors="replace")
            for line in content.splitlines():
                if len(line) < 16:
                    continue
                sym = line[1:11].strip().upper()
                if not sym:
                    continue
                rf = line[-15:]
                flags_map[sym] = {
                    "dow30": rf[4:5].strip() == "1",
                    "nas100": rf[5:6].strip() == "1",
                    "sp500": rf[6:7].strip() == "1",
                }
    except Exception as exc:
        logger.warning(f"⚠️ 해외지수 마스터 파싱 중 오류: {exc}")

    return flags_map


def get_us_master_dataframe() -> pd.DataFrame:
    """
    미국 및 글로벌 9대 시장 마스터를 다운로드하여 통합 DataFrame을 반환합니다.
    - 주요 컬럼: Symbol, KoreaName, EnglishName, Exchange, BlueChips, IsADR, SecurityType
    """
    frgn_flags = _get_foreign_index_flags()
    records = []

    for m_code, m_name in GLOBAL_MARKETS:
        url = f"https://new.real.download.dws.co.kr/common/master/{m_code}mst.cod.zip"
        z = _download_and_extract_zip(url)
        if not z:
            continue

        try:
            with z.open(z.namelist()[0]) as f:
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
                    sec_type = parts[8].strip() if len(parts) > 8 else ""
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

                    # 일본 도쿄 증시 (.T 접미사 인덱스 동시 등록)
                    if m_name == "TSE":
                        rec_tse = rec.copy()
                        rec_tse["Symbol"] = f"{symbol}.T"
                        records.append(rec_tse)
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
    증권사 공식 테마 마스터(theme_code.mst)를 파싱하여 종목별 테마 리스트를 반환합니다.
    - 반환: Code, ThemeCode, ThemeName 컬럼을 포함하는 DataFrame
    """
    url = "https://new.real.download.dws.co.kr/common/master/theme_code.mst.zip"
    z = _download_and_extract_zip(url)
    records = []

    if z:
        try:
            with z.open(z.namelist()[0]) as f:
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

    return pd.DataFrame(records)
