# -*- coding: utf-8 -*-
r"""
Fetch E.SUN market-data index ticker lists and write local JSON files.

Usage:
    python generate_indices_json.py
    python generate_indices_json.py --config C:\path\to\config.ini
    python generate_indices_json.py --industry-map-output C:\path\to\industry_index_map.json
"""

import argparse
import json
import os
import re
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from esun_marketdata import EsunMarketdata


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.ini"
DEFAULT_TWSE_OUTPUT = BASE_DIR / "twse_indices.json"
DEFAULT_TPEX_OUTPUT = BASE_DIR / "tpex_indices.json"
DEFAULT_INDUSTRY_MAP_OUTPUT = BASE_DIR / "industry_index_map.json"


# Source: E.SUN MarketData Intraday Tickers "產業別代碼".
# Update this table manually if E.SUN changes the code list.
INDUSTRY_CODES: Dict[str, str] = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "14": "建材營造",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險",
    "18": "貿易百貨",
    "19": "綜合",
    "20": "其他",
    "21": "化學工業",
    "22": "生技醫療業",
    "23": "油電燃氣業",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "34": "電子商務",
    "35": "綠能環保",
    "36": "數位雲端",
    "37": "運動休閒",
    "38": "居家生活",
    "80": "管理股票",
}


# Index names are exchange-specific and do not share the E.SUN industry-code
# numbering. These aliases describe the industry label we expect to see in
# twse_indices.json / tpex_indices.json.
INDEX_NAME_ALIASES: Dict[str, List[str]] = {
    "01": ["水泥"],
    "02": ["食品"],
    "03": ["塑膠"],
    "04": ["紡織纖維", "紡纖"],
    "05": ["電機機械", "機械"],
    "06": ["電器電纜"],
    "08": ["玻璃陶瓷"],
    "09": ["造紙"],
    "10": ["鋼鐵"],
    "11": ["橡膠"],
    "12": ["汽車"],
    "14": ["建材營造", "營建"],
    "15": ["航運"],
    "16": ["觀光餐旅"],
    "17": ["金融保險"],
    "18": ["貿易百貨"],
    "20": ["其他"],
    "21": ["化學", "化工"],
    "22": ["生技醫療"],
    "23": ["油電燃氣"],
    "24": ["半導體"],
    "25": ["電腦及週邊設備", "電腦及週邊"],
    "26": ["光電"],
    "27": ["通信網路"],
    "28": ["電子零組件"],
    "29": ["電子通路"],
    "30": ["資訊服務"],
    "31": ["其他電子"],
    "32": ["文化創意"],
    "35": ["綠能環保"],
    "36": ["數位雲端"],
    "37": ["運動休閒"],
    "38": ["居家生活"],
}


def load_config(config_path: Path) -> ConfigParser:
    config = ConfigParser()
    read_files = config.read(config_path, encoding="utf-8")
    if not read_files:
        raise FileNotFoundError(f"讀取 config.ini 失敗: {config_path}")
    return config


def fetch_index_list(sdk: EsunMarketdata, exchange: str) -> Dict[str, Any]:
    rest_stock = sdk.rest_client.stock
    return rest_stock.intraday.tickers(type="INDEX", exchange=exchange)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def normalize_name(name: str) -> str:
    normalized = str(name)
    normalized = normalized.replace("臺", "台")
    normalized = normalized.replace("櫃買", "")
    normalized = normalized.replace("業", "")
    normalized = normalized.replace("工業", "")
    normalized = normalized.replace("類", "")
    normalized = normalized.replace("指數", "")
    normalized = normalized.replace("及", "")
    normalized = normalized.replace("週邊", "周邊")
    return re.sub(r"\s+", "", normalized)


def iter_ix_indices(payload: Dict[str, Any]) -> Iterable[Dict[str, str]]:
    for item in payload.get("data", []):
        symbol = str(item.get("symbol", ""))
        name = str(item.get("name", ""))
        if symbol.startswith("IX") and name:
            yield {"symbol": symbol, "name": name}


def score_index_name(index_name: str, aliases: List[str]) -> int:
    normalized_index = normalize_name(index_name)
    best = 0
    for alias in aliases:
        normalized_alias = normalize_name(alias)
        if normalized_alias == normalized_index:
            best = max(best, 100)
        elif normalized_alias in normalized_index:
            best = max(best, 80)
    return best


def find_best_index(
    indices: Iterable[Dict[str, str]],
    industry_code: str,
) -> Optional[Dict[str, str]]:
    aliases = INDEX_NAME_ALIASES.get(industry_code)
    if not aliases:
        return None

    best_match: Optional[Dict[str, str]] = None
    best_score = 0
    for item in indices:
        score = score_index_name(item["name"], aliases)
        if score > best_score:
            best_score = score
            best_match = item

    if best_match is None:
        return None
    return {
        "symbol": best_match["symbol"],
        "name": best_match["name"],
        "match_score": best_score,
    }


def build_exchange_mapping(exchange: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    indices = list(iter_ix_indices(payload))
    industry_map: Dict[str, Any] = {}
    missing: List[str] = []

    for industry_code, industry_name in INDUSTRY_CODES.items():
        match = find_best_index(indices, industry_code)
        if match is None:
            industry_map[industry_code] = {
                "industry_name": industry_name,
                "index": None,
            }
            missing.append(industry_code)
            continue

        industry_map[industry_code] = {
            "industry_name": industry_name,
            "index": match,
        }

    return {
        "exchange": exchange,
        "source_date": payload.get("date"),
        "source_type": payload.get("type"),
        "industries": industry_map,
        "missing_industry_codes": missing,
    }


def build_industry_index_map(
    twse_payload: Dict[str, Any],
    tpex_payload: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "note": "industry code table is maintained in generate_indices_json.py",
        "industry_codes": INDUSTRY_CODES,
        "exchanges": {
            "TWSE": build_exchange_mapping("TWSE", twse_payload),
            "TPEX": build_exchange_mapping("TPEX", tpex_payload),
        },
    }


def print_industry_map_summary(output: Dict[str, Any]) -> None:
    for exchange, exchange_data in output["exchanges"].items():
        missing = exchange_data["missing_industry_codes"]
        mapped_count = len(INDUSTRY_CODES) - len(missing)
        print(f"{exchange}: 已對應 {mapped_count} 個，未對應 {len(missing)} 個")
        if missing:
            missing_text = ", ".join(
                f"{code}:{INDUSTRY_CODES[code]}" for code in missing
            )
            print(f"{exchange} 未對應: {missing_text}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="產生玉山行情 API 可訂閱指數清單 JSON 檔，並接續產生產業類股指數對應檔。"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"config.ini 路徑，預設 {DEFAULT_CONFIG_PATH}",
    )
    parser.add_argument(
        "--twse-output",
        type=Path,
        default=DEFAULT_TWSE_OUTPUT,
        help=f"TWSE JSON 輸出路徑，預設 {DEFAULT_TWSE_OUTPUT}",
    )
    parser.add_argument(
        "--tpex-output",
        type=Path,
        default=DEFAULT_TPEX_OUTPUT,
        help=f"TPEx JSON 輸出路徑，預設 {DEFAULT_TPEX_OUTPUT}",
    )
    parser.add_argument(
        "--raw-twse",
        action="store_true",
        help="TWSE 直接輸出 API 原始格式；未指定時維持目前 twse_indices.json 的 index_list 包裝格式。",
    )
    parser.add_argument(
        "--industry-map-output",
        type=Path,
        default=DEFAULT_INDUSTRY_MAP_OUTPUT,
        help=f"產業類股指數對應檔輸出路徑，預設 {DEFAULT_INDUSTRY_MAP_OUTPUT}",
    )
    parser.add_argument(
        "--skip-industry-map",
        action="store_true",
        help="只產生 twse_indices.json / tpex_indices.json，不產生 industry_index_map.json。",
    )
    args = parser.parse_args()

    config_path = args.config.resolve()
    original_cwd = Path.cwd()

    try:
        os.chdir(config_path.parent)
        config = load_config(config_path)
        sdk = EsunMarketdata(config)
        sdk.login()

        twse_indices = fetch_index_list(sdk, "TWSE")
        tpex_indices = fetch_index_list(sdk, "TPEx")

        twse_payload = twse_indices if args.raw_twse else {"index_list": twse_indices}
        write_json(args.twse_output.resolve(), twse_payload)
        write_json(args.tpex_output.resolve(), tpex_indices)

        print(f"已寫入 {args.twse_output.resolve()}")
        print(f"已寫入 {args.tpex_output.resolve()}")
        print(f"TWSE 指數數量: {len(twse_indices.get('data', []))}")
        print(f"TPEx 指數數量: {len(tpex_indices.get('data', []))}")

        if not args.skip_industry_map:
            industry_map = build_industry_index_map(twse_indices, tpex_indices)
            write_json(args.industry_map_output.resolve(), industry_map)
            print(f"已寫入 {args.industry_map_output.resolve()}")
            print_industry_map_summary(industry_map)

        return 0
    finally:
        os.chdir(original_cwd)


if __name__ == "__main__":
    raise SystemExit(main())
