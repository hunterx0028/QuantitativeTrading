import argparse
from dataclasses import replace
from datetime import date

from .config import Settings
from .finmind import update_corporate_actions
from .paths import ensure_runtime_dirs
from .universe import load_selected_stocks


def main() -> None:
    parser = argparse.ArgumentParser(description="重新同步 FinMind 公司行動資料")
    parser.add_argument("--as-of", required=True, help="公司行動同步截止日 YYYY-MM-DD")
    parser.add_argument("--settings", default=None)
    parser.add_argument(
        "--include-extended",
        action="store_true",
        help="本次同步納入減資、分割、面額變更資料，不需修改 settings.json",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="只重同步指定股票代號；預設使用 stock_data.py 的 selected_stocks",
    )
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    if args.include_extended:
        settings = replace(settings, finmind_extended_corporate_actions=True)
    ensure_runtime_dirs()

    symbols = sorted(set(args.symbols)) if args.symbols else [stock.symbol for stock in load_selected_stocks()]
    if not symbols:
        raise RuntimeError("沒有可同步的股票代號")

    print(
        f"重新同步公司行動: symbols={len(symbols)} as_of={as_of.isoformat()} "
        f"include_extended={settings.finmind_extended_corporate_actions}"
    )
    for index, symbol in enumerate(symbols, start=1):
        actions = update_corporate_actions(
            symbol,
            as_of,
            settings,
            force_from_start=True,
            replace_existing=True,
        )
        print(f"{index}/{len(symbols)} {symbol}: corporate_actions={len(actions)}")


if __name__ == "__main__":
    main()
