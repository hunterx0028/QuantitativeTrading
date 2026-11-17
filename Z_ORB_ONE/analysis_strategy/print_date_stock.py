"""從策略結果檔擷取每日交易股票，僅打印至控制台。"""
from datetime import datetime
from pathlib import Path
import re


TARGET_DATE = "20260911"  # YYYYMMDD，例如 "20261105"；空字串表示所有交易日。
TARGET_VERSION = "v40"  # 例如 "v18"、"v40"；空字串表示所有版本，同日代碼合併去重。

RESULT_PATTERN = 'analysis_strategy_broken_each_stock_special_short_*_result.txt'
FILE_VERSION = re.compile(r'analysis_strategy_broken_each_stock_special_short_v(\d+)_result\.txt$')
TRADE_LINE = re.compile(
    r'^(?P<day>\d{4}-\d{2}-\d{2})\s+'
    r'[^\s:|]+:(?P<code>\d+)(?:\.TWO|\.TW)?(?=\s|$)'
)


def collect_date_stocks(directory, target_date='', target_version=''):
    """依日期分組；保留代碼前導零與首次出現順序，同日同股僅列一次。"""
    selected_day = None
    if target_date:
        if not re.fullmatch(r'\d{8}', target_date):
            raise ValueError('TARGET_DATE 須為 8 位數 YYYYMMDD 或空字串')
        selected_day = datetime.strptime(target_date, '%Y%m%d').strftime('%Y-%m-%d')
    if target_version and not re.fullmatch(r'v\d+', target_version):
        raise ValueError('TARGET_VERSION 須為 v 加版本數字（例如 v40）或空字串')

    directory = Path(directory)
    if target_version:
        path = directory / f'analysis_strategy_broken_each_stock_special_short_{target_version}_result.txt'
        if not path.is_file():
            raise FileNotFoundError(f'找不到結果檔：{path}')
        files = [path]
    else:
        files = sorted(
            directory.glob(RESULT_PATTERN),
            key=lambda path: (
                int(match.group(1)) if (match := FILE_VERSION.fullmatch(path.name)) else float('inf'),
                path.name,
            ),
        )
        if not files:
            raise FileNotFoundError(f'找不到結果檔：{directory / RESULT_PATTERN}')

    stocks_by_date = {}
    for path in files:
        with path.open(encoding='utf-8-sig') as source:
            for line in source:
                match = TRADE_LINE.match(line)
                if match is None:
                    continue  # 排除標頭及每日／每月／總計等非交易列。
                day = match.group('day')
                if selected_day is not None and day != selected_day:
                    continue
                datetime.strptime(day, '%Y-%m-%d')
                stocks_by_date.setdefault(day, {}).setdefault(match.group('code'), None)
    return {day: list(stocks_by_date[day]) for day in sorted(stocks_by_date)}


def main():
    stocks_by_date = collect_date_stocks(
        Path(__file__).resolve().parent, TARGET_DATE, TARGET_VERSION,
    )
    if not stocks_by_date:
        print('沒有符合條件的交易紀錄。')
        return
    for day, codes in stocks_by_date.items():
        print(f'"{day.replace("-", "")}"')
        print(','.join(f'"{code}"' for code in codes))


if __name__ == '__main__':
    main()
