"""列出同日出現在不同策略結果檔的股票；僅輸出至控制台。"""
from collections import defaultdict
from datetime import date
from pathlib import Path
import re


RESULT_PATTERN = 'analysis_strategy_broken_each_stock_special_short_v*_result.txt'
FILE_VERSION = re.compile(r'analysis_strategy_broken_each_stock_special_short_v(\d+)_result\.txt$')
TRADE_DATE = re.compile(r'^(\d{4}-\d{2}-\d{2})\s+')
STOCK = re.compile(r'(?<!\S)(?P<name>[^\s:|]+):(?P<code>\d+)(?:\.TWO|\.TW)?(?=\s|$)')
ENTRY = re.compile(r'\|\s*\d{2}:\d{2}\s+\d+(?:\.\d+)?\s*->')


def collect_overlaps(directory):
    """以日期及代碼比對；同檔重複記錄只算一次，忽略市場後綴差異。"""
    appearances = defaultdict(dict)
    names = {}
    files = sorted(
        (path for path in Path(directory).glob(RESULT_PATTERN)
         if FILE_VERSION.fullmatch(path.name)),
        key=lambda path: int(FILE_VERSION.fullmatch(path.name).group(1)),
    )
    for path in files:
        with path.open(encoding='utf-8-sig') as source:
            for line in source:
                day_match = TRADE_DATE.match(line)
                if day_match is None or ENTRY.search(line) is None:
                    continue  # 排除標頭、每日／每月及總計等非交易列。
                stock = STOCK.search(line.split('|', 1)[0])
                if stock is None:
                    continue
                day = day_match.group(1)
                date.fromisoformat(day)
                key = day, stock.group('code')
                names.setdefault(key, stock.group('name'))
                # 同檔的同日同股僅計為一個策略，保留各筆不同的原始 log。
                logs = appearances[key].setdefault(path, [])
                original = line.rstrip('\r\n')
                if original not in logs:
                    logs.append(original)
    overlaps = [(day, f'{names[(day, code)]}:{code}',
                 {path: paths[path] for path in sorted(
                     paths, key=lambda path: int(FILE_VERSION.fullmatch(path.name).group(1)))})
                for (day, code), paths in sorted(appearances.items()) if len(paths) > 1]
    return files, overlaps


def main():
    files, overlaps = collect_overlaps(Path(__file__).resolve().parent)
    print(f'掃描結果檔數={len(files)}')
    if not overlaps:
        print('沒有同日出現在不同策略結果檔的股票。')
        return
    for day, stock, paths in overlaps:
        versions = ', '.join(f'v{FILE_VERSION.fullmatch(path.name).group(1)}' for path in paths)
        print(f'{day} {stock} 策略數={len(paths)} 策略={versions}')
        for path, logs in paths.items():
            version = FILE_VERSION.fullmatch(path.name).group(1)
            for line in logs:
                print(f'v{version} {line}')
    print(f'同日符合複數策略的股票筆數={len(overlaps)}')


if __name__ == '__main__':
    main()
