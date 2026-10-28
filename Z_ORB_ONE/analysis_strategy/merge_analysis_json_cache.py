"""彙整 analysis_json_cache/*.json，直接執行即可，不連線 API。

目標檔既有內容先載入，再依來源檔修改時間由舊到新合併。
同一股票／指數、同一時間只保留一筆；內容衝突時較新來源覆蓋。
來源檔不變動；任一來源格式錯誤即中止，不覆寫目標檔。
"""
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

TARGET_NAME = 'analysis_strategy_broken_each_stock_special_short_api_cache_xxxxxxxx.json'
DEFAULT_CACHE_DIR = Path(__file__).resolve().parent / 'analysis_json_cache'
TAIPEI = timezone(timedelta(hours=8))


def row_key(row):
    """日K按日期去重；分K按台北時間去重，容許不同 ISO 格式。"""
    if not isinstance(row, dict) or not isinstance(row.get('date'), str):
        raise ValueError('K棒須為含 date 字串的物件')
    value = row['date']
    if len(value) == 10:
        return datetime.strptime(value, '%Y-%m-%d').date().isoformat()
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TAIPEI)
    return dt.astimezone(TAIPEI).isoformat()


def merge_cache(cache_dir=DEFAULT_CACHE_DIR):
    cache_dir = Path(cache_dir)
    if not cache_dir.is_dir():
        raise ValueError(f'找不到 cache 資料夾：{cache_dir}')
    target = cache_dir / TARGET_NAME
    sources = sorted((p for p in cache_dir.glob('*.json') if p != target),
                     key=lambda p: (p.stat().st_mtime_ns, p.name))
    paths = ([target] if target.exists() else []) + sources
    merged = {'stock_names': []}
    tables = {}
    duplicates = conflicts = 0
    for path in paths:
        print(f'讀取：{path.name}', flush=True)
        payload = json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(payload, dict) or not isinstance(payload.get('minute_raw_by_symbol'), dict):
            raise ValueError(f'{path.name}：缺少 minute_raw_by_symbol 物件')
        names = payload.get('stock_names', [])
        if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
            raise ValueError(f'{path.name}：stock_names 須為字串陣列')
        merged['stock_names'].extend(names)
        for field, value in payload.items():
            if field == 'stock_names':
                continue
            if field.endswith('_by_symbol') or field == 'index_minute_raw_by_key':
                if not isinstance(value, dict):
                    raise ValueError(f'{path.name}：{field} 須為物件')
                table = tables.setdefault(field, {})
                for symbol, rows in value.items():
                    if not isinstance(rows, list):
                        raise ValueError(f'{path.name}：{field}/{symbol} 須為陣列')
                    indexed = table.setdefault(symbol, {})
                    for row in rows:
                        key = row_key(row)
                        if key in indexed:
                            duplicates += 1
                            conflicts += indexed[key] != row
                        indexed[key] = row
            else:
                merged[field] = value
    for field, table in tables.items():
        merged[field] = {symbol: [rows[key] for key in sorted(rows, reverse=True)]
                         for symbol, rows in sorted(table.items())}
    minute = merged.setdefault('minute_raw_by_symbol', {})
    merged.setdefault('index_minute_raw_by_key', {})
    merged['stock_names'] = list(dict.fromkeys([*merged['stock_names'], *minute]))
    missing = [name for name in merged['stock_names'] if name not in minute]
    if missing:
        raise ValueError(f'股票缺少分K欄位：{missing}')
    temporary = None
    try:
        with NamedTemporaryFile('w', encoding='utf-8', dir=cache_dir,
                                suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(merged, stream, ensure_ascii=False)
        temporary.replace(target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    print(f'完成：{target}\n來源檔數={len(sources)} 股票數={len(minute)} '
          f'股票分K數={sum(len(rows) for rows in minute.values())} '
          f'重複筆數={duplicates} 其中內容衝突={conflicts}')
    return target


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache-dir', type=Path, default=DEFAULT_CACHE_DIR)
    merge_cache(parser.parse_args().cache_dir)
