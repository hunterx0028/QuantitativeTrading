"""依日期更新台指夜盤收盤資料。"""

import json
from pathlib import Path
from tempfile import NamedTemporaryFile


def update_tx_night_session_close(record, cache_dir=None):
    if not isinstance(record, dict) or not isinstance(record.get('date'), str):
        raise ValueError('tx_night_session_close 須包含 date 字串')
    directory = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parent / 'analysis_json_cache'
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / 'TX_night_session_close.json'
    rows = json.loads(target.read_text(encoding='utf-8-sig')) if target.exists() else []
    if not isinstance(rows, list) or any(not isinstance(row, dict) or 'date' not in row for row in rows):
        raise ValueError(f'{target} 須為包含 date 的物件陣列')
    for row in rows:
        if row['date'] == record['date']:
            row.update(record)
            break
    else:
        rows.append(dict(record))
    temporary = None
    try:
        with NamedTemporaryFile('w', encoding='utf-8', dir=directory, suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(rows, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
        temporary.replace(target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
