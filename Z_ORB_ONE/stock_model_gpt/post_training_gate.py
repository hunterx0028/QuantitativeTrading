import argparse
import json

from .paths import EVALUATIONS_DIR
from .signals import OUTPUT_SCHEMA


def main() -> None:
    parser = argparse.ArgumentParser(description="依已驗證交易結果產生 post-training gate 建議")
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--short-window", type=int, default=5)
    parser.add_argument("--min-signals", type=int, default=3)
    parser.add_argument("--min-success-rate", type=float, default=0.5)
    parser.add_argument("--strong-success-rate", type=float, default=0.7)
    args = parser.parse_args()

    evaluations = load_evaluations()[-args.days :]
    if not evaluations:
        raise RuntimeError(f"找不到 evaluation 檔案: {EVALUATIONS_DIR}")

    print_window("最近區間", evaluations, args)
    if args.short_window < len(evaluations):
        print_window(f"最近 {args.short_window} 日", evaluations[-args.short_window :], args)


def load_evaluations() -> list[dict]:
    paths = sorted(EVALUATIONS_DIR.glob("*.json"))
    rows: list[dict] = []
    for path in paths:
        rows.append(json.loads(path.read_text(encoding="utf-8")))
    rows = [row for row in rows if row.get("output_schema") == OUTPUT_SCHEMA]
    if not rows:
        return []
    selected = rows[-1].get("signal_thresholds")
    return [row for row in rows if row.get("signal_thresholds") == selected]


def print_window(label: str, evaluations: list[dict], args) -> None:
    signals = [signal for item in evaluations for signal in item.get("signals", [])]
    print(f"{label}: {len(evaluations)} 個交易日")
    print(f"篩選設定: {evaluations[-1].get('signal_thresholds')}")
    print_directional_signals(signals, args)


def print_directional_signals(signals: list[dict], args) -> None:
    print(f"篩選訊號: {len(signals)} 筆")
    print_trade_recommendation("所選刻度訊號", signals, args)



def print_trade_recommendation(label: str, signals: list[dict], args) -> None:
    count = len(signals)
    if count == 0:
        print(f"{label}: 無訊號，暫不調整")
        return
    success_count = sum(1 for signal in signals if signal["success"])
    success_rate = success_count / count

    if count < args.min_signals:
        recommendation = "KEEP: 樣本不足，先累積資料"
    elif success_rate < args.min_success_rate:
        recommendation = "RAISE_THRESHOLD_OR_PAUSE: 成功率偏低，建議提高門檻或暫停此方向"
    elif success_rate >= args.strong_success_rate:
        recommendation = "KEEP_OR_LOWER_SLIGHTLY: 成功率強，可維持或小幅降低門檻增加機會"
    else:
        recommendation = "KEEP: 表現可接受，先不調整"

    print(
        f"{label}: success={success_count}/{count}={success_rate:.2%}, "
        f"（實際 high_price 落在所選刻度的比例） -> {recommendation}"
    )


if __name__ == "__main__":
    main()
