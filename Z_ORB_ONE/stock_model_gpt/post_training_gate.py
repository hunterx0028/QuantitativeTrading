import argparse
import json

from .paths import EVALUATIONS_DIR


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
    return rows


def print_window(label: str, evaluations: list[dict], args) -> None:
    signals = [signal for item in evaluations for signal in item.get("signals", [])]
    direction_signals = [
        signal for item in evaluations for signal in item.get("direction_signals", [])
    ]
    print(f"{label}: {len(evaluations)} 個交易日")
    print_price_move_signals(signals, args)
    print_direction_signals(direction_signals, args)


def print_price_move_signals(signals: list[dict], args) -> None:
    print(f"漲跌訊號: {len(signals)} 筆")
    for side in ("LONG", "SHORT"):
        side_signals = [signal for signal in signals if signal["side"] == side]
        print_trade_recommendation(f"漲跌訊號 {side}", side_signals, args)
        print_reason_breakdown(side_signals)


def print_direction_signals(signals: list[dict], args) -> None:
    print(f"方向訊號: {len(signals)} 筆")
    for side in ("LONG", "SHORT"):
        side_signals = [signal for signal in signals if signal["side"] == side]
        print_direction_recommendation(f"方向訊號 {side}", side_signals, args)


def print_trade_recommendation(label: str, signals: list[dict], args) -> None:
    count = len(signals)
    if count == 0:
        print(f"{label}: 無訊號，暫不調整")
        return
    success_count = sum(1 for signal in signals if signal["success"])
    success_rate = success_count / count
    avg_best = sum(signal["best_profit_pct"] for signal in signals) / count
    avg_close = sum(signal["close_profit_pct"] for signal in signals) / count
    avg_adverse = sum(signal["adverse_pct"] for signal in signals) / count

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
        f"avg_best={avg_best:.2f}%, avg_close={avg_close:.2f}%, "
        f"avg_adverse={avg_adverse:.2f}% -> {recommendation}"
    )


def print_reason_breakdown(signals: list[dict]) -> None:
    if not signals:
        return
    for reason in ("hit", "price", "both"):
        reason_signals = [signal for signal in signals if signal.get("reason") == reason]
        if not reason_signals:
            continue
        count = len(reason_signals)
        success_count = sum(1 for signal in reason_signals if signal["success"])
        avg_best = sum(signal["best_profit_pct"] for signal in reason_signals) / count
        avg_adverse = sum(signal["adverse_pct"] for signal in reason_signals) / count
        print(
            f"  reason={reason}: success={success_count}/{count}={success_count / count:.2%}, "
            f"avg_best={avg_best:.2f}%, avg_adverse={avg_adverse:.2f}%"
        )


def print_direction_recommendation(label: str, signals: list[dict], args) -> None:
    count = len(signals)
    if count == 0:
        print(f"{label}: 無訊號，暫不調整")
        return
    direction_hits = sum(1 for signal in signals if direction_matches(signal))
    close_positive = sum(1 for signal in signals if signal["close_profit_pct"] > 0)
    success_count = sum(1 for signal in signals if signal["success"])
    avg_best = sum(signal["best_profit_pct"] for signal in signals) / count
    avg_close = sum(signal["close_profit_pct"] for signal in signals) / count
    avg_adverse = sum(signal["adverse_pct"] for signal in signals) / count
    direction_rate = direction_hits / count
    close_positive_rate = close_positive / count
    success_rate = success_count / count

    if count < args.min_signals:
        recommendation = "KEEP: 樣本不足，先累積資料"
    elif direction_rate < args.min_success_rate or close_positive_rate < args.min_success_rate:
        recommendation = "RAISE_THRESHOLD_OR_PAUSE: 方向或收盤獲利比例偏低，建議提高門檻或暫停此方向"
    elif direction_rate >= args.strong_success_rate and close_positive_rate >= args.strong_success_rate:
        recommendation = "KEEP_OR_LOWER_SLIGHTLY: 方向表現強，可維持或小幅降低門檻增加機會"
    else:
        recommendation = "KEEP: 表現可接受，先不調整"

    print(
        f"{label}: direction={direction_hits}/{count}={direction_rate:.2%}, "
        f"close_positive={close_positive}/{count}={close_positive_rate:.2%}, "
        f"success={success_count}/{count}={success_rate:.2%}, "
        f"avg_best={avg_best:.2f}%, avg_close={avg_close:.2f}%, "
        f"avg_adverse={avg_adverse:.2f}% -> {recommendation}"
    )


def direction_matches(signal: dict) -> bool:
    actual_price = int(signal["actual_price"])
    if signal["side"] == "LONG":
        return actual_price in (1, 2)
    return actual_price in (-1, -2)


if __name__ == "__main__":
    main()
