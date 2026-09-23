"""Five-class price probabilities and independent target filtering."""
import math
from dataclasses import dataclass

CLASSES = (-2, -1, 0, 1, 2)
OUTPUT_SCHEMA = "high_price_five_classes_v1"


@dataclass(frozen=True)
class SignalThresholds:
    classes: tuple[int, ...] = (1, 2)
    threshold_pct: float = 60.0

    def __post_init__(self):
        if not self.classes or any(type(c) is not int or c not in CLASSES for c in self.classes):
            raise ValueError("signal-classes 必須選擇 -2,-1,0,1,2 中至少一個刻度")
        object.__setattr__(self, "classes", tuple(sorted(set(self.classes))))
        if not math.isfinite(self.threshold_pct) or not 0 <= self.threshold_pct <= 100:
            raise ValueError("signal-threshold-pct 必須介於 0 到 100")

    def signal_values(self) -> dict:
        return {"classes": list(self.classes), "threshold_pct": self.threshold_pct}


def add_signal_arguments(parser, *, saved_defaults=False):
    parser.add_argument("--signal-classes", default=None if saved_defaults else "1,2",
                        help='篩選刻度，逗號分隔，例如 "1,2"；負數用 --signal-classes=-1,0')
    parser.add_argument("--signal-threshold-pct", type=float, default=None if saved_defaults else 60.0,
                        help="所選刻度機率總和門檻，50 代表 50%%")


def build_signal_thresholds(args, saved=None):
    saved = saved or SignalThresholds().signal_values()
    raw = getattr(args, "signal_classes", None)
    try:
        classes = tuple(int(part.strip()) for part in raw.split(",")) if raw is not None else tuple(saved["classes"])
    except (ValueError, AttributeError):
        raise ValueError("signal-classes 必須為逗號分隔的整數，例如 1,2") from None
    pct = getattr(args, "signal_threshold_pct", None)
    return SignalThresholds(classes, saved["threshold_pct"] if pct is None else pct)


def probabilities(prediction, target="high_price"):
    values = prediction.get(target)
    if not isinstance(values, dict) or set(values) != {str(c) for c in CLASSES}:
        raise ValueError(f"預測檔不是 {target} 五分類格式，請使用新模型重新預測")
    if any(not isinstance(p, (int, float)) or not math.isfinite(p) or not 0 <= p <= 1 for p in values.values()):
        raise ValueError(f"{target} 機率必須是 0 到 1 的有限數值")
    if not math.isclose(sum(values.values()), 1.0, abs_tol=1e-6):
        raise ValueError(f"{target} 五種機率總和必須為 1")
    return values


def predicted_class(values):
    # Deterministic tie break: the smaller bucket wins.
    return max(CLASSES, key=lambda c: values[str(c)])


def detect_signal(prediction, thresholds=SignalThresholds(), target="high_price"):
    values = probabilities(prediction, target)
    score = math.fsum(values[str(c)] for c in thresholds.classes)
    cutoff = thresholds.threshold_pct / 100
    if score < cutoff and not math.isclose(score, cutoff, rel_tol=0, abs_tol=1e-12):
        return None
    return {"symbol": prediction["symbol"], "prediction_date": prediction["prediction_date"],
            target: values, "predicted_class": predicted_class(values),
            "selected_classes": list(thresholds.classes), "target_probability": score,
            "target_key": target + "[" + ",".join(map(str, thresholds.classes)) + "]"}


def sorted_signals(signals):
    return sorted(signals, key=lambda s: (-s["target_probability"], s["symbol"]))


def build_signal_report_lines(prediction_date, thresholds, signals, target="high_price"):
    selected = ",".join(map(str, thresholds.classes))
    lines = [f"prediction_date={prediction_date.isoformat()}",
             f"條件：P({target} in {{{selected}}}) >= {thresholds.threshold_pct:g}%",
             "股票  P(-2)  P(-1)  P(0)  P(1)  P(2)  所選合計  最高機率類別"]
    for signal in sorted_signals(signals):
        values = "  ".join(f"{signal[target][str(c)]:.2%}" for c in CLASSES)
        lines.append(f"{signal['symbol']}  {values}  {signal['target_probability']:.2%}  {signal['predicted_class']}")
    if not signals:
        lines.append("沒有符合訊號門檻的標的")
    return lines
