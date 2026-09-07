from datetime import date, timedelta

import pytest
import torch

from ..dataset import encode_state
from ..features import calculate_atr, encode_candles
from ..model import StockAutoregressiveModel
from ..training import ensure_checkpoint_compatible, weighted_loss
from ..config import Settings
from ..atr_calibration import ATR_ENCODING


def candles(count):
    return [
        {"date": (date(2026, 1, 1) + timedelta(days=i)).isoformat(),
         "open": 100, "high": 101, "low": 99, "close": 100, "volume": 100}
        for i in range(count)
    ]


def test_atr_seed_gaps_and_wilder_smoothing():
    rows = candles(16)
    # A gap: high-low=2, but high-previous close=6.
    rows[14].update(open=105, high=106, low=104, close=105)
    rows[15].update(open=105, high=106, low=104, close=105)
    values = calculate_atr(rows)
    assert values[:14] == [None] * 14
    seed = (13 * 2 + 6) / 14
    assert values[14] == pytest.approx(seed)
    assert values[15] == pytest.approx((seed * 13 + 2) / 14)


def test_atr_no_future_leakage_and_normalisation():
    rows = candles(23)
    prefix = encode_candles(rows[:21])
    rows[21].update(high=1000, close=900)
    assert encode_candles(rows)[:1] == prefix
    assert prefix[0].atr == 2
    assert prefix[0].atr_ratio == 0.02
    scaled = [{**r, **{k: r[k] * 10 for k in ("open", "high", "low", "close")}}
              for r in rows[:21]]
    assert encode_candles(scaled)[0].atr == 20
    assert encode_candles(scaled)[0].atr_ratio == prefix[0].atr_ratio


def test_atr_warmup_and_flat_market():
    assert encode_candles(candles(14), warmup_days=1) == []
    assert len(encode_candles(candles(15), warmup_days=1)) == 1
    rows = candles(21)
    for row in rows:
        row.update(high=100, low=100)
    assert encode_candles(rows)[0].atr == 0


@pytest.mark.parametrize("factor", [0.9, 0.5, 2.0])
@pytest.mark.parametrize("action_index", [7, 14, 21])
def test_atr_company_action_scales_seed_and_running_atr(factor, action_index):
    rows = candles(24)
    before = calculate_atr(rows[:action_index])
    for row in rows[action_index:]:
        row.update(open=100 * factor, high=101 * factor,
                   low=99 * factor, close=100 * factor)
    rows[action_index]["reference_price"] = 100 * factor
    values = calculate_atr(rows)
    assert values[:action_index] == before
    for value in values[max(14, action_index):]:
        assert value == pytest.approx(2 * factor)
    assert encode_candles(rows)[-1].atr_ratio == pytest.approx(0.02)


def test_atr_retains_real_gap_after_company_action_and_multiple_actions():
    rows = candles(17)
    rows[15].update(open=54, high=55, low=53, close=54, reference_price=50)
    rows[16].update(open=27, high=28, low=26, close=27, reference_price=27)
    values = calculate_atr(rows)
    assert values[14] == 2
    # Scale old ATR to 1, then retain the genuine 5-unit move above reference.
    assert values[15] == pytest.approx((13 + 5) / 14)
    assert values[16] == pytest.approx((values[15] * 0.5 * 13 + 2) / 14)


@pytest.mark.parametrize("reference", [0, -1, float("nan"), float("inf")])
def test_atr_rejects_invalid_reference(reference):
    rows = candles(16)
    rows[15]["reference_price"] = reference
    with pytest.raises(ValueError, match="交易參考價"):
        calculate_atr(rows)


def test_missing_or_invalid_atr_requires_rebuild():
    with pytest.raises(ValueError, match="prepare_features"):
        encode_state({})
    for invalid in (-1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="atr_ratio"):
            encode_state({"atr_ratio": invalid})


def test_six_input_forward_backward_and_checkpoint_roundtrip(tmp_path):
    torch.manual_seed(42)
    model = StockAutoregressiveModel(2, d_model=16, n_heads=2, n_layers=1, dropout=0)
    state = encode_candles(candles(21))[0].to_dict()
    states = torch.tensor([[encode_state(state)] * 2], dtype=torch.long)
    outputs = model(states)
    assert {k: tuple(v.shape) for k, v in outputs.items()} == {
        "price": (1, 5), "hit_up": (1, 2), "hit_down": (1, 2)}
    targets = {k: torch.zeros(1, dtype=torch.long) for k in outputs}
    loss = weighted_loss(outputs, targets, Settings())
    loss.backward()
    assert torch.isfinite(loss)
    assert model.atr_embedding.weight.grad.abs().sum() > 0
    changed = states.clone()
    changed[..., 5] = 4
    assert not torch.allclose(outputs["price"], model(changed)["price"])
    path = tmp_path / "model.pt"
    torch.save({"model": model.state_dict(), "atr_encoding": ATR_ENCODING,
                "settings": {"atr_boundaries_pct": [1, 2, 3, 5]},
                "atr_calibration": {"method": "explicit"}}, path)
    checkpoint = torch.load(path, weights_only=True)
    ensure_checkpoint_compatible(checkpoint)
    restored = StockAutoregressiveModel(2, d_model=16, n_heads=2, n_layers=1, dropout=0)
    restored.load_state_dict(checkpoint["model"])
    torch.testing.assert_close(restored(states)["price"], outputs["price"])
    with pytest.raises(ValueError, match="6"):
        model(states[..., :5])
    old_weights = {k: v for k, v in checkpoint["model"].items()
                   if not k.startswith("atr_embedding.")}
    with pytest.raises(RuntimeError, match="train_initial"):
        ensure_checkpoint_compatible({"model": old_weights})
    with pytest.raises(ValueError, match="連續值"):
        model(states.float())
