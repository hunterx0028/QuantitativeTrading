from __future__ import annotations

import math

import torch
from torch import nn

from .input_schema import INDEX_FIELDS, INPUT_FIELDS, FEATURE_WEIGHT_NAMES


class StockAutoregressiveModel(nn.Module):
    """每個 timestep 是一天；輸出為隔日 high_price 與 low_price。
    兩項輸出分別為目標日最高價、最低價相對交易參考價的五級分類。
    歷史開高低收等為輸入；兩個 head 共用歷史序列表示。
    每列為個股九項、IX0001/IX0043 各四項 OHLC、下一交易日夜盤，共十八項。
    三組 embedding 先依組內項數平方根正規化，再依設定加權。"""

    def __init__(
        self,
        context_days: int,
        d_model: int = 128,
        n_heads: int = 4,
        n_layers: int = 4,
        dropout: float = 0.1,
        stock_price_weight: float = 1.5,
        stock_activity_weight: float = 1.0,
        market_weight: float = 0.75,
    ):
        super().__init__()
        self.context_days = context_days
        weights = (stock_price_weight, stock_activity_weight, market_weight)
        for name, value in zip(FEATURE_WEIGHT_NAMES, weights):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} 必須是有限正數")
        self.stock_price_weight, self.stock_activity_weight, self.market_weight = weights
        self.open_price_embedding = nn.Embedding(5, d_model)
        self.high_price_embedding = nn.Embedding(5, d_model)
        self.low_price_embedding = nn.Embedding(5, d_model)
        self.close_price_embedding = nn.Embedding(5, d_model)
        self.hit_up_embedding = nn.Embedding(2, d_model)
        self.hit_down_embedding = nn.Embedding(2, d_model)
        self.close_embedding = nn.Embedding(3, d_model)
        self.volume_embedding = nn.Embedding(6, d_model)
        self.atr_embedding = nn.Embedding(5, d_model)
        self.night_futures_embedding = nn.Embedding(5, d_model)
        self.index_embeddings = nn.ModuleDict({field: nn.Embedding(5, d_model) for field in INDEX_FIELDS})
        self.position_embedding = nn.Embedding(context_days, d_model)
        layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
            norm_first=True,
            activation="gelu",
        )
        self.transformer = nn.TransformerEncoder(
            layer,
            num_layers=n_layers,
            enable_nested_tensor=False,
        )
        self.norm = nn.LayerNorm(d_model)
        self.high_price_head = nn.Linear(d_model, 5)
        self.low_price_head = nn.Linear(d_model, 5)

    def forward(self, states: torch.Tensor) -> dict[str, torch.Tensor]:
        if states.ndim != 3 or states.shape[-1] != len(INPUT_FIELDS):
            raise ValueError("states shape 必須是 [batch, days, 18]，含個股九項、指數八項與夜盤")
        if states.dtype != torch.long:
            raise ValueError("十八項輸入必須為 torch.long 離散刻度，ATR 不接受連續值")
        days = states.shape[1]
        if days > self.context_days:
            raise ValueError(f"輸入 {days} 日超過模型上限 {self.context_days}")
        positions = torch.arange(days, device=states.device)
        stock_prices = (
            self.open_price_embedding(states[..., 0])
            + self.high_price_embedding(states[..., 1])
            + self.low_price_embedding(states[..., 2])
            + self.close_price_embedding(states[..., 3])
            + self.hit_up_embedding(states[..., 4])
            + self.hit_down_embedding(states[..., 5])
            + self.close_embedding(states[..., 6])
        )
        activity = (
            self.volume_embedding(states[..., 7])
            + self.atr_embedding(states[..., 8])
        )
        market = sum(self.index_embeddings[field](states[..., 9 + i])
                     for i, field in enumerate(INDEX_FIELDS))
        market = market + self.night_futures_embedding(states[..., 17])
        hidden = (
            self.stock_price_weight * stock_prices / math.sqrt(7)
            + self.stock_activity_weight * activity / math.sqrt(2)
            + self.market_weight * market / math.sqrt(9)
            + self.position_embedding(positions)[None, :, :]
        )
        causal_mask = torch.triu(
            torch.ones(days, days, device=states.device, dtype=torch.bool), diagonal=1
        )
        hidden = self.norm(self.transformer(hidden, mask=causal_mask)[:, -1, :])
        return {
            "high_price": self.high_price_head(hidden),
            "low_price": self.low_price_head(hidden),
        }
