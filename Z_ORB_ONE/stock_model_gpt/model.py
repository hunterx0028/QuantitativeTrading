from __future__ import annotations

import torch
from torch import nn


class StockAutoregressiveModel(nn.Module):
    """每個 timestep 是一天；成交量只作輸入，輸出為隔日四項狀態。"""

    def __init__(
        self,
        context_days: int,
        d_model: int = 128,
        n_heads: int = 4,
        n_layers: int = 4,
        dropout: float = 0.1,
    ):
        super().__init__()
        self.context_days = context_days
        self.price_embedding = nn.Embedding(5, d_model)
        self.hit_up_embedding = nn.Embedding(2, d_model)
        self.hit_down_embedding = nn.Embedding(2, d_model)
        self.close_embedding = nn.Embedding(3, d_model)
        self.volume_embedding = nn.Embedding(6, d_model)
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
        self.transformer = nn.TransformerEncoder(layer, num_layers=n_layers)
        self.norm = nn.LayerNorm(d_model)
        self.price_head = nn.Linear(d_model, 5)
        self.hit_up_head = nn.Linear(d_model, 2)
        self.hit_down_head = nn.Linear(d_model, 2)
        self.close_head = nn.Linear(d_model, 3)

    def forward(self, states: torch.Tensor) -> dict[str, torch.Tensor]:
        if states.ndim != 3 or states.shape[-1] != 5:
            raise ValueError("states shape 必須是 [batch, days, 5]")
        days = states.shape[1]
        if days > self.context_days:
            raise ValueError(f"輸入 {days} 日超過模型上限 {self.context_days}")
        positions = torch.arange(days, device=states.device)
        hidden = (
            self.price_embedding(states[..., 0])
            + self.hit_up_embedding(states[..., 1])
            + self.hit_down_embedding(states[..., 2])
            + self.close_embedding(states[..., 3])
            + self.volume_embedding(states[..., 4])
            + self.position_embedding(positions)[None, :, :]
        )
        causal_mask = torch.triu(
            torch.ones(days, days, device=states.device, dtype=torch.bool), diagonal=1
        )
        hidden = self.norm(self.transformer(hidden, mask=causal_mask)[:, -1, :])
        return {
            "price": self.price_head(hidden),
            "hit_up": self.hit_up_head(hidden),
            "hit_down": self.hit_down_head(hidden),
            "close_limit": self.close_head(hidden),
        }
