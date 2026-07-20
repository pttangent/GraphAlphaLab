from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class LabelSpec:
    label_id: str
    horizon_rows: int
    price_column: str = "price"
    entry_lag_rows: int = 0


def build_forward_return_labels(
    prices: pd.DataFrame,
    specs: Iterable[LabelSpec],
    *,
    symbol_column: str = "symbol",
    time_column: str = "timestamp",
) -> pd.DataFrame:
    required = {symbol_column, time_column}
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"Price frame is missing columns: {missing}")
    data = prices.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    data = data.dropna(subset=[symbol_column, time_column]).sort_values([symbol_column, time_column])
    rows = []
    for spec in specs:
        if spec.price_column not in data.columns:
            raise ValueError(f"Price column {spec.price_column!r} is missing")
        grouped = data.groupby(symbol_column, observed=True)[spec.price_column]
        entry = grouped.shift(-spec.entry_lag_rows)
        exit_price = grouped.shift(-(spec.entry_lag_rows + spec.horizon_rows))
        label = data[[symbol_column, time_column]].copy()
        label["label_id"] = spec.label_id
        label["horizon_rows"] = spec.horizon_rows
        label["entry_lag_rows"] = spec.entry_lag_rows
        label["entry_price"] = entry
        label["exit_price"] = exit_price
        label["target_return"] = exit_price / entry - 1.0
        label["label_available_time"] = data.groupby(symbol_column, observed=True)[time_column].shift(
            -(spec.entry_lag_rows + spec.horizon_rows)
        )
        rows.append(label)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
