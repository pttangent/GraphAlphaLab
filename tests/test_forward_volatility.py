from __future__ import annotations

import numpy as np
import pandas as pd

from graphalphalab.forward_volatility import (
    ForwardVolatilityConfig,
    _build_symbol_rows,
)


def test_forward_label_window_is_entry_lagged_and_exact() -> None:
    timestamps = pd.date_range("2026-07-01T13:30:00Z", periods=390, freq="min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol_id": 1,
            "ret_1m": np.full(len(timestamps), 0.001),
        }
    )
    decisions = np.array([pd.Timestamp("2026-07-01T15:00:00Z").value])
    config = ForwardVolatilityConfig(
        horizons=(30, 60),
        targets=("forward_log_rv",),
        entry_lag_minutes=1,
        min_coverage=1.0,
    )
    rows = _build_symbol_rows(
        frame,
        decisions,
        config,
        trade_date="2026-07-01",
        symbol_id=1,
    )
    assert len(rows) == 2
    by_horizon = {int(row["horizon_minutes"]): row for row in rows}
    assert by_horizon[30]["future_observation_count"] == 30
    assert by_horizon[60]["future_observation_count"] == 60
    assert by_horizon[30]["entry_time"] > by_horizon[30]["decision_time"]
    assert by_horizon[30]["exit_time"] - by_horizon[30]["entry_time"] == pd.Timedelta(minutes=30)
    assert by_horizon[30]["label_available_time"] == by_horizon[30]["exit_time"]


def test_missing_tail_is_not_truncated() -> None:
    timestamps = pd.date_range("2026-07-01T13:30:00Z", periods=390, freq="min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol_id": 1,
            "ret_1m": np.full(len(timestamps), 0.001),
        }
    )
    decision = pd.Timestamp("2026-07-01T19:30:00Z").value
    config = ForwardVolatilityConfig(
        horizons=(60,),
        targets=("forward_log_rv",),
        entry_lag_minutes=1,
        min_coverage=0.9,
    )
    rows = _build_symbol_rows(
        frame,
        np.array([decision]),
        config,
        trade_date="2026-07-01",
        symbol_id=1,
    )
    assert rows == []
