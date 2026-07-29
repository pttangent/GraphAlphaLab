from __future__ import annotations

import numpy as np
import pandas as pd

from graphalphalab.forward_volatility_hardening import install as install_hardening

install_hardening()

from graphalphalab.forward_volatility import (  # noqa: E402
    ForwardVolatilityConfig,
    _build_symbol_rows,
)


def _frame() -> pd.DataFrame:
    timestamps = pd.date_range("2026-07-01T13:30:00Z", periods=390, freq="min")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "available_time_1m": timestamps + pd.Timedelta(seconds=3),
            "symbol_id": 1,
            "ret_1m": np.full(len(timestamps), 0.001),
        }
    )


def test_forward_label_window_is_entry_lagged_and_exact() -> None:
    frame = _frame()
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
    assert by_horizon[30]["label_available_time"] == by_horizon[30]["exit_time"] + pd.Timedelta(seconds=3)


def test_missing_tail_is_not_truncated() -> None:
    frame = _frame()
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


def test_forward_rv_survives_incomplete_past_window() -> None:
    frame = _frame()
    decision = pd.Timestamp("2026-07-01T14:00:00Z").value
    config = ForwardVolatilityConfig(
        horizons=(60,),
        targets=("forward_log_rv", "forward_vol_expansion"),
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
    assert len(rows) == 1
    assert np.isfinite(rows[0]["forward_log_rv"])
    assert not rows[0]["past_window_complete"]
    assert np.isnan(rows[0]["forward_vol_expansion"])
