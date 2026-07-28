from __future__ import annotations

import numpy as np
import pandas as pd

from graphalphalab.execution_frequency import ExecutionPolicy
from graphalphalab.execution_frequency_v2 import (
    evaluate_execution_policy_v2,
)


def _frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day in pd.date_range("2026-01-05", periods=3, freq="B"):
        for minute in (0, 30, 60):
            decision = (
                pd.Timestamp(day, tz="UTC")
                + pd.Timedelta(hours=14, minutes=30 + minute)
            )
            for symbol in range(20):
                score = float(symbol) + minute / 1000
                rows.append(
                    {
                        "trade_date": day.date().isoformat(),
                        "decision_time": decision,
                        "symbol_id": f"S{symbol:02d}",
                        "score": score,
                        "target_return": score / 100_000,
                        "context_theme_id": f"T{symbol // 5}",
                    }
                )
    return pd.DataFrame(rows)


def test_execution_v2_resets_intraday_book_each_session() -> None:
    policy = ExecutionPolicy("fixed_30m", 30)
    metrics, returns = evaluate_execution_policy_v2(
        _frame(),
        policy,
        score_column="score",
        return_column="target_return",
        fixed_direction=1,
        carry_overnight=False,
    )
    first = (
        returns.sort_values("decision_time")
        .groupby("trade_date", observed=True)
        .head(1)
    )
    assert len(first) == 3
    assert np.allclose(first["turnover"], 1.0)
    assert metrics.iloc[0]["date_count"] == 3
    assert "rank_gate_pass" in returns.columns
    assert "mean_minutes_between_rebalances" in metrics.columns


def test_execution_v2_combined_gate_exposes_component_rates() -> None:
    policy = ExecutionPolicy(
        "combined_30m",
        30,
        gate_mode="combined",
        min_rank_change=0.1,
        confidence_history_quantile=0.6,
        min_theme_retention=0.5,
        min_target_turnover=0.1,
        max_turnover_per_rebalance=0.5,
    )
    metrics, returns = evaluate_execution_policy_v2(
        _frame(),
        policy,
        score_column="score",
        return_column="target_return",
        fixed_direction=1,
    )
    assert not returns.empty
    for column in (
        "rank_gate_pass_rate",
        "confidence_gate_pass_rate",
        "theme_gate_pass_rate",
        "turnover_gate_pass_rate",
    ):
        assert column in metrics.columns
