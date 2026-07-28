from __future__ import annotations

import numpy as np
import pandas as pd

from graphalphalab.execution_frequency import (
    ExecutionPolicy,
    add_frequency_deltas,
    evaluate_execution_policy,
    pareto_frontier,
    walk_forward_direction_by_date,
)


def _frame(decisions: int = 24, symbols: int = 20) -> pd.DataFrame:
    rows = []
    base = pd.Timestamp("2026-01-05 14:30:00", tz="UTC")
    for step in range(decisions):
        decision = base + pd.Timedelta(minutes=5 * step)
        for symbol in range(symbols):
            score = float(symbol) + (0.05 * step if symbol % 2 == 0 else -0.05 * step)
            target = 0.0001 * score
            rows.append(
                {
                    "trade_date": "2026-01-05",
                    "decision_time": decision,
                    "symbol_id": symbol,
                    "context_theme_id": f"T{symbol // 5}",
                    "score": score,
                    "target_return": target,
                }
            )
    return pd.DataFrame(rows)


def test_slower_frequency_reduces_turnover() -> None:
    frame = _frame()
    fast, _ = evaluate_execution_policy(
        frame,
        ExecutionPolicy("fast", 5),
        score_column="score",
        return_column="target_return",
        fixed_direction=1,
    )
    slow, _ = evaluate_execution_policy(
        frame,
        ExecutionPolicy("slow", 60),
        score_column="score",
        return_column="target_return",
        fixed_direction=1,
    )
    assert slow.loc[0, "rebalance_count"] < fast.loc[0, "rebalance_count"]
    assert slow.loc[0, "total_turnover"] <= fast.loc[0, "total_turnover"]


def test_rank_change_gate_skips_unchanged_signal() -> None:
    frame = _frame()
    fixed, _ = evaluate_execution_policy(
        frame,
        ExecutionPolicy("fixed", 5),
        score_column="score",
        return_column="target_return",
        fixed_direction=1,
    )
    gated, _ = evaluate_execution_policy(
        frame,
        ExecutionPolicy(
            "gated",
            5,
            gate_mode="rank_change",
            min_rank_change=0.40,
            min_target_turnover=0.01,
        ),
        score_column="score",
        return_column="target_return",
        fixed_direction=1,
    )
    assert gated.loc[0, "rebalance_count"] <= fixed.loc[0, "rebalance_count"]


def test_turnover_cap_is_enforced() -> None:
    frame = _frame()
    policy = ExecutionPolicy(
        "cap",
        5,
        max_turnover_per_rebalance=0.25,
    )
    _, returns = evaluate_execution_policy(
        frame,
        policy,
        score_column="score",
        return_column="target_return",
        fixed_direction=1,
    )
    assert returns["turnover"].max() <= 0.2500001


def test_walk_forward_direction_never_uses_current_date() -> None:
    frame = _frame(decisions=6)
    copies = []
    for day in range(6):
        copy = frame.copy()
        copy["trade_date"] = f"2026-01-{5 + day:02d}"
        copy["decision_time"] = copy["decision_time"] + pd.Timedelta(days=day)
        if day == 5:
            copy["target_return"] *= -100.0
        copies.append(copy)
    all_days = pd.concat(copies, ignore_index=True)
    direction = walk_forward_direction_by_date(
        all_days,
        score_column="score",
        return_column="target_return",
        min_train_dates=3,
        rolling_dates=None,
    )
    assert direction["2026-01-10"] == 1


def test_frequency_deltas_and_pareto() -> None:
    metrics = pd.DataFrame(
        {
            "policy_id": ["fixed_5m", "fixed_30m", "combined_60m"],
            "mean_turnover": [1.0, 0.5, 0.2],
            "net_mean_5bps": [-0.0010, -0.0002, 0.0001],
        }
    )
    enriched = add_frequency_deltas(metrics)
    assert np.isclose(enriched.loc[1, "turnover_reduction_vs_baseline"], 0.5)
    frontier = pareto_frontier(enriched)
    assert "combined_60m" in set(frontier["policy_id"])
