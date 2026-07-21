from __future__ import annotations

import numpy as np
import pandas as pd

from graphalphalab.alpha import evaluate_alpha


def _frame() -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(7)
    symbols = [f"S{i:03d}" for i in range(30)]
    for date_index in range(20):
        day = pd.Timestamp("2026-06-01T14:30:00Z") + pd.Timedelta(days=date_index)
        for t in range(5):
            decision = day + pd.Timedelta(minutes=5 * t)
            scores = rng.normal(size=len(symbols))
            returns = 0.01 * scores + rng.normal(scale=0.002, size=len(symbols))
            for symbol, score, target in zip(symbols, scores, returns):
                rows.append(
                    {
                        "batch_id": "implemented27",
                        "factor_id": "positive_factor",
                        "layer_id": "L1",
                        "scale_minutes": 15,
                        "horizon": "15m",
                        "trade_date": str(decision.date()),
                        "decision_time": decision,
                        "symbol_id": symbol,
                        "score": score,
                        "target_return": target,
                        "expected_direction": 1,
                    }
                )
    return pd.DataFrame(rows)


def test_alpha_metrics_detect_signal() -> None:
    result = evaluate_alpha(
        _frame(),
        min_cross_section=10,
        annualization_factor=19656,
        label_overlapping=False,
    )
    row = result.metrics.iloc[0]
    assert row["mean_spearman_ic"] > 0.8
    assert row["oriented_long_short_mean"] > 0
    assert row["fdr_pass"]
    assert row["direction_consistent"]
    assert row["research_status"] == "candidate"
    assert not result.quantile_returns.empty
    assert not result.portfolio_returns.empty
    assert not result.daily_ic.empty
    assert not result.stability.empty
