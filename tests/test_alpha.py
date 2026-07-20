from __future__ import annotations

import numpy as np
import pandas as pd

from graphalphalab.alpha import evaluate_alpha


def _frame() -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(7)
    symbols = [f"S{i:03d}" for i in range(30)]
    for t in range(30):
        decision = pd.Timestamp("2026-06-01T14:30:00Z") + pd.Timedelta(minutes=5 * t)
        scores = rng.normal(size=len(symbols))
        returns = 0.01 * scores + rng.normal(scale=0.002, size=len(symbols))
        for symbol, score, target in zip(symbols, scores, returns):
            rows.append({
                "batch_id": "implemented27",
                "factor_id": "positive_factor",
                "layer_id": "L1",
                "scale_minutes": 15,
                "horizon": "15m",
                "decision_time": decision,
                "symbol": symbol,
                "score": score,
                "target_return": target,
            })
    return pd.DataFrame(rows)


def test_alpha_metrics_detect_signal() -> None:
    result = evaluate_alpha(_frame(), min_cross_section=10, annualization_factor=252)
    row = result.metrics.iloc[0]
    assert row["mean_spearman_ic"] > 0.8
    assert row["long_short_mean"] > 0
    assert row["fdr_pass"]
    assert row["research_status"] == "candidate"
    assert not result.quantile_returns.empty
    assert not result.stability.empty
