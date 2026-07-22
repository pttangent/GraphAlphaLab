from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gal_warehouse" / "run_within_theme_alpha.py"
SPEC = importlib.util.spec_from_file_location("run_within_theme_alpha", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_fixed_negative_direction_builds_theme_neutral_positive_portfolio() -> None:
    rng = np.random.default_rng(7)
    rows: list[dict[str, object]] = []
    for date in ("2026-06-01", "2026-06-02"):
        for decision in pd.date_range(f"{date} 14:00:00+00:00", periods=4, freq="5min"):
            for theme_id, start in (("theme_a", 0), ("theme_b", 100)):
                for offset in range(20):
                    score = float(offset)
                    rows.append(
                        {
                            "batch_id": "implemented27",
                            "factor_id": "trade_intensity_to_volatility__graph_forward",
                            "layer_id": "trade_intensity_to_volatility",
                            "scale_minutes": 30,
                            "variant_id": "graph_forward",
                            "trade_date": date,
                            "decision_time": decision,
                            "theme_decision_time": decision - pd.Timedelta(minutes=5),
                            "theme_id": theme_id,
                            "theme_size": 20,
                            "tree_depth": 1,
                            "symbol_id": start + offset,
                            "score": score,
                            "own_score": rng.normal(),
                            "target_return": -0.001 * score,
                        }
                    )
    frame = pd.DataFrame(rows)
    metric, ic, portfolio, theme_detail = MODULE.evaluate_factor(
        frame,
        {
            "batch_id": "implemented27",
            "factor_id": "trade_intensity_to_volatility__graph_forward",
            "layer_id": "trade_intensity_to_volatility",
            "scale_minutes": 30,
            "variant_id": "graph_forward",
        },
        quantiles=5,
        min_theme_size=20,
        min_themes=2,
        direction_mode="negative",
        controls=[],
        costs=[0.0, 5.0],
    )

    assert not ic.empty
    assert not portfolio.empty
    assert not theme_detail.empty
    assert metric["expected_direction"] == -1
    assert metric["direction_predeclared"] is True
    assert metric["theme_lag_safe"] is True
    assert metric["oriented_long_short_mean"] > 0
    assert portfolio["theme_count"].eq(2).all()
    assert theme_detail.groupby(["trade_date", "decision_time"])["theme_id"].nunique().eq(2).all()
