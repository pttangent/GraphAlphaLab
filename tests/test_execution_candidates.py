from __future__ import annotations

import pandas as pd

from graphalphalab.execution_candidates import (
    select_execution_candidates,
)


def _row(
    scope: str,
    horizon: str,
    variant: str,
    ic: float,
    factor: str,
) -> dict[str, object]:
    return {
        "scope": scope,
        "theme_family": (
            "shared_global" if scope == "global" else "momentum_state"
        ),
        "factor_id": f"{scope}::family::{factor}::{variant}",
        "layer_id": factor,
        "scale_minutes": 30,
        "variant_id": variant,
        "horizon": horizon,
        "horizon_minutes": int(horizon.removesuffix("m")),
        "mean_spearman_ic": ic,
        "date_count": 40,
        "observations": 10_000,
        "oriented_long_short_mean": abs(ic) / 10,
        "mean_turnover": 1.0,
        "daily_ic_sign_consistency": 0.75,
        "spearman_icir": abs(ic) * 10,
        "financial_role": "direct_return_alpha",
        "semantic_promotion_eligible": True,
    }


def test_select_execution_candidates_prefers_incremental_graph_rows() -> None:
    rows: list[dict[str, object]] = []
    for horizon in ("30m", "60m"):
        for factor, values in {
            "strong": (0.04, 0.01, 0.005),
            "weak": (0.01, 0.02, 0.015),
            "medium": (0.025, 0.015, 0.012),
        }.items():
            graph, node, reverse = values
            rows.extend(
                [
                    _row(
                        "global",
                        horizon,
                        "graph_forward",
                        graph,
                        factor,
                    ),
                    _row(
                        "global",
                        horizon,
                        "node_baseline",
                        node,
                        factor,
                    ),
                    _row(
                        "global",
                        horizon,
                        "graph_reverse_placebo",
                        reverse,
                        factor,
                    ),
                ]
            )
    selected = select_execution_candidates(
        pd.DataFrame(rows),
        min_date_count=20,
        max_per_horizon=2,
        min_per_horizon=1,
    )
    assert len(selected) == 4
    assert set(selected["variant_id"]) == {"graph_forward"}
    top = selected[selected["candidate_rank"] == 1]
    assert set(top["layer_id"]) == {"strong"}
    assert set(top["selection_tier"]) == {"strict_incremental"}


def test_select_execution_candidates_respects_minimum_dates() -> None:
    rows = [
        _row("global", "30m", variant, value, "factor")
        for variant, value in (
            ("graph_forward", 0.04),
            ("node_baseline", 0.01),
            ("graph_reverse_placebo", 0.005),
        )
    ]
    for row in rows:
        row["date_count"] = 10
    selected = select_execution_candidates(
        pd.DataFrame(rows),
        min_date_count=20,
    )
    assert selected.empty
