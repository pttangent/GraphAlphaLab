from __future__ import annotations

from pathlib import Path

import pandas as pd

from graphalphalab.forward_volatility_report_streaming import (
    _stream_quantile_summary,
    _stream_snapshot_stability,
)


def test_quantile_evidence_is_compacted_by_factor_and_quantile(tmp_path: Path) -> None:
    rows = []
    for factor_id in ("factor_a", "factor_b"):
        for quantile in (1, 10):
            for index, value in enumerate((0.1, 0.2, 0.3), start=1):
                rows.append(
                    {
                        "batch_id": "dual_theme_igc",
                        "factor_id": factor_id,
                        "scope": "global",
                        "theme_family": "shared_global",
                        "layer_id": factor_id,
                        "scale_minutes": 30,
                        "variant_id": "graph_forward",
                        "trade_date": f"2026-07-0{index}",
                        "decision_time": f"2026-07-0{index}T15:00:00Z",
                        "quantile": quantile,
                        "mean_return": value + quantile / 100,
                    }
                )
    path = tmp_path / "quantile_returns.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    summary = _stream_quantile_summary(path, chunksize=5)

    assert len(summary) == 4
    assert set(summary["observation_count"]) == {3}
    selected = summary[(summary["factor_id"] == "factor_a") & (summary["quantile"] == 1)]
    assert len(selected) == 1
    assert abs(float(selected.iloc[0]["mean_return"]) - 0.21) < 1e-12
    assert selected.iloc[0]["first_trade_date"] == "2026-07-01"
    assert selected.iloc[0]["last_trade_date"] == "2026-07-03"


def test_snapshot_ic_is_compacted_to_all_and_hour_summaries(tmp_path: Path) -> None:
    rows = []
    for factor_id in ("factor_a", "factor_b"):
        for decision_time, ic in (
            ("2026-07-01T14:00:00Z", 0.01),
            ("2026-07-01T14:05:00Z", -0.02),
            ("2026-07-01T15:00:00Z", 0.03),
            ("2026-07-02T15:00:00Z", 0.04),
        ):
            rows.append(
                {
                    "batch_id": "dual_theme_igc",
                    "factor_id": factor_id,
                    "scope": "global",
                    "theme_family": "shared_global",
                    "layer_id": factor_id,
                    "scale_minutes": 30,
                    "variant_id": "graph_forward",
                    "trade_date": decision_time[:10],
                    "decision_time": decision_time,
                    "spearman_ic": ic,
                }
            )
    path = tmp_path / "ic_series.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    summary = _stream_snapshot_stability(path, chunksize=3)

    all_rows = summary[summary["slice_dimension"] == "snapshot_all"]
    hour_rows = summary[summary["slice_dimension"] == "snapshot_hour"]
    assert len(all_rows) == 2
    assert set(all_rows["observations"]) == {4}
    assert len(hour_rows) == 4
    assert set(hour_rows["slice_value"].astype(str)) == {"14", "15"}
    assert summary["spearman_ic_positive_rate"].between(0.0, 1.0).all()
