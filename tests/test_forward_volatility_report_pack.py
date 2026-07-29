from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from graphalphalab.forward_volatility import ForwardVolatilityConfig
from graphalphalab.forward_volatility_report_pack import (
    write_forward_volatility_report_pack,
)


def _metric(layer: str, variant: str, ic: float, qvalue: float, sufficient: bool) -> dict[str, object]:
    return {
        "batch_id": "dual_theme_igc",
        "factor_id": f"global::shared_global::{layer}::{variant}",
        "scope": "global",
        "theme_family": "shared_global",
        "layer_id": layer,
        "scale_minutes": 30,
        "variant_id": variant,
        "observations": 10000 if sufficient else 100,
        "decision_count": 500 if sufficient else 10,
        "date_count": 70 if sufficient else 5,
        "symbol_count": 1000,
        "label_mean": -8.0,
        "label_std": 1.0,
        "score_mean": 0.0,
        "score_std": 1.0,
        "mean_spearman_ic": ic,
        "median_spearman_ic": ic,
        "spearman_ic_std": 0.02,
        "spearman_icir": ic / 0.02,
        "snapshot_ic_positive_rate": 0.65,
        "daily_ic_positive_rate": 0.70 if ic > 0 else 0.30,
        "daily_ic_negative_rate": 0.30 if ic > 0 else 0.70,
        "daily_ic_sign_consistency": 0.70,
        "spearman_ic_tstat_daily": 2.5 if qvalue <= 0.05 else 0.5,
        "spearman_ic_pvalue_daily": qvalue,
        "fdr_qvalue": qvalue,
        "fdr_pass": qvalue <= 0.05,
        "mean_pearson_ic_daily": ic,
        "raw_top_minus_bottom_mean": ic * 10,
        "quantile_monotonicity": 0.8,
        "sample_sufficient": sufficient,
        "expected_direction": 1,
        "direction_source": "observed_daily_ic",
        "direction_predeclared": False,
        "annualization_valid": False,
        "direction_consistent": True,
        "governance_ready": False,
        "research_status": "needs_falsification" if sufficient else "insufficient_or_rejected",
        "oriented_long_short_mean": 0.001,
        "mean_turnover": 1.0,
        "net_mean_5bps": 0.0005,
        "cost_survives_5bps": True,
    }


def _write_horizon(raw: Path) -> None:
    horizon = raw / "horizon=forward_log_rv_30m"
    horizon.mkdir(parents=True)
    (horizon / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    rows = [
        _metric("strong_layer", "graph_forward", 0.040, 0.01, True),
        _metric("strong_layer", "node_baseline", 0.010, 0.40, True),
        _metric("strong_layer", "graph_reverse_placebo", 0.004, 0.60, True),
        _metric("weak_layer", "graph_forward", 0.005, 0.50, True),
        _metric("weak_layer", "node_baseline", 0.020, 0.10, True),
        _metric("weak_layer", "graph_reverse_placebo", 0.010, 0.20, True),
        _metric("insufficient_layer", "graph_forward", 0.030, 0.01, False),
        _metric("insufficient_layer", "node_baseline", 0.005, 0.50, False),
        _metric("insufficient_layer", "graph_reverse_placebo", 0.004, 0.60, False),
    ]
    pd.DataFrame(rows).to_csv(horizon / "alpha_metrics.csv", index=False)
    daily = []
    for row in rows:
        for date in ("2026-07-01", "2026-07-02"):
            daily.append(
                {
                    **{key: row[key] for key in ("batch_id", "factor_id", "layer_id", "scale_minutes", "variant_id", "scope", "theme_family")},
                    "trade_date": date,
                    "spearman_ic": row["mean_spearman_ic"],
                    "pearson_ic": row["mean_spearman_ic"],
                }
            )
    pd.DataFrame(daily).to_csv(horizon / "daily_ic.csv", index=False)
    quantiles = []
    for row in rows:
        for quantile in range(1, 11):
            quantiles.append(
                {
                    **{key: row[key] for key in ("batch_id", "factor_id", "layer_id", "scale_minutes", "variant_id", "scope", "theme_family")},
                    "decision_time": "2026-07-01T15:00:00Z",
                    "trade_date": "2026-07-01",
                    "quantile": quantile,
                    "mean_return": quantile / 1000,
                    "n": 100,
                }
            )
    pd.DataFrame(quantiles).to_csv(horizon / "quantile_returns.csv", index=False)
    stability = []
    for row in rows:
        stability.append(
            {
                **{key: row[key] for key in ("batch_id", "factor_id", "layer_id", "scale_minutes", "variant_id", "scope", "theme_family")},
                "slice_dimension": "date",
                "slice_value": "2026-07-01",
                "observations": 1000,
                "spearman_ic": row["mean_spearman_ic"],
                "mean_target_return": -8.0,
            }
        )
    pd.DataFrame(stability).to_csv(horizon / "stability_slices.csv", index=False)
    pd.DataFrame(daily).to_csv(horizon / "ic_series.csv", index=False)
    pd.DataFrame(columns=["factor_id", "trade_date", "oriented_long_short_return"]).to_csv(
        horizon / "portfolio_returns.csv", index=False
    )
    pd.DataFrame(columns=["factor_a", "factor_b", "score_spearman_correlation"]).to_csv(
        horizon / "score_correlation.csv", index=False
    )
    (horizon / "summary.json").write_text("{}\n", encoding="utf-8")
    (horizon / "run_manifest.json").write_text("{}\n", encoding="utf-8")
    (horizon / "horizon_checkpoint.json").write_text("{}\n", encoding="utf-8")


def _write_labels(output: Path) -> None:
    target = output / "labels" / "trade_date=2026-07-01"
    target.mkdir(parents=True)
    rows = []
    decision = pd.Timestamp("2026-07-01T15:00:00Z")
    for symbol_id in range(200):
        rows.append(
            {
                "trade_date": "2026-07-01",
                "decision_time": decision,
                "symbol_id": symbol_id,
                "label_id": "forward_volatility_30m_v1",
                "entry_time": decision + pd.Timedelta(minutes=1),
                "exit_time": decision + pd.Timedelta(minutes=31),
                "label_available_time": decision + pd.Timedelta(minutes=32),
                "horizon_minutes": 30,
                "future_observation_count": 30,
                "past_observation_count": 30,
                "past_window_complete": True,
                "forward_log_rv": -8.0 + symbol_id / 1000,
            }
        )
    pd.DataFrame(rows).to_parquet(target / "data.parquet", index=False)


def test_report_pack_retains_all_results_and_writes_small_shards(tmp_path: Path) -> None:
    output = tmp_path / "output"
    raw = output / "raw_volatility_diagnostic"
    _write_horizon(raw)
    _write_labels(output)
    config = ForwardVolatilityConfig(horizons=(30,), targets=("forward_log_rv",))

    report = write_forward_volatility_report_pack(output, raw, config)

    assert (report / "_SUCCESS").exists()
    assert (report / "REPORT.md").exists()
    assert (report / "README_UPLOAD.md").exists()
    assert (report / "overview" / "factor_master_compact.csv").exists()
    assert (report / "overview" / "matched_variant_comparison.csv").exists()
    assert (report / "overview" / "non_confirmation_reason_counts.csv").exists()
    assert (report / "overview" / "label_date_coverage.csv").exists()
    assert (report / "indexes" / "UPLOAD_MANIFEST.csv").exists()
    assert (report / "indexes" / "daily_ic_shard_index.csv").exists()
    assert not (report / "prediction_metrics.parquet").exists()

    master = pd.read_csv(report / "overview" / "factor_master_compact.csv")
    assert len(master) == 9
    assert set(master["prediction_status"]) == {
        "predictive_candidate",
        "needs_falsification",
        "insufficient_or_rejected",
    }
    comparisons = pd.read_csv(report / "overview" / "matched_variant_comparison.csv")
    assert len(comparisons) == 3
    assert comparisons["graph_incremental_confirmed"].sum() == 1
    assert comparisons["non_confirmation_reasons"].astype(str).str.contains("sample_insufficient").any()
    assert comparisons["non_confirmation_reasons"].astype(str).str.contains("graph_not_stronger_than_node").any()

    manifest = pd.read_csv(report / "indexes" / "UPLOAD_MANIFEST.csv")
    assert manifest["relative_path"].astype(str).str.contains("factor_metrics/").any()
    assert manifest["relative_path"].astype(str).str.contains("factor_coverage/").any()
    assert manifest["relative_path"].astype(str).str.contains("daily_ic/").any()
    target_report = report / "target_reports" / "target=forward_log_rv" / "REPORT.md"
    text = target_report.read_text(encoding="utf-8")
    assert "Strongest graph increments" in text
    assert "Weakest / failed graph increments" in text

    before = (report / "checkpoint.json").stat().st_mtime_ns
    same = write_forward_volatility_report_pack(output, raw, config)
    assert same == report
    assert (report / "checkpoint.json").stat().st_mtime_ns == before
    summary = json.loads((report / "summary.json").read_text(encoding="utf-8"))
    assert summary["factor_metric_rows"] == 9
    assert summary["report_policy"] == "retain_all_results_in_narrow_shards"
