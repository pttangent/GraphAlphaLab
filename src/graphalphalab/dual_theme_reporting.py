from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from .alpha import AlphaResult
from .batch import get_batch
from .contracts import LabelContract
from .dual_theme_common import (
    DUAL_THEME_ALPHA_VERSION,
    DUAL_THEME_BATCH_ID,
    load_horizon_manifest,
    parse_factor_id,
)
from .governance import (
    ResourceBudget,
    atomic_write_frame,
    atomic_write_json,
    atomic_write_text,
    directory_parquet_records,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
    sha256_file,
)
from .metadata import normalize_metadata
from .reports import write_report_bundle
from .streaming import evaluate_alpha_streaming


def _annotate_result(result: AlphaResult, horizon_name: str, contract: LabelContract) -> AlphaResult:
    for frame in (
        result.metrics,
        result.ic_series,
        result.daily_ic,
        result.quantile_returns,
        result.portfolio_returns,
        result.stability,
    ):
        if frame.empty:
            continue
        frame["horizon"] = horizon_name
        frame["horizon_minutes"] = int(contract.horizon_minutes)
        if "factor_id" in frame.columns:
            parsed = frame["factor_id"].map(parse_factor_id).apply(pd.Series)
            for column in ("scope", "theme_family"):
                frame[column] = parsed[column].values
    return result


def _matched_variant_comparison(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    keys = [
        column
        for column in (
            "scope",
            "theme_family",
            "layer_id",
            "scale_minutes",
            "horizon",
            "horizon_minutes",
        )
        if column in metrics.columns
    ]
    value_columns = [column for column in ("mean_spearman_ic", "net_mean_5bps") if column in metrics.columns]
    if not keys or not value_columns or "variant_id" not in metrics.columns:
        return pd.DataFrame()
    pivot = metrics.pivot_table(index=keys, columns="variant_id", values=value_columns, aggfunc="first")
    pivot.columns = [f"{metric}__{variant}" for metric, variant in pivot.columns]
    pivot = pivot.reset_index()
    graph_ic = "mean_spearman_ic__graph_forward"
    node_ic = "mean_spearman_ic__node_baseline"
    placebo_ic = "mean_spearman_ic__graph_reverse_placebo"
    if graph_ic in pivot and node_ic in pivot:
        pivot["abs_ic_increment_vs_node"] = pivot[graph_ic].abs() - pivot[node_ic].abs()
    if graph_ic in pivot and placebo_ic in pivot:
        pivot["abs_ic_increment_vs_reverse_placebo"] = pivot[graph_ic].abs() - pivot[placebo_ic].abs()
    graph_net = "net_mean_5bps__graph_forward"
    node_net = "net_mean_5bps__node_baseline"
    placebo_net = "net_mean_5bps__graph_reverse_placebo"
    if graph_net in pivot and node_net in pivot:
        pivot["net_5bps_increment_vs_node"] = pivot[graph_net] - pivot[node_net]
    if graph_net in pivot and placebo_net in pivot:
        pivot["net_5bps_increment_vs_reverse_placebo"] = pivot[graph_net] - pivot[placebo_net]
    return pivot


def _scope_summary(metrics: pd.DataFrame, matched: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    keys = [column for column in ("scope", "theme_family", "horizon", "horizon_minutes") if column in metrics]
    graph = metrics[
        metrics.get("variant_id", pd.Series(index=metrics.index, dtype=str)) == "graph_forward"
    ].copy()
    if graph.empty:
        return pd.DataFrame()
    summary = (
        graph.groupby(keys, observed=True, dropna=False)
        .agg(
            factor_count=("factor_id", "nunique"),
            mean_abs_ic=(
                "mean_spearman_ic",
                lambda values: float(pd.to_numeric(values, errors="coerce").abs().mean()),
            ),
            median_abs_ic=(
                "mean_spearman_ic",
                lambda values: float(pd.to_numeric(values, errors="coerce").abs().median()),
            ),
            mean_net_5bps=("net_mean_5bps", "mean"),
            cost_survival_rate=("cost_survives_5bps", "mean"),
        )
        .reset_index()
    )
    if not matched.empty:
        increments = [
            column
            for column in (
                "abs_ic_increment_vs_node",
                "abs_ic_increment_vs_reverse_placebo",
                "net_5bps_increment_vs_node",
                "net_5bps_increment_vs_reverse_placebo",
            )
            if column in matched
        ]
        if increments:
            extra = matched.groupby(keys, observed=True, dropna=False)[increments].mean().reset_index()
            summary = summary.merge(extra, on=keys, how="left", validate="one_to_one")
    return summary


def run_dual_theme_alpha_campaign(
    signals_root: str | Path,
    horizon_manifest: str | Path,
    output_root: str | Path,
    *,
    metadata_path: str | Path | None = None,
    metadata_id: str = "symbol_id",
    metadata_signal_id: str = "symbol_id",
    slice_dimensions: Iterable[str] = (),
    join_keys: Iterable[str] = ("trade_date", "decision_time", "symbol_id"),
    score_column: str = "score",
    symbol_column: str = "symbol_id",
    quantiles: int = 5,
    min_cross_section: int = 100,
    direction_column: str | None = "expected_direction",
    default_direction: str = "auto",
    control_columns: Iterable[str] = ("own_score",),
    annualization_factor: float | None = None,
    resource_budget: ResourceBudget = ResourceBudget(),
    expected_git_commit: str | None = None,
    require_clean: bool = False,
    allow_legacy_signals: bool = False,
    correlation_sample_modulus: int = 1000,
    allow_partial: bool = False,
) -> Path:
    signals = Path(signals_root).expanduser().resolve()
    output = Path(output_root).expanduser().resolve()
    success = signals / "_SUCCESS"
    export_manifest_path = signals / "export_manifest.json"
    if not success.exists() or not export_manifest_path.exists():
        raise FileNotFoundError(f"Dual-theme signals are incomplete: {signals}")
    export_manifest = json.loads(export_manifest_path.read_text(encoding="utf-8"))
    expected_factors = int(export_manifest.get("factor_count") or 0)
    if expected_factors <= 0:
        raise ValueError("Export manifest has no governed factors")
    horizon_specs = load_horizon_manifest(horizon_manifest)
    metadata = None
    metadata_profile = None
    dimensions = list(slice_dimensions)
    if metadata_path is not None:
        metadata_file = Path(metadata_path).expanduser().resolve()
        raw = (
            pd.read_parquet(metadata_file)
            if metadata_file.suffix.lower() in {".parquet", ".pq"}
            else pd.read_csv(metadata_file)
        )
        metadata, metadata_profile = normalize_metadata(
            raw,
            id_column=metadata_id,
            dimensions=dimensions or None,
        )
        dimensions = list(metadata_profile.dimensions)
    output.mkdir(parents=True, exist_ok=True)
    all_metrics: list[pd.DataFrame] = []
    horizon_summaries: list[dict[str, object]] = []
    for spec in horizon_specs:
        contract = LabelContract.from_json(spec.label_contract)
        result = evaluate_alpha_streaming(
            signals,
            spec.labels,
            label_contract=contract,
            join_keys=join_keys,
            metadata=metadata,
            metadata_signal_id=metadata_signal_id,
            metadata_id=metadata_profile.id_column if metadata_profile else metadata_id,
            slice_columns=dimensions,
            score_column=score_column,
            symbol_column=symbol_column,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=control_columns,
            annualization_factor=annualization_factor,
            resource_budget=resource_budget,
            allow_legacy_signals=allow_legacy_signals,
            correlation_sample_modulus=correlation_sample_modulus,
        )
        result = _annotate_result(result, spec.name, contract)
        observed = int(result.metrics["factor_id"].nunique()) if not result.metrics.empty else 0
        complete = observed == expected_factors
        if not complete and not allow_partial:
            raise ValueError(
                f"Horizon {spec.name} expected {expected_factors} factors, observed {observed}; "
                "use --allow-partial only for an explicit diagnostic"
            )
        batch_status = {
            "batch_id": DUAL_THEME_BATCH_ID,
            "expected_contracts": expected_factors,
            "observed_contracts": observed,
            "complete": complete,
            "partial": not complete,
            "report_scope": list(get_batch(DUAL_THEME_BATCH_ID).report_scope),
        }
        horizon_output = output / f"horizon={spec.name}"
        inputs = {
            "signal_manifest": file_record(export_manifest_path),
            "signal_success": file_record(success),
            "label_files": directory_parquet_records(spec.labels),
            "label_contract": file_record(spec.label_contract),
            **({"metadata": file_record(metadata_path)} if metadata_path else {}),
        }
        manifest_inputs = [
            file_record(export_manifest_path),
            file_record(spec.label_contract),
            *directory_parquet_records(spec.labels),
        ]
        if metadata_path:
            manifest_inputs.append(file_record(metadata_path))
        run_manifest = implementation_manifest(
            operation="dual_theme_alpha_report",
            parameters={
                "version": DUAL_THEME_ALPHA_VERSION,
                "horizon": spec.name,
                "label_contract": contract.as_dict(),
                "join_keys": list(join_keys),
                "expected_factors": expected_factors,
            },
            inputs=manifest_inputs,
            resource_budget=resource_budget,
        )
        enforce_git_lineage(
            run_manifest,
            expected_commit=expected_git_commit,
            require_clean=require_clean,
        )
        write_report_bundle(
            horizon_output,
            batch_id=DUAL_THEME_BATCH_ID,
            batch_status=batch_status,
            alpha=result,
            lineage=inputs,
            run_manifest=run_manifest,
        )
        if not result.metrics.empty:
            result.metrics["source_report"] = str(horizon_output)
            all_metrics.append(result.metrics)
        horizon_summaries.append(
            {
                "horizon": spec.name,
                "horizon_minutes": contract.horizon_minutes,
                "report": str(horizon_output),
                "observed_factors": observed,
                "expected_factors": expected_factors,
                "complete": complete,
            }
        )
    combined = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()
    matched = _matched_variant_comparison(combined)
    scope_summary = _scope_summary(combined, matched)
    atomic_write_frame(combined, output / "all_horizons_alpha_metrics.csv")
    atomic_write_frame(matched, output / "matched_variant_comparison.csv")
    atomic_write_frame(scope_summary, output / "scope_family_horizon_summary.csv")
    ranking = matched.copy()
    ranking_columns = [
        column
        for column in (
            "abs_ic_increment_vs_node",
            "abs_ic_increment_vs_reverse_placebo",
            "net_5bps_increment_vs_node",
            "net_5bps_increment_vs_reverse_placebo",
        )
        if column in ranking
    ]
    if ranking_columns:
        ranking = ranking.sort_values(ranking_columns, ascending=False, na_position="last")
    atomic_write_frame(ranking, output / "ranking.csv")
    summary = {
        "version": DUAL_THEME_ALPHA_VERSION,
        "batch_id": DUAL_THEME_BATCH_ID,
        "signals_root": str(signals),
        "export_manifest_sha256": sha256_file(export_manifest_path),
        "expected_factor_count_per_horizon": expected_factors,
        "horizons": horizon_summaries,
        "metric_rows": int(len(combined)),
        "matched_rows": int(len(matched)),
        "scope_summary_rows": int(len(scope_summary)),
        "one_day_inference_warning": (
            "One-session results are diagnostic and cannot satisfy the >=20-date governance gate."
        ),
    }
    atomic_write_json(output / "summary.json", summary)
    lines = [
        "# GraphAlphaLab dual-theme multi-horizon Alpha report",
        "",
        f"- Signals: `{signals}`",
        f"- Expected factor identities per horizon: {expected_factors}",
        f"- Horizons: {', '.join(spec.name for spec in horizon_specs)}",
        "- Identity: `theme_family × scope × layer × scale × variant × horizon`",
        "- Global signals are shared once; Within and Inter are evaluated separately for Momentum and Residual themes.",
        "- Inter-theme theme-node signals are projected back to canonical member stocks before label joins.",
        "",
        "One-day output is a mechanism and runtime comparison, not statistical promotion evidence.",
        "",
        "Key files: `scope_family_horizon_summary.csv`, `matched_variant_comparison.csv`, `ranking.csv`.",
    ]
    atomic_write_text(output / "REPORT.md", "\n".join(lines) + "\n")
    atomic_write_json(
        output / "_SUCCESS",
        {"complete": all(row["complete"] for row in horizon_summaries)},
    )
    return output
