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
    DUAL_THEME_EXPORT_VERSION,
    load_horizon_manifest,
    parse_factor_id,
)
from .dual_theme_scope_alpha import evaluate_scope_alpha_streaming
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


def _annotate_result(
    result: AlphaResult,
    horizon_name: str,
    contract: LabelContract,
) -> AlphaResult:
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


def _combine_alpha_results(results: Iterable[AlphaResult]) -> AlphaResult:
    rows = tuple(results)
    fields = (
        "metrics",
        "ic_series",
        "daily_ic",
        "quantile_returns",
        "portfolio_returns",
        "stability",
        "score_correlation",
    )
    payload: dict[str, pd.DataFrame] = {}
    for field in fields:
        frames = [getattr(row, field) for row in rows if not getattr(row, field).empty]
        payload[field] = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    governance = {
        "scope_governance": {
            str(index): row.governance for index, row in enumerate(rows)
        }
    }
    return AlphaResult(**payload, governance=governance)


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
            "alpha_semantics",
        )
        if column in metrics.columns
    ]
    value_columns = [
        column
        for column in ("mean_spearman_ic", "net_mean_5bps")
        if column in metrics.columns
    ]
    if not keys or not value_columns or "variant_id" not in metrics.columns:
        return pd.DataFrame()
    pivot = metrics.pivot_table(
        index=keys,
        columns="variant_id",
        values=value_columns,
        aggfunc="first",
    )
    pivot.columns = [f"{metric}__{variant}" for metric, variant in pivot.columns]
    pivot = pivot.reset_index()
    graph_ic = "mean_spearman_ic__graph_forward"
    node_ic = "mean_spearman_ic__node_baseline"
    placebo_ic = "mean_spearman_ic__graph_reverse_placebo"
    if graph_ic in pivot and node_ic in pivot:
        pivot["abs_ic_increment_vs_node"] = (
            pivot[graph_ic].abs() - pivot[node_ic].abs()
        )
    if graph_ic in pivot and placebo_ic in pivot:
        pivot["abs_ic_increment_vs_reverse_placebo"] = (
            pivot[graph_ic].abs() - pivot[placebo_ic].abs()
        )
    graph_net = "net_mean_5bps__graph_forward"
    node_net = "net_mean_5bps__node_baseline"
    placebo_net = "net_mean_5bps__graph_reverse_placebo"
    if graph_net in pivot and node_net in pivot:
        pivot["net_5bps_increment_vs_node"] = pivot[graph_net] - pivot[node_net]
    if graph_net in pivot and placebo_net in pivot:
        pivot["net_5bps_increment_vs_reverse_placebo"] = (
            pivot[graph_net] - pivot[placebo_net]
        )
    return pivot


def _scope_summary(metrics: pd.DataFrame, matched: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    keys = [
        column
        for column in (
            "scope",
            "theme_family",
            "horizon",
            "horizon_minutes",
            "alpha_semantics",
        )
        if column in metrics
    ]
    variant = metrics.get(
        "variant_id",
        pd.Series(index=metrics.index, dtype=str),
    )
    graph = metrics[variant == "graph_forward"].copy()
    if graph.empty:
        return pd.DataFrame()
    identity_columns = [
        column
        for column in ("factor_id", "layer_id", "scale_minutes", "variant_id")
        if column in graph.columns
    ]
    graph["_factor_identity"] = graph[identity_columns].astype(str).agg("|".join, axis=1)
    summary = (
        graph.groupby(keys, observed=True, dropna=False)
        .agg(
            factor_count=("_factor_identity", "nunique"),
            mean_abs_ic=(
                "mean_spearman_ic",
                lambda values: float(
                    pd.to_numeric(values, errors="coerce").abs().mean()
                ),
            ),
            median_abs_ic=(
                "mean_spearman_ic",
                lambda values: float(
                    pd.to_numeric(values, errors="coerce").abs().median()
                ),
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
            extra = (
                matched.groupby(keys, observed=True, dropna=False)[increments]
                .mean()
                .reset_index()
            )
            summary = summary.merge(
                extra,
                on=keys,
                how="left",
                validate="one_to_one",
            )
    return summary


def _cross_scope_comparison(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    graph = metrics[metrics.get("variant_id") == "graph_forward"].copy()
    if graph.empty:
        return pd.DataFrame()
    index = [
        column
        for column in (
            "theme_family",
            "layer_id",
            "scale_minutes",
            "horizon",
            "horizon_minutes",
        )
        if column in graph.columns
    ]
    values = [
        column
        for column in ("mean_spearman_ic", "net_mean_5bps")
        if column in graph.columns
    ]
    if not index or not values or "scope" not in graph.columns:
        return pd.DataFrame()
    pivot = graph.pivot_table(index=index, columns="scope", values=values, aggfunc="first")
    pivot.columns = [f"{metric}__{scope}" for metric, scope in pivot.columns]
    pivot = pivot.reset_index()
    for metric in values:
        global_column = f"{metric}__global"
        for scope in ("within_theme", "inter_theme"):
            scoped = f"{metric}__{scope}"
            if scoped in pivot and global_column in pivot:
                pivot[f"{metric}_increment__{scope}_vs_global"] = (
                    pivot[scoped] - pivot[global_column]
                )
    return pivot


def _scope_expected_factors(export_manifest: dict[str, object]) -> dict[str, int]:
    rows = export_manifest.get("output_files")
    if not isinstance(rows, list):
        return {}
    keys: dict[str, set[tuple[str, str, int, str]]] = {}
    for row in rows:
        if not isinstance(row, dict) or int(row.get("rows", 0)) <= 0:
            continue
        scope = str(row.get("scope") or "")
        keys.setdefault(scope, set()).add(
            (
                str(row.get("theme_family") or ""),
                str(row.get("layer_id") or ""),
                int(row.get("scale_minutes") or 0),
                str(row.get("variant_id") or ""),
            )
        )
    return {scope: len(values) for scope, values in keys.items()}


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
    min_theme_size: int = 5,
    min_theme_cross_section: int = 5,
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
    output.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)
    (output / "_PARTIAL").unlink(missing_ok=True)
    success = signals / "_SUCCESS"
    export_manifest_path = signals / "export_manifest.json"
    if not success.exists() or not export_manifest_path.exists():
        raise FileNotFoundError(f"Dual-theme signals are incomplete: {signals}")
    export_manifest = json.loads(export_manifest_path.read_text(encoding="utf-8"))
    export_version = str(export_manifest.get("export_version") or "")
    if export_version != DUAL_THEME_EXPORT_VERSION and not allow_legacy_signals:
        raise ValueError(
            f"Dual-theme scope Alpha requires export_version={DUAL_THEME_EXPORT_VERSION}; "
            f"observed={export_version!r}. Re-export signals instead of mixing semantics."
        )
    expected_factors = int(export_manifest.get("factor_count") or 0)
    if expected_factors <= 0:
        raise ValueError("Export manifest has no governed factors")
    expected_by_scope = _scope_expected_factors(export_manifest)
    horizon_specs = load_horizon_manifest(horizon_manifest)
    join_key_list = list(join_keys)
    control_column_list = list(control_columns)
    metadata = None
    metadata_profile = None
    dimensions = list(slice_dimensions)
    metadata_file: Path | None = None
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
    all_metrics: list[pd.DataFrame] = []
    all_scope_metrics: dict[str, list[pd.DataFrame]] = {
        "global": [],
        "within_theme": [],
        "inter_theme": [],
    }
    horizon_summaries: list[dict[str, object]] = []
    for spec in horizon_specs:
        contract = LabelContract.from_json(spec.label_contract)
        global_result = evaluate_alpha_streaming(
            signals,
            spec.labels,
            label_contract=contract,
            join_keys=join_key_list,
            metadata=metadata,
            metadata_signal_id=metadata_signal_id,
            metadata_id=(
                metadata_profile.id_column if metadata_profile else metadata_id
            ),
            slice_columns=dimensions,
            score_column=score_column,
            symbol_column=symbol_column,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=control_column_list,
            annualization_factor=annualization_factor,
            resource_budget=resource_budget,
            allow_legacy_signals=allow_legacy_signals,
            correlation_sample_modulus=correlation_sample_modulus,
            signal_filter="CAST(scope AS VARCHAR)='global'",
        )
        within_result = evaluate_scope_alpha_streaming(
            signals,
            spec.labels,
            scope="within_theme",
            label_contract=contract,
            join_keys=join_key_list,
            metadata=metadata,
            metadata_signal_id=metadata_signal_id,
            metadata_id=(
                metadata_profile.id_column if metadata_profile else metadata_id
            ),
            slice_columns=dimensions,
            score_column=score_column,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            min_theme_size=min_theme_size,
            min_theme_cross_section=min_theme_cross_section,
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=control_column_list,
            annualization_factor=annualization_factor,
            resource_budget=resource_budget,
            allow_legacy_signals=allow_legacy_signals,
        )
        inter_result = evaluate_scope_alpha_streaming(
            signals,
            spec.labels,
            scope="inter_theme",
            label_contract=contract,
            join_keys=join_key_list,
            score_column=score_column,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            min_theme_size=min_theme_size,
            min_theme_cross_section=min_theme_cross_section,
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=control_column_list,
            annualization_factor=annualization_factor,
            resource_budget=resource_budget,
            allow_legacy_signals=allow_legacy_signals,
        )
        global_result = _annotate_result(global_result, spec.name, contract)
        within_result = _annotate_result(within_result, spec.name, contract)
        inter_result = _annotate_result(inter_result, spec.name, contract)
        for frame in (
            global_result.metrics,
            global_result.ic_series,
            global_result.daily_ic,
            global_result.quantile_returns,
            global_result.portfolio_returns,
            global_result.stability,
        ):
            if not frame.empty:
                frame["alpha_semantics"] = "stock_cross_section"
        result = _combine_alpha_results((global_result, within_result, inter_result))
        observed_by_scope: dict[str, int] = {}
        for scope, scope_result in (
            ("global", global_result),
            ("within_theme", within_result),
            ("inter_theme", inter_result),
        ):
            identity_columns = [
                column
                for column in ("factor_id", "layer_id", "scale_minutes", "variant_id")
                if column in scope_result.metrics.columns
            ]
            observed_by_scope[scope] = (
                int(scope_result.metrics[identity_columns].drop_duplicates().shape[0])
                if identity_columns and not scope_result.metrics.empty
                else 0
            )
            if not scope_result.metrics.empty:
                all_scope_metrics[scope].append(scope_result.metrics.copy())
        observed = sum(observed_by_scope.values())
        complete = all(
            observed_by_scope.get(scope, 0) == expected
            for scope, expected in expected_by_scope.items()
        ) and observed == expected_factors
        if not complete and not allow_partial:
            raise ValueError(
                f"Horizon {spec.name} expected {expected_factors} factors {expected_by_scope}, "
                f"observed {observed} {observed_by_scope}; use --allow-partial only for an explicit diagnostic"
            )
        batch_status = {
            "batch_id": DUAL_THEME_BATCH_ID,
            "expected_contracts": expected_factors,
            "observed_contracts": observed,
            "expected_by_scope": expected_by_scope,
            "observed_by_scope": observed_by_scope,
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
            **({"metadata": file_record(metadata_file)} if metadata_file else {}),
        }
        manifest_inputs = [
            file_record(export_manifest_path),
            file_record(spec.label_contract),
            *directory_parquet_records(spec.labels),
        ]
        if metadata_file is not None:
            manifest_inputs.append(file_record(metadata_file))
        run_manifest = implementation_manifest(
            operation="dual_theme_scope_alpha_report",
            parameters={
                "version": DUAL_THEME_ALPHA_VERSION,
                "horizon": spec.name,
                "label_contract": contract.as_dict(),
                "join_keys": join_key_list,
                "control_columns": control_column_list,
                "expected_factors": expected_factors,
                "expected_by_scope": expected_by_scope,
                "min_theme_size": min_theme_size,
                "min_theme_cross_section": min_theme_cross_section,
                "scope_semantics": {
                    "global": "stock_cross_section",
                    "within_theme": "stock_within_theme_neutral",
                    "inter_theme": "theme_portfolio_weighted_member_return",
                },
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
                "observed_by_scope": observed_by_scope,
                "expected_by_scope": expected_by_scope,
                "complete": complete,
            }
        )
    combined = (
        pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()
    )
    matched = _matched_variant_comparison(combined)
    scope_summary = _scope_summary(combined, matched)
    cross_scope = _cross_scope_comparison(combined)
    atomic_write_frame(combined, output / "all_horizons_alpha_metrics.csv")
    for scope, frames in all_scope_metrics.items():
        frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        atomic_write_frame(frame, output / f"{scope}_alpha_metrics.csv")
    atomic_write_frame(matched, output / "matched_variant_comparison.csv")
    atomic_write_frame(scope_summary, output / "scope_family_horizon_summary.csv")
    atomic_write_frame(cross_scope, output / "cross_scope_comparison.csv")
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
        ranking = ranking.sort_values(
            ranking_columns,
            ascending=False,
            na_position="last",
        )
    atomic_write_frame(ranking, output / "ranking.csv")
    complete_all = bool(horizon_summaries) and all(
        bool(row["complete"]) for row in horizon_summaries
    )
    summary = {
        "version": DUAL_THEME_ALPHA_VERSION,
        "batch_id": DUAL_THEME_BATCH_ID,
        "signals_root": str(signals),
        "export_manifest_sha256": sha256_file(export_manifest_path),
        "export_version": export_version,
        "gff_campaign_version": export_manifest.get("gff_campaign_version"),
        "expected_factor_count_per_horizon": expected_factors,
        "expected_by_scope": expected_by_scope,
        "horizons": horizon_summaries,
        "metric_rows": int(len(combined)),
        "matched_rows": int(len(matched)),
        "scope_summary_rows": int(len(scope_summary)),
        "cross_scope_rows": int(len(cross_scope)),
        "complete": complete_all,
        "scope_semantics": {
            "global": "stock_cross_section",
            "within_theme": "theme_mean_return_removed_and_score_residualized_inside_theme",
            "inter_theme": "membership_weighted_theme_portfolio_return_ranked_across_themes",
        },
        "cost_boundary": (
            "Inter-theme costs are theme-notional diagnostics. Production implementation must add constituent execution and membership migration costs."
        ),
        "one_day_inference_warning": (
            "One-session results are diagnostic and cannot satisfy the >=20-date governance gate."
        ),
    }
    atomic_write_json(output / "summary.json", summary)
    lines = [
        "# GraphAlphaLab dual-theme, scope-correct multi-horizon Alpha report",
        "",
        f"- Signals: `{signals}`",
        f"- Expected factor identities per horizon: {expected_factors}",
        f"- Horizons: {', '.join(spec.name for spec in horizon_specs)}",
        "- Identity: `theme_family × scope × layer × scale × variant × horizon`",
        "- Global: ordinary stock cross-sectional Alpha.",
        "- Within-theme: stock score is neutralized inside each PIT theme; the theme mean forward return is removed before IC and portfolio evaluation.",
        "- Inter-theme: stock labels are aggregated into membership-weighted theme portfolio returns; ranking occurs across themes, never across duplicated member-stock scores.",
        "- Reverse-edge placebo and node baseline remain matched inside each scope.",
        "- Inter-theme transaction costs are diagnostic theme-notional costs, not a constituent execution backtest.",
        "",
        "Key files: `global_alpha_metrics.csv`, `within_theme_alpha_metrics.csv`, `inter_theme_alpha_metrics.csv`, `cross_scope_comparison.csv`, `scope_family_horizon_summary.csv`, `matched_variant_comparison.csv`, `ranking.csv`.",
    ]
    atomic_write_text(output / "REPORT.md", "\n".join(lines) + "\n")
    marker = output / ("_SUCCESS" if complete_all else "_PARTIAL")
    atomic_write_json(marker, {"complete": complete_all})
    return output
