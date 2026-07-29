from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from . import forward_volatility as core
from .governance import (
    atomic_write_frame,
    atomic_write_json,
    atomic_write_text,
    file_record,
    sha256_file,
    sha256_json,
)


REPORT_PACK_VERSION = "GAL_FORWARD_VOLATILITY_REPORT_PACK_V2"
IDENTITY_COLUMNS = (
    "batch_id",
    "factor_id",
    "scope",
    "theme_family",
    "layer_id",
    "scale_minutes",
    "variant_id",
    "target_kind",
    "horizon_minutes",
    "horizon_name",
)
PREDICTIVE_COLUMNS = (
    "mean_spearman_ic",
    "median_spearman_ic",
    "spearman_ic_std",
    "spearman_icir",
    "snapshot_ic_positive_rate",
    "daily_ic_positive_rate",
    "daily_ic_negative_rate",
    "daily_ic_sign_consistency",
    "spearman_ic_tstat_daily",
    "spearman_ic_pvalue_daily",
    "fdr_qvalue",
    "fdr_pass",
    "mean_pearson_ic_daily",
    "raw_top_minus_bottom_mean",
    "quantile_monotonicity",
)
COVERAGE_COLUMNS = (
    "observations",
    "decision_count",
    "date_count",
    "symbol_count",
    "label_mean",
    "label_std",
    "score_mean",
    "score_std",
    "sample_sufficient",
)
GOVERNANCE_COLUMNS = (
    "expected_direction",
    "direction_source",
    "direction_predeclared",
    "annualization_valid",
    "direction_consistent",
    "governance_ready",
    "research_status",
)
GENERIC_EVALUATOR_COLUMNS = (
    "oriented_long_short_mean",
    "oriented_long_short_std",
    "oriented_long_short_hit_rate",
    "oriented_long_short_tstat_daily",
    "annualized_sharpe",
    "daily_diagnostic_sharpe",
    "max_drawdown",
    "var_5pct",
    "cvar_5pct",
    "skew",
    "kurtosis",
    "mean_turnover",
    "net_mean_0bps",
    "net_mean_1bps",
    "net_mean_2bps",
    "net_mean_5bps",
    "net_mean_10bps",
    "cost_survives_5bps",
)
RAW_EVIDENCE_FILES = (
    "alpha_metrics.csv",
    "daily_ic.csv",
    "quantile_returns.csv",
    "stability_slices.csv",
    "ic_series.csv",
    "portfolio_returns.csv",
    "score_correlation.csv",
    "summary.json",
    "run_manifest.json",
    "horizon_checkpoint.json",
    "_SUCCESS",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe(value: object) -> str:
    text = str(value if value is not None else "unknown").strip()
    return "".join(character if character.isalnum() or character in "._-" else "_" for character in text)


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_frame(frame, path)


def _row_count(path: Path) -> int | None:
    try:
        if path.suffix.lower() in {".parquet", ".pq"}:
            return int(pq.ParquetFile(path).metadata.num_rows)
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8") as handle:
                return max(0, sum(1 for _ in handle) - 1)
    except Exception:
        return None
    return None


def _artifact_record(path: Path, root: Path) -> dict[str, object]:
    record = file_record(path)
    record["path"] = path.relative_to(root).as_posix()
    rows = _row_count(path)
    if rows is not None:
        record["rows"] = rows
    return record


def _record_matches(root: Path, record: object) -> bool:
    if not isinstance(record, dict) or not record.get("path"):
        return False
    path = root / str(record["path"])
    if not path.exists():
        return False
    try:
        if int(record.get("size_bytes", -1)) != int(path.stat().st_size):
            return False
        if str(record.get("sha256")) != sha256_file(path):
            return False
        expected_rows = record.get("rows")
        return expected_rows is None or int(expected_rows) == int(_row_count(path) or 0)
    except Exception:
        return False


def _records_match(root: Path, records: object) -> bool:
    return isinstance(records, list) and bool(records) and all(
        _record_matches(root, record) for record in records
    )


def _horizon_name(target: str, horizon: int) -> str:
    return f"{target}_{int(horizon)}m"


def _annotate(frame: pd.DataFrame, *, target: str, horizon: int) -> pd.DataFrame:
    result = frame.copy()
    result["target_kind"] = str(target)
    result["horizon_minutes"] = int(horizon)
    result["horizon_name"] = _horizon_name(target, horizon)
    return result


def _raw_inputs(
    raw_root: Path,
    config: core.ForwardVolatilityConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[dict[str, object]]]:
    metric_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    quantile_frames: list[pd.DataFrame] = []
    stability_frames: list[pd.DataFrame] = []
    catalog: list[dict[str, object]] = []
    sources: list[dict[str, object]] = []
    for horizon_value in config.horizons:
        horizon = int(horizon_value)
        for target in config.targets:
            name = _horizon_name(str(target), horizon)
            root = raw_root / f"horizon={name}"
            required = ("_SUCCESS", "alpha_metrics.csv", "daily_ic.csv", "quantile_returns.csv", "stability_slices.csv")
            missing = [filename for filename in required if not (root / filename).exists()]
            if missing:
                raise FileNotFoundError(f"Incomplete volatility horizon {name}: {missing}")
            metric_frames.append(_annotate(_read_csv(root / "alpha_metrics.csv"), target=str(target), horizon=horizon))
            daily_frames.append(_annotate(_read_csv(root / "daily_ic.csv"), target=str(target), horizon=horizon))
            quantile_frames.append(_annotate(_read_csv(root / "quantile_returns.csv"), target=str(target), horizon=horizon))
            stability_frames.append(_annotate(_read_csv(root / "stability_slices.csv"), target=str(target), horizon=horizon))
            for filename in RAW_EVIDENCE_FILES:
                path = root / filename
                if not path.exists():
                    continue
                record = file_record(path)
                sources.append(record)
                catalog.append(
                    {
                        "target_kind": str(target),
                        "horizon_minutes": horizon,
                        "horizon_name": name,
                        "evidence_file": filename,
                        "path": str(path),
                        "size_bytes": int(record["size_bytes"]),
                        "sha256": str(record["sha256"]),
                        "rows": _row_count(path),
                        "copied_into_report_pack": filename in required,
                        "evidence_role": (
                            "snapshot_level_raw_evidence"
                            if filename == "ic_series.csv"
                            else "generic_return_evaluator_diagnostic_only"
                            if filename == "portfolio_returns.csv"
                            else "redundancy_evidence"
                            if filename == "score_correlation.csv"
                            else "report_source"
                        ),
                    }
                )
    return (
        pd.concat(metric_frames, ignore_index=True) if metric_frames else pd.DataFrame(),
        pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame(),
        pd.concat(quantile_frames, ignore_index=True) if quantile_frames else pd.DataFrame(),
        pd.concat(stability_frames, ignore_index=True) if stability_frames else pd.DataFrame(),
        pd.DataFrame(catalog),
        sources,
    )


def _prediction_status(metrics: pd.DataFrame) -> pd.DataFrame:
    result = metrics.copy()
    if "raw_top_minus_bottom_mean" in result:
        result = result.rename(columns={"raw_top_minus_bottom_mean": "top_bottom_target_spread"})
    sample = result.get("sample_sufficient", pd.Series(False, index=result.index)).fillna(False).astype(bool)
    fdr = result.get("fdr_pass", pd.Series(False, index=result.index)).fillna(False).astype(bool)
    ic = pd.to_numeric(result.get("mean_spearman_ic"), errors="coerce").abs()
    sign = pd.to_numeric(result.get("daily_ic_sign_consistency"), errors="coerce")
    result["prediction_status"] = np.select(
        [sample & fdr & (ic >= 0.01) & (sign >= 0.55), sample & (ic >= 0.01)],
        ["predictive_candidate", "needs_falsification"],
        default="insufficient_or_rejected",
    )
    result["evaluation_family"] = "volatility_diagnostic"
    return result


def _match_columns(frame: pd.DataFrame) -> list[str]:
    return [
        column
        for column in (
            "scope",
            "theme_family",
            "layer_id",
            "scale_minutes",
            "target_kind",
            "horizon_minutes",
            "horizon_name",
        )
        if column in frame.columns
    ]


def _non_confirmation_reasons(row: dict[str, object]) -> str:
    reasons: list[str] = []
    if not bool(row.get("graph_present")):
        reasons.append("graph_forward_missing")
    if not bool(row.get("node_present")):
        reasons.append("node_baseline_missing")
    if not bool(row.get("reverse_present")):
        reasons.append("reverse_placebo_missing")
    if not bool(row.get("graph_sample_sufficient")):
        reasons.append("sample_insufficient")
    qvalue = pd.to_numeric(pd.Series([row.get("graph_fdr_qvalue")]), errors="coerce").iloc[0]
    if pd.isna(qvalue) or float(qvalue) > 0.05:
        reasons.append("fdr_not_passed")
    consistency = pd.to_numeric(pd.Series([row.get("graph_sign_consistency")]), errors="coerce").iloc[0]
    if pd.isna(consistency) or float(consistency) < 0.55:
        reasons.append("daily_sign_consistency_below_0.55")
    node_increment = pd.to_numeric(pd.Series([row.get("graph_minus_node_abs_ic")]), errors="coerce").iloc[0]
    if pd.isna(node_increment) or float(node_increment) <= 0:
        reasons.append("graph_not_stronger_than_node")
    reverse_increment = pd.to_numeric(pd.Series([row.get("graph_minus_reverse_abs_ic")]), errors="coerce").iloc[0]
    if pd.isna(reverse_increment) or float(reverse_increment) <= 0.002:
        reasons.append("graph_not_clearly_stronger_than_reverse")
    return "|".join(dict.fromkeys(reasons)) if reasons else "confirmed"


def _matched_variants(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty or "variant_id" not in metrics:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    keys = _match_columns(metrics)
    for values, group in metrics.groupby(keys, observed=True, dropna=False, sort=True):
        key_values = values if isinstance(values, tuple) else (values,)
        row = dict(zip(keys, key_values))
        variants = {
            str(record["variant_id"]): record
            for _, record in group.drop_duplicates("variant_id", keep="last").iterrows()
        }
        graph = variants.get("graph_forward")
        node = variants.get("node_baseline")
        reverse = variants.get("graph_reverse_placebo")
        row.update(
            {
                "graph_present": graph is not None,
                "node_present": node is not None,
                "reverse_present": reverse is not None,
                "graph_mean_spearman_ic": graph.get("mean_spearman_ic") if graph is not None else np.nan,
                "node_mean_spearman_ic": node.get("mean_spearman_ic") if node is not None else np.nan,
                "reverse_mean_spearman_ic": reverse.get("mean_spearman_ic") if reverse is not None else np.nan,
                "graph_fdr_qvalue": graph.get("fdr_qvalue") if graph is not None else np.nan,
                "graph_sign_consistency": graph.get("daily_ic_sign_consistency") if graph is not None else np.nan,
                "graph_sample_sufficient": bool(graph.get("sample_sufficient", False)) if graph is not None else False,
                "graph_target_spread": graph.get("top_bottom_target_spread") if graph is not None else np.nan,
                "graph_prediction_status": graph.get("prediction_status") if graph is not None else "missing",
            }
        )
        graph_ic = pd.to_numeric(pd.Series([row["graph_mean_spearman_ic"]]), errors="coerce").iloc[0]
        node_ic = pd.to_numeric(pd.Series([row["node_mean_spearman_ic"]]), errors="coerce").iloc[0]
        reverse_ic = pd.to_numeric(pd.Series([row["reverse_mean_spearman_ic"]]), errors="coerce").iloc[0]
        row["graph_minus_node_abs_ic"] = abs(float(graph_ic)) - abs(float(node_ic)) if pd.notna(graph_ic) and pd.notna(node_ic) else np.nan
        row["graph_minus_reverse_abs_ic"] = abs(float(graph_ic)) - abs(float(reverse_ic)) if pd.notna(graph_ic) and pd.notna(reverse_ic) else np.nan
        row["non_confirmation_reasons"] = _non_confirmation_reasons(row)
        row["graph_incremental_confirmed"] = row["non_confirmation_reasons"] == "confirmed"
        rows.append(row)
    return pd.DataFrame(rows)


def _identity(frame: pd.DataFrame) -> list[str]:
    return [column for column in IDENTITY_COLUMNS if column in frame.columns]


def _selected(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    keep = list(dict.fromkeys([*_identity(frame), *[column for column in columns if column in frame.columns]]))
    return frame[keep].copy() if keep else frame.copy()


def _write_shards(
    frame: pd.DataFrame,
    root: Path,
    *,
    filename: str,
    sort_columns: Iterable[str] = (),
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if frame.empty:
        return pd.DataFrame(columns=["target_kind", "horizon_minutes", "scope", "rows", "relative_path"])
    keys = [column for column in ("target_kind", "horizon_minutes", "scope") if column in frame.columns]
    for values, shard in frame.groupby(keys, observed=True, dropna=False, sort=True):
        key_values = values if isinstance(values, tuple) else (values,)
        identity = dict(zip(keys, key_values))
        target = root
        for key in keys:
            target = target / f"{key}={_safe(identity[key])}"
        destination = target / filename
        order = [column for column in sort_columns if column in shard.columns]
        _write_frame(shard.sort_values(order) if order else shard, destination)
        rows.append(
            {
                **identity,
                "rows": int(len(shard)),
                "columns": int(len(shard.columns)),
                "relative_path": destination.relative_to(root.parent).as_posix(),
            }
        )
    return pd.DataFrame(rows)


def _label_diagnostics(
    output_root: Path,
    config: core.ForwardVolatilityConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[dict[str, object]]]:
    coverage_rows: list[dict[str, object]] = []
    distribution_rows: list[dict[str, object]] = []
    sources: list[dict[str, object]] = []
    for path in sorted((output_root / "labels").glob("trade_date=*/data.parquet")):
        sources.append(file_record(path))
        frame = pd.read_parquet(path)
        if frame.empty:
            continue
        frame["decision_time"] = pd.to_datetime(frame["decision_time"], utc=True, errors="coerce")
        frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True, errors="coerce")
        frame["label_available_time"] = pd.to_datetime(frame["label_available_time"], utc=True, errors="coerce")
        frame["availability_delay_seconds"] = (frame["label_available_time"] - frame["exit_time"]).dt.total_seconds()
        trade_date = str(frame["trade_date"].iloc[0])
        for horizon_value, group in frame.groupby("horizon_minutes", observed=True, sort=True):
            horizon = int(horizon_value)
            coverage_rows.append(
                {
                    "trade_date": trade_date,
                    "horizon_minutes": horizon,
                    "rows": int(len(group)),
                    "symbol_count": int(group["symbol_id"].nunique()),
                    "decision_count": int(group["decision_time"].nunique()),
                    "first_decision_time": group["decision_time"].min(),
                    "last_decision_time": group["decision_time"].max(),
                    "future_observation_count_mean": pd.to_numeric(group.get("future_observation_count"), errors="coerce").mean(),
                    "future_observation_count_min": pd.to_numeric(group.get("future_observation_count"), errors="coerce").min(),
                    "past_window_complete_rate": group.get("past_window_complete", pd.Series(False, index=group.index)).fillna(False).astype(bool).mean(),
                    "availability_delay_seconds_mean": pd.to_numeric(group["availability_delay_seconds"], errors="coerce").mean(),
                    "availability_delay_seconds_max": pd.to_numeric(group["availability_delay_seconds"], errors="coerce").max(),
                }
            )
            for target in config.targets:
                if target not in group.columns:
                    continue
                values = pd.to_numeric(group[target], errors="coerce")
                clean = values.dropna()
                distribution_rows.append(
                    {
                        "trade_date": trade_date,
                        "horizon_minutes": horizon,
                        "target_kind": str(target),
                        "rows": int(len(values)),
                        "non_null_rows": int(len(clean)),
                        "missing_rate": float(values.isna().mean()),
                        "mean": float(clean.mean()) if len(clean) else np.nan,
                        "std": float(clean.std(ddof=1)) if len(clean) > 1 else np.nan,
                        "p01": float(clean.quantile(0.01)) if len(clean) else np.nan,
                        "p10": float(clean.quantile(0.10)) if len(clean) else np.nan,
                        "median": float(clean.median()) if len(clean) else np.nan,
                        "p90": float(clean.quantile(0.90)) if len(clean) else np.nan,
                        "p99": float(clean.quantile(0.99)) if len(clean) else np.nan,
                    }
                )
    coverage = pd.DataFrame(coverage_rows)
    by_date = pd.DataFrame(distribution_rows)
    if by_date.empty:
        summary = pd.DataFrame()
    else:
        summary = (
            by_date.groupby(["target_kind", "horizon_minutes"], observed=True, dropna=False)
            .agg(
                date_count=("trade_date", "nunique"),
                rows=("rows", "sum"),
                non_null_rows=("non_null_rows", "sum"),
                mean_missing_rate=("missing_rate", "mean"),
                mean_of_daily_mean=("mean", "mean"),
                median_of_daily_median=("median", "median"),
                mean_daily_std=("std", "mean"),
                minimum_daily_p01=("p01", "min"),
                maximum_daily_p99=("p99", "max"),
            )
            .reset_index()
        )
    return coverage, by_date, summary, sources


def _summary_tables(metrics: pd.DataFrame, comparisons: pd.DataFrame) -> dict[str, pd.DataFrame]:
    graph = metrics[metrics.get("variant_id", pd.Series(index=metrics.index, dtype=str)).astype(str) == "graph_forward"].copy()
    summary_keys = [column for column in ("target_kind", "horizon_minutes", "scope", "theme_family") if column in graph.columns]
    if graph.empty:
        broad = pd.DataFrame()
    else:
        broad = (
            graph.groupby(summary_keys, observed=True, dropna=False)
            .agg(
                factor_rows=("factor_id", "size"),
                unique_layers=("layer_id", "nunique"),
                mean_abs_ic=("mean_spearman_ic", lambda values: pd.to_numeric(values, errors="coerce").abs().mean()),
                median_abs_ic=("mean_spearman_ic", lambda values: pd.to_numeric(values, errors="coerce").abs().median()),
                mean_icir=("spearman_icir", "mean"),
                mean_sign_consistency=("daily_ic_sign_consistency", "mean"),
                fdr_pass_rate=("fdr_pass", lambda values: values.fillna(False).astype(bool).mean()),
                sample_sufficient_rate=("sample_sufficient", lambda values: values.fillna(False).astype(bool).mean()),
            )
            .reset_index()
        )
    layer_keys = [column for column in ("layer_id", "scale_minutes", "target_kind", "horizon_minutes", "scope") if column in graph.columns]
    layer = (
        graph.groupby(layer_keys, observed=True, dropna=False)
        .agg(
            metric_rows=("factor_id", "size"),
            mean_spearman_ic=("mean_spearman_ic", "mean"),
            mean_abs_ic=("mean_spearman_ic", lambda values: pd.to_numeric(values, errors="coerce").abs().mean()),
            mean_icir=("spearman_icir", "mean"),
            mean_sign_consistency=("daily_ic_sign_consistency", "mean"),
            fdr_pass_rate=("fdr_pass", lambda values: values.fillna(False).astype(bool).mean()),
        )
        .reset_index()
        if not graph.empty
        else pd.DataFrame()
    status_keys = [column for column in ("target_kind", "horizon_minutes", "scope", "variant_id", "prediction_status") if column in metrics.columns]
    status = metrics.groupby(status_keys, observed=True, dropna=False).size().rename("factor_rows").reset_index() if status_keys else pd.DataFrame()
    reason_rows: list[dict[str, object]] = []
    if not comparisons.empty:
        for _, record in comparisons.iterrows():
            reasons = str(record.get("non_confirmation_reasons", "")).split("|")
            for reason in reasons:
                if not reason or reason == "confirmed":
                    continue
                reason_rows.append(
                    {
                        "target_kind": record.get("target_kind"),
                        "horizon_minutes": record.get("horizon_minutes"),
                        "scope": record.get("scope"),
                        "reason": reason,
                    }
                )
    reason_counts = (
        pd.DataFrame(reason_rows)
        .groupby(["target_kind", "horizon_minutes", "scope", "reason"], observed=True, dropna=False)
        .size()
        .rename("matched_groups")
        .reset_index()
        if reason_rows
        else pd.DataFrame(columns=["target_kind", "horizon_minutes", "scope", "reason", "matched_groups"])
    return {
        "target_horizon_scope_summary": broad,
        "layer_target_horizon_summary": layer,
        "factor_status_counts": status,
        "non_confirmation_reason_counts": reason_counts,
    }


def _top_bottom(comparisons: pd.DataFrame, count: int = 20) -> pd.DataFrame:
    if comparisons.empty:
        return comparisons
    rows: list[pd.DataFrame] = []
    keys = [column for column in ("target_kind", "horizon_minutes", "scope") if column in comparisons.columns]
    for _, group in comparisons.groupby(keys, observed=True, dropna=False):
        ordered = group.sort_values(
            ["graph_minus_reverse_abs_ic", "graph_minus_node_abs_ic"],
            ascending=False,
            na_position="last",
        )
        strongest = ordered.head(count).copy()
        strongest["selection_side"] = "strongest_increment"
        weakest = ordered.tail(count).copy()
        weakest["selection_side"] = "weakest_increment"
        rows.extend((strongest, weakest))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _target_reports(metrics: pd.DataFrame, comparisons: pd.DataFrame, root: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for target in sorted(metrics.get("target_kind", pd.Series(dtype=str)).dropna().astype(str).unique()):
        target_metrics = metrics[metrics["target_kind"].astype(str) == target].copy()
        target_comparison = comparisons[comparisons["target_kind"].astype(str) == target].copy() if not comparisons.empty else pd.DataFrame()
        target_root = root / f"target={_safe(target)}"
        target_root.mkdir(parents=True, exist_ok=True)
        summary = (
            target_metrics.groupby([column for column in ("horizon_minutes", "scope", "variant_id", "prediction_status") if column in target_metrics.columns], observed=True, dropna=False)
            .agg(
                factor_rows=("factor_id", "size"),
                mean_abs_ic=("mean_spearman_ic", lambda values: pd.to_numeric(values, errors="coerce").abs().mean()),
                mean_icir=("spearman_icir", "mean"),
                mean_sign_consistency=("daily_ic_sign_consistency", "mean"),
            )
            .reset_index()
        )
        _write_frame(summary, target_root / "summary.csv")
        _write_frame(target_comparison, target_root / "matched_variant_comparison.csv")
        strongest = target_comparison.sort_values("graph_minus_reverse_abs_ic", ascending=False, na_position="last").head(10) if not target_comparison.empty else pd.DataFrame()
        weakest = target_comparison.sort_values("graph_minus_reverse_abs_ic", ascending=True, na_position="last").head(10) if not target_comparison.empty else pd.DataFrame()
        columns = [column for column in ("scope", "theme_family", "layer_id", "scale_minutes", "horizon_minutes", "graph_mean_spearman_ic", "graph_minus_node_abs_ic", "graph_minus_reverse_abs_ic", "graph_fdr_qvalue", "graph_incremental_confirmed", "non_confirmation_reasons") if column in target_comparison.columns]
        lines = [
            f"# Target report: {target}",
            "",
            f"- Complete factor rows: {len(target_metrics)}",
            f"- Matched graph/node/reverse groups: {len(target_comparison)}",
            f"- Confirmed graph-incremental groups: {int(target_comparison.get('graph_incremental_confirmed', pd.Series(dtype=bool)).sum())}",
            "",
            "All significant, non-significant, insufficient and rejected factors remain in the detailed shards.",
            "",
            "## Strongest graph increments",
            "",
        ]
        for table, title in ((strongest, None), (weakest, "## Weakest / failed graph increments")):
            if title:
                lines.extend(["", title, ""])
            if table.empty:
                lines.append("No matched rows.")
            else:
                lines.append("| " + " | ".join(columns) + " |")
                lines.append("|" + "|".join("---" for _ in columns) + "|")
                for values in table[columns].itertuples(index=False, name=None):
                    lines.append("| " + " | ".join(str(value) for value in values) + " |")
        atomic_write_text(target_root / "REPORT.md", "\n".join(lines) + "\n")
        rows.append(
            {
                "target_kind": target,
                "factor_rows": int(len(target_metrics)),
                "matched_groups": int(len(target_comparison)),
                "confirmed_groups": int(target_comparison.get("graph_incremental_confirmed", pd.Series(dtype=bool)).sum()),
                "relative_path": (target_root / "REPORT.md").relative_to(root.parent).as_posix(),
            }
        )
    return pd.DataFrame(rows)


def _write_docs(report: Path, summary: dict[str, object]) -> None:
    atomic_write_text(
        report / "REPORT.md",
        "\n".join(
            [
                "# Forward Volatility Prediction Report Pack",
                "",
                f"- Version: `{REPORT_PACK_VERSION}`",
                f"- Complete factor rows: {summary['factor_metric_rows']}",
                f"- Matched graph/node/reverse groups: {summary['matched_variant_groups']}",
                f"- Confirmed graph-incremental groups: {summary['confirmed_graph_incremental']}",
                f"- Targets: {summary['targets']}",
                f"- Horizons: {summary['horizons']}",
                "",
                "This is a complete discussion pack, not a significance-only screen. Every factor is retained, and failed matched comparisons carry explicit non-confirmation reasons.",
                "",
                "## Start here",
                "",
                "1. `overview/factor_master_compact.csv`",
                "2. `overview/target_horizon_scope_summary.csv`",
                "3. `overview/matched_variant_comparison.csv`",
                "4. `overview/non_confirmation_reason_counts.csv`",
                "5. `overview/label_date_coverage.csv`",
                "6. `overview/label_distribution_summary.csv`",
                "7. `indexes/UPLOAD_MANIFEST.csv`",
                "",
                "## Detailed evidence",
                "",
                "- `factor_metrics/`: predictive statistics only.",
                "- `factor_coverage/`: observations, dates, decisions and symbols.",
                "- `factor_governance/`: status and governance fields.",
                "- `generic_evaluator_diagnostics/`: retained for audit but not interpreted as option P&L.",
                "- `daily_ic/`, `quantiles/`, `stability/`: complete non-significance-filtered evidence.",
                "- `indexes/raw_evidence_index.csv`: raw snapshot and redundancy files are catalogued, not duplicated.",
                "",
                "The report tests future realized-volatility diagnostics. It does not prove implied-volatility mispricing or option profitability.",
                "",
            ]
        ),
    )
    atomic_write_text(
        report / "README_UPLOAD.md",
        "# Upload order\n\nUpload `REPORT.md`, `summary.json`, the complete `overview/` folder and `indexes/UPLOAD_MANIFEST.csv` first. Then use the shard indexes to upload only requested target/horizon/scope files. No factor is omitted.\n",
    )
    atomic_write_text(
        report / "DATA_DICTIONARY.md",
        "# Data dictionary\n\n- `top_bottom_target_spread`: high-score target minus low-score target; not traded P&L.\n- `graph_minus_node_abs_ic`: graph absolute IC minus node-baseline absolute IC.\n- `graph_minus_reverse_abs_ic`: graph absolute IC minus reverse-placebo absolute IC.\n- `graph_incremental_confirmed`: sample, FDR, sign and matched-increment gates all pass.\n- `non_confirmation_reasons`: explicit failure reasons for every non-confirmed match.\n- `past_window_complete_rate`: availability of the backward window needed for volatility expansion.\n",
    )
    atomic_write_text(
        report / "ANALYSIS_GUIDE.md",
        "# Analysis guide\n\n1. Audit label coverage and missingness.\n2. Analyze Global, Within-theme and Inter-theme separately.\n3. Compare graph-forward against node baseline and reverse placebo.\n4. Read daily IC and stability even when FDR fails.\n5. Separate total RV, expansion, downside, jump and tail targets.\n6. Treat insufficient/rejected rows as failure-mode evidence.\n",
    )


def write_forward_volatility_report_pack(
    output_root: str | Path,
    raw_root: str | Path,
    config: core.ForwardVolatilityConfig,
) -> Path:
    output = Path(output_root).expanduser().resolve()
    raw = Path(raw_root).expanduser().resolve()
    report = output / "prediction_report"
    raw_metrics, daily, quantiles, stability, raw_catalog, raw_sources = _raw_inputs(raw, config)
    metrics = _prediction_status(raw_metrics)
    comparisons = _matched_variants(metrics)
    label_coverage, label_by_date, label_summary, label_sources = _label_diagnostics(output, config)
    source_records = [*raw_sources, *label_sources]
    contract_hash = sha256_json(
        {
            "version": REPORT_PACK_VERSION,
            "config": config.as_dict(),
            "sources": [
                {
                    "path": str(record.get("path")),
                    "size_bytes": int(record.get("size_bytes", -1)),
                    "sha256": str(record.get("sha256")),
                }
                for record in source_records
            ],
        }
    )
    try:
        checkpoint = json.loads((report / "checkpoint.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        checkpoint = {}
    if (
        checkpoint.get("status") == "complete"
        and checkpoint.get("contract_hash") == contract_hash
        and (report / "_SUCCESS").exists()
        and _records_match(report, checkpoint.get("artifacts"))
    ):
        return report

    pending = report.with_name(f".{report.name}.{os.getpid()}.pending")
    shutil.rmtree(pending, ignore_errors=True)
    pending.mkdir(parents=True, exist_ok=False)
    overview = pending / "overview"
    indexes = pending / "indexes"
    overview.mkdir(parents=True)
    indexes.mkdir(parents=True)

    compact = _selected(
        metrics,
        (
            *COVERAGE_COLUMNS,
            *PREDICTIVE_COLUMNS,
            "top_bottom_target_spread",
            "prediction_status",
        ),
    )
    _write_frame(compact, overview / "factor_master_compact.csv")
    _write_frame(comparisons, overview / "matched_variant_comparison.csv")
    _write_frame(_top_bottom(comparisons), overview / "strongest_weakest_graph_increments.csv")
    _write_frame(label_coverage, overview / "label_date_coverage.csv")
    _write_frame(label_by_date, overview / "label_distribution_by_date.csv")
    _write_frame(label_summary, overview / "label_distribution_summary.csv")
    for name, frame in _summary_tables(metrics, comparisons).items():
        _write_frame(frame, overview / f"{name}.csv")

    indexes_to_write = {
        "predictive_metric_shard_index.csv": _write_shards(
            _selected(metrics, (*PREDICTIVE_COLUMNS, "top_bottom_target_spread", "prediction_status")),
            pending / "factor_metrics",
            filename="predictive_metrics.csv",
            sort_columns=("theme_family", "layer_id", "scale_minutes", "variant_id"),
        ),
        "coverage_metric_shard_index.csv": _write_shards(
            _selected(metrics, COVERAGE_COLUMNS),
            pending / "factor_coverage",
            filename="coverage_metrics.csv",
            sort_columns=("theme_family", "layer_id", "scale_minutes", "variant_id"),
        ),
        "governance_metric_shard_index.csv": _write_shards(
            _selected(metrics, (*GOVERNANCE_COLUMNS, "prediction_status")),
            pending / "factor_governance",
            filename="governance_metrics.csv",
            sort_columns=("theme_family", "layer_id", "scale_minutes", "variant_id"),
        ),
        "generic_evaluator_shard_index.csv": _write_shards(
            _selected(metrics, GENERIC_EVALUATOR_COLUMNS),
            pending / "generic_evaluator_diagnostics",
            filename="generic_evaluator_diagnostics.csv",
            sort_columns=("theme_family", "layer_id", "scale_minutes", "variant_id"),
        ),
        "daily_ic_shard_index.csv": _write_shards(
            daily,
            pending / "daily_ic",
            filename="daily_ic.csv",
            sort_columns=("factor_id", "trade_date"),
        ),
        "quantile_shard_index.csv": _write_shards(
            quantiles.rename(columns={"mean_return": "mean_target"}),
            pending / "quantiles",
            filename="quantile_targets.csv",
            sort_columns=("factor_id", "decision_time", "quantile"),
        ),
        "stability_shard_index.csv": _write_shards(
            stability.rename(columns={"mean_target_return": "mean_target"}),
            pending / "stability",
            filename="stability_slices.csv",
            sort_columns=("factor_id", "slice_dimension", "slice_value"),
        ),
        "variant_comparison_shard_index.csv": _write_shards(
            comparisons,
            pending / "variant_comparisons",
            filename="matched_variants.csv",
            sort_columns=("theme_family", "layer_id", "scale_minutes"),
        ),
    }
    for filename, frame in indexes_to_write.items():
        _write_frame(frame, indexes / filename)
    _write_frame(raw_catalog, indexes / "raw_evidence_index.csv")
    target_index = _target_reports(metrics, comparisons, pending / "target_reports")
    _write_frame(target_index, indexes / "target_report_index.csv")

    summary = {
        "version": REPORT_PACK_VERSION,
        "status": "complete",
        "factor_metric_rows": int(len(metrics)),
        "unique_factor_ids": int(metrics["factor_id"].nunique()) if "factor_id" in metrics else 0,
        "matched_variant_groups": int(len(comparisons)),
        "confirmed_graph_incremental": int(comparisons.get("graph_incremental_confirmed", pd.Series(dtype=bool)).sum()),
        "targets": [str(value) for value in config.targets],
        "horizons": [int(value) for value in config.horizons],
        "label_coverage_rows": int(len(label_coverage)),
        "label_distribution_rows": int(len(label_summary)),
        "daily_ic_rows_retained": int(len(daily)),
        "quantile_rows_retained": int(len(quantiles)),
        "stability_rows_retained": int(len(stability)),
        "report_policy": "retain_all_results_in_narrow_shards",
        "raw_snapshot_policy": "catalog_without_duplicate_copy",
    }
    _write_docs(pending, summary)
    atomic_write_json(pending / "summary.json", summary)

    upload_rows: list[dict[str, object]] = []
    for path in sorted(pending.rglob("*")):
        if not path.is_file() or path.name in {"checkpoint.json", "_SUCCESS"}:
            continue
        relative = path.relative_to(pending).as_posix()
        priority = 1 if relative in {
            "REPORT.md",
            "summary.json",
            "README_UPLOAD.md",
            "overview/factor_master_compact.csv",
            "overview/target_horizon_scope_summary.csv",
            "overview/matched_variant_comparison.csv",
            "overview/non_confirmation_reason_counts.csv",
            "overview/label_date_coverage.csv",
            "overview/label_distribution_summary.csv",
        } else 2 if relative.startswith("indexes/") else 3 if relative.startswith("target_reports/") else 4
        upload_rows.append(
            {
                "relative_path": relative,
                "size_bytes": int(path.stat().st_size),
                "rows": _row_count(path),
                "upload_priority": priority,
                "content_role": "overview" if priority == 1 else "index" if priority == 2 else "target_report" if priority == 3 else "detail_shard",
            }
        )
    _write_frame(pd.DataFrame(upload_rows).sort_values(["upload_priority", "relative_path"]), indexes / "UPLOAD_MANIFEST.csv")

    artifacts = [
        _artifact_record(path, pending)
        for path in sorted(pending.rglob("*"))
        if path.is_file() and path.name not in {"checkpoint.json", "_SUCCESS"}
    ]
    atomic_write_json(
        pending / "checkpoint.json",
        {
            "status": "complete",
            "contract_hash": contract_hash,
            "version": REPORT_PACK_VERSION,
            "created_at": _utc_now(),
            "sources": source_records,
            "artifacts": artifacts,
        },
    )
    atomic_write_text(pending / "_SUCCESS", _utc_now() + "\n")
    if report.exists():
        shutil.rmtree(report, ignore_errors=True)
    os.replace(pending, report)
    return report


def install() -> None:
    core._finalize_volatility_report = write_forward_volatility_report_pack


__all__ = ["REPORT_PACK_VERSION", "install", "write_forward_volatility_report_pack"]
