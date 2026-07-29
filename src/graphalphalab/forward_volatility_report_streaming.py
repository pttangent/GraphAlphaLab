from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from . import forward_volatility_report_pack as report_pack
from .governance import file_record


STREAMING_REPORT_VERSION = "GAL_FORWARD_VOLATILITY_REPORT_STREAMING_V3"
IDENTITY_CANDIDATES = (
    "batch_id",
    "factor_id",
    "scope",
    "theme_family",
    "layer_id",
    "scale_minutes",
    "variant_id",
)


def _keys(columns: list[str] | pd.Index, extra: tuple[str, ...] = ()) -> list[str]:
    available = set(str(value) for value in columns)
    return [column for column in (*IDENTITY_CANDIDATES, *extra) if column in available]


def _empty_quantiles() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *IDENTITY_CANDIDATES,
            "quantile",
            "mean_return",
            "target_std",
            "observation_count",
            "target_min",
            "target_max",
            "first_trade_date",
            "last_trade_date",
        ]
    )


def _stream_quantile_summary(path: Path, *, chunksize: int = 250_000) -> pd.DataFrame:
    partials: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, chunksize=chunksize):
        if chunk.empty or "mean_return" not in chunk.columns:
            continue
        chunk = chunk.copy()
        chunk["_target"] = pd.to_numeric(chunk["mean_return"], errors="coerce")
        chunk = chunk.dropna(subset=["_target"])
        if chunk.empty:
            continue
        group_keys = _keys(chunk.columns, ("quantile",))
        if not group_keys:
            raise ValueError(f"Quantile evidence has no factor identity: {path}")
        chunk["_target_sq"] = chunk["_target"] * chunk["_target"]
        if "trade_date" not in chunk.columns:
            chunk["trade_date"] = None
        partial = (
            chunk.groupby(group_keys, observed=True, dropna=False)
            .agg(
                _count=("_target", "count"),
                _sum=("_target", "sum"),
                _sum_sq=("_target_sq", "sum"),
                target_min=("_target", "min"),
                target_max=("_target", "max"),
                first_trade_date=("trade_date", "min"),
                last_trade_date=("trade_date", "max"),
            )
            .reset_index()
        )
        partials.append(partial)
    if not partials:
        return _empty_quantiles()
    combined = pd.concat(partials, ignore_index=True)
    group_keys = _keys(combined.columns, ("quantile",))
    summary = (
        combined.groupby(group_keys, observed=True, dropna=False)
        .agg(
            observation_count=("_count", "sum"),
            _sum=("_sum", "sum"),
            _sum_sq=("_sum_sq", "sum"),
            target_min=("target_min", "min"),
            target_max=("target_max", "max"),
            first_trade_date=("first_trade_date", "min"),
            last_trade_date=("last_trade_date", "max"),
        )
        .reset_index()
    )
    count = pd.to_numeric(summary["observation_count"], errors="coerce").clip(lower=1)
    summary["mean_return"] = summary["_sum"] / count
    variance = (summary["_sum_sq"] - summary["_sum"] * summary["_sum"] / count) / (count - 1).clip(lower=1)
    summary["target_std"] = np.sqrt(variance.clip(lower=0.0))
    return summary.drop(columns=["_sum", "_sum_sq"])


def _snapshot_partial(chunk: pd.DataFrame, *, hourly: bool) -> pd.DataFrame:
    if chunk.empty or "spearman_ic" not in chunk.columns:
        return pd.DataFrame()
    frame = chunk.copy()
    frame["_ic"] = pd.to_numeric(frame["spearman_ic"], errors="coerce")
    frame = frame.dropna(subset=["_ic"])
    if frame.empty:
        return pd.DataFrame()
    if "trade_date" not in frame.columns:
        frame["trade_date"] = None
    if hourly:
        if "decision_time" not in frame.columns:
            return pd.DataFrame()
        decision = pd.to_datetime(frame["decision_time"], utc=True, errors="coerce")
        frame["_slice_value"] = decision.dt.hour.astype("Int64").astype("string")
        slice_dimension = "snapshot_hour"
    else:
        frame["_slice_value"] = "all"
        slice_dimension = "snapshot_all"
    frame["_ic_sq"] = frame["_ic"] * frame["_ic"]
    frame["_positive"] = (frame["_ic"] > 0).astype(int)
    group_keys = _keys(frame.columns, ("_slice_value",))
    partial = (
        frame.groupby(group_keys, observed=True, dropna=False)
        .agg(
            _count=("_ic", "count"),
            _sum=("_ic", "sum"),
            _sum_sq=("_ic_sq", "sum"),
            _positive=("_positive", "sum"),
            first_trade_date=("trade_date", "min"),
            last_trade_date=("trade_date", "max"),
        )
        .reset_index()
    )
    partial["slice_dimension"] = slice_dimension
    partial = partial.rename(columns={"_slice_value": "slice_value"})
    return partial


def _stream_snapshot_stability(path: Path, *, chunksize: int = 250_000) -> pd.DataFrame:
    partials: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, chunksize=chunksize):
        for hourly in (False, True):
            partial = _snapshot_partial(chunk, hourly=hourly)
            if not partial.empty:
                partials.append(partial)
    if not partials:
        return pd.DataFrame()
    combined = pd.concat(partials, ignore_index=True)
    group_keys = _keys(combined.columns, ("slice_dimension", "slice_value"))
    summary = (
        combined.groupby(group_keys, observed=True, dropna=False)
        .agg(
            observations=("_count", "sum"),
            _sum=("_sum", "sum"),
            _sum_sq=("_sum_sq", "sum"),
            _positive=("_positive", "sum"),
            first_trade_date=("first_trade_date", "min"),
            last_trade_date=("last_trade_date", "max"),
        )
        .reset_index()
    )
    count = pd.to_numeric(summary["observations"], errors="coerce").clip(lower=1)
    summary["spearman_ic"] = summary["_sum"] / count
    variance = (summary["_sum_sq"] - summary["_sum"] * summary["_sum"] / count) / (count - 1).clip(lower=1)
    summary["spearman_ic_std"] = np.sqrt(variance.clip(lower=0.0))
    summary["spearman_ic_positive_rate"] = summary["_positive"] / count
    summary["mean_target_return"] = np.nan
    return summary.drop(columns=["_sum", "_sum_sq", "_positive"])


def _raw_inputs(
    raw_root: Path,
    config,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[dict[str, object]]]:
    metric_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    quantile_frames: list[pd.DataFrame] = []
    stability_frames: list[pd.DataFrame] = []
    catalog: list[dict[str, object]] = []
    sources: list[dict[str, object]] = []
    for horizon_value in config.horizons:
        horizon = int(horizon_value)
        for target_value in config.targets:
            target = str(target_value)
            name = report_pack._horizon_name(target, horizon)
            root = raw_root / f"horizon={name}"
            required = (
                "_SUCCESS",
                "alpha_metrics.csv",
                "daily_ic.csv",
                "quantile_returns.csv",
                "stability_slices.csv",
            )
            missing = [filename for filename in required if not (root / filename).exists()]
            if missing:
                raise FileNotFoundError(f"Incomplete volatility horizon {name}: {missing}")
            metric_frames.append(
                report_pack._annotate(
                    pd.read_csv(root / "alpha_metrics.csv"),
                    target=target,
                    horizon=horizon,
                )
            )
            daily_frames.append(
                report_pack._annotate(
                    pd.read_csv(root / "daily_ic.csv"),
                    target=target,
                    horizon=horizon,
                )
            )
            quantile_frames.append(
                report_pack._annotate(
                    _stream_quantile_summary(root / "quantile_returns.csv"),
                    target=target,
                    horizon=horizon,
                )
            )
            stability_frames.append(
                report_pack._annotate(
                    pd.read_csv(root / "stability_slices.csv"),
                    target=target,
                    horizon=horizon,
                )
            )
            snapshot_path = root / "ic_series.csv"
            if snapshot_path.exists():
                snapshot = _stream_snapshot_stability(snapshot_path)
                if not snapshot.empty:
                    stability_frames.append(
                        report_pack._annotate(snapshot, target=target, horizon=horizon)
                    )
            for filename in report_pack.RAW_EVIDENCE_FILES:
                path = root / filename
                if not path.exists():
                    continue
                record = file_record(path)
                sources.append(record)
                catalog.append(
                    {
                        "target_kind": target,
                        "horizon_minutes": horizon,
                        "horizon_name": name,
                        "evidence_file": filename,
                        "path": str(path),
                        "size_bytes": int(record["size_bytes"]),
                        "sha256": str(record["sha256"]),
                        "rows": report_pack._row_count(path),
                        "copied_into_report_pack": filename in {
                            "alpha_metrics.csv",
                            "daily_ic.csv",
                            "stability_slices.csv",
                        },
                        "compacted_into_report_pack": filename in {
                            "quantile_returns.csv",
                            "ic_series.csv",
                        },
                        "evidence_role": (
                            "snapshot_ic_compacted_to_all_and_hour_summaries"
                            if filename == "ic_series.csv"
                            else "quantile_evidence_compacted_by_factor_and_quantile"
                            if filename == "quantile_returns.csv"
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


def install() -> None:
    report_pack._raw_inputs = _raw_inputs


__all__ = [
    "STREAMING_REPORT_VERSION",
    "install",
    "_stream_quantile_summary",
    "_stream_snapshot_stability",
]
