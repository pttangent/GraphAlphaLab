from __future__ import annotations

from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from .governance import atomic_write_frame, atomic_write_json, atomic_write_text


def _safe(value: object) -> str:
    text = str(value or "unknown").strip()
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in text)


def _load_metrics(report: Path) -> pd.DataFrame:
    root = report / "all_horizons_alpha_metrics.csv"
    if root.exists():
        return pd.read_csv(root)
    frames: list[pd.DataFrame] = []
    for horizon in sorted(report.glob("horizon=*")):
        path = horizon / "alpha_metrics.csv"
        if path.exists():
            frame = pd.read_csv(path)
            if "horizon" not in frame:
                frame["horizon"] = horizon.name.split("=", 1)[1]
            frames.append(frame)
    if not frames:
        raise FileNotFoundError(f"No Alpha metrics below {report}")
    return pd.concat(frames, ignore_index=True)


def _write_factor_shards(metrics: pd.DataFrame, output: Path) -> pd.DataFrame:
    root = output / "factor_shards"
    if root.exists():
        shutil.rmtree(root)
    rows: list[dict[str, object]] = []
    keys = [column for column in ("horizon", "scope") if column in metrics.columns]
    for values, frame in metrics.groupby(keys, observed=True, dropna=False, sort=True):
        values = values if isinstance(values, tuple) else (values,)
        identity = dict(zip(keys, values))
        target = root
        for key in keys:
            target = target / f"{key}={_safe(identity[key])}"
        target.mkdir(parents=True, exist_ok=True)
        destination = target / "all_factor_metrics.csv"
        sort_columns = [column for column in ("theme_family", "layer_id", "scale_minutes", "variant_id") if column in frame.columns]
        atomic_write_frame(frame.sort_values(sort_columns), destination)
        rows.append({**identity, "rows": int(len(frame)), "relative_path": destination.relative_to(output).as_posix()})
    index = pd.DataFrame(rows)
    atomic_write_frame(index, output / "factor_shard_index.csv")
    return index


def write_discussion_pack(report_root: str | Path, rolling_root: str | Path, output_root: str | Path, *, top_n: int = 15) -> Path:
    report = Path(report_root).resolve()
    rolling = Path(rolling_root).resolve()
    output = Path(output_root).resolve()
    if output.exists():
        shutil.rmtree(output)
    overview = output / "overview"
    overview.mkdir(parents=True, exist_ok=True)

    metrics = _load_metrics(report)
    if metrics.empty:
        raise ValueError("Alpha metrics are empty")
    shard_index = _write_factor_shards(metrics, output)
    for column in ("mean_spearman_ic", "spearman_icir", "daily_ic_sign_consistency", "oriented_long_short_mean", "mean_turnover", "net_mean_5bps", "date_count"):
        if column in metrics:
            metrics[column] = pd.to_numeric(metrics[column], errors="coerce")

    summary_keys = [column for column in ("horizon", "horizon_minutes", "scope", "theme_family", "variant_id", "financial_role") if column in metrics.columns]
    summary = metrics.groupby(summary_keys, observed=True, dropna=False).agg(
        factor_rows=("factor_id", "size"),
        unique_factors=("factor_id", "nunique"),
        mean_abs_ic=("mean_spearman_ic", lambda values: values.abs().mean()),
        median_abs_ic=("mean_spearman_ic", lambda values: values.abs().median()),
        positive_ic_rate=("mean_spearman_ic", lambda values: (values > 0).mean()),
        mean_net_5bps=("net_mean_5bps", "mean"),
        positive_net_5bps_rate=("net_mean_5bps", lambda values: (values > 0).mean()),
        mean_turnover=("mean_turnover", "mean"),
        mean_date_count=("date_count", "mean"),
    ).reset_index()
    atomic_write_frame(summary, overview / "full_period_scope_horizon_summary.csv")

    status_keys = [column for column in ("horizon", "scope", "theme_family", "variant_id", "research_status", "financial_role") if column in metrics.columns]
    status = metrics.groupby(status_keys, observed=True, dropna=False).size().rename("factor_rows").reset_index()
    atomic_write_frame(status, overview / "factor_status_counts.csv")

    ranking = metrics.copy()
    ranking["abs_ic"] = ranking["mean_spearman_ic"].abs()
    ranking["cost_efficiency"] = (ranking["oriented_long_short_mean"].fillna(0) / ranking["mean_turnover"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    ranking["discussion_score"] = ranking["abs_ic"].rank(pct=True).fillna(0) * 0.35 + ranking["daily_ic_sign_consistency"].rank(pct=True).fillna(0) * 0.20 + ranking["net_mean_5bps"].rank(pct=True).fillna(0) * 0.25 + ranking["cost_efficiency"].rank(pct=True).fillna(0) * 0.20
    ranked: list[pd.DataFrame] = []
    rank_keys = [column for column in ("horizon", "scope") if column in ranking.columns]
    for _, frame in ranking.groupby(rank_keys, observed=True, dropna=False):
        ordered = frame.sort_values(["discussion_score", "abs_ic"], ascending=False)
        top = ordered.head(top_n).copy(); top["selection_side"] = "top"
        bottom = ordered.tail(top_n).copy(); bottom["selection_side"] = "bottom"
        ranked.extend((top, bottom))
    atomic_write_frame(pd.concat(ranked, ignore_index=True), overview / "top_bottom_factors_by_horizon_scope.csv")

    master = metrics.copy()
    stability_path = rolling / "rolling_factor_stability.csv"
    if stability_path.exists():
        stability = pd.read_csv(stability_path)
        promotion = stability[stability.get("promotion_window", pd.Series(False, index=stability.index)).fillna(False).astype(bool)].copy()
        identity = [column for column in ("factor_id", "layer_id", "scale_minutes", "variant_id", "scope", "theme_family", "horizon") if column in master.columns and column in promotion.columns]
        if identity and not promotion.empty:
            rolling_agg = promotion.groupby(identity, observed=True, dropna=False).agg(
                rolling_windows=("window_sessions", "nunique"),
                rolling_complete_rate=("complete_rate", "mean"),
                rolling_ic_mean=("ic_mean", "mean"),
                rolling_ic_sign_consistency=("ic_sign_consistency", "mean"),
                rolling_net_5bps_mean=("net_5bps_mean", "mean"),
                rolling_net_5bps_positive_rate=("net_5bps_positive_rate", "mean"),
                rolling_coverage_mean=("coverage_mean", "mean"),
                rolling_promotion_ready_rate=("promotion_ready_rate", "mean"),
            ).reset_index()
            master = master.merge(rolling_agg, on=identity, how="left", validate="many_to_one")
    atomic_write_frame(master, overview / "factor_master_summary.csv")
    compact_columns = [column for column in ("factor_id", "layer_id", "scale_minutes", "variant_id", "scope", "theme_family", "horizon", "financial_role", "mean_spearman_ic", "spearman_icir", "daily_ic_sign_consistency", "oriented_long_short_mean", "mean_turnover", "net_mean_5bps", "date_count", "research_status", "rolling_complete_rate", "rolling_ic_mean", "rolling_ic_sign_consistency", "rolling_net_5bps_mean", "rolling_net_5bps_positive_rate", "rolling_coverage_mean") if column in master.columns]
    atomic_write_frame(master[compact_columns], overview / "factor_master_compact.csv")

    files: list[dict[str, object]] = []
    for path in sorted(output.rglob("*")):
        if not path.is_file():
            continue
        row_count = None
        if path.suffix.lower() == ".csv":
            try:
                row_count = max(0, sum(1 for _ in path.open("r", encoding="utf-8")) - 1)
            except Exception:
                row_count = None
        files.append({"relative_path": path.relative_to(output).as_posix(), "size_bytes": path.stat().st_size, "rows": row_count, "upload_priority": 1 if path.parent == overview else 2 if path.name.endswith("index.csv") else 3})
    atomic_write_frame(pd.DataFrame(files).sort_values(["upload_priority", "relative_path"]), output / "UPLOAD_MANIFEST.csv")

    payload = {"version": "GAL_DISCUSSION_PACK_V1", "factor_metric_rows": int(len(metrics)), "unique_factors": int(metrics["factor_id"].nunique()), "factor_shards": int(len(shard_index)), "upload_strategy": "upload overview first, then only requested horizon/scope shards"}
    atomic_write_json(output / "summary.json", payload)
    atomic_write_text(output / "README_UPLOAD.md", "# GAL discussion upload pack\n\nThis pack avoids one giant raw file while retaining every factor.\n\n## First upload\n\n1. `overview/factor_master_compact.csv`\n2. `overview/full_period_scope_horizon_summary.csv`\n3. `overview/top_bottom_factors_by_horizon_scope.csv`\n4. `overview/factor_status_counts.csv`\n5. `summary.json`\n6. `UPLOAD_MANIFEST.csv`\n\nAfter reviewing these, upload only the requested files from `factor_shard_index.csv` and the matching rolling shards.\n")
    atomic_write_json(output / "_SUCCESS", {"status": "complete", **payload})
    return output
