from __future__ import annotations

from pathlib import Path
import json
import math
import shutil
from typing import Iterable, Mapping

import numpy as np
import pandas as pd
from scipy import stats

from .governance import atomic_write_frame, atomic_write_json, atomic_write_text


DEFAULT_ROLLING_SCHEDULE: dict[int, int] = {5: 1, 10: 1, 15: 2, 20: 2, 30: 5}
DEFAULT_PROMOTION_WINDOWS = (15, 20, 30)


def _safe(value: object) -> str:
    text = str(value or "unknown").strip()
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in text)


def _ttest(values: pd.Series) -> tuple[float, float]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 2 or clean.std(ddof=1) <= 0:
        return np.nan, np.nan
    statistic = float(clean.mean() / (clean.std(ddof=1) / math.sqrt(len(clean))))
    return statistic, float(2 * stats.t.sf(abs(statistic), len(clean) - 1))


def _max_drawdown(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna().to_numpy(float)
    if len(clean) == 0:
        return np.nan
    wealth = np.cumprod(1 + clean)
    peak = np.maximum.accumulate(wealth)
    return float(np.min(wealth / peak - 1))


def _role(window: int) -> str:
    if window == 5:
        return "pulse_diagnostic"
    if window == 10:
        return "tactical_monitor"
    if window in {15, 20}:
        return "confirmation"
    if window >= 30:
        return "structural_validation"
    return "diagnostic"


def _profiles(path: str | Path | None) -> tuple[dict[str, dict[str, object]], list[str]]:
    if not path:
        return {}, []
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    profiles = {str(row["label_id"]): row for row in payload.get("labels", []) if isinstance(row, dict) and row.get("label_id")}
    return profiles, [str(value) for value in payload.get("analysis_dates", [])]


def _write_shards(result: pd.DataFrame, output: Path) -> pd.DataFrame:
    root = output / "shards"
    if root.exists():
        shutil.rmtree(root)
    rows: list[dict[str, object]] = []
    keys = [column for column in ("horizon", "window_sessions", "scope") if column in result.columns]
    for values, frame in result.groupby(keys, observed=True, dropna=False, sort=True):
        values = values if isinstance(values, tuple) else (values,)
        identity = dict(zip(keys, values))
        target = root
        for key in keys:
            target = target / f"{key}={_safe(identity[key])}"
        target.mkdir(parents=True, exist_ok=True)
        destination = target / "rolling_metrics.csv"
        sort_columns = [column for column in ("factor_id", "variant_id", "window_end") if column in frame.columns]
        atomic_write_frame(frame.sort_values(sort_columns), destination)
        rows.append({**identity, "rows": int(len(frame)), "relative_path": destination.relative_to(output).as_posix()})
    index = pd.DataFrame(rows)
    atomic_write_frame(index, output / "rolling_shard_index.csv")
    return index


def run_rolling_alpha_v2(
    report_root: str | Path,
    output_root: str | Path,
    *,
    label_manifest: str | Path | None = None,
    windows: Iterable[int] = tuple(DEFAULT_ROLLING_SCHEDULE),
    window_steps: Mapping[int, int] | None = None,
    promotion_windows: Iterable[int] = DEFAULT_PROMOTION_WINDOWS,
    min_coverage_ratio: float = 0.80,
    min_direction_train_dates: int = 15,
    direction_rolling_dates: int = 30,
) -> Path:
    report = Path(report_root).resolve()
    output = Path(output_root).resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)
    profiles, analysis_dates = _profiles(label_manifest)
    normalized = sorted(set(int(value) for value in windows if int(value) > 0))
    if not normalized:
        raise ValueError("At least one rolling window is required")
    schedule = {window: max(1, int((window_steps or {}).get(window, DEFAULT_ROLLING_SCHEDULE.get(window, 1)))) for window in normalized}
    promotion_set = {int(value) for value in promotion_windows}
    all_rows: list[dict[str, object]] = []

    for horizon_dir in sorted(report.glob("horizon=*")):
        if not (horizon_dir / "_SUCCESS").exists():
            continue
        horizon = horizon_dir.name.split("=", 1)[1]
        daily_ic = pd.read_csv(horizon_dir / "daily_ic.csv")
        portfolio = pd.read_csv(horizon_dir / "portfolio_returns.csv")
        metrics = pd.read_csv(horizon_dir / "alpha_metrics.csv")
        keys = [column for column in ("batch_id", "factor_id", "layer_id", "scale_minutes", "variant_id", "scope", "theme_family") if column in daily_ic.columns or column in portfolio.columns]
        ic = daily_ic.groupby([*keys, "trade_date"], observed=True, dropna=False)["spearman_ic"].mean().reset_index()
        port = portfolio.groupby([*keys, "trade_date"], observed=True, dropna=False).agg(raw_return=("raw_top_minus_bottom_return", "mean"), turnover=("turnover", "mean")).reset_index()
        daily = ic.merge(port, on=[*keys, "trade_date"], how="outer")
        dates = analysis_dates or sorted(daily["trade_date"].astype(str).unique())
        profile = profiles.get(horizon, {})

        for factor_values, factor in daily.groupby(keys, observed=True, dropna=False):
            values = factor_values if isinstance(factor_values, tuple) else (factor_values,)
            identity = dict(zip(keys, values))
            metric = metrics.copy()
            for key, value in identity.items():
                if key in metric:
                    metric = metric[metric[key].isna()] if pd.isna(value) else metric[metric[key] == value]
            predeclared: int | None = None
            if not metric.empty and bool(metric.iloc[0].get("direction_predeclared", False)):
                raw = pd.to_numeric(pd.Series([metric.iloc[0].get("expected_direction")]), errors="coerce").iloc[0]
                if pd.notna(raw) and raw != 0:
                    predeclared = int(np.sign(raw))
            factor = factor.copy()
            factor["trade_date"] = factor["trade_date"].astype(str)
            by_date = factor.set_index("trade_date")

            for window in normalized:
                for end_pos in range(window - 1, len(dates), schedule[window]):
                    window_dates = dates[end_pos - window + 1 : end_pos + 1]
                    observed_dates = [value for value in window_dates if value in by_date.index]
                    observed = by_date.loc[observed_dates].reset_index()
                    coverage = len(observed_dates) / window
                    train_dates = [value for value in dates[: end_pos - window + 1] if value in by_date.index][-direction_rolling_dates:]
                    train_ic = pd.to_numeric(by_date.loc[train_dates]["spearman_ic"], errors="coerce").dropna() if train_dates else pd.Series(dtype=float)
                    direction = predeclared
                    source = "predeclared" if predeclared else "unavailable"
                    if direction is None and len(train_ic) >= min_direction_train_dates:
                        direction = 1 if train_ic.mean() >= 0 else -1
                        source = "prior_daily_ic"
                    ic_values = pd.to_numeric(observed["spearman_ic"], errors="coerce").dropna()
                    returns = observed[["raw_return", "turnover"]].apply(pd.to_numeric, errors="coerce").dropna(subset=["raw_return"])
                    oriented = returns["raw_return"] * direction if direction in (-1, 1) else pd.Series(dtype=float)
                    statistic, pvalue = _ttest(ic_values)
                    promotion_window = window in promotion_set
                    row: dict[str, object] = {
                        **identity,
                        "horizon": horizon,
                        "horizon_type": profile.get("horizon_type", "intraday" if horizon.endswith("m") else "daily_next_open"),
                        "window_sessions": window,
                        "window_step_sessions": schedule[window],
                        "window_role": _role(window),
                        "promotion_window": promotion_window,
                        "window_start": window_dates[0],
                        "window_end": window_dates[-1],
                        "expected_sessions": window,
                        "observed_sessions": len(observed_dates),
                        "coverage_ratio": coverage,
                        "coverage_pass": coverage >= min_coverage_ratio,
                        "direction": direction if direction else np.nan,
                        "direction_source": source,
                        "direction_train_dates": len(train_ic),
                        "mean_spearman_ic": ic_values.mean() if len(ic_values) else np.nan,
                        "spearman_ic_tstat": statistic,
                        "spearman_ic_pvalue": pvalue,
                        "pit_oriented_gross_mean": oriented.mean() if len(oriented) else np.nan,
                        "mean_turnover": returns["turnover"].mean() if len(returns) else np.nan,
                        "pit_oriented_hit_rate": (oriented > 0).mean() if len(oriented) else np.nan,
                        "pit_oriented_max_drawdown": _max_drawdown(oriented),
                        "label_last_available_date": profile.get("last_available_date"),
                        "label_tail_missing_dates": profile.get("tail_missing_dates", ""),
                    }
                    for bps in (0, 1, 2, 5, 10):
                        row[f"pit_net_mean_{bps}bps"] = (oriented - returns["turnover"].fillna(0) * bps / 10000).mean() if len(oriented) else np.nan
                    row["rolling_status"] = "complete" if row["coverage_pass"] and source != "unavailable" else "insufficient_direction_history" if source == "unavailable" else "insufficient_coverage"
                    row["promotion_ready"] = bool(promotion_window and row["rolling_status"] == "complete" and pd.notna(row["pit_net_mean_5bps"]) and float(row["pit_net_mean_5bps"]) > 0)
                    all_rows.append(row)

    result = pd.DataFrame(all_rows)
    if result.empty:
        raise ValueError("No completed horizon reports available for rolling Alpha")
    shard_index = _write_shards(result, output)
    graph = result[result["variant_id"] == "graph_forward"] if "variant_id" in result else result
    summary_keys = [column for column in ("horizon_type", "scope", "theme_family", "horizon", "window_sessions", "window_step_sessions", "window_role", "rolling_status") if column in graph.columns]
    summary = graph.groupby(summary_keys, observed=True, dropna=False).agg(factor_rows=("factor_id", "size"), unique_factors=("factor_id", "nunique"), mean_abs_ic=("mean_spearman_ic", lambda values: pd.to_numeric(values, errors="coerce").abs().mean()), mean_net_5bps=("pit_net_mean_5bps", "mean"), positive_net_5bps_rate=("pit_net_mean_5bps", lambda values: (pd.to_numeric(values, errors="coerce") > 0).mean()), mean_turnover=("mean_turnover", "mean"), mean_coverage=("coverage_ratio", "mean"), promotion_ready_rows=("promotion_ready", "sum")).reset_index()
    atomic_write_frame(summary, output / "rolling_scope_summary.csv")

    stability_keys = [column for column in ("factor_id", "layer_id", "scale_minutes", "variant_id", "scope", "theme_family", "horizon", "horizon_type", "window_sessions", "window_role", "promotion_window") if column in result.columns]
    stability = result.groupby(stability_keys, observed=True, dropna=False).agg(window_count=("window_end", "size"), complete_window_count=("rolling_status", lambda values: int((values.astype(str) == "complete").sum())), ic_mean=("mean_spearman_ic", "mean"), ic_median=("mean_spearman_ic", "median"), ic_std=("mean_spearman_ic", "std"), ic_sign_consistency=("mean_spearman_ic", lambda values: max((pd.to_numeric(values, errors="coerce") > 0).mean(), (pd.to_numeric(values, errors="coerce") < 0).mean())), net_5bps_mean=("pit_net_mean_5bps", "mean"), net_5bps_positive_rate=("pit_net_mean_5bps", lambda values: (pd.to_numeric(values, errors="coerce") > 0).mean()), turnover_mean=("mean_turnover", "mean"), coverage_mean=("coverage_ratio", "mean"), complete_rate=("rolling_status", lambda values: (values.astype(str) == "complete").mean()), promotion_ready_rate=("promotion_ready", "mean")).reset_index()
    atomic_write_frame(stability, output / "rolling_factor_stability.csv")
    promotion = stability[stability["promotion_window"].fillna(False).astype(bool)].copy()
    if not promotion.empty:
        promotion["stability_score"] = promotion["ic_sign_consistency"].fillna(0) * 0.35 + promotion["net_5bps_positive_rate"].fillna(0) * 0.35 + promotion["complete_rate"].fillna(0) * 0.20 + promotion["coverage_mean"].fillna(0) * 0.10
        promotion = promotion.sort_values(["scope", "horizon", "stability_score", "net_5bps_mean"], ascending=[True, True, False, False])
    atomic_write_frame(promotion, output / "rolling_promotion_candidates.csv")

    payload = {
        "version": "GAL_ROLLING_ALPHA_V2_SHARDED",
        "windows": normalized,
        "window_steps": {str(key): value for key, value in schedule.items()},
        "window_roles": {str(window): _role(window) for window in normalized},
        "promotion_windows": sorted(promotion_set),
        "min_direction_train_dates": min_direction_train_dates,
        "direction_rolling_dates": direction_rolling_dates,
        "row_count": int(len(result)),
        "shard_count": int(len(shard_index)),
        "complete_rows": int((result["rolling_status"] == "complete").sum()),
        "promotion_ready_rows": int(result["promotion_ready"].sum()),
        "large_file_policy": "raw rolling rows split by horizon x window x scope",
        "tail_policy": "never synthesize unavailable future returns",
    }
    atomic_write_json(output / "summary.json", payload)
    atomic_write_text(output / "REPORT.md", "# Rolling Alpha report\n\n" + "\n".join(f"- {key}: {value}" for key, value in payload.items()) + "\n\n## Upload order\n\n1. `summary.json`\n2. `rolling_scope_summary.csv`\n3. `rolling_factor_stability.csv`\n4. `rolling_promotion_candidates.csv`\n5. Relevant files from `rolling_shard_index.csv`\n")
    atomic_write_json(output / "_SUCCESS", {"status": "complete", "summary": payload})
    return output
