from __future__ import annotations

import pandas as pd


def cross_scope_comparison(metrics: pd.DataFrame) -> pd.DataFrame:
    """Match each family-specific scope to the single shared Global factor.

    Global intentionally uses theme_family=shared_global, so a normal pivot by
    theme_family cannot align it with Momentum or Residual Within/Inter rows.
    This function performs the governed many-to-one match explicitly.
    """
    if metrics.empty or "scope" not in metrics.columns:
        return pd.DataFrame()
    global_rows = metrics[metrics["scope"].astype(str) == "global"].copy()
    scoped = metrics[
        metrics["scope"].astype(str).isin(("within_theme", "inter_theme"))
    ].copy()
    if global_rows.empty or scoped.empty:
        return pd.DataFrame()
    match_keys = [
        column
        for column in (
            "layer_id",
            "scale_minutes",
            "variant_id",
            "horizon",
            "horizon_minutes",
        )
        if column in metrics.columns
    ]
    if not match_keys:
        return pd.DataFrame()
    metric_columns = [
        column
        for column in (
            "mean_spearman_ic",
            "net_mean_5bps",
            "cost_survives_5bps",
            "decision_count",
            "date_count",
        )
        if column in metrics.columns
    ]
    global_keep = [*match_keys, *metric_columns]
    global_rows = global_rows[global_keep].drop_duplicates(match_keys)
    rename_global = {
        column: f"{column}__global" for column in metric_columns
    }
    global_rows = global_rows.rename(columns=rename_global)
    scoped_identity = [
        column
        for column in (
            "scope",
            "theme_family",
            "scope_alpha_unit",
            "factor_id",
            *match_keys,
        )
        if column in scoped.columns
    ]
    scoped_keep = list(dict.fromkeys([*scoped_identity, *metric_columns]))
    scoped = scoped[scoped_keep].rename(
        columns={column: f"{column}__scoped" for column in metric_columns}
    )
    result = scoped.merge(
        global_rows,
        on=match_keys,
        how="left",
        validate="many_to_one",
    )
    if "mean_spearman_ic__scoped" in result and "mean_spearman_ic__global" in result:
        result["mean_spearman_ic_increment_vs_global"] = (
            result["mean_spearman_ic__scoped"]
            - result["mean_spearman_ic__global"]
        )
        result["abs_ic_increment_vs_global"] = (
            result["mean_spearman_ic__scoped"].abs()
            - result["mean_spearman_ic__global"].abs()
        )
    if "net_mean_5bps__scoped" in result and "net_mean_5bps__global" in result:
        result["net_5bps_increment_vs_global"] = (
            result["net_mean_5bps__scoped"]
            - result["net_mean_5bps__global"]
        )
    return result.sort_values(
        [column for column in ("horizon_minutes", "theme_family", "scope", "layer_id", "scale_minutes", "variant_id") if column in result.columns],
        kind="stable",
    ).reset_index(drop=True)
