from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable

import numpy as np
import pandas as pd
from scipy import stats


@dataclass
class AlphaResult:
    metrics: pd.DataFrame
    ic_series: pd.DataFrame
    quantile_returns: pd.DataFrame
    portfolio_returns: pd.DataFrame
    stability: pd.DataFrame
    score_correlation: pd.DataFrame


def _factor_keys(frame: pd.DataFrame) -> list[str]:
    keys = [column for column in ("batch_id", "factor_id", "layer_id", "scale_minutes", "horizon", "variant_id") if column in frame.columns]
    if not keys:
        frame["factor_id"] = "factor"
        keys = ["factor_id"]
    return keys


def _safe_corr(x: pd.Series, y: pd.Series, method: str) -> float:
    valid = pd.concat([x, y], axis=1).dropna()
    if len(valid) < 3 or valid.iloc[:, 0].nunique() < 2 or valid.iloc[:, 1].nunique() < 2:
        return np.nan
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1], method=method))


def _max_drawdown(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").dropna().to_numpy(dtype=float)
    if len(values) == 0:
        return np.nan
    wealth = np.cumprod(1.0 + values)
    peak = np.maximum.accumulate(wealth)
    drawdown = wealth / peak - 1.0
    return float(drawdown.min())


def _turnover(sets: list[set[object]]) -> float:
    values = []
    for previous, current in zip(sets, sets[1:]):
        denominator = max(len(previous), len(current), 1)
        values.append(1.0 - len(previous & current) / denominator)
    return float(np.mean(values)) if values else np.nan


def _bh_fdr(pvalues: pd.Series, alpha: float = 0.05) -> tuple[pd.Series, pd.Series]:
    values = pd.to_numeric(pvalues, errors="coerce")
    valid = values.dropna().sort_values()
    q = pd.Series(np.nan, index=values.index, dtype=float)
    passed = pd.Series(False, index=values.index, dtype=bool)
    if valid.empty:
        return q, passed
    n = len(valid)
    adjusted = valid.to_numpy() * n / np.arange(1, n + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    q.loc[valid.index] = np.minimum(adjusted, 1.0)
    passed.loc[valid.index] = q.loc[valid.index] <= alpha
    return q, passed


def evaluate_alpha(
    frame: pd.DataFrame,
    *,
    score_column: str = "score",
    label_column: str = "target_return",
    time_column: str = "decision_time",
    symbol_column: str = "symbol",
    quantiles: int = 5,
    annualization_factor: float = 252.0,
    min_cross_section: int = 10,
    cost_bps: Iterable[float] = (0.0, 1.0, 2.0, 5.0, 10.0),
    slice_columns: Iterable[str] = (),
) -> AlphaResult:
    required = {score_column, label_column, time_column, symbol_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Alpha input is missing columns: {missing}")
    data = frame.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    data[score_column] = pd.to_numeric(data[score_column], errors="coerce")
    data[label_column] = pd.to_numeric(data[label_column], errors="coerce")
    data = data.dropna(subset=[time_column, symbol_column, score_column, label_column])
    keys = _factor_keys(data)
    ic_rows: list[dict[str, object]] = []
    quantile_rows: list[dict[str, object]] = []
    portfolio_rows: list[dict[str, object]] = []
    metric_rows: list[dict[str, object]] = []
    stability_rows: list[dict[str, object]] = []

    for key_values, factor in data.groupby(keys, observed=True, dropna=False):
        key = dict(zip(keys, key_values if isinstance(key_values, tuple) else (key_values,)))
        top_sets: list[set[object]] = []
        bottom_sets: list[set[object]] = []
        factor_ic_rows: list[dict[str, object]] = []
        factor_quantile_rows: list[dict[str, object]] = []
        factor_portfolios: list[dict[str, object]] = []
        for decision_time, cross in factor.groupby(time_column, observed=True):
            if len(cross) < min_cross_section:
                continue
            pearson = _safe_corr(cross[score_column], cross[label_column], "pearson")
            spearman = _safe_corr(cross[score_column], cross[label_column], "spearman")
            ic_row = {**key, time_column: decision_time, "n": len(cross), "pearson_ic": pearson, "spearman_ic": spearman}
            ic_rows.append(ic_row)
            factor_ic_rows.append(ic_row)
            ranks = cross[score_column].rank(method="first", pct=True)
            q = np.minimum((ranks * quantiles).apply(np.ceil).astype(int), quantiles)
            q = q.clip(1, quantiles)
            cross = cross.assign(_quantile=q)
            means = cross.groupby("_quantile", observed=True)[label_column].mean()
            for quantile, value in means.items():
                quantile_row = {**key, time_column: decision_time, "quantile": int(quantile), "mean_return": float(value), "n": int((q == quantile).sum())}
                quantile_rows.append(quantile_row)
                factor_quantile_rows.append(quantile_row)
            top = cross[cross._quantile == quantiles]
            bottom = cross[cross._quantile == 1]
            top_set = set(top[symbol_column])
            bottom_set = set(bottom[symbol_column])
            top_sets.append(top_set)
            bottom_sets.append(bottom_set)
            top_return = float(top[label_column].mean()) if len(top) else np.nan
            bottom_return = float(bottom[label_column].mean()) if len(bottom) else np.nan
            ls_return = top_return - bottom_return if np.isfinite(top_return) and np.isfinite(bottom_return) else np.nan
            row = {**key, time_column: decision_time, "top_return": top_return, "bottom_return": bottom_return, "long_short_return": ls_return, "top_n": len(top), "bottom_n": len(bottom)}
            portfolio_rows.append(row)
            factor_portfolios.append(row)

        ic_frame = pd.DataFrame(factor_ic_rows)
        portfolio = pd.DataFrame(factor_portfolios)
        quantile_factor = pd.DataFrame(factor_quantile_rows)
        spearman_values = ic_frame.get("spearman_ic", pd.Series(dtype=float)).dropna()
        pearson_values = ic_frame.get("pearson_ic", pd.Series(dtype=float)).dropna()
        ls = portfolio.get("long_short_return", pd.Series(dtype=float)).dropna()
        top = portfolio.get("top_return", pd.Series(dtype=float)).dropna()
        bottom = portfolio.get("bottom_return", pd.Series(dtype=float)).dropna()
        mean_ic = float(spearman_values.mean()) if len(spearman_values) else np.nan
        ic_std = float(spearman_values.std(ddof=1)) if len(spearman_values) > 1 else np.nan
        ic_t = float(mean_ic / (ic_std / math.sqrt(len(spearman_values)))) if len(spearman_values) > 1 and ic_std > 0 else np.nan
        ic_p = float(2 * stats.t.sf(abs(ic_t), len(spearman_values) - 1)) if np.isfinite(ic_t) else np.nan
        ls_mean = float(ls.mean()) if len(ls) else np.nan
        ls_std = float(ls.std(ddof=1)) if len(ls) > 1 else np.nan
        sharpe = float(ls_mean / ls_std * math.sqrt(annualization_factor)) if np.isfinite(ls_std) and ls_std > 0 else np.nan
        ls_t = float(ls_mean / (ls_std / math.sqrt(len(ls)))) if len(ls) > 1 and ls_std > 0 else np.nan
        turnover = float(np.nanmean([_turnover(top_sets), _turnover(bottom_sets)]))
        q_mean = quantile_factor.groupby("quantile", observed=True)["mean_return"].mean() if not quantile_factor.empty else pd.Series(dtype=float)
        monotonicity = _safe_corr(pd.Series(q_mean.index, dtype=float), q_mean.reset_index(drop=True), "spearman") if len(q_mean) >= 3 else np.nan
        cost_metrics: dict[str, float] = {}
        for bps in cost_bps:
            net = ls - turnover * float(bps) / 10000.0 if len(ls) and np.isfinite(turnover) else ls
            net_std = float(net.std(ddof=1)) if len(net) > 1 else np.nan
            cost_metrics[f"net_mean_{bps:g}bps"] = float(net.mean()) if len(net) else np.nan
            cost_metrics[f"net_sharpe_{bps:g}bps"] = float(net.mean() / net_std * math.sqrt(annualization_factor)) if np.isfinite(net_std) and net_std > 0 else np.nan
        metric_rows.append({
            **key,
            "observations": int(len(factor)),
            "decision_count": int(factor[time_column].nunique()),
            "symbol_count": int(factor[symbol_column].nunique()),
            "label_mean": float(factor[label_column].mean()),
            "label_std": float(factor[label_column].std(ddof=1)),
            "score_mean": float(factor[score_column].mean()),
            "score_std": float(factor[score_column].std(ddof=1)),
            "mean_spearman_ic": mean_ic,
            "median_spearman_ic": float(spearman_values.median()) if len(spearman_values) else np.nan,
            "spearman_ic_std": ic_std,
            "spearman_icir": float(mean_ic / ic_std) if np.isfinite(ic_std) and ic_std > 0 else np.nan,
            "spearman_ic_positive_rate": float((spearman_values > 0).mean()) if len(spearman_values) else np.nan,
            "spearman_ic_tstat": ic_t,
            "spearman_ic_pvalue": ic_p,
            "mean_pearson_ic": float(pearson_values.mean()) if len(pearson_values) else np.nan,
            "long_short_mean": ls_mean,
            "long_short_std": ls_std,
            "long_short_sharpe": sharpe,
            "long_short_tstat": ls_t,
            "long_short_hit_rate": float((ls > 0).mean()) if len(ls) else np.nan,
            "top_leg_mean": float(top.mean()) if len(top) else np.nan,
            "bottom_leg_mean": float(bottom.mean()) if len(bottom) else np.nan,
            "max_drawdown": _max_drawdown(ls),
            "var_5pct": float(ls.quantile(0.05)) if len(ls) else np.nan,
            "cvar_5pct": float(ls[ls <= ls.quantile(0.05)].mean()) if len(ls) else np.nan,
            "skew": float(ls.skew()) if len(ls) >= 3 else np.nan,
            "kurtosis": float(ls.kurt()) if len(ls) >= 4 else np.nan,
            "turnover": turnover,
            "quantile_monotonicity": monotonicity,
            **cost_metrics,
        })
        factor_stability = factor.assign(_date=factor[time_column].dt.date, _hour=factor[time_column].dt.hour)
        dimensions = ["_date", "_hour", *[column for column in slice_columns if column in factor_stability.columns]]
        for dimension in dimensions:
            for value, subset in factor_stability.groupby(dimension, observed=True, dropna=False):
                stability_rows.append({
                    **key,
                    "slice_dimension": dimension.removeprefix("_"),
                    "slice_value": str(value),
                    "observations": int(len(subset)),
                    "spearman_ic": _safe_corr(subset[score_column], subset[label_column], "spearman"),
                    "mean_target_return": float(subset[label_column].mean()),
                })

    metrics = pd.DataFrame(metric_rows)
    if not metrics.empty:
        metrics["fdr_qvalue"], metrics["fdr_pass"] = _bh_fdr(metrics["spearman_ic_pvalue"])
        metrics["cost_survives_5bps"] = metrics.get("net_mean_5bps", np.nan) > 0
        metrics["direction_consistent"] = metrics["spearman_ic_positive_rate"] >= 0.55
        metrics["sample_sufficient"] = (metrics["decision_count"] >= 20) & (metrics["observations"] >= 500)
        metrics["research_status"] = np.select(
            [
                metrics["sample_sufficient"] & metrics["fdr_pass"] & metrics["cost_survives_5bps"] & metrics["direction_consistent"],
                metrics["sample_sufficient"] & (metrics["mean_spearman_ic"].abs() >= 0.01),
            ],
            ["candidate", "needs_falsification"],
            default="insufficient_or_rejected",
        )

    corr_keys = [column for column in keys if column != "factor_id"]
    correlations: list[pd.DataFrame] = []
    if "factor_id" in data.columns:
        grouping = data.groupby(corr_keys, observed=True, dropna=False) if corr_keys else [((), data)]
        for values, group in grouping:
            pivot = group.pivot_table(index=[time_column, symbol_column], columns="factor_id", values=score_column, aggfunc="mean")
            if pivot.shape[1] >= 2:
                corr = pivot.corr(method="spearman").stack().rename("score_spearman_correlation").reset_index()
                base = dict(zip(corr_keys, values if isinstance(values, tuple) else (values,)))
                for column, value in base.items():
                    corr[column] = value
                correlations.append(corr)
    return AlphaResult(
        metrics=metrics,
        ic_series=pd.DataFrame(ic_rows),
        quantile_returns=pd.DataFrame(quantile_rows),
        portfolio_returns=pd.DataFrame(portfolio_rows),
        stability=pd.DataFrame(stability_rows),
        score_correlation=pd.concat(correlations, ignore_index=True) if correlations else pd.DataFrame(),
    )
