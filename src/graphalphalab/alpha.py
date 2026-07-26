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
    daily_ic: pd.DataFrame
    quantile_returns: pd.DataFrame
    portfolio_returns: pd.DataFrame
    stability: pd.DataFrame
    score_correlation: pd.DataFrame
    governance: dict[str, object] | None = None


def _factor_keys(frame: pd.DataFrame) -> list[str]:
    keys = [
        column
        for column in ("batch_id", "factor_id", "layer_id", "scale_minutes", "horizon", "variant_id")
        if column in frame.columns
    ]
    if not keys:
        frame["factor_id"] = "factor"
        keys = ["factor_id"]
    return keys


def _factor_name(key: dict[str, object]) -> str:
    return "|".join(f"{column}={key.get(column)}" for column in key)


def _safe_corr(x: pd.Series, y: pd.Series, method: str) -> float:
    valid = pd.concat([x, y], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    if len(valid) < 3 or valid.iloc[:, 0].nunique() < 2 or valid.iloc[:, 1].nunique() < 2:
        return np.nan
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1], method=method))


def _max_drawdown(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().to_numpy(float)
    if len(values) == 0:
        return np.nan
    wealth = np.cumprod(1.0 + values)
    peak = np.maximum.accumulate(wealth)
    return float(np.min(wealth / peak - 1.0))


def _bh_fdr(pvalues: pd.Series, alpha: float = 0.05) -> tuple[pd.Series, pd.Series]:
    values = pd.to_numeric(pvalues, errors="coerce")
    valid = values.dropna().sort_values()
    q = pd.Series(np.nan, index=values.index, dtype=float)
    passed = pd.Series(False, index=values.index, dtype=bool)
    if valid.empty:
        return q, passed
    n = len(valid)
    adjusted = valid.to_numpy(float) * n / np.arange(1, n + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    q.loc[valid.index] = np.minimum(adjusted, 1.0)
    passed.loc[valid.index] = q.loc[valid.index] <= alpha
    return q, passed


def _parse_direction(value: object) -> int | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"positive", "+1", "1", "continuation", "long_high"}:
            return 1
        if text in {"negative", "-1", "reversal", "long_low"}:
            return -1
        if text in {"auto", "unknown", ""}:
            return None
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric) != 0:
        return 1 if float(numeric) > 0 else -1
    return None


def _equal_weight_positions(long_symbols: Iterable[object], short_symbols: Iterable[object]) -> dict[object, float]:
    long_list = list(long_symbols)
    short_list = list(short_symbols)
    weights: dict[object, float] = {}
    if long_list:
        for symbol in long_list:
            weights[symbol] = weights.get(symbol, 0.0) + 0.5 / len(long_list)
    if short_list:
        for symbol in short_list:
            weights[symbol] = weights.get(symbol, 0.0) - 0.5 / len(short_list)
    return weights


def _traded_notional(previous: dict[object, float], current: dict[object, float]) -> float:
    universe = set(previous) | set(current)
    return float(sum(abs(current.get(symbol, 0.0) - previous.get(symbol, 0.0)) for symbol in universe))


def _residualize_score(frame: pd.DataFrame, score_column: str, controls: list[str]) -> pd.Series:
    selected = [column for column in controls if column in frame.columns]
    if not selected:
        return pd.to_numeric(frame[score_column], errors="coerce")
    matrix = frame[[score_column, *selected]].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    valid = matrix.dropna()
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    if len(valid) < max(20, len(selected) + 5):
        return result
    y = valid[score_column].rank(method="average", pct=True).to_numpy(float)
    columns = [np.ones(len(valid))]
    for control in selected:
        series = valid[control]
        if series.nunique() >= 2:
            columns.append(series.rank(method="average", pct=True).to_numpy(float))
    x = np.column_stack(columns)
    beta = np.linalg.lstsq(x, y, rcond=None)[0]
    result.loc[valid.index] = y - x @ beta
    return result


def _daily_ttest(values: pd.Series) -> tuple[float, float]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 2:
        return np.nan, np.nan
    std = float(clean.std(ddof=1))
    if not np.isfinite(std) or std <= 0:
        return np.nan, np.nan
    tstat = float(clean.mean() / (std / math.sqrt(len(clean))))
    pvalue = float(2 * stats.t.sf(abs(tstat), len(clean) - 1))
    return tstat, pvalue


def _insufficient_metric(
    key: dict[str, object],
    data: pd.DataFrame,
    *,
    score_column: str,
    label_column: str,
    symbol_column: str,
    cost_bps: tuple[float, ...],
    label_overlapping: bool,
    declared_direction: int | None,
) -> dict[str, object]:
    # Single-factor callers (e.g. the global factor DAG) turn this dict into a
    # one-row metrics frame, so it must expose the exact column set of the full
    # path below; otherwise downstream FDR/status columns raise KeyError.
    metric: dict[str, object] = {
        **key,
        "observations": int(len(data)),
        "decision_count": 0,
        "date_count": 0,
        "symbol_count": int(data[symbol_column].nunique()),
        "label_mean": float(data[label_column].mean()),
        "label_std": float(data[label_column].std(ddof=1)),
        "score_mean": float(data[score_column].mean()),
        "score_std": float(data[score_column].std(ddof=1)),
        "mean_spearman_ic": np.nan,
        "median_spearman_ic": np.nan,
        "spearman_ic_std": np.nan,
        "spearman_icir": np.nan,
        "snapshot_ic_positive_rate": np.nan,
        "daily_ic_positive_rate": np.nan,
        "daily_ic_negative_rate": np.nan,
        "daily_ic_sign_consistency": np.nan,
        "spearman_ic_tstat_daily": np.nan,
        "spearman_ic_pvalue_daily": np.nan,
        "mean_pearson_ic_daily": np.nan,
        "expected_direction": declared_direction if declared_direction is not None else np.nan,
        "direction_source": "predeclared" if declared_direction is not None else "insufficient_data",
        "direction_predeclared": declared_direction is not None,
        "raw_top_minus_bottom_mean": np.nan,
        "oriented_long_short_mean": np.nan,
        "oriented_long_short_std": np.nan,
        "oriented_long_short_hit_rate": np.nan,
        "oriented_long_short_tstat_daily": np.nan,
        "annualization_valid": False,
        "annualized_sharpe": np.nan,
        "daily_diagnostic_sharpe": np.nan,
        "label_overlapping": label_overlapping,
        "max_drawdown": np.nan,
        "var_5pct": np.nan,
        "cvar_5pct": np.nan,
        "skew": np.nan,
        "kurtosis": np.nan,
        "mean_turnover": np.nan,
        "quantile_monotonicity": np.nan,
    }
    for bps in cost_bps:
        metric[f"net_mean_{bps:g}bps"] = np.nan
        metric[f"net_daily_diagnostic_sharpe_{bps:g}bps"] = np.nan
    metric["cost_survives_5bps"] = False
    metric["direction_consistent"] = False
    metric["sample_sufficient"] = False
    metric["research_status"] = "insufficient_or_rejected"
    return metric


def _evaluate_factor(
    factor: pd.DataFrame,
    *,
    key: dict[str, object],
    score_column: str,
    label_column: str,
    time_column: str,
    symbol_column: str,
    trade_date_column: str,
    quantiles: int,
    min_cross_section: int,
    cost_bps: tuple[float, ...],
    slice_columns: tuple[str, ...],
    direction_column: str | None,
    default_direction: str,
    control_columns: tuple[str, ...],
    label_overlapping: bool,
    annualization_factor: float | None,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    data = factor.copy()
    if control_columns:
        residual_parts = []
        for _, cross in data.groupby(time_column, observed=True, sort=True):
            residual = _residualize_score(cross, score_column, list(control_columns))
            residual_parts.append(pd.Series(residual, index=cross.index))
        if residual_parts:
            data[score_column] = pd.concat(residual_parts).sort_index()
    declared_direction: int | None = None
    if direction_column and direction_column in data.columns:
        values = {_parse_direction(value) for value in data[direction_column].dropna().unique()}
        values.discard(None)
        if len(values) > 1:
            raise ValueError(f"Factor {_factor_name(key)} has conflicting expected directions: {sorted(values)}")
        declared_direction = next(iter(values), None)
    if declared_direction is None and default_direction in {"positive", "negative"}:
        declared_direction = 1 if default_direction == "positive" else -1
    direction_predeclared = declared_direction is not None

    ic_rows: list[dict[str, object]] = []
    quantile_rows: list[dict[str, object]] = []
    portfolio_rows: list[dict[str, object]] = []
    stability_rows: list[dict[str, object]] = []
    raw_decisions: list[tuple[pd.Timestamp, str, pd.DataFrame, pd.Series]] = []

    for decision_time, cross in data.groupby(time_column, observed=True, sort=True):
        cross = cross.replace([np.inf, -np.inf], np.nan).dropna(subset=[score_column, label_column, symbol_column])
        if len(cross) < min_cross_section or cross[score_column].nunique() < 2:
            continue
        pearson = _safe_corr(cross[score_column], cross[label_column], "pearson")
        spearman = _safe_corr(cross[score_column], cross[label_column], "spearman")
        date_value = str(cross[trade_date_column].iloc[0]) if trade_date_column in cross.columns else str(decision_time.date())
        ic_rows.append({**key, time_column: decision_time, trade_date_column: date_value, "n": len(cross), "pearson_ic": pearson, "spearman_ic": spearman})
        ranks = cross[score_column].rank(method="first", pct=True)
        buckets = np.minimum(np.ceil(ranks * quantiles).astype(int), quantiles)
        buckets = pd.Series(buckets, index=cross.index).clip(1, quantiles)
        assigned = cross.assign(_quantile=buckets)
        means = assigned.groupby("_quantile", observed=True)[label_column].mean()
        for quantile, value in means.items():
            quantile_rows.append({**key, time_column: decision_time, trade_date_column: date_value, "quantile": int(quantile), "mean_return": float(value), "n": int((buckets == quantile).sum())})
        raw_decisions.append((decision_time, date_value, assigned, buckets))

    ic_frame = pd.DataFrame(ic_rows)
    if ic_frame.empty:
        metric = _insufficient_metric(
            key,
            data,
            score_column=score_column,
            label_column=label_column,
            symbol_column=symbol_column,
            cost_bps=cost_bps,
            label_overlapping=label_overlapping,
            declared_direction=declared_direction,
        )
        return metric, ic_rows, [], quantile_rows, portfolio_rows, stability_rows
    daily_ic = (
        ic_frame.groupby(trade_date_column, observed=True)[["spearman_ic", "pearson_ic"]]
        .mean()
        .reset_index()
    )
    mean_ic = float(daily_ic["spearman_ic"].mean())
    if declared_direction is None:
        direction = 1 if mean_ic >= 0 else -1
        direction_source = "observed_daily_ic"
    else:
        direction = declared_direction
        direction_source = "predeclared"

    previous_by_date: dict[str, dict[object, float]] = {}
    for decision_time, date_value, assigned, buckets in raw_decisions:
        top = assigned[buckets == quantiles]
        bottom = assigned[buckets == 1]
        raw_return = float(top[label_column].mean() - bottom[label_column].mean())
        if direction > 0:
            long_leg, short_leg = top, bottom
        else:
            long_leg, short_leg = bottom, top
        oriented_return = float(long_leg[label_column].mean() - short_leg[label_column].mean())
        weights = _equal_weight_positions(long_leg[symbol_column], short_leg[symbol_column])
        previous = previous_by_date.get(date_value, {})
        turnover = _traded_notional(previous, weights)
        previous_by_date[date_value] = weights
        row: dict[str, object] = {
            **key,
            time_column: decision_time,
            trade_date_column: date_value,
            "expected_direction": direction,
            "raw_top_minus_bottom_return": raw_return,
            "oriented_long_short_return": oriented_return,
            "turnover": turnover,
            "long_n": len(long_leg),
            "short_n": len(short_leg),
        }
        for bps in cost_bps:
            row[f"net_return_{bps:g}bps"] = oriented_return - turnover * bps / 10000.0
        portfolio_rows.append(row)

    portfolio = pd.DataFrame(portfolio_rows)
    daily_portfolio = portfolio.groupby(trade_date_column, observed=True)["oriented_long_short_return"].mean()
    spearman_values = ic_frame["spearman_ic"].dropna()
    daily_spearman = daily_ic["spearman_ic"].dropna()
    pearson_values = daily_ic["pearson_ic"].dropna()
    ic_t, ic_p = _daily_ttest(daily_spearman)
    ic_std = float(daily_spearman.std(ddof=1)) if len(daily_spearman) > 1 else np.nan
    raw_ls = portfolio["raw_top_minus_bottom_return"].dropna()
    oriented = portfolio["oriented_long_short_return"].dropna()
    daily_t, _ = _daily_ttest(daily_portfolio)
    q_frame = pd.DataFrame(quantile_rows)
    q_mean = q_frame.groupby("quantile", observed=True)["mean_return"].mean() if not q_frame.empty else pd.Series(dtype=float)
    monotonicity = _safe_corr(pd.Series(q_mean.index, dtype=float), q_mean.reset_index(drop=True), "spearman") if len(q_mean) >= 3 else np.nan
    positive_rate = float((daily_spearman > 0).mean()) if len(daily_spearman) else np.nan
    negative_rate = float((daily_spearman < 0).mean()) if len(daily_spearman) else np.nan
    sign_consistency = max(positive_rate, negative_rate) if np.isfinite(positive_rate) and np.isfinite(negative_rate) else np.nan
    annualization_valid = bool(not label_overlapping and annualization_factor and annualization_factor > 0)
    if annualization_valid and len(oriented) > 1 and oriented.std(ddof=1) > 0:
        annualized_sharpe = float(oriented.mean() / oriented.std(ddof=1) * math.sqrt(float(annualization_factor)))
    else:
        annualized_sharpe = np.nan
    daily_diagnostic_sharpe = (
        float(daily_portfolio.mean() / daily_portfolio.std(ddof=1) * math.sqrt(252.0))
        if len(daily_portfolio) > 1 and daily_portfolio.std(ddof=1) > 0
        else np.nan
    )
    metric: dict[str, object] = {
        **key,
        "observations": int(len(data)),
        "decision_count": int(ic_frame[time_column].nunique()),
        "date_count": int(daily_ic[trade_date_column].nunique()),
        "symbol_count": int(data[symbol_column].nunique()),
        "label_mean": float(data[label_column].mean()),
        "label_std": float(data[label_column].std(ddof=1)),
        "score_mean": float(data[score_column].mean()),
        "score_std": float(data[score_column].std(ddof=1)),
        "mean_spearman_ic": float(daily_spearman.mean()) if len(daily_spearman) else np.nan,
        "median_spearman_ic": float(daily_spearman.median()) if len(daily_spearman) else np.nan,
        "spearman_ic_std": ic_std,
        "spearman_icir": float(daily_spearman.mean() / ic_std) if np.isfinite(ic_std) and ic_std > 0 else np.nan,
        "snapshot_ic_positive_rate": float((spearman_values > 0).mean()) if len(spearman_values) else np.nan,
        "daily_ic_positive_rate": positive_rate,
        "daily_ic_negative_rate": negative_rate,
        "daily_ic_sign_consistency": sign_consistency,
        "spearman_ic_tstat_daily": ic_t,
        "spearman_ic_pvalue_daily": ic_p,
        "mean_pearson_ic_daily": float(pearson_values.mean()) if len(pearson_values) else np.nan,
        "expected_direction": direction,
        "direction_source": direction_source,
        "direction_predeclared": direction_predeclared,
        "raw_top_minus_bottom_mean": float(raw_ls.mean()) if len(raw_ls) else np.nan,
        "oriented_long_short_mean": float(oriented.mean()) if len(oriented) else np.nan,
        "oriented_long_short_std": float(oriented.std(ddof=1)) if len(oriented) > 1 else np.nan,
        "oriented_long_short_hit_rate": float((oriented > 0).mean()) if len(oriented) else np.nan,
        "oriented_long_short_tstat_daily": daily_t,
        "annualization_valid": annualization_valid,
        "annualized_sharpe": annualized_sharpe,
        "daily_diagnostic_sharpe": daily_diagnostic_sharpe,
        "label_overlapping": label_overlapping,
        "max_drawdown": _max_drawdown(oriented),
        "var_5pct": float(oriented.quantile(0.05)) if len(oriented) else np.nan,
        "cvar_5pct": float(oriented[oriented <= oriented.quantile(0.05)].mean()) if len(oriented) else np.nan,
        "skew": float(oriented.skew()) if len(oriented) >= 3 else np.nan,
        "kurtosis": float(oriented.kurt()) if len(oriented) >= 4 else np.nan,
        "mean_turnover": float(portfolio["turnover"].mean()) if not portfolio.empty else np.nan,
        "quantile_monotonicity": monotonicity,
    }
    for bps in cost_bps:
        values = portfolio[f"net_return_{bps:g}bps"].dropna()
        metric[f"net_mean_{bps:g}bps"] = float(values.mean()) if len(values) else np.nan
        metric[f"net_daily_diagnostic_sharpe_{bps:g}bps"] = (
            float(
                portfolio.groupby(trade_date_column, observed=True)[f"net_return_{bps:g}bps"].mean().pipe(
                    lambda x: x.mean() / x.std(ddof=1) * math.sqrt(252.0)
                )
            )
            if portfolio[trade_date_column].nunique() > 1
            and portfolio.groupby(trade_date_column, observed=True)[f"net_return_{bps:g}bps"].mean().std(ddof=1) > 0
            else np.nan
        )
    metric["cost_survives_5bps"] = bool(metric.get("net_mean_5bps", np.nan) > 0)
    metric["direction_consistent"] = bool(np.isfinite(sign_consistency) and sign_consistency >= 0.55)
    metric["sample_sufficient"] = bool(metric["date_count"] >= 20 and metric["decision_count"] >= 100 and metric["observations"] >= 500)

    stability_data = data.assign(
        _date=data[trade_date_column].astype(str) if trade_date_column in data.columns else data[time_column].dt.date.astype(str),
        _hour=data[time_column].dt.hour,
    )
    for dimension in ("_date", "_hour", *[column for column in slice_columns if column in stability_data.columns]):
        for value, subset in stability_data.groupby(dimension, observed=True, dropna=False):
            stability_rows.append(
                {
                    **key,
                    "slice_dimension": dimension.removeprefix("_"),
                    "slice_value": str(value),
                    "observations": int(len(subset)),
                    "spearman_ic": _safe_corr(subset[score_column], subset[label_column], "spearman"),
                    "mean_target_return": float(subset[label_column].mean()),
                }
            )
    daily_rows = [{**key, **row} for row in daily_ic.to_dict("records")]
    return metric, ic_rows, daily_rows, quantile_rows, portfolio_rows, stability_rows


def evaluate_alpha(
    frame: pd.DataFrame,
    *,
    score_column: str = "score",
    label_column: str = "target_return",
    time_column: str = "decision_time",
    symbol_column: str = "symbol_id",
    trade_date_column: str = "trade_date",
    quantiles: int = 5,
    annualization_factor: float | None = None,
    min_cross_section: int = 100,
    cost_bps: Iterable[float] = (0.0, 1.0, 2.0, 5.0, 10.0),
    slice_columns: Iterable[str] = (),
    direction_column: str | None = "expected_direction",
    default_direction: str = "auto",
    control_columns: Iterable[str] = (),
    label_overlapping: bool = True,
    governance: dict[str, object] | None = None,
) -> AlphaResult:
    required = {score_column, label_column, time_column, symbol_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Alpha input is missing columns: {missing}")
    if default_direction not in {"auto", "positive", "negative"}:
        raise ValueError("default_direction must be auto, positive or negative")
    data = frame.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    data[score_column] = pd.to_numeric(data[score_column], errors="coerce")
    data[label_column] = pd.to_numeric(data[label_column], errors="coerce")
    if trade_date_column not in data.columns:
        data[trade_date_column] = data[time_column].dt.date.astype(str)
    data = data.dropna(subset=[time_column, symbol_column, score_column, label_column])
    keys = _factor_keys(data)
    metric_rows: list[dict[str, object]] = []
    ic_rows: list[dict[str, object]] = []
    daily_rows: list[dict[str, object]] = []
    quantile_rows: list[dict[str, object]] = []
    portfolio_rows: list[dict[str, object]] = []
    stability_rows: list[dict[str, object]] = []
    # Evaluate each factor independently to keep peak memory bounded when callers stream factor shards.
    for key_values, factor in data.groupby(keys, observed=True, dropna=False, sort=False):
        values = key_values if isinstance(key_values, tuple) else (key_values,)
        key = dict(zip(keys, values))
        metric, factor_ic, factor_daily, factor_quantiles, factor_portfolios, factor_stability = _evaluate_factor(
            factor,
            key=key,
            score_column=score_column,
            label_column=label_column,
            time_column=time_column,
            symbol_column=symbol_column,
            trade_date_column=trade_date_column,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            cost_bps=tuple(float(value) for value in cost_bps),
            slice_columns=tuple(slice_columns),
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=tuple(control_columns),
            label_overlapping=label_overlapping,
            annualization_factor=annualization_factor,
        )
        metric_rows.append(metric)
        ic_rows.extend(factor_ic)
        daily_rows.extend(factor_daily)
        quantile_rows.extend(factor_quantiles)
        portfolio_rows.extend(factor_portfolios)
        stability_rows.extend(factor_stability)
    metrics = pd.DataFrame(metric_rows)
    if not metrics.empty:
        metrics["fdr_qvalue"], metrics["fdr_pass"] = _bh_fdr(metrics["spearman_ic_pvalue_daily"])
        governance_ready = (
            metrics["direction_predeclared"].fillna(False)
            & metrics["annualization_valid"].fillna(False)
        )
        metrics["governance_ready"] = governance_ready
        metrics["research_status"] = np.select(
            [
                metrics["sample_sufficient"]
                & metrics["fdr_pass"]
                & metrics["cost_survives_5bps"]
                & metrics["direction_consistent"]
                & metrics["governance_ready"],
                metrics["sample_sufficient"] & (metrics["mean_spearman_ic"].abs() >= 0.01),
            ],
            ["candidate", "needs_falsification"],
            default="insufficient_or_rejected",
        )
    # Correct redundancy calculation: columns are complete factor identities, not grouped away by layer/variant.
    score_correlation = pd.DataFrame()
    if "factor_id" in data.columns and data["factor_id"].nunique() >= 2:
        identity_columns = [column for column in keys if column != "batch_id"]
        data = data.copy()
        data["_factor_key"] = data[identity_columns].astype(str).agg("|".join, axis=1)
        sampled = data
        if len(sampled) > 2_000_000:
            hashed = pd.util.hash_pandas_object(sampled[[time_column, symbol_column]], index=False)
            sampled = sampled[(hashed % max(1, len(sampled) // 500_000)) == 0]
        pivot = sampled.pivot_table(index=[time_column, symbol_column], columns="_factor_key", values=score_column, aggfunc="mean")
        if pivot.shape[1] >= 2:
            correlation_matrix = pivot.corr(method="spearman")
            # index and columns share the same axis name; stacking would create
            # duplicate index level names and make reset_index raise.
            correlation_matrix.columns = correlation_matrix.columns.rename(None)
            score_correlation = correlation_matrix.stack(dropna=False).rename("score_spearman_correlation").reset_index()
            score_correlation.columns = ["factor_a", "factor_b", "score_spearman_correlation"]
            score_correlation["sample_rows"] = int(len(sampled))
    return AlphaResult(
        metrics=metrics,
        ic_series=pd.DataFrame(ic_rows),
        daily_ic=pd.DataFrame(daily_rows),
        quantile_returns=pd.DataFrame(quantile_rows),
        portfolio_returns=pd.DataFrame(portfolio_rows),
        stability=pd.DataFrame(stability_rows),
        score_correlation=score_correlation,
        governance=governance,
    )
