from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Iterable, Mapping

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ExecutionPolicy:
    policy_id: str
    rebalance_minutes: int
    gate_mode: str = "fixed"
    min_rank_change: float = 0.0
    confidence_history_quantile: float = 0.0
    min_theme_retention: float = 0.0
    min_target_turnover: float = 0.0
    max_turnover_per_rebalance: float | None = None

    def validate(self) -> None:
        if self.rebalance_minutes <= 0:
            raise ValueError("rebalance_minutes must be positive")
        if self.gate_mode not in {
            "fixed",
            "rank_change",
            "confidence",
            "theme_stability",
            "combined",
        }:
            raise ValueError(f"Unsupported gate_mode={self.gate_mode!r}")
        if not 0.0 <= self.min_rank_change <= 2.0:
            raise ValueError("min_rank_change must be between 0 and 2")
        if not 0.0 <= self.confidence_history_quantile <= 1.0:
            raise ValueError("confidence_history_quantile must be between 0 and 1")
        if not 0.0 <= self.min_theme_retention <= 1.0:
            raise ValueError("min_theme_retention must be between 0 and 1")
        if self.min_target_turnover < 0.0:
            raise ValueError("min_target_turnover must be non-negative")
        if self.max_turnover_per_rebalance is not None and self.max_turnover_per_rebalance <= 0:
            raise ValueError("max_turnover_per_rebalance must be positive")

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def default_execution_policy_grid() -> tuple[ExecutionPolicy, ...]:
    policies: list[ExecutionPolicy] = [
        ExecutionPolicy(f"fixed_{minutes}m", minutes)
        for minutes in (5, 15, 30, 60, 120, 180)
    ]
    for minutes in (15, 30, 60, 120):
        policies.extend(
            [
                ExecutionPolicy(
                    f"rank_change_{minutes}m",
                    minutes,
                    gate_mode="rank_change",
                    min_rank_change=0.25,
                    min_target_turnover=0.10,
                ),
                ExecutionPolicy(
                    f"confidence_{minutes}m",
                    minutes,
                    gate_mode="confidence",
                    confidence_history_quantile=0.60,
                    min_target_turnover=0.10,
                ),
                ExecutionPolicy(
                    f"combined_{minutes}m",
                    minutes,
                    gate_mode="combined",
                    min_rank_change=0.20,
                    confidence_history_quantile=0.60,
                    min_theme_retention=0.70,
                    min_target_turnover=0.10,
                    max_turnover_per_rebalance=0.50,
                ),
            ]
        )
    for policy in policies:
        policy.validate()
    return tuple(policies)


def _max_drawdown(returns: pd.Series) -> float:
    clean = pd.to_numeric(returns, errors="coerce").dropna().to_numpy(float)
    if len(clean) == 0:
        return np.nan
    wealth = np.cumprod(1.0 + clean)
    peak = np.maximum.accumulate(wealth)
    return float(np.min(wealth / peak - 1.0))


def _daily_diagnostic_sharpe(frame: pd.DataFrame, return_column: str) -> float:
    if frame.empty:
        return np.nan
    daily = frame.groupby("trade_date", observed=True)[return_column].mean()
    std = float(daily.std(ddof=1)) if len(daily) > 1 else np.nan
    if not np.isfinite(std) or std <= 0:
        return np.nan
    return float(daily.mean() / std * math.sqrt(252.0))


def _traded_notional(previous: Mapping[object, float], current: Mapping[object, float]) -> float:
    universe = set(previous) | set(current)
    return float(
        sum(abs(float(current.get(symbol, 0.0)) - float(previous.get(symbol, 0.0))) for symbol in universe)
    )


def _blend_to_turnover_cap(
    previous: Mapping[object, float],
    target: Mapping[object, float],
    cap: float | None,
) -> dict[object, float]:
    if cap is None:
        return dict(target)
    turnover = _traded_notional(previous, target)
    if turnover <= cap or turnover <= 0:
        return dict(target)
    ratio = float(cap / turnover)
    universe = set(previous) | set(target)
    blended = {
        symbol: float(previous.get(symbol, 0.0))
        + ratio * (float(target.get(symbol, 0.0)) - float(previous.get(symbol, 0.0)))
        for symbol in universe
    }
    return {symbol: weight for symbol, weight in blended.items() if abs(weight) > 1e-15}


def _target_weights(
    cross: pd.DataFrame,
    *,
    score_column: str,
    symbol_column: str,
    quantiles: int,
    direction: int,
) -> tuple[dict[object, float], pd.Series]:
    if quantiles < 2:
        raise ValueError("quantiles must be at least 2")
    selected = cross[[symbol_column, score_column]].dropna().copy()
    selected = selected.drop_duplicates(symbol_column, keep="last")
    if len(selected) < quantiles * 2 or selected[score_column].nunique() < 2:
        return {}, pd.Series(dtype=float)
    ranks = selected[score_column].rank(method="average", pct=True)
    bottom = selected[ranks <= 1.0 / quantiles]
    top = selected[ranks > 1.0 - 1.0 / quantiles]
    if top.empty or bottom.empty:
        return {}, pd.Series(dtype=float)
    long_leg, short_leg = (top, bottom) if direction > 0 else (bottom, top)
    weights: dict[object, float] = {}
    for symbol in long_leg[symbol_column]:
        weights[symbol] = weights.get(symbol, 0.0) + 0.5 / len(long_leg)
    for symbol in short_leg[symbol_column]:
        weights[symbol] = weights.get(symbol, 0.0) - 0.5 / len(short_leg)
    rank_series = pd.Series(ranks.to_numpy(float), index=selected[symbol_column].tolist(), dtype=float)
    return weights, rank_series


def _rank_change(previous: pd.Series, current: pd.Series) -> float:
    common = previous.index.intersection(current.index)
    if len(common) < 3:
        return 1.0
    left = previous.loc[common]
    right = current.loc[common]
    if left.nunique() < 2 or right.nunique() < 2:
        return 1.0
    corr = left.corr(right, method="spearman")
    if not np.isfinite(corr):
        return 1.0
    return float(1.0 - corr)


def _theme_retention(
    previous: Mapping[object, object],
    current: Mapping[object, object],
) -> float:
    common = set(previous) & set(current)
    if not common:
        return 1.0
    return float(sum(previous[symbol] == current[symbol] for symbol in common) / len(common))


def _portfolio_return(
    weights: Mapping[object, float],
    returns: Mapping[object, float],
) -> float:
    long_rows = [(weight, returns[symbol]) for symbol, weight in weights.items() if weight > 0 and symbol in returns]
    short_rows = [(-weight, returns[symbol]) for symbol, weight in weights.items() if weight < 0 and symbol in returns]
    if not long_rows or not short_rows:
        return np.nan
    long_weight = sum(weight for weight, _ in long_rows)
    short_weight = sum(weight for weight, _ in short_rows)
    if long_weight <= 0 or short_weight <= 0:
        return np.nan
    long_return = sum(weight * value for weight, value in long_rows) / long_weight
    short_return = sum(weight * value for weight, value in short_rows) / short_weight
    return float(long_return - short_return)


def walk_forward_direction_by_date(
    frame: pd.DataFrame,
    *,
    score_column: str,
    return_column: str,
    time_column: str = "decision_time",
    trade_date_column: str = "trade_date",
    min_train_dates: int = 20,
    rolling_dates: int | None = 60,
) -> dict[str, int]:
    data = frame[[score_column, return_column, time_column, trade_date_column]].copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    data[trade_date_column] = data[trade_date_column].astype(str)
    rows: list[dict[str, object]] = []
    for (trade_date, decision_time), cross in data.groupby(
        [trade_date_column, time_column], observed=True, sort=True
    ):
        clean = cross[[score_column, return_column]].apply(pd.to_numeric, errors="coerce").dropna()
        if len(clean) < 3 or clean[score_column].nunique() < 2 or clean[return_column].nunique() < 2:
            continue
        rows.append(
            {
                "trade_date": str(trade_date),
                "decision_time": decision_time,
                "ic": float(clean[score_column].corr(clean[return_column], method="spearman")),
            }
        )
    if not rows:
        return {}
    daily = pd.DataFrame(rows).groupby("trade_date", observed=True)["ic"].mean().sort_index()
    dates = list(daily.index)
    result: dict[str, int] = {}
    for index, trade_date in enumerate(dates):
        history = daily.iloc[:index]
        if rolling_dates is not None:
            history = history.iloc[-int(rolling_dates) :]
        if len(history) < int(min_train_dates):
            continue
        mean_ic = float(history.mean())
        if np.isfinite(mean_ic) and mean_ic != 0:
            result[str(trade_date)] = 1 if mean_ic > 0 else -1
    return result


def _gate_passes(
    policy: ExecutionPolicy,
    *,
    rank_change: float,
    score_dispersion: float,
    dispersion_history: list[float],
    theme_retention: float,
    target_turnover: float,
) -> tuple[bool, float | None]:
    threshold: float | None = None
    confidence_pass = True
    if policy.confidence_history_quantile > 0:
        clean = [value for value in dispersion_history if np.isfinite(value)]
        if len(clean) >= 20:
            threshold = float(np.quantile(clean, policy.confidence_history_quantile))
            confidence_pass = bool(score_dispersion >= threshold)
    rank_pass = bool(rank_change >= policy.min_rank_change)
    theme_pass = bool(theme_retention >= policy.min_theme_retention)
    turnover_pass = bool(target_turnover >= policy.min_target_turnover)

    if policy.gate_mode == "fixed":
        passed = turnover_pass
    elif policy.gate_mode == "rank_change":
        passed = rank_pass and turnover_pass
    elif policy.gate_mode == "confidence":
        passed = confidence_pass and turnover_pass
    elif policy.gate_mode == "theme_stability":
        passed = theme_pass and turnover_pass
    else:
        passed = rank_pass and confidence_pass and theme_pass and turnover_pass
    return passed, threshold


def evaluate_execution_policy(
    frame: pd.DataFrame,
    policy: ExecutionPolicy,
    *,
    score_column: str,
    return_column: str,
    symbol_column: str = "symbol_id",
    time_column: str = "decision_time",
    trade_date_column: str = "trade_date",
    theme_column: str | None = "context_theme_id",
    quantiles: int = 5,
    fixed_direction: int | None = None,
    direction_by_date: Mapping[str, int] | None = None,
    cost_bps: Iterable[float] = (0.0, 1.0, 2.0, 5.0, 10.0),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    policy.validate()
    if fixed_direction not in {None, -1, 1}:
        raise ValueError("fixed_direction must be -1, 1, or None")
    if fixed_direction is None and not direction_by_date:
        raise ValueError("Use a predeclared fixed_direction or a PIT-safe direction_by_date map")
    required = {score_column, return_column, symbol_column, time_column, trade_date_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Execution frame is missing columns: {missing}")

    data = frame.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    data[trade_date_column] = data[trade_date_column].astype(str)
    data[score_column] = pd.to_numeric(data[score_column], errors="coerce")
    data[return_column] = pd.to_numeric(data[return_column], errors="coerce")
    data = data.dropna(subset=[time_column, score_column, return_column, symbol_column])
    data = data.sort_values([trade_date_column, time_column, symbol_column])

    previous_weights: dict[object, float] = {}
    previous_ranks = pd.Series(dtype=float)
    previous_themes: dict[object, object] = {}
    previous_rebalance_time: pd.Timestamp | None = None
    previous_trade_date: str | None = None
    dispersion_history: list[float] = []
    rows: list[dict[str, object]] = []

    for (trade_date, decision_time), cross in data.groupby(
        [trade_date_column, time_column], observed=True, sort=True
    ):
        trade_date = str(trade_date)
        direction = fixed_direction
        if direction is None:
            direction = int((direction_by_date or {}).get(trade_date, 0))
        if direction not in {-1, 1}:
            continue

        target_weights, current_ranks = _target_weights(
            cross,
            score_column=score_column,
            symbol_column=symbol_column,
            quantiles=quantiles,
            direction=direction,
        )
        if not target_weights:
            continue
        score_dispersion = float(
            cross[score_column].quantile(0.90) - cross[score_column].quantile(0.10)
        )
        rank_change = _rank_change(previous_ranks, current_ranks)
        current_themes: dict[object, object] = {}
        if theme_column and theme_column in cross.columns:
            current_themes = (
                cross[[symbol_column, theme_column]]
                .dropna()
                .drop_duplicates(symbol_column, keep="last")
                .set_index(symbol_column)[theme_column]
                .to_dict()
            )
        retention = _theme_retention(previous_themes, current_themes)
        target_turnover = _traded_notional(previous_weights, target_weights)

        new_date = previous_trade_date is None or trade_date != previous_trade_date
        scheduled = (
            previous_rebalance_time is None
            or new_date
            or (decision_time - previous_rebalance_time).total_seconds()
            >= policy.rebalance_minutes * 60
        )
        gate_pass, confidence_threshold = _gate_passes(
            policy,
            rank_change=rank_change,
            score_dispersion=score_dispersion,
            dispersion_history=dispersion_history,
            theme_retention=retention,
            target_turnover=target_turnover,
        )
        rebalance = bool(scheduled and gate_pass)
        if rebalance:
            next_weights = _blend_to_turnover_cap(
                previous_weights,
                target_weights,
                policy.max_turnover_per_rebalance,
            )
            turnover = _traded_notional(previous_weights, next_weights)
            previous_weights = next_weights
            previous_rebalance_time = decision_time
            previous_ranks = current_ranks
            previous_themes = current_themes
        else:
            turnover = 0.0

        return_map = (
            cross[[symbol_column, return_column]]
            .dropna()
            .drop_duplicates(symbol_column, keep="last")
            .set_index(symbol_column)[return_column]
            .to_dict()
        )
        gross_return = _portfolio_return(previous_weights, return_map)
        if not np.isfinite(gross_return):
            dispersion_history.append(score_dispersion)
            previous_trade_date = trade_date
            continue

        row: dict[str, object] = {
            "policy_id": policy.policy_id,
            "rebalance_minutes": policy.rebalance_minutes,
            "gate_mode": policy.gate_mode,
            "trade_date": trade_date,
            "decision_time": decision_time,
            "direction": direction,
            "scheduled": scheduled,
            "gate_pass": gate_pass,
            "rebalanced": rebalance,
            "score_dispersion": score_dispersion,
            "confidence_threshold": confidence_threshold,
            "rank_change": rank_change,
            "theme_retention": retention,
            "target_turnover": target_turnover,
            "turnover": turnover,
            "gross_return": gross_return,
            "active_positions": len(previous_weights),
        }
        for bps in cost_bps:
            row[f"net_return_{float(bps):g}bps"] = gross_return - turnover * float(bps) / 10000.0
        rows.append(row)
        dispersion_history.append(score_dispersion)
        previous_trade_date = trade_date

    returns = pd.DataFrame(rows)
    if returns.empty:
        metrics = pd.DataFrame(
            [
                {
                    **policy.as_dict(),
                    "decision_count": 0,
                    "date_count": 0,
                    "rebalance_count": 0,
                    "rebalance_rate": np.nan,
                    "mean_turnover": np.nan,
                    "total_turnover": 0.0,
                    "gross_mean": np.nan,
                    "gross_daily_diagnostic_sharpe": np.nan,
                    "max_drawdown": np.nan,
                    "hit_rate": np.nan,
                }
            ]
        )
        return metrics, returns

    metrics_row: dict[str, object] = {
        **policy.as_dict(),
        "decision_count": int(len(returns)),
        "date_count": int(returns["trade_date"].nunique()),
        "rebalance_count": int(returns["rebalanced"].sum()),
        "rebalance_rate": float(returns["rebalanced"].mean()),
        "mean_turnover": float(returns["turnover"].mean()),
        "mean_turnover_when_rebalanced": float(
            returns.loc[returns["rebalanced"], "turnover"].mean()
        )
        if bool(returns["rebalanced"].any())
        else 0.0,
        "total_turnover": float(returns["turnover"].sum()),
        "gross_mean": float(returns["gross_return"].mean()),
        "gross_daily_diagnostic_sharpe": _daily_diagnostic_sharpe(returns, "gross_return"),
        "max_drawdown": _max_drawdown(returns["gross_return"]),
        "hit_rate": float((returns["gross_return"] > 0).mean()),
        "mean_rank_change": float(returns["rank_change"].mean()),
        "mean_theme_retention": float(returns["theme_retention"].mean()),
        "mean_score_dispersion": float(returns["score_dispersion"].mean()),
    }
    for bps in cost_bps:
        column = f"net_return_{float(bps):g}bps"
        metrics_row[f"net_mean_{float(bps):g}bps"] = float(returns[column].mean())
        metrics_row[f"net_daily_diagnostic_sharpe_{float(bps):g}bps"] = _daily_diagnostic_sharpe(
            returns, column
        )
    return pd.DataFrame([metrics_row]), returns


def evaluate_policy_grid(
    frame: pd.DataFrame,
    policies: Iterable[ExecutionPolicy],
    **kwargs: object,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics_rows: list[pd.DataFrame] = []
    return_rows: list[pd.DataFrame] = []
    for policy in policies:
        metrics, returns = evaluate_execution_policy(frame, policy, **kwargs)
        metrics_rows.append(metrics)
        if not returns.empty:
            return_rows.append(returns)
    metrics_frame = pd.concat(metrics_rows, ignore_index=True) if metrics_rows else pd.DataFrame()
    returns_frame = pd.concat(return_rows, ignore_index=True) if return_rows else pd.DataFrame()
    return metrics_frame, returns_frame


def add_frequency_deltas(
    metrics: pd.DataFrame,
    *,
    baseline_policy_id: str = "fixed_5m",
    net_column: str = "net_mean_5bps",
) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    result = metrics.copy()
    baseline = result[result["policy_id"].astype(str) == baseline_policy_id]
    if baseline.empty:
        result["turnover_reduction_vs_baseline"] = np.nan
        result[f"{net_column}_increment_vs_baseline"] = np.nan
        return result
    row = baseline.iloc[0]
    base_turnover = float(row.get("mean_turnover", np.nan))
    base_net = float(row.get(net_column, np.nan))
    result["turnover_reduction_vs_baseline"] = (
        1.0 - result["mean_turnover"] / base_turnover
        if np.isfinite(base_turnover) and base_turnover > 0
        else np.nan
    )
    result[f"{net_column}_increment_vs_baseline"] = result[net_column] - base_net
    return result


def pareto_frontier(
    metrics: pd.DataFrame,
    *,
    return_column: str = "net_mean_5bps",
    turnover_column: str = "mean_turnover",
) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    data = metrics.dropna(subset=[return_column, turnover_column]).copy()
    keep: list[int] = []
    for index, row in data.iterrows():
        dominated = (
            (data[return_column] >= row[return_column])
            & (data[turnover_column] <= row[turnover_column])
            & (
                (data[return_column] > row[return_column])
                | (data[turnover_column] < row[turnover_column])
            )
        ).any()
        if not bool(dominated):
            keep.append(index)
    return data.loc[keep].sort_values(
        [turnover_column, return_column], ascending=[True, False]
    ).reset_index(drop=True)
