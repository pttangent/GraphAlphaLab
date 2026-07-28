from __future__ import annotations

import math
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from .execution_frequency import (
    ExecutionPolicy,
    _blend_to_turnover_cap,
    _max_drawdown,
    _portfolio_return,
    _rank_change,
    _target_weights,
    _theme_retention,
    _traded_notional,
)


def _daily_sharpe(frame: pd.DataFrame, column: str) -> float:
    if frame.empty:
        return np.nan
    daily = frame.groupby("trade_date", observed=True)[column].mean()
    if len(daily) < 2:
        return np.nan
    std = float(daily.std(ddof=1))
    return (
        float(daily.mean() / std * math.sqrt(252.0))
        if np.isfinite(std) and std > 0
        else np.nan
    )


def _gate_components(
    policy: ExecutionPolicy,
    *,
    rank_change: float,
    score_dispersion: float,
    dispersion_history: list[float],
    theme_retention: float,
    target_turnover: float,
) -> tuple[bool, float | None, dict[str, bool]]:
    threshold: float | None = None
    confidence_pass = True
    if policy.confidence_history_quantile > 0:
        clean = [value for value in dispersion_history if np.isfinite(value)]
        if len(clean) >= 20:
            threshold = float(
                np.quantile(clean, policy.confidence_history_quantile)
            )
            confidence_pass = bool(score_dispersion >= threshold)
    rank_pass = bool(rank_change >= policy.min_rank_change)
    theme_pass = bool(theme_retention >= policy.min_theme_retention)
    turnover_pass = bool(target_turnover >= policy.min_target_turnover)
    components = {
        "rank_gate_pass": rank_pass,
        "confidence_gate_pass": confidence_pass,
        "theme_gate_pass": theme_pass,
        "turnover_gate_pass": turnover_pass,
    }
    if policy.gate_mode == "fixed":
        passed = turnover_pass
    elif policy.gate_mode == "rank_change":
        passed = rank_pass and turnover_pass
    elif policy.gate_mode == "confidence":
        passed = confidence_pass and turnover_pass
    elif policy.gate_mode == "theme_stability":
        passed = theme_pass and turnover_pass
    elif policy.gate_mode == "combined":
        passed = rank_pass and confidence_pass and theme_pass and turnover_pass
    else:
        raise ValueError(f"Unsupported gate_mode={policy.gate_mode!r}")
    return passed, threshold, components


def evaluate_execution_policy_v2(
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
    carry_overnight: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    policy.validate()
    if fixed_direction not in {None, -1, 1}:
        raise ValueError("fixed_direction must be -1, 1, or None")
    if fixed_direction is None and not direction_by_date:
        raise ValueError(
            "Use a predeclared fixed_direction or a PIT-safe direction_by_date map"
        )
    required = {
        score_column,
        return_column,
        symbol_column,
        time_column,
        trade_date_column,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Execution frame is missing columns: {missing}")

    data = frame.copy()
    data[time_column] = pd.to_datetime(
        data[time_column], utc=True, errors="coerce"
    )
    data[trade_date_column] = data[trade_date_column].astype(str)
    data[score_column] = pd.to_numeric(data[score_column], errors="coerce")
    data[return_column] = pd.to_numeric(data[return_column], errors="coerce")
    data = data.dropna(
        subset=[time_column, score_column, return_column, symbol_column]
    )
    data = data.sort_values(
        [trade_date_column, time_column, symbol_column]
    )

    previous_weights: dict[object, float] = {}
    previous_ranks = pd.Series(dtype=float)
    previous_themes: dict[object, object] = {}
    previous_rebalance_time: pd.Timestamp | None = None
    previous_trade_date: str | None = None
    dispersion_history: list[float] = []
    rows: list[dict[str, object]] = []

    for (trade_date, decision_time), cross in data.groupby(
        [trade_date_column, time_column],
        observed=True,
        sort=True,
    ):
        trade_date = str(trade_date)
        new_date = previous_trade_date is None or trade_date != previous_trade_date
        if new_date and not carry_overnight:
            previous_weights = {}
            previous_ranks = pd.Series(dtype=float)
            previous_themes = {}
            previous_rebalance_time = None

        direction = fixed_direction
        if direction is None:
            direction = int((direction_by_date or {}).get(trade_date, 0))
        if direction not in {-1, 1}:
            previous_trade_date = trade_date
            continue

        target_weights, current_ranks = _target_weights(
            cross,
            score_column=score_column,
            symbol_column=symbol_column,
            quantiles=quantiles,
            direction=direction,
        )
        if not target_weights:
            previous_trade_date = trade_date
            continue
        score_dispersion = float(
            cross[score_column].quantile(0.90)
            - cross[score_column].quantile(0.10)
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
        scheduled = (
            previous_rebalance_time is None
            or (decision_time - previous_rebalance_time).total_seconds()
            >= policy.rebalance_minutes * 60
        )
        gate_pass, confidence_threshold, gate_components = _gate_components(
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
        dispersion_history.append(score_dispersion)
        previous_trade_date = trade_date
        if not np.isfinite(gross_return):
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
            **gate_components,
            "score_dispersion": score_dispersion,
            "confidence_threshold": confidence_threshold,
            "rank_change": rank_change,
            "theme_retention": retention,
            "target_turnover": target_turnover,
            "turnover": turnover,
            "gross_return": gross_return,
            "active_positions": len(previous_weights),
            "carry_overnight": bool(carry_overnight),
        }
        for bps in cost_bps:
            row[f"net_return_{float(bps):g}bps"] = (
                gross_return - turnover * float(bps) / 10000.0
            )
        rows.append(row)

    returns = pd.DataFrame(rows)
    return (
        summarize_execution_returns(policy, returns, cost_bps=cost_bps),
        returns,
    )


def summarize_execution_returns(
    policy: ExecutionPolicy,
    returns: pd.DataFrame,
    *,
    cost_bps: Iterable[float] = (0.0, 1.0, 2.0, 5.0, 10.0),
) -> pd.DataFrame:
    if returns.empty:
        return pd.DataFrame(
            [
                {
                    **policy.as_dict(),
                    "decision_count": 0,
                    "date_count": 0,
                    "scheduled_count": 0,
                    "rebalance_count": 0,
                    "rebalance_rate": np.nan,
                    "gate_pass_rate": np.nan,
                    "mean_turnover": np.nan,
                    "total_turnover": 0.0,
                    "gross_mean": np.nan,
                    "gross_daily_diagnostic_sharpe": np.nan,
                    "max_drawdown": np.nan,
                    "hit_rate": np.nan,
                    "mean_minutes_between_rebalances": np.nan,
                }
            ]
        )
    rebalanced = returns[returns["rebalanced"]].sort_values(
        ["trade_date", "decision_time"]
    )
    intervals: list[float] = []
    for _, group in rebalanced.groupby("trade_date", observed=True):
        times = pd.to_datetime(
            group["decision_time"], utc=True, errors="coerce"
        ).dropna()
        if len(times) > 1:
            intervals.extend(
                times.diff()
                .dropna()
                .dt.total_seconds()
                .div(60.0)
                .tolist()
            )
    row: dict[str, object] = {
        **policy.as_dict(),
        "decision_count": int(len(returns)),
        "date_count": int(returns["trade_date"].nunique()),
        "scheduled_count": int(returns["scheduled"].sum()),
        "rebalance_count": int(returns["rebalanced"].sum()),
        "rebalance_rate": float(returns["rebalanced"].mean()),
        "gate_pass_rate": float(returns["gate_pass"].mean()),
        "mean_turnover": float(returns["turnover"].mean()),
        "mean_turnover_when_rebalanced": (
            float(rebalanced["turnover"].mean())
            if not rebalanced.empty
            else 0.0
        ),
        "total_turnover": float(returns["turnover"].sum()),
        "gross_mean": float(returns["gross_return"].mean()),
        "gross_daily_diagnostic_sharpe": _daily_sharpe(
            returns,
            "gross_return",
        ),
        "max_drawdown": _max_drawdown(returns["gross_return"]),
        "hit_rate": float((returns["gross_return"] > 0).mean()),
        "mean_rank_change": float(returns["rank_change"].mean()),
        "mean_theme_retention": float(
            returns["theme_retention"].mean()
        ),
        "mean_score_dispersion": float(
            returns["score_dispersion"].mean()
        ),
        "mean_minutes_between_rebalances": (
            float(np.mean(intervals)) if intervals else np.nan
        ),
    }
    for gate in (
        "rank_gate_pass",
        "confidence_gate_pass",
        "theme_gate_pass",
        "turnover_gate_pass",
    ):
        row[f"{gate}_rate"] = (
            float(returns[gate].mean()) if gate in returns else np.nan
        )
    for bps in cost_bps:
        column = f"net_return_{float(bps):g}bps"
        row[f"net_mean_{float(bps):g}bps"] = float(
            returns[column].mean()
        )
        row[f"net_daily_diagnostic_sharpe_{float(bps):g}bps"] = (
            _daily_sharpe(returns, column)
        )
    return pd.DataFrame([row])


def evaluate_policy_grid_v2(
    frame: pd.DataFrame,
    policies: Iterable[ExecutionPolicy],
    **kwargs: object,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics: list[pd.DataFrame] = []
    returns: list[pd.DataFrame] = []
    for policy in policies:
        metric, result = evaluate_execution_policy_v2(
            frame,
            policy,
            **kwargs,
        )
        metrics.append(metric)
        if not result.empty:
            returns.append(result)
    return (
        pd.concat(metrics, ignore_index=True)
        if metrics
        else pd.DataFrame(),
        pd.concat(returns, ignore_index=True)
        if returns
        else pd.DataFrame(),
    )
