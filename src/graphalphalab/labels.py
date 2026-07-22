from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class LabelSpec:
    label_id: str
    horizon_minutes: int
    price_column: str = "price"
    entry_lag_minutes: int = 1
    max_entry_delay_seconds: int = 120
    max_exit_delay_seconds: int = 120
    overlapping: bool = True
    rebalance_minutes: int | None = None

    def validate(self) -> None:
        if not self.label_id.strip():
            raise ValueError("label_id must not be empty")
        if self.horizon_minutes <= 0:
            raise ValueError("horizon_minutes must be positive")
        if self.entry_lag_minutes < 0:
            raise ValueError("entry_lag_minutes must be non-negative")
        if self.max_entry_delay_seconds < 0 or self.max_exit_delay_seconds < 0:
            raise ValueError("maximum delays must be non-negative")


def _future_asof(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    query_column: str,
    symbol_column: str,
    time_column: str,
    suffix: str,
) -> pd.DataFrame:
    left_sorted = left.sort_values([query_column, symbol_column])
    right_sorted = right.sort_values([time_column, symbol_column])
    merged = pd.merge_asof(
        left_sorted,
        right_sorted,
        left_on=query_column,
        right_on=time_column,
        by=symbol_column,
        direction="forward",
        allow_exact_matches=True,
        suffixes=("", suffix),
    )
    return merged.sort_index()


def build_forward_return_labels(
    prices: pd.DataFrame,
    specs: Iterable[LabelSpec],
    *,
    symbol_column: str = "symbol_id",
    time_column: str = "timestamp",
    available_time_column: str | None = "available_time",
    decision_times: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build clock-time forward labels with explicit entry/exit timestamps.

    This function intentionally does not use row shifts. Missing minutes therefore do not silently
    turn a 5-row horizon into more than five minutes.
    """
    required = {symbol_column, time_column}
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"Price frame is missing columns: {missing}")
    data = prices.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    if available_time_column and available_time_column in data.columns:
        data[available_time_column] = pd.to_datetime(data[available_time_column], utc=True, errors="coerce")
    data = data.dropna(subset=[symbol_column, time_column]).sort_values([symbol_column, time_column])
    if decision_times is None:
        decisions = data[[symbol_column, time_column]].rename(columns={time_column: "decision_time"}).copy()
    else:
        decisions = decision_times.copy()
        if "decision_time" not in decisions.columns or symbol_column not in decisions.columns:
            raise ValueError(f"decision_times requires {symbol_column!r} and 'decision_time'")
        decisions["decision_time"] = pd.to_datetime(decisions["decision_time"], utc=True, errors="coerce")
        decisions = decisions.dropna(subset=[symbol_column, "decision_time"])
    rows: list[pd.DataFrame] = []
    for spec in specs:
        spec.validate()
        if spec.price_column not in data.columns:
            raise ValueError(f"Price column {spec.price_column!r} is missing")
        base = decisions.copy()
        base["_entry_query"] = base["decision_time"] + pd.to_timedelta(spec.entry_lag_minutes, unit="m")
        price_columns = [symbol_column, time_column, spec.price_column]
        if available_time_column and available_time_column in data.columns:
            price_columns.append(available_time_column)
        entry = _future_asof(
            base,
            data[price_columns],
            query_column="_entry_query",
            symbol_column=symbol_column,
            time_column=time_column,
            suffix="_entry",
        )
        entry = entry.rename(
            columns={
                time_column: "entry_time",
                spec.price_column: "entry_price",
                **({available_time_column: "entry_available_time"} if available_time_column and available_time_column in entry.columns else {}),
            }
        )
        entry["_exit_query"] = entry["entry_time"] + pd.to_timedelta(spec.horizon_minutes, unit="m")
        exit_frame = _future_asof(
            entry,
            data[price_columns],
            query_column="_exit_query",
            symbol_column=symbol_column,
            time_column=time_column,
            suffix="_exit",
        )
        exit_frame = exit_frame.rename(
            columns={
                time_column: "exit_time",
                spec.price_column: "exit_price",
                **({available_time_column: "exit_available_time"} if available_time_column and available_time_column in exit_frame.columns else {}),
            }
        )
        entry_delay = (exit_frame["entry_time"] - exit_frame["_entry_query"]).dt.total_seconds()
        exit_delay = (exit_frame["exit_time"] - exit_frame["_exit_query"]).dt.total_seconds()
        valid = (
            exit_frame["entry_time"].notna()
            & exit_frame["exit_time"].notna()
            & entry_delay.between(0, spec.max_entry_delay_seconds, inclusive="both")
            & exit_delay.between(0, spec.max_exit_delay_seconds, inclusive="both")
            & (exit_frame["entry_time"] > exit_frame["decision_time"])
            & (exit_frame["exit_time"] > exit_frame["entry_time"])
        )
        result = exit_frame.loc[valid, [symbol_column, "decision_time", "entry_time", "exit_time", "entry_price", "exit_price"]].copy()
        result["label_id"] = spec.label_id
        result["horizon_minutes"] = spec.horizon_minutes
        result["entry_lag_minutes"] = spec.entry_lag_minutes
        result["target_return"] = result["exit_price"] / result["entry_price"] - 1.0
        if "exit_available_time" in exit_frame.columns:
            result["label_available_time"] = exit_frame.loc[valid, "exit_available_time"].fillna(result["exit_time"])
        else:
            result["label_available_time"] = result["exit_time"]
        result["overlapping"] = spec.overlapping
        result["rebalance_minutes"] = spec.rebalance_minutes
        rows.append(result)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
