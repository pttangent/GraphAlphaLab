from __future__ import annotations

import gc
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd

from .alpha import AlphaResult, _bh_fdr, evaluate_alpha
from .contracts import LabelContract
from .governance import ResourceBudget, configure_duckdb
from .streaming import (
    _audit_pit,
    _columns,
    _factor_keys,
    _factor_where,
    _sql_literal,
    _table_expression,
)


SCOPE_ALPHA_SEMANTICS = {
    "within_theme": "stock_within_theme_neutral",
    "inter_theme": "theme_portfolio_weighted_member_return",
}


def _empty_result(governance: dict[str, object] | None = None) -> AlphaResult:
    return AlphaResult(
        metrics=pd.DataFrame(),
        ic_series=pd.DataFrame(),
        daily_ic=pd.DataFrame(),
        quantile_returns=pd.DataFrame(),
        portfolio_returns=pd.DataFrame(),
        stability=pd.DataFrame(),
        score_correlation=pd.DataFrame(),
        governance=governance,
    )


def _concat_frames(results: Iterable[AlphaResult], attribute: str) -> pd.DataFrame:
    frames = [
        getattr(result, attribute)
        for result in results
        if not getattr(result, attribute).empty
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _refresh_governance(metrics: pd.DataFrame, *, pit_passed: bool) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    metrics = metrics.copy()
    metrics["fdr_qvalue"], metrics["fdr_pass"] = _bh_fdr(
        metrics["spearman_ic_pvalue_daily"]
    )
    metrics["governance_ready"] = (
        metrics["direction_predeclared"].fillna(False)
        & metrics["annualization_valid"].fillna(False)
        & bool(pit_passed)
    )
    metrics["research_status"] = np.select(
        [
            metrics["sample_sufficient"]
            & metrics["fdr_pass"]
            & metrics["cost_survives_5bps"]
            & metrics["direction_consistent"]
            & metrics["governance_ready"],
            metrics["sample_sufficient"]
            & (metrics["mean_spearman_ic"].abs() >= 0.01),
        ],
        ["candidate", "needs_falsification"],
        default="insufficient_or_rejected",
    )
    return metrics


def combine_alpha_results(
    results: Iterable[AlphaResult],
    *,
    governance: dict[str, object] | None = None,
) -> AlphaResult:
    rows = list(results)
    pit_passed = all(
        bool(
            (result.governance or {})
            .get("pit_audit", {})
            .get("passed", True)
        )
        for result in rows
    )
    return AlphaResult(
        metrics=_refresh_governance(
            _concat_frames(rows, "metrics"),
            pit_passed=pit_passed,
        ),
        ic_series=_concat_frames(rows, "ic_series"),
        daily_ic=_concat_frames(rows, "daily_ic"),
        quantile_returns=_concat_frames(rows, "quantile_returns"),
        portfolio_returns=_concat_frames(rows, "portfolio_returns"),
        stability=_concat_frames(rows, "stability"),
        score_correlation=_concat_frames(rows, "score_correlation"),
        governance=governance or {
            "pit_passed": pit_passed,
            "component_count": len(rows),
        },
    )


def _rank_residualize(
    frame: pd.DataFrame,
    *,
    group_columns: list[str],
    score_column: str,
    control_columns: list[str],
    min_group_size: int,
) -> pd.Series:
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for _, group in frame.groupby(
        group_columns,
        observed=True,
        sort=False,
        dropna=False,
    ):
        values = group[[score_column, *control_columns]].apply(
            pd.to_numeric,
            errors="coerce",
        )
        valid = values.dropna(subset=[score_column])
        if len(valid) < min_group_size:
            continue
        y = valid[score_column].rank(method="average", pct=True).to_numpy(float)
        columns = [np.ones(len(valid))]
        for control in control_columns:
            series = valid[control]
            if series.notna().sum() < min_group_size or series.nunique(dropna=True) < 2:
                continue
            ranked = series.rank(method="average", pct=True)
            ranked = ranked.fillna(ranked.mean())
            columns.append(ranked.to_numpy(float))
        x = np.column_stack(columns)
        beta = np.linalg.lstsq(x, y, rcond=None)[0]
        result.loc[valid.index] = y - x @ beta
    return result


def _prepare_within_factor(
    factor: pd.DataFrame,
    *,
    score_column: str,
    control_columns: Iterable[str],
    min_theme_size: int,
) -> pd.DataFrame:
    if "context_theme_id" not in factor.columns:
        raise ValueError(
            "Within-Theme signals require context_theme_id. Re-export with "
            "GAL_DUAL_THEME_GFF_EXPORT_V2_SCOPE_SEMANTICS."
        )
    data = factor.copy()
    data["context_theme_id"] = data["context_theme_id"].astype("string")
    data = data.dropna(subset=["context_theme_id", score_column, "target_return"])
    group_columns = ["decision_time", "context_theme_id"]
    group_size = data.groupby(group_columns, observed=True)[score_column].transform(
        "size"
    )
    data = data[group_size >= int(min_theme_size)].copy()
    if data.empty:
        return data
    selected_controls = [
        column for column in control_columns if column in data.columns
    ]
    data["scope_score"] = _rank_residualize(
        data,
        group_columns=group_columns,
        score_column=score_column,
        control_columns=selected_controls,
        min_group_size=int(min_theme_size),
    )
    target = pd.to_numeric(data["target_return"], errors="coerce")
    data["target_return"] = target
    data["scope_target_return"] = target - data.groupby(
        group_columns,
        observed=True,
    )["target_return"].transform("mean")
    data["scope_alpha_unit"] = SCOPE_ALPHA_SEMANTICS["within_theme"]
    data["scope_member_count"] = group_size.loc[data.index].astype(int)
    return data.dropna(subset=["scope_score", "scope_target_return"])


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce")
    weight = pd.to_numeric(weights, errors="coerce")
    valid = numeric.notna()
    numeric = numeric[valid]
    weight = weight[valid].fillna(0.0).clip(lower=0.0)
    if numeric.empty:
        return np.nan
    if float(weight.sum()) <= 0:
        return float(numeric.mean())
    return float(np.average(numeric.to_numpy(float), weights=weight.to_numpy(float)))


def _prepare_inter_factor(
    factor: pd.DataFrame,
    *,
    score_column: str,
    control_columns: Iterable[str],
    min_theme_cross_section: int,
    min_theme_size: int = 1,
) -> pd.DataFrame:
    if "context_theme_id" not in factor.columns:
        raise ValueError(
            "Inter-Theme signals require context_theme_id. Re-export governed "
            "dual-theme signals."
        )
    data = factor.copy()
    data["context_theme_id"] = data["context_theme_id"].astype("string")
    data = data.dropna(subset=["context_theme_id", score_column, "target_return"])
    if data.empty:
        return data
    if "membership_weight" not in data.columns:
        data["membership_weight"] = 1.0
    identity_columns = [
        column
        for column in (
            "batch_id",
            "factor_id",
            "layer_id",
            "scale_minutes",
            "variant_id",
            "trade_date",
            "decision_time",
            "context_theme_id",
        )
        if column in data.columns
    ]
    optional_columns = [
        column
        for column in (
            "signal_available_time",
            "expected_direction",
            *control_columns,
        )
        if column in data.columns
    ]
    rows: list[dict[str, object]] = []
    for key_values, group in data.groupby(
        identity_columns,
        observed=True,
        sort=False,
        dropna=False,
    ):
        values = key_values if isinstance(key_values, tuple) else (key_values,)
        row = dict(zip(identity_columns, values))
        member_weight = pd.to_numeric(
            group["membership_weight"],
            errors="coerce",
        ).fillna(0.0)
        row[score_column] = _weighted_mean(group[score_column], member_weight)
        row["target_return"] = _weighted_mean(
            group["target_return"],
            member_weight,
        )
        for column in optional_columns:
            if column in control_columns:
                row[column] = _weighted_mean(group[column], member_weight)
            else:
                non_null = group[column].dropna()
                if column == "expected_direction" and non_null.nunique() > 1:
                    raise ValueError(
                        "Inter-Theme rows contain conflicting expected_direction "
                        f"inside context_theme_id={row.get('context_theme_id')!r}"
                    )
                row[column] = (
                    non_null.max()
                    if column == "signal_available_time" and not non_null.empty
                    else non_null.iloc[0] if not non_null.empty else None
                )
        score_values = pd.to_numeric(group[score_column], errors="coerce").dropna()
        row["broadcast_score_spread"] = (
            float(score_values.max() - score_values.min())
            if not score_values.empty
            else np.nan
        )
        row["scope_member_count"] = int(group["symbol_id"].nunique())
        if int(row["scope_member_count"]) < int(min_theme_size):
            continue
        row["symbol_id"] = str(row["context_theme_id"])
        rows.append(row)
    themes = pd.DataFrame(rows)
    if themes.empty:
        return themes
    spread = pd.to_numeric(
        themes["broadcast_score_spread"],
        errors="coerce",
    ).fillna(0.0)
    if bool((spread.abs() > 1e-12).any()):
        raise ValueError(
            "Inter-Theme broadcast produced non-identical stock scores inside "
            "the same context_theme_id; refusing ambiguous theme aggregation."
        )
    selected_controls = [
        column for column in control_columns if column in themes.columns
    ]
    themes["scope_score"] = _rank_residualize(
        themes,
        group_columns=["decision_time"],
        score_column=score_column,
        control_columns=selected_controls,
        min_group_size=int(min_theme_cross_section),
    )
    themes["scope_target_return"] = pd.to_numeric(
        themes["target_return"],
        errors="coerce",
    )
    themes["scope_alpha_unit"] = SCOPE_ALPHA_SEMANTICS["inter_theme"]
    return themes.dropna(subset=["scope_score", "scope_target_return"])


def evaluate_dual_theme_scope_streaming(
    signals_path: str | Path,
    labels_path: str | Path,
    *,
    scope: str,
    label_contract: LabelContract,
    join_keys: Iterable[str] = ("trade_date", "decision_time", "symbol_id"),
    metadata: pd.DataFrame | None = None,
    metadata_signal_id: str = "symbol_id",
    metadata_id: str = "symbol_id",
    slice_columns: Iterable[str] = (),
    score_column: str = "score",
    symbol_column: str = "symbol_id",
    quantiles: int = 5,
    min_cross_section: int = 100,
    min_theme_size: int = 5,
    min_theme_cross_section: int = 5,
    direction_column: str | None = "expected_direction",
    default_direction: str = "auto",
    control_columns: Iterable[str] = ("own_score",),
    annualization_factor: float | None = None,
    resource_budget: ResourceBudget = ResourceBudget(),
    allow_legacy_signals: bool = False,
) -> AlphaResult:
    if scope not in SCOPE_ALPHA_SEMANTICS:
        raise ValueError(f"Unsupported specialized scope: {scope!r}")
    label_contract.validate()
    keys_join = list(join_keys)
    controls = list(control_columns)
    connection = duckdb.connect()
    configure_duckdb(connection, resource_budget)
    connection.execute(
        f"CREATE VIEW signals AS SELECT * FROM {_table_expression(signals_path)}"
    )
    connection.execute(
        f"CREATE VIEW labels AS SELECT * FROM {_table_expression(labels_path)}"
    )
    pit_audit = _audit_pit(
        connection,
        label_contract,
        join_keys=keys_join,
        allow_legacy_signals=allow_legacy_signals,
    )
    signal_columns = _columns(connection, "signals")
    required_signal = {"context_theme_id", "membership_weight"}
    missing_scope = sorted(required_signal - signal_columns)
    if missing_scope:
        raise ValueError(
            f"{scope} signals are missing scope semantics columns: {missing_scope}. "
            "Re-export the GFF campaign with the V2 GAL exporter."
        )
    factor_keys, factors = _factor_keys(connection)
    if factors.empty:
        return _empty_result(
            {
                "pit_audit": pit_audit.as_dict(),
                "scope": scope,
                "scope_semantics": SCOPE_ALPHA_SEMANTICS[scope],
            }
        )
    select_signal = [
        *factor_keys,
        *keys_join,
        symbol_column,
        score_column,
        "context_theme_id",
        "membership_weight",
        *[
            column
            for column in (
                "signal_available_time",
                direction_column,
                *controls,
            )
            if column and column in signal_columns
        ],
    ]
    select_signal = list(dict.fromkeys(select_signal))
    join_expression = " AND ".join(
        f's."{key}"=l."{key}"' for key in keys_join
    )
    label_filter = (
        f"CAST(l.label_id AS VARCHAR)={_sql_literal(label_contract.label_id)}"
    )
    results: list[AlphaResult] = []
    for _, factor_row in factors.iterrows():
        where = _factor_where(factor_keys, factor_row)
        signal_projection = ", ".join(
            f's."{column}"' for column in select_signal
        )
        query = f"""
        SELECT {signal_projection},
               l."{label_contract.target_column}" AS target_return,
               l.label_id,
               l."{label_contract.entry_time_column}" AS entry_time,
               l."{label_contract.exit_time_column}" AS exit_time,
               l."{label_contract.available_time_column}" AS label_available_time
        FROM signals s
        JOIN labels l ON {join_expression}
        WHERE {where} AND {label_filter}
        ORDER BY s.trade_date, s.decision_time, s."{symbol_column}"
        """
        factor = connection.execute(query).fetch_df()
        if factor.empty:
            continue
        if scope == "within_theme":
            if metadata is not None:
                factor = factor.merge(
                    metadata,
                    left_on=metadata_signal_id,
                    right_on=metadata_id,
                    how="left",
                    validate="many_to_one",
                )
            prepared = _prepare_within_factor(
                factor,
                score_column=score_column,
                control_columns=controls,
                min_theme_size=min_theme_size,
            )
            scope_min_cross_section = int(min_cross_section)
            scope_symbol_column = symbol_column
            scope_slices = list(slice_columns)
        else:
            prepared = _prepare_inter_factor(
                factor,
                score_column=score_column,
                control_columns=controls,
                min_theme_cross_section=min_theme_cross_section,
                min_theme_size=min_theme_size,
            )
            scope_min_cross_section = int(min_theme_cross_section)
            scope_symbol_column = "symbol_id"
            scope_slices = []
        if prepared.empty:
            continue
        result = evaluate_alpha(
            prepared,
            score_column="scope_score",
            label_column="scope_target_return",
            time_column="decision_time",
            symbol_column=scope_symbol_column,
            trade_date_column="trade_date",
            quantiles=quantiles,
            annualization_factor=annualization_factor,
            min_cross_section=scope_min_cross_section,
            slice_columns=scope_slices,
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=(),
            label_overlapping=label_contract.overlapping,
            governance={
                "pit_audit": pit_audit.as_dict(),
                "label_contract": label_contract.as_dict(),
                "scope": scope,
                "scope_semantics": SCOPE_ALPHA_SEMANTICS[scope],
            },
        )
        for frame in (
            result.metrics,
            result.ic_series,
            result.daily_ic,
            result.quantile_returns,
            result.portfolio_returns,
            result.stability,
        ):
            if not frame.empty:
                frame["scope_alpha_unit"] = SCOPE_ALPHA_SEMANTICS[scope]
        if not result.metrics.empty:
            result.metrics["scope"] = scope
            result.metrics["inter_theme_cost_semantics"] = (
                "theme_portfolio_notional"
                if scope == "inter_theme"
                else "stock_turnover"
            )
        results.append(result)
        del factor, prepared, result
        gc.collect()
    if not results:
        return _empty_result(
            {
                "pit_audit": pit_audit.as_dict(),
                "scope": scope,
                "scope_semantics": SCOPE_ALPHA_SEMANTICS[scope],
            }
        )
    combined = combine_alpha_results(
        results,
        governance={
            "pit_audit": pit_audit.as_dict(),
            "label_contract": label_contract.as_dict(),
            "scope": scope,
            "scope_semantics": SCOPE_ALPHA_SEMANTICS[scope],
            "factor_count": int(len(factors)),
            "streaming_mode": "factor_sequential_duckdb_scope_aware",
            "resource_budget": resource_budget.as_dict(),
        },
    )
    return combined


prepare_within_theme_factor = _prepare_within_factor
prepare_inter_theme_factor = _prepare_inter_factor
