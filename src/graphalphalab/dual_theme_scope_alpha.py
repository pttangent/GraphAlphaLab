from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd

from .alpha import AlphaResult, _bh_fdr, evaluate_alpha
from .contracts import LabelContract, PitAudit
from .governance import ResourceBudget, configure_duckdb
from .streaming import _audit_pit, _columns, _factor_keys, _factor_where, _sql_literal, _table_expression


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


def _rank_residualize(
    frame: pd.DataFrame,
    *,
    score_column: str,
    controls: Iterable[str],
    group_columns: Iterable[str],
    min_rows: int,
) -> pd.Series:
    group_list = list(group_columns)
    selected_controls = [column for column in controls if column in frame.columns]
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for _, group in frame.groupby(group_list, observed=True, dropna=False, sort=False):
        columns = [score_column, *selected_controls]
        matrix = (
            group[columns]
            .apply(pd.to_numeric, errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
        )
        valid = matrix.dropna()
        if len(valid) < max(min_rows, len(selected_controls) + 3):
            continue
        y = valid[score_column].rank(method="average", pct=True).to_numpy(float)
        design = [np.ones(len(valid))]
        for control in selected_controls:
            values = valid[control]
            if values.nunique() >= 2:
                design.append(values.rank(method="average", pct=True).to_numpy(float))
        x = np.column_stack(design)
        beta = np.linalg.lstsq(x, y, rcond=None)[0]
        residual = y - x @ beta
        residual = residual - float(np.nanmean(residual))
        result.loc[valid.index] = residual
    return result


def prepare_within_theme_factor(
    frame: pd.DataFrame,
    *,
    score_column: str = "score",
    label_column: str = "target_return",
    time_column: str = "decision_time",
    theme_column: str = "context_theme_id",
    control_columns: Iterable[str] = ("own_score",),
    min_theme_size: int = 5,
) -> pd.DataFrame:
    required = {score_column, label_column, time_column, theme_column, "symbol_id"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Within-theme Alpha input is missing columns: {missing}")
    data = frame.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    data[score_column] = pd.to_numeric(data[score_column], errors="coerce")
    data[label_column] = pd.to_numeric(data[label_column], errors="coerce")
    data = data.dropna(subset=[time_column, theme_column, "symbol_id", score_column, label_column])
    sizes = data.groupby([time_column, theme_column], observed=True)["symbol_id"].transform("nunique")
    data = data[sizes >= int(min_theme_size)].copy()
    if data.empty:
        return data
    data["raw_score"] = data[score_column]
    data[score_column] = _rank_residualize(
        data,
        score_column=score_column,
        controls=control_columns,
        group_columns=(time_column, theme_column),
        min_rows=int(min_theme_size),
    )
    theme_mean = data.groupby([time_column, theme_column], observed=True)[label_column].transform("mean")
    data["raw_target_return"] = data[label_column]
    data[label_column] = data[label_column] - theme_mean
    data["alpha_semantics"] = SCOPE_ALPHA_SEMANTICS["within_theme"]
    return data.dropna(subset=[score_column, label_column])


def prepare_inter_theme_factor(
    frame: pd.DataFrame,
    *,
    score_column: str = "score",
    label_column: str = "target_return",
    time_column: str = "decision_time",
    theme_column: str = "context_theme_id",
    membership_weight_column: str = "membership_weight",
    control_columns: Iterable[str] = ("own_score",),
    min_theme_size: int = 5,
    min_theme_cross_section: int = 5,
) -> pd.DataFrame:
    required = {
        score_column,
        label_column,
        time_column,
        theme_column,
        membership_weight_column,
        "symbol_id",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Inter-theme Alpha input is missing columns: {missing}")
    data = frame.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True, errors="coerce")
    data[score_column] = pd.to_numeric(data[score_column], errors="coerce")
    data[label_column] = pd.to_numeric(data[label_column], errors="coerce")
    data[membership_weight_column] = pd.to_numeric(
        data[membership_weight_column], errors="coerce"
    )
    data = data.dropna(
        subset=[time_column, theme_column, "symbol_id", score_column, label_column]
    )
    data[membership_weight_column] = data[membership_weight_column].fillna(1.0).clip(lower=0.0)
    keys = [
        column
        for column in (
            "batch_id",
            "factor_id",
            "layer_id",
            "scale_minutes",
            "variant_id",
            "trade_date",
            time_column,
            theme_column,
        )
        if column in data.columns
    ]
    score_spread = data.groupby(keys, observed=True)[score_column].agg(lambda values: float(values.max() - values.min()))
    if not score_spread.empty and float(score_spread.max()) > 1e-12:
        raise ValueError(
            "Inter-theme broadcast rows disagree on the theme score; refusing to aggregate "
            f"max_spread={float(score_spread.max())}"
        )
    controls = [column for column in control_columns if column in data.columns]

    def aggregate(group: pd.DataFrame) -> pd.Series:
        weights = pd.to_numeric(group[membership_weight_column], errors="coerce").fillna(0.0)
        positive = weights > 0
        if int(group.loc[positive, "symbol_id"].nunique()) < int(min_theme_size):
            return pd.Series(dtype=object)
        if float(weights.sum()) <= 0:
            weights = pd.Series(1.0, index=group.index)
        weights = weights / float(weights.sum())
        row: dict[str, object] = {
            score_column: float(pd.to_numeric(group[score_column], errors="coerce").iloc[0]),
            label_column: float(np.average(pd.to_numeric(group[label_column], errors="coerce"), weights=weights)),
            "symbol_id": str(group[theme_column].iloc[0]),
            "theme_member_count": int(group["symbol_id"].nunique()),
            "membership_weight_sum": float(pd.to_numeric(group[membership_weight_column], errors="coerce").sum()),
            "signal_available_time": pd.to_datetime(group["signal_available_time"], utc=True).max()
            if "signal_available_time" in group.columns
            else pd.to_datetime(group[time_column], utc=True).iloc[0],
        }
        for control in controls:
            row[control] = float(pd.to_numeric(group[control], errors="coerce").iloc[0])
        if "expected_direction" in group.columns:
            declared = group["expected_direction"].dropna().unique()
            if len(declared) > 1:
                raise ValueError("Inter-theme rows contain conflicting expected directions")
            row["expected_direction"] = declared[0] if len(declared) else None
        return pd.Series(row)

    theme = data.groupby(keys, observed=True, dropna=False, sort=False).apply(
        aggregate, include_groups=False
    ).reset_index()
    if theme.empty or score_column not in theme.columns:
        return pd.DataFrame(columns=[*keys, score_column, label_column, "symbol_id"])
    theme = theme.dropna(subset=[score_column, label_column, "symbol_id"])
    counts = theme.groupby(time_column, observed=True)["symbol_id"].transform("nunique")
    theme = theme[counts >= int(min_theme_cross_section)].copy()
    if theme.empty:
        return theme
    theme["raw_score"] = theme[score_column]
    theme[score_column] = _rank_residualize(
        theme,
        score_column=score_column,
        controls=controls,
        group_columns=(time_column,),
        min_rows=int(min_theme_cross_section),
    )
    theme["alpha_semantics"] = SCOPE_ALPHA_SEMANTICS["inter_theme"]
    return theme.dropna(subset=[score_column, label_column])


def _refresh_governance(result: AlphaResult, audit: PitAudit) -> AlphaResult:
    metrics = result.metrics
    if metrics.empty:
        result.governance = {"pit_audit": audit.as_dict()}
        return result
    metrics["fdr_qvalue"], metrics["fdr_pass"] = _bh_fdr(
        metrics["spearman_ic_pvalue_daily"]
    )
    direction = metrics.get("direction_predeclared", pd.Series(False, index=metrics.index)).fillna(False).astype(bool)
    annualization = metrics.get("annualization_valid", pd.Series(False, index=metrics.index)).fillna(False).astype(bool)
    metrics["governance_ready"] = direction & annualization & bool(audit.passed)
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
    result.governance = {"pit_audit": audit.as_dict()}
    return result


def _concat_results(results: list[AlphaResult], audit: PitAudit) -> AlphaResult:
    if not results:
        return _empty_result({"pit_audit": audit.as_dict()})
    fields = (
        "metrics",
        "ic_series",
        "daily_ic",
        "quantile_returns",
        "portfolio_returns",
        "stability",
        "score_correlation",
    )
    payload = {
        field: pd.concat(
            [getattr(result, field) for result in results if not getattr(result, field).empty],
            ignore_index=True,
        )
        if any(not getattr(result, field).empty for result in results)
        else pd.DataFrame()
        for field in fields
    }
    combined = AlphaResult(**payload, governance={"pit_audit": audit.as_dict()})
    return _refresh_governance(combined, audit)


def evaluate_scope_alpha_streaming(
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
    if symbol_column != "symbol_id":
        raise ValueError("Dual-theme scoped Alpha currently requires symbol_column='symbol_id'")
    label_contract.validate()
    keys_join = list(join_keys)
    connection = duckdb.connect()
    configure_duckdb(connection, resource_budget)
    connection.execute(f"CREATE VIEW all_signals AS SELECT * FROM {_table_expression(signals_path)}")
    connection.execute(
        f"CREATE VIEW signals AS SELECT * FROM all_signals WHERE CAST(scope AS VARCHAR)={_sql_literal(scope)}"
    )
    connection.execute(f"CREATE VIEW labels AS SELECT * FROM {_table_expression(labels_path)}")
    audit = _audit_pit(
        connection,
        label_contract,
        join_keys=keys_join,
        allow_legacy_signals=allow_legacy_signals,
    )
    signal_columns = _columns(connection, "signals")
    label_columns = _columns(connection, "labels")
    required_signal = {*keys_join, "factor_id", "context_theme_id", score_column}
    if scope == "inter_theme":
        required_signal.add("membership_weight")
    missing_signal = sorted(required_signal - signal_columns)
    if missing_signal:
        raise ValueError(f"{scope} signals are missing columns: {missing_signal}")
    missing_label = sorted(set(keys_join) - label_columns)
    if missing_label:
        raise ValueError(f"Labels are missing join columns: {missing_label}")
    factor_keys, factors = _factor_keys(connection)
    select_signal = [
        *factor_keys,
        *keys_join,
        "symbol",
        "symbol_id",
        score_column,
        "context_theme_id",
        "membership_weight",
        "signal_available_time",
        *[column for column in (direction_column, *control_columns) if column and column in signal_columns],
    ]
    select_signal = list(dict.fromkeys(column for column in select_signal if column in signal_columns))
    join_expression = " AND ".join(f's."{key}"=l."{key}"' for key in keys_join)
    label_filter = f"CAST(l.label_id AS VARCHAR)={_sql_literal(label_contract.label_id)}"
    results: list[AlphaResult] = []
    for _, factor_row in factors.iterrows():
        where = _factor_where(factor_keys, factor_row)
        projection = ", ".join(f's."{column}"' for column in select_signal)
        query = f"""
        SELECT {projection},
               l."{label_contract.target_column}" AS target_return,
               l.label_id,
               l."{label_contract.entry_time_column}" AS entry_time,
               l."{label_contract.exit_time_column}" AS exit_time,
               l."{label_contract.available_time_column}" AS label_available_time
        FROM signals s
        JOIN labels l ON {join_expression}
        WHERE {where} AND {label_filter}
        ORDER BY s.trade_date, s.decision_time, s.symbol_id
        """
        frame = connection.execute(query).fetch_df()
        if frame.empty:
            continue
        if scope == "within_theme":
            prepared = prepare_within_theme_factor(
                frame,
                score_column=score_column,
                control_columns=control_columns,
                min_theme_size=min_theme_size,
            )
            evaluation_min_cross_section = min_cross_section
            symbol_column = "symbol_id"
        else:
            prepared = prepare_inter_theme_factor(
                frame,
                score_column=score_column,
                control_columns=control_columns,
                min_theme_size=min_theme_size,
                min_theme_cross_section=min_theme_cross_section,
            )
            evaluation_min_cross_section = min_theme_cross_section
            symbol_column = "symbol_id"
        if prepared.empty:
            continue
        if metadata is not None and scope == "within_theme":
            if metadata_signal_id not in prepared.columns or metadata_id not in metadata.columns:
                raise ValueError(
                    f"Metadata join columns missing: signals={metadata_signal_id!r}, metadata={metadata_id!r}"
                )
            prepared = prepared.merge(
                metadata,
                left_on=metadata_signal_id,
                right_on=metadata_id,
                how="left",
                validate="many_to_one",
            )
        result = evaluate_alpha(
            prepared,
            score_column=score_column,
            label_column="target_return",
            time_column="decision_time",
            symbol_column=symbol_column,
            trade_date_column="trade_date",
            quantiles=quantiles,
            annualization_factor=annualization_factor,
            min_cross_section=evaluation_min_cross_section,
            slice_columns=slice_columns if scope == "within_theme" else (),
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=(),
            label_overlapping=label_contract.overlapping,
            governance={
                "pit_audit": audit.as_dict(),
                "label_contract": label_contract.as_dict(),
                "alpha_semantics": SCOPE_ALPHA_SEMANTICS[scope],
            },
        )
        for output in (
            result.metrics,
            result.ic_series,
            result.daily_ic,
            result.quantile_returns,
            result.portfolio_returns,
            result.stability,
        ):
            if not output.empty:
                output["scope"] = scope
                output["alpha_semantics"] = SCOPE_ALPHA_SEMANTICS[scope]
        if not result.metrics.empty:
            result.metrics["cost_semantics"] = (
                "constituent_stock_turnover"
                if scope == "within_theme"
                else "theme_notional_turnover_diagnostic_requires_constituent_execution_overlay"
            )
        results.append(result)
    return _concat_results(results, audit)


def combine_alpha_results(
    results: Iterable[AlphaResult],
    *,
    governance: dict[str, object] | None = None,
) -> AlphaResult:
    rows = tuple(results)
    fields = (
        "metrics",
        "ic_series",
        "daily_ic",
        "quantile_returns",
        "portfolio_returns",
        "stability",
        "score_correlation",
    )
    payload: dict[str, pd.DataFrame] = {}
    for field in fields:
        frames = [getattr(row, field) for row in rows if not getattr(row, field).empty]
        payload[field] = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return AlphaResult(
        **payload,
        governance=governance or {
            "component_governance": [row.governance for row in rows],
        },
    )


def evaluate_dual_theme_scope_streaming(
    signals_path: str | Path,
    labels_path: str | Path,
    **kwargs: object,
) -> AlphaResult:
    return evaluate_scope_alpha_streaming(signals_path, labels_path, **kwargs)
