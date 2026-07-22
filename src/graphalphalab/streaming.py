from __future__ import annotations

from pathlib import Path
from typing import Iterable
import gc

import duckdb
import numpy as np
import pandas as pd

from .alpha import AlphaResult, _bh_fdr, evaluate_alpha
from .contracts import LabelContract, PitAudit
from .governance import ResourceBudget, configure_duckdb


def _sql_literal(value: object) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "NULL"
    if isinstance(value, (int, float, np.integer, np.floating)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _table_expression(path: str | Path) -> str:
    resolved = Path(path).expanduser().resolve()
    if resolved.is_dir():
        pattern = (resolved / "**" / "data.parquet").as_posix().replace("'", "''")
    else:
        pattern = resolved.as_posix().replace("'", "''")
    return f"read_parquet('{pattern}', union_by_name=true, hive_partitioning=false)"


def _columns(connection: duckdb.DuckDBPyConnection, view: str) -> set[str]:
    rows = connection.execute(f"DESCRIBE {view}").fetchall()
    return {str(row[0]) for row in rows}


def _combine_audits(signal: tuple[int, int, int], label: tuple[int, int, int, int, int, int]) -> PitAudit:
    return PitAudit(
        signal_rows=int(signal[0]),
        signal_available_time_missing=int(signal[1]),
        signal_after_decision=int(signal[2]),
        label_rows=int(label[0]),
        duplicate_label_keys=int(label[1]),
        entry_not_after_decision=int(label[2]),
        exit_not_after_entry=int(label[3]),
        label_available_before_exit=int(label[4]),
        horizon_mismatch=int(label[5]),
    )


def _audit_pit(
    connection: duckdb.DuckDBPyConnection,
    contract: LabelContract,
    *,
    join_keys: list[str],
    allow_legacy_signals: bool,
) -> PitAudit:
    signal_columns = _columns(connection, "signals")
    if "signal_available_time" not in signal_columns:
        if not allow_legacy_signals:
            raise ValueError(
                "Signals are missing signal_available_time. Export governed signals with `gal export-gff-signals`, "
                "or use --allow-legacy-signals only for non-governed diagnostics."
            )
        signal = connection.execute("SELECT count(*), count(*), 0 FROM signals").fetchone()
    else:
        signal = connection.execute(
            """
            SELECT count(*),
                   sum(CASE WHEN signal_available_time IS NULL THEN 1 ELSE 0 END),
                   sum(CASE WHEN signal_available_time > decision_time THEN 1 ELSE 0 END)
            FROM signals
            """
        ).fetchone()
    label_columns = _columns(connection, "labels")
    required = {
        *join_keys,
        "label_id",
        contract.target_column,
        contract.decision_time_column,
        contract.entry_time_column,
        contract.exit_time_column,
        contract.available_time_column,
    }
    missing = sorted(required - label_columns)
    if missing:
        raise ValueError(f"Labels are missing governed contract columns: {missing}")
    keys_sql = ", ".join(f'"{key}"' for key in join_keys)
    contract_filter = f"CAST(label_id AS VARCHAR)={_sql_literal(contract.label_id)}"
    duplicate = connection.execute(
        f"""
        SELECT coalesce(sum(row_count), 0)
        FROM (
          SELECT count(*) AS row_count
          FROM labels
          WHERE {contract_filter}
          GROUP BY {keys_sql}
          HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    entry_op = "<=" if contract.require_entry_after_decision else "<"
    label = connection.execute(
        f"""
        SELECT
          count(*),
          {int(duplicate or 0)},
          sum(CASE WHEN {contract.entry_time_column} {entry_op} {contract.decision_time_column}
                        OR {contract.entry_time_column} IS NULL OR {contract.decision_time_column} IS NULL
                   THEN 1 ELSE 0 END),
          sum(CASE WHEN {contract.exit_time_column} <= {contract.entry_time_column}
                        OR {contract.exit_time_column} IS NULL OR {contract.entry_time_column} IS NULL
                   THEN 1 ELSE 0 END),
          sum(CASE WHEN {contract.available_time_column} < {contract.exit_time_column}
                        OR {contract.available_time_column} IS NULL OR {contract.exit_time_column} IS NULL
                   THEN 1 ELSE 0 END),
          sum(CASE WHEN abs(date_diff('second', {contract.entry_time_column}, {contract.exit_time_column})
                                - {int(contract.horizon_minutes * 60)}) > {int(contract.horizon_tolerance_seconds)}
                        OR {contract.entry_time_column} IS NULL OR {contract.exit_time_column} IS NULL
                   THEN 1 ELSE 0 END)
        FROM labels
        WHERE {contract_filter}
        """
    ).fetchone()
    audit = _combine_audits(tuple(int(value or 0) for value in signal), tuple(int(value or 0) for value in label))
    hard_fail = any((
        audit.signal_after_decision,
        audit.duplicate_label_keys,
        audit.entry_not_after_decision,
        audit.exit_not_after_entry,
        audit.label_available_before_exit,
        audit.horizon_mismatch,
    )) or (audit.signal_available_time_missing and not allow_legacy_signals)
    if hard_fail:
        raise ValueError(f"PIT/label contract audit failed: {audit.as_dict()}")
    return audit


def _factor_keys(connection: duckdb.DuckDBPyConnection) -> tuple[list[str], pd.DataFrame]:
    columns = _columns(connection, "signals")
    keys = [
        column
        for column in ("batch_id", "factor_id", "layer_id", "scale_minutes", "horizon", "variant_id")
        if column in columns
    ]
    if "factor_id" not in keys:
        raise ValueError("Signals require factor_id for governed streaming evaluation")
    selected = ", ".join(f'"{column}"' for column in keys)
    factors = connection.execute(f"SELECT DISTINCT {selected} FROM signals ORDER BY {selected}").fetch_df()
    return keys, factors


def _factor_where(keys: list[str], row: pd.Series) -> str:
    conditions = []
    for key in keys:
        value = row[key]
        if pd.isna(value):
            conditions.append(f'"{key}" IS NULL')
        else:
            conditions.append(f'"{key}"={_sql_literal(value)}')
    return " AND ".join(conditions)


def evaluate_alpha_streaming(
    signals_path: str | Path,
    labels_path: str | Path,
    *,
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
    cost_bps: Iterable[float] = (0.0, 1.0, 2.0, 5.0, 10.0),
    direction_column: str | None = "expected_direction",
    default_direction: str = "auto",
    control_columns: Iterable[str] = ("own_score",),
    annualization_factor: float | None = None,
    resource_budget: ResourceBudget = ResourceBudget(),
    allow_legacy_signals: bool = False,
    correlation_sample_modulus: int = 1000,
) -> AlphaResult:
    label_contract.validate()
    keys_join = list(join_keys)
    connection = duckdb.connect()
    configure_duckdb(connection, resource_budget)
    connection.execute(f"CREATE VIEW signals AS SELECT * FROM {_table_expression(signals_path)}")
    connection.execute(f"CREATE VIEW labels AS SELECT * FROM {_table_expression(labels_path)}")
    pit_audit = _audit_pit(
        connection,
        label_contract,
        join_keys=keys_join,
        allow_legacy_signals=allow_legacy_signals,
    )
    signal_columns = _columns(connection, "signals")
    label_columns = _columns(connection, "labels")
    missing_signal_keys = sorted(set(keys_join) - signal_columns)
    missing_label_keys = sorted(set(keys_join) - label_columns)
    if missing_signal_keys or missing_label_keys:
        raise ValueError(
            f"Join keys missing; signal_missing={missing_signal_keys}, label_missing={missing_label_keys}"
        )
    factor_keys, factors = _factor_keys(connection)
    all_metrics: list[pd.DataFrame] = []
    all_ic: list[pd.DataFrame] = []
    all_daily: list[pd.DataFrame] = []
    all_quantiles: list[pd.DataFrame] = []
    all_portfolios: list[pd.DataFrame] = []
    all_stability: list[pd.DataFrame] = []
    select_signal = [
        *factor_keys,
        *keys_join,
        symbol_column,
        score_column,
        *[column for column in ("signal_available_time", direction_column, *control_columns) if column and column in signal_columns],
    ]
    select_signal = list(dict.fromkeys(select_signal))
    join_expression = " AND ".join(f's."{key}"=l."{key}"' for key in keys_join)
    label_filter = f"CAST(l.label_id AS VARCHAR)={_sql_literal(label_contract.label_id)}"
    for _, factor_row in factors.iterrows():
        where = _factor_where(factor_keys, factor_row)
        signal_projection = ", ".join(f's."{column}"' for column in select_signal)
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
        if metadata is not None:
            if metadata_signal_id not in factor.columns or metadata_id not in metadata.columns:
                raise ValueError(
                    f"Metadata join columns missing: signals={metadata_signal_id!r}, metadata={metadata_id!r}"
                )
            factor = factor.merge(
                metadata,
                left_on=metadata_signal_id,
                right_on=metadata_id,
                how="left",
                validate="many_to_one",
            )
        result = evaluate_alpha(
            factor,
            score_column=score_column,
            label_column="target_return",
            time_column="decision_time",
            symbol_column=symbol_column,
            trade_date_column="trade_date",
            quantiles=quantiles,
            annualization_factor=annualization_factor,
            min_cross_section=min_cross_section,
            cost_bps=cost_bps,
            slice_columns=slice_columns,
            direction_column=direction_column,
            default_direction=default_direction,
            control_columns=control_columns,
            label_overlapping=label_contract.overlapping,
            governance={"pit_audit": pit_audit.as_dict(), "label_contract": label_contract.as_dict()},
        )
        all_metrics.append(result.metrics)
        all_ic.append(result.ic_series)
        all_daily.append(result.daily_ic)
        all_quantiles.append(result.quantile_returns)
        all_portfolios.append(result.portfolio_returns)
        all_stability.append(result.stability)
        del factor, result
        gc.collect()
    metrics = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()
    if not metrics.empty:
        metrics["fdr_qvalue"], metrics["fdr_pass"] = _bh_fdr(metrics["spearman_ic_pvalue_daily"])
        metrics["governance_ready"] = (
            metrics["direction_predeclared"].fillna(False)
            & metrics["annualization_valid"].fillna(False)
            & pit_audit.passed
        )
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
    correlation = pd.DataFrame()
    if correlation_sample_modulus > 0 and not factors.empty:
        identity = " || '|' || ".join(
            f"coalesce(CAST({column} AS VARCHAR), '')" for column in factor_keys
        )
        sample_query = f"""
        SELECT {identity} AS factor_key, decision_time, "{symbol_column}" AS symbol_id, "{score_column}" AS score
        FROM signals
        WHERE abs(hash(CAST(decision_time AS VARCHAR) || '|' || CAST("{symbol_column}" AS VARCHAR)))
              % {int(correlation_sample_modulus)} = 0
        """
        sample = connection.execute(sample_query).fetch_df()
        if not sample.empty:
            pivot = sample.pivot_table(index=["decision_time", "symbol_id"], columns="factor_key", values="score", aggfunc="mean")
            if pivot.shape[1] >= 2:
                correlation = pivot.corr(method="spearman").stack(dropna=False).rename("score_spearman_correlation").reset_index()
                correlation.columns = ["factor_a", "factor_b", "score_spearman_correlation"]
                correlation["sample_rows"] = len(sample)
                correlation["sample_modulus"] = correlation_sample_modulus
    return AlphaResult(
        metrics=metrics,
        ic_series=pd.concat(all_ic, ignore_index=True) if all_ic else pd.DataFrame(),
        daily_ic=pd.concat(all_daily, ignore_index=True) if all_daily else pd.DataFrame(),
        quantile_returns=pd.concat(all_quantiles, ignore_index=True) if all_quantiles else pd.DataFrame(),
        portfolio_returns=pd.concat(all_portfolios, ignore_index=True) if all_portfolios else pd.DataFrame(),
        stability=pd.concat(all_stability, ignore_index=True) if all_stability else pd.DataFrame(),
        score_correlation=correlation,
        governance={
            "pit_audit": pit_audit.as_dict(),
            "label_contract": label_contract.as_dict(),
            "factor_count": int(len(factors)),
            "streaming_mode": "factor_sequential_duckdb",
            "resource_budget": resource_budget.as_dict(),
        },
    )
