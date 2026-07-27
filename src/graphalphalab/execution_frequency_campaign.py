from __future__ import annotations

from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd

from .alpha import _residualize_score
from .checkpoint import (
    CheckpointSpec,
    checkpoint_valid,
    commit_frames,
    file_signature,
    load_checkpoint_frames,
    safe_key,
    write_progress,
)
from .contracts import LabelContract
from .core4_execution_semantics import (
    CORE4_SCOPE_EXECUTION_SEMANTICS,
    semantics_for_scope,
)
from .dual_theme_common import load_horizon_manifest
from .dual_theme_scope_alpha import (
    _prepare_inter_factor,
    _prepare_within_factor,
)
from .execution_frequency import (
    ExecutionPolicy,
    add_frequency_deltas,
    default_execution_policy_grid,
    pareto_frontier,
    walk_forward_direction_by_date,
)
from .execution_frequency_v2 import evaluate_policy_grid_v2
from .governance import (
    ResourceBudget,
    atomic_write_frame,
    atomic_write_json,
    atomic_write_text,
    configure_duckdb,
    sha256_json,
)


EXECUTION_CAMPAIGN_VERSION = "GAL_EXECUTION_FREQUENCY_DAG_V1"
CHECKPOINT_FILES = (
    "metrics.parquet",
    "daily_returns.parquet",
    "decision_returns.parquet",
    "frontier.parquet",
    "gate_effectiveness.parquet",
)


@dataclass(frozen=True)
class FrequencyTask:
    ordinal: int
    candidate: dict[str, object]
    signals_root: str
    labels_path: str
    label_contract_path: str
    output_root: str
    contract_hash: str
    source_hash: str
    worker_memory_limit_gb: float
    worker_threads: int
    temp_directory: str | None
    min_theme_size: int
    min_theme_cross_section: int
    min_direction_train_dates: int
    direction_rolling_dates: int
    control_columns: tuple[str, ...]
    quantiles: int
    carry_overnight: bool

    @property
    def identity(self) -> dict[str, object]:
        return {
            key: self.candidate[key]
            for key in (
                "scope",
                "theme_family",
                "factor_id",
                "layer_id",
                "scale_minutes",
                "variant_id",
                "horizon",
                "horizon_minutes",
            )
        }

    @property
    def unit(self) -> str:
        return "|".join(
            f"{key}={value}" for key, value in self.identity.items()
        )

    @property
    def checkpoint_dir(self) -> Path:
        return (
            Path(self.output_root)
            / "_checkpoints"
            / "execution_frequency"
            / f"horizon={self.candidate['horizon']}"
            / f"scope={self.candidate['scope']}"
            / safe_key(self.identity, prefix="candidate")
        )


def governed_execution_policy_grid() -> tuple[ExecutionPolicy, ...]:
    rows = list(default_execution_policy_grid())
    existing = {row.policy_id for row in rows}
    for minutes in (30, 60, 120):
        policy = ExecutionPolicy(
            f"theme_stability_{minutes}m",
            minutes,
            gate_mode="theme_stability",
            min_theme_retention=0.70,
            min_target_turnover=0.10,
            max_turnover_per_rebalance=0.50,
        )
        if policy.policy_id not in existing:
            rows.append(policy)
    for row in rows:
        row.validate()
    return tuple(rows)


def _sql_path(path: str | Path) -> str:
    return (
        Path(path)
        .expanduser()
        .resolve()
        .as_posix()
        .replace("'", "''")
    )


def _signal_glob(root: str | Path, scope: str) -> str:
    return _sql_path(
        Path(root)
        / "batch_id=dual_theme_igc"
        / f"scope={scope}"
        / "**"
        / "*.parquet"
    )


def _relation_columns(
    connection: duckdb.DuckDBPyConnection,
    expression: str,
) -> set[str]:
    rows = connection.execute(
        f"DESCRIBE SELECT * FROM {expression}"
    ).fetch_df()
    column = (
        "column_name"
        if "column_name" in rows.columns
        else rows.columns[0]
    )
    return set(rows[column].astype(str))


def _optional_signal_expression(
    columns: set[str],
    column: str,
    sql_type: str,
) -> str:
    if column in columns:
        return f's."{column}"'
    return f"CAST(NULL AS {sql_type}) AS \"{column}\""


def _audit_joined_frame(
    frame: pd.DataFrame,
    contract: LabelContract,
) -> dict[str, object]:
    decision = pd.to_datetime(
        frame["decision_time"], utc=True, errors="coerce"
    )
    signal_available = pd.to_datetime(
        frame["signal_available_time"], utc=True, errors="coerce"
    )
    entry = pd.to_datetime(
        frame["entry_time"], utc=True, errors="coerce"
    )
    exit_time = pd.to_datetime(
        frame["exit_time"], utc=True, errors="coerce"
    )
    missing_available = int(signal_available.isna().sum())
    signal_after = int((signal_available > decision).fillna(False).sum())
    entry_bad = int(
        (
            (entry <= decision)
            if contract.require_entry_after_decision
            else (entry < decision)
        )
        .fillna(True)
        .sum()
    )
    exit_bad = int((exit_time <= entry).fillna(True).sum())
    actual_seconds = (exit_time - entry).dt.total_seconds()
    expected_seconds = float(contract.horizon_minutes * 60)
    horizon_bad = int(
        (
            (actual_seconds - expected_seconds).abs()
            > float(contract.horizon_tolerance_seconds)
        )
        .fillna(True)
        .sum()
    )
    audit = {
        "rows": int(len(frame)),
        "signal_available_time_missing": missing_available,
        "signal_after_decision": signal_after,
        "entry_not_after_decision": entry_bad,
        "exit_not_after_entry": exit_bad,
        "horizon_mismatch": horizon_bad,
    }
    audit["passed"] = not any(
        audit[key]
        for key in (
            "signal_available_time_missing",
            "signal_after_decision",
            "entry_not_after_decision",
            "exit_not_after_entry",
            "horizon_mismatch",
        )
    )
    if not audit["passed"]:
        raise ValueError(
            f"Execution candidate PIT audit failed: {audit}"
        )
    return audit


def load_execution_candidate_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    signals_root: str | Path,
    labels_path: str | Path,
    contract: LabelContract,
    candidate: dict[str, object],
) -> tuple[pd.DataFrame, dict[str, object]]:
    scope = str(candidate["scope"])
    signals_expression = (
        f"read_parquet('{_signal_glob(signals_root, scope)}', "
        "union_by_name=true)"
    )
    labels_expression = (
        f"read_parquet('{_sql_path(labels_path)}', "
        "union_by_name=true)"
    )
    signal_columns = _relation_columns(connection, signals_expression)
    label_columns = _relation_columns(connection, labels_expression)
    required_signal = {
        "batch_id",
        "factor_id",
        "layer_id",
        "scale_minutes",
        "variant_id",
        "trade_date",
        "decision_time",
        "symbol_id",
        "score",
        "signal_available_time",
    }
    required_label = {
        "label_id",
        "trade_date",
        "decision_time",
        "symbol_id",
        contract.target_column,
        contract.entry_time_column,
        contract.exit_time_column,
    }
    missing_signal = sorted(required_signal - signal_columns)
    missing_label = sorted(required_label - label_columns)
    if missing_signal or missing_label:
        raise ValueError(
            "Execution source schema mismatch: "
            f"signals={missing_signal}, labels={missing_label}"
        )

    selected = [
        's."batch_id"',
        's."factor_id"',
        's."layer_id"',
        's."scale_minutes"',
        's."variant_id"',
        's."trade_date"',
        's."decision_time"',
        's."symbol_id"',
        's."score"',
        's."signal_available_time"',
        _optional_signal_expression(
            signal_columns,
            "own_score",
            "DOUBLE",
        ),
        _optional_signal_expression(
            signal_columns,
            "context_theme_id",
            "VARCHAR",
        ),
        _optional_signal_expression(
            signal_columns,
            "membership_weight",
            "DOUBLE",
        ),
        f'l."{contract.target_column}" AS target_return',
        f'l."{contract.entry_time_column}" AS entry_time',
        f'l."{contract.exit_time_column}" AS exit_time',
    ]
    query = f"""
    SELECT {', '.join(selected)}
    FROM {signals_expression} s
    JOIN {labels_expression} l
      ON s.trade_date=l.trade_date
     AND s.decision_time=l.decision_time
     AND s.symbol_id=l.symbol_id
    WHERE CAST(l.label_id AS VARCHAR)=?
      AND CAST(s.factor_id AS VARCHAR)=?
      AND CAST(s.layer_id AS VARCHAR)=?
      AND CAST(s.scale_minutes AS INTEGER)=?
      AND CAST(s.variant_id AS VARCHAR)=?
    ORDER BY s.trade_date, s.decision_time, s.symbol_id
    """
    frame = connection.execute(
        query,
        [
            contract.label_id,
            str(candidate["factor_id"]),
            str(candidate["layer_id"]),
            int(candidate["scale_minutes"]),
            str(candidate["variant_id"]),
        ],
    ).fetch_df()
    if frame.empty:
        raise ValueError(
            f"No joined signal/label rows for candidate={candidate}"
        )
    return frame, _audit_joined_frame(frame, contract)


def _prepare_global(
    frame: pd.DataFrame,
    controls: Iterable[str],
) -> pd.DataFrame:
    data = frame.copy()
    selected_controls = [
        column for column in controls if column in data.columns
    ]
    if selected_controls:
        parts: list[pd.Series] = []
        for _, cross in data.groupby(
            "decision_time",
            observed=True,
            sort=True,
        ):
            residual = _residualize_score(
                cross,
                "score",
                selected_controls,
            )
            parts.append(pd.Series(residual, index=cross.index))
        data["execution_score"] = (
            pd.concat(parts).sort_index() if parts else np.nan
        )
    else:
        data["execution_score"] = pd.to_numeric(
            data["score"], errors="coerce"
        )
    data["execution_target_return"] = pd.to_numeric(
        data["target_return"], errors="coerce"
    )
    return data.dropna(
        subset=["execution_score", "execution_target_return"]
    )


def prepare_execution_candidate_frame(
    frame: pd.DataFrame,
    *,
    scope: str,
    control_columns: Iterable[str],
    min_theme_size: int,
    min_theme_cross_section: int,
) -> pd.DataFrame:
    controls = [
        column for column in control_columns if column in frame.columns
    ]
    if scope == "within_theme":
        return _prepare_within_factor(
            frame,
            score_column="score",
            control_columns=controls,
            min_theme_size=min_theme_size,
        ).rename(
            columns={
                "scope_score": "execution_score",
                "scope_target_return": "execution_target_return",
            }
        )
    if scope == "inter_theme":
        return _prepare_inter_factor(
            frame,
            score_column="score",
            control_columns=controls,
            min_theme_cross_section=min_theme_cross_section,
            min_theme_size=min_theme_size,
        ).rename(
            columns={
                "scope_score": "execution_score",
                "scope_target_return": "execution_target_return",
            }
        )
    if scope == "global":
        return _prepare_global(frame, controls)
    raise ValueError(f"Unsupported execution scope={scope!r}")


def _daily_returns(returns: pd.DataFrame) -> pd.DataFrame:
    if returns.empty:
        return returns
    identity = [
        column
        for column in (
            "scope",
            "theme_family",
            "factor_id",
            "layer_id",
            "scale_minutes",
            "variant_id",
            "horizon",
            "horizon_minutes",
            "policy_id",
            "rebalance_minutes",
            "gate_mode",
            "trade_date",
        )
        if column in returns.columns
    ]
    aggregations: dict[str, str] = {
        "gross_return": "mean",
        "turnover": "sum",
        "rebalanced": "sum",
        "gate_pass": "mean",
    }
    for column in returns.columns:
        if column.startswith("net_return_"):
            aggregations[column] = "mean"
    return (
        returns.groupby(
            identity,
            observed=True,
            dropna=False,
        )
        .agg(aggregations)
        .reset_index()
    )


def _gate_effectiveness(
    metrics: pd.DataFrame,
    returns: pd.DataFrame,
) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    rows: list[dict[str, object]] = []
    by_policy = metrics.set_index("policy_id", drop=False)
    gate_columns = [
        "rank_gate_pass",
        "confidence_gate_pass",
        "theme_gate_pass",
        "turnover_gate_pass",
        "gate_pass",
    ]
    gate_rates = (
        returns.groupby("policy_id", observed=True)[gate_columns].mean()
        if not returns.empty
        else pd.DataFrame()
    )
    for _, row in metrics.iterrows():
        baseline_id = f"fixed_{int(row['rebalance_minutes'])}m"
        baseline = (
            by_policy.loc[baseline_id]
            if baseline_id in by_policy.index
            else None
        )
        record = row.to_dict()
        record["same_frequency_baseline_policy_id"] = baseline_id
        if baseline is not None:
            base_turnover = float(
                baseline.get("mean_turnover", np.nan)
            )
            record[
                "turnover_reduction_vs_same_frequency_fixed"
            ] = (
                1.0
                - float(row.get("mean_turnover", np.nan))
                / base_turnover
                if np.isfinite(base_turnover) and base_turnover > 0
                else np.nan
            )
            record[
                "net_mean_5bps_increment_vs_same_frequency_fixed"
            ] = float(row.get("net_mean_5bps", np.nan)) - float(
                baseline.get("net_mean_5bps", np.nan)
            )
            record[
                "rebalance_rate_reduction_vs_same_frequency_fixed"
            ] = float(baseline.get("rebalance_rate", np.nan)) - float(
                row.get("rebalance_rate", np.nan)
            )
        if row["policy_id"] in gate_rates.index:
            for column, value in gate_rates.loc[
                row["policy_id"]
            ].items():
                record[f"observed_{column}_rate"] = float(value)
        rows.append(record)
    return pd.DataFrame(rows)


def _task_spec(task: FrequencyTask) -> CheckpointSpec:
    return CheckpointSpec(
        stage="execution-frequency-candidate",
        unit=task.unit,
        contract_hash=task.contract_hash,
        source_hash=task.source_hash,
    )


def _run_frequency_task(task: FrequencyTask) -> dict[str, object]:
    checkpoint = task.checkpoint_dir
    spec = _task_spec(task)
    if checkpoint_valid(
        checkpoint,
        spec,
        required_files=CHECKPOINT_FILES,
    ):
        return {
            "unit": task.unit,
            "status": "reused",
            "checkpoint": str(checkpoint),
            "ordinal": task.ordinal,
        }

    worker_temp = None
    if task.temp_directory:
        worker_temp = str(
            Path(task.temp_directory)
            .expanduser()
            .resolve()
            / f"execution-worker-{os.getpid()}"
        )
    budget = ResourceBudget(
        task.worker_memory_limit_gb,
        task.worker_threads,
        worker_temp,
    )
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, budget)
        contract = LabelContract.from_json(
            task.label_contract_path
        )
        raw, pit_audit = load_execution_candidate_frame(
            connection,
            signals_root=task.signals_root,
            labels_path=task.labels_path,
            contract=contract,
            candidate=task.candidate,
        )
    finally:
        connection.close()

    prepared = prepare_execution_candidate_frame(
        raw,
        scope=str(task.candidate["scope"]),
        control_columns=task.control_columns,
        min_theme_size=task.min_theme_size,
        min_theme_cross_section=task.min_theme_cross_section,
    )
    if prepared.empty:
        raise ValueError(
            f"Candidate preparation produced no rows: {task.identity}"
        )
    direction_by_date = walk_forward_direction_by_date(
        prepared,
        score_column="execution_score",
        return_column="execution_target_return",
        min_train_dates=task.min_direction_train_dates,
        rolling_dates=task.direction_rolling_dates,
    )
    if not direction_by_date:
        raise ValueError(
            "No PIT-safe direction available after warm-up: "
            f"{task.identity}"
        )

    metrics, returns = evaluate_policy_grid_v2(
        prepared,
        governed_execution_policy_grid(),
        score_column="execution_score",
        return_column="execution_target_return",
        symbol_column="symbol_id",
        theme_column=(
            "context_theme_id"
            if "context_theme_id" in prepared.columns
            else None
        ),
        quantiles=task.quantiles,
        direction_by_date=direction_by_date,
        carry_overnight=task.carry_overnight,
    )
    semantic = semantics_for_scope(
        str(task.candidate["scope"])
    ).as_dict()
    identity = {**task.identity, **semantic}
    for key, value in identity.items():
        metrics[key] = value
        if not returns.empty:
            returns[key] = value
    metrics = add_frequency_deltas(metrics)
    frontier = pareto_frontier(metrics)
    gates = _gate_effectiveness(metrics, returns)
    daily = _daily_returns(returns)
    commit_frames(
        checkpoint,
        spec,
        {
            "metrics.parquet": metrics,
            "daily_returns.parquet": daily,
            "decision_returns.parquet": returns,
            "frontier.parquet": frontier,
            "gate_effectiveness.parquet": gates,
        },
        metadata={
            "version": EXECUTION_CAMPAIGN_VERSION,
            "identity": identity,
            "label_contract": contract.as_dict(),
            "pit_audit": pit_audit,
            "input_rows": int(len(raw)),
            "prepared_rows": int(len(prepared)),
            "direction_dates": len(direction_by_date),
            "policy_count": int(len(metrics)),
            "resource_budget": budget.as_dict(),
            "carry_overnight": task.carry_overnight,
        },
    )
    return {
        "unit": task.unit,
        "status": "completed",
        "checkpoint": str(checkpoint),
        "ordinal": task.ordinal,
        "prepared_rows": int(len(prepared)),
        "policy_count": int(len(metrics)),
    }


def _round_robin_tasks(
    tasks: Iterable[FrequencyTask],
) -> list[FrequencyTask]:
    buckets: dict[
        tuple[str, str], deque[FrequencyTask]
    ] = defaultdict(deque)
    for task in tasks:
        buckets[
            (
                str(task.candidate["horizon"]),
                str(task.candidate["scope"]),
            )
        ].append(task)
    ordered: list[FrequencyTask] = []
    keys = sorted(buckets)
    while keys:
        next_keys: list[tuple[str, str]] = []
        for key in keys:
            if buckets[key]:
                ordered.append(buckets[key].popleft())
            if buckets[key]:
                next_keys.append(key)
        keys = next_keys
    return ordered


def _load_candidate_manifest(
    path: str | Path,
) -> list[dict[str, object]]:
    source = Path(path).expanduser().resolve()
    payload = json.loads(source.read_text(encoding="utf-8"))
    if payload.get("version") != "GAL_EXECUTION_CANDIDATES_V1":
        raise ValueError(
            "Unsupported candidate manifest: "
            f"{payload.get('version')!r}"
        )
    rows = payload.get("candidates")
    if not isinstance(rows, list) or not rows:
        raise ValueError("Candidate manifest contains no candidates")
    required = {
        "scope",
        "theme_family",
        "factor_id",
        "layer_id",
        "scale_minutes",
        "variant_id",
        "horizon",
        "horizon_minutes",
    }
    for row in rows:
        if not isinstance(row, dict) or required - set(row):
            raise ValueError(
                f"Malformed execution candidate row: {row!r}"
            )
        semantics_for_scope(str(row["scope"]))
        if str(row["variant_id"]) != "graph_forward":
            raise ValueError(
                "Execution campaign accepts graph_forward candidates only"
            )
    return rows


def build_frequency_tasks(
    *,
    signals_root: str | Path,
    horizon_manifest: str | Path,
    candidate_manifest: str | Path,
    output_root: str | Path,
    resource_budget: ResourceBudget,
    workers: int,
    min_theme_size: int,
    min_theme_cross_section: int,
    min_direction_train_dates: int,
    direction_rolling_dates: int,
    control_columns: Iterable[str],
    quantiles: int,
    carry_overnight: bool,
) -> list[FrequencyTask]:
    candidates = _load_candidate_manifest(candidate_manifest)
    horizons = {
        row.name: row
        for row in load_horizon_manifest(horizon_manifest)
    }
    signals_root_path = Path(signals_root).expanduser().resolve()
    signals_manifest = signals_root_path / "export_manifest.json"
    if (
        not signals_manifest.exists()
        or not (signals_root_path / "_SUCCESS").exists()
    ):
        raise FileNotFoundError(
            f"Incomplete governed signals root: {signals_root}"
        )
    shared_records = {
        "signals_manifest": file_signature(signals_manifest),
        "candidate_manifest": file_signature(candidate_manifest),
        "horizon_manifest": file_signature(horizon_manifest),
    }
    workers = max(1, int(workers))
    worker_memory = max(
        1.0,
        float(resource_budget.memory_limit_gb) / workers,
    )
    worker_threads = max(
        1,
        int(resource_budget.threads) // workers,
    )
    policies = [
        row.as_dict() for row in governed_execution_policy_grid()
    ]
    tasks: list[FrequencyTask] = []
    horizon_records: dict[str, dict[str, object]] = {}
    control_columns = tuple(control_columns)
    for ordinal, candidate in enumerate(candidates):
        horizon = str(candidate["horizon"])
        if horizon not in horizons:
            raise ValueError(
                f"Candidate horizon {horizon!r} is absent from horizon manifest"
            )
        spec = horizons[horizon]
        contract = LabelContract.from_json(spec.label_contract)
        if int(candidate["horizon_minutes"]) != int(
            contract.horizon_minutes
        ):
            raise ValueError(
                "Candidate/label horizon mismatch for "
                f"{candidate['factor_id']}: "
                f"{candidate['horizon_minutes']} vs "
                f"{contract.horizon_minutes}"
            )
        if horizon not in horizon_records:
            horizon_records[horizon] = {
                "labels": file_signature(spec.labels),
                "label_contract": file_signature(
                    spec.label_contract
                ),
            }
        mathematical = {
            "version": EXECUTION_CAMPAIGN_VERSION,
            "candidate": candidate,
            "label_contract": contract.as_dict(),
            "policies": policies,
            "min_theme_size": int(min_theme_size),
            "min_theme_cross_section": int(
                min_theme_cross_section
            ),
            "min_direction_train_dates": int(
                min_direction_train_dates
            ),
            "direction_rolling_dates": int(
                direction_rolling_dates
            ),
            "control_columns": list(control_columns),
            "quantiles": int(quantiles),
            "carry_overnight": bool(carry_overnight),
        }
        sources = {
            **shared_records,
            **horizon_records[horizon],
        }
        tasks.append(
            FrequencyTask(
                ordinal=ordinal,
                candidate=dict(candidate),
                signals_root=str(signals_root_path),
                labels_path=str(spec.labels),
                label_contract_path=str(spec.label_contract),
                output_root=str(
                    Path(output_root).expanduser().resolve()
                ),
                contract_hash=sha256_json(mathematical),
                source_hash=sha256_json(sources),
                worker_memory_limit_gb=worker_memory,
                worker_threads=worker_threads,
                temp_directory=resource_budget.temp_directory,
                min_theme_size=int(min_theme_size),
                min_theme_cross_section=int(
                    min_theme_cross_section
                ),
                min_direction_train_dates=int(
                    min_direction_train_dates
                ),
                direction_rolling_dates=int(
                    direction_rolling_dates
                ),
                control_columns=control_columns,
                quantiles=int(quantiles),
                carry_overnight=bool(carry_overnight),
            )
        )
    return _round_robin_tasks(tasks)


def _progress_payload(
    tasks: list[FrequencyTask],
    states: dict[int, dict[str, object]],
    *,
    status: str,
    workers: int,
) -> dict[str, object]:
    completed = sum(
        states.get(task.ordinal, {}).get("status")
        in {"completed", "reused"}
        for task in tasks
    )
    reused = sum(
        states.get(task.ordinal, {}).get("status") == "reused"
        for task in tasks
    )
    failed = sum(
        states.get(task.ordinal, {}).get("status") == "failed"
        for task in tasks
    )
    return {
        "version": EXECUTION_CAMPAIGN_VERSION,
        "scheduler": (
            "global_dag_candidate_horizon_no_barrier"
        ),
        "status": status,
        "workers": int(workers),
        "horizon_barrier": False,
        "scope_barrier": False,
        "total_units": len(tasks),
        "completed_units": completed,
        "reused_units": reused,
        "failed_units": failed,
        "units": [
            {
                "unit": task.unit,
                "status": states.get(
                    task.ordinal, {}
                ).get("status", "pending"),
                "detail": states.get(
                    task.ordinal, {}
                ).get("detail", ""),
                "attempt": 1,
            }
            for task in tasks
        ],
    }


def _write_execution_progress(
    output: Path,
    tasks: list[FrequencyTask],
    states: dict[int, dict[str, object]],
    *,
    status: str,
    workers: int,
    current: str | None = None,
) -> None:
    root = output / "_checkpoints" / "execution_frequency"
    payload = _progress_payload(
        tasks,
        states,
        status=status,
        workers=workers,
    )
    atomic_write_json(
        root / "global_dag_progress.json",
        payload,
    )
    write_progress(
        root,
        stage="execution-frequency-global-dag",
        total=len(tasks),
        completed=int(payload["completed_units"]),
        reused=int(payload["reused_units"]),
        failed=int(payload["failed_units"]),
        current=current,
        status=status,
        units=payload["units"],
        extra={
            "scheduler": payload["scheduler"],
            "workers": workers,
            "horizon_barrier": False,
            "scope_barrier": False,
        },
    )


def _aggregate_frequency_results(
    tasks: list[FrequencyTask],
    output: Path,
) -> dict[str, object]:
    metrics_parts: list[pd.DataFrame] = []
    daily_parts: list[pd.DataFrame] = []
    frontier_parts: list[pd.DataFrame] = []
    gate_parts: list[pd.DataFrame] = []
    catalog: list[dict[str, object]] = []
    for task in tasks:
        frames = load_checkpoint_frames(
            task.checkpoint_dir,
            (
                "metrics.parquet",
                "daily_returns.parquet",
                "frontier.parquet",
                "gate_effectiveness.parquet",
            ),
        )
        if not frames["metrics.parquet"].empty:
            metrics_parts.append(frames["metrics.parquet"])
        if not frames["daily_returns.parquet"].empty:
            daily_parts.append(frames["daily_returns.parquet"])
        if not frames["frontier.parquet"].empty:
            frontier_parts.append(frames["frontier.parquet"])
        if not frames["gate_effectiveness.parquet"].empty:
            gate_parts.append(
                frames["gate_effectiveness.parquet"]
            )
        decision_path = (
            task.checkpoint_dir / "decision_returns.parquet"
        )
        catalog.append(
            {
                **task.identity,
                "checkpoint": str(task.checkpoint_dir),
                "decision_returns": str(decision_path),
                "decision_return_rows": (
                    int(
                        pd.read_parquet(
                            decision_path,
                            columns=["policy_id"],
                        ).shape[0]
                    )
                    if decision_path.exists()
                    else 0
                ),
            }
        )
    metrics = (
        pd.concat(metrics_parts, ignore_index=True)
        if metrics_parts
        else pd.DataFrame()
    )
    daily = (
        pd.concat(daily_parts, ignore_index=True)
        if daily_parts
        else pd.DataFrame()
    )
    frontier = (
        pd.concat(frontier_parts, ignore_index=True)
        if frontier_parts
        else pd.DataFrame()
    )
    gates = (
        pd.concat(gate_parts, ignore_index=True)
        if gate_parts
        else pd.DataFrame()
    )
    catalog_frame = pd.DataFrame(catalog)
    if metrics.empty:
        raise ValueError(
            "Execution campaign produced no policy metrics"
        )

    identity = [
        "scope",
        "theme_family",
        "factor_id",
        "layer_id",
        "scale_minutes",
        "variant_id",
        "horizon",
        "horizon_minutes",
    ]
    frontier_keys = (
        set(
            tuple(row)
            for row in frontier[
                [*identity, "policy_id"]
            ].itertuples(index=False, name=None)
        )
        if not frontier.empty
        else set()
    )
    metrics["on_return_turnover_pareto_frontier"] = [
        tuple(row) in frontier_keys
        for row in metrics[
            [*identity, "policy_id"]
        ].itertuples(index=False, name=None)
    ]
    metrics["promotion_eligible"] = (
        metrics["on_return_turnover_pareto_frontier"]
        & (
            pd.to_numeric(
                metrics["net_mean_5bps"],
                errors="coerce",
            )
            > 0
        )
        & (
            pd.to_numeric(
                metrics["turnover_reduction_vs_baseline"],
                errors="coerce",
            )
            > 0
        )
        & (
            pd.to_numeric(
                metrics["date_count"],
                errors="coerce",
            )
            >= 20
        )
    )
    ranked = metrics.sort_values(
        [
            *identity,
            "promotion_eligible",
            "net_mean_5bps",
            "mean_turnover",
        ],
        ascending=[
            *([True] * len(identity)),
            False,
            False,
            True,
        ],
        na_position="last",
    )
    best = (
        ranked.groupby(
            identity,
            observed=True,
            dropna=False,
        )
        .head(1)
        .copy()
    )
    best["recommendation"] = np.where(
        best["promotion_eligible"],
        "promote_for_falsification",
        "research_only",
    )

    atomic_write_frame(
        metrics,
        output / "frequency_policy_metrics.csv",
    )
    atomic_write_frame(
        daily,
        output / "frequency_policy_returns.csv",
    )
    atomic_write_frame(
        frontier,
        output / "frequency_pareto_frontier.csv",
    )
    atomic_write_frame(
        gates,
        output / "gate_effectiveness.csv",
    )
    atomic_write_frame(
        best,
        output / "frequency_horizon_matrix.csv",
    )
    atomic_write_frame(
        best,
        output / "candidate_execution_recommendations.csv",
    )
    atomic_write_frame(
        catalog_frame,
        output / "decision_returns_catalog.csv",
    )
    atomic_write_json(
        output / "scope_semantics.json",
        {
            scope: semantics.as_dict()
            for scope, semantics in (
                CORE4_SCOPE_EXECUTION_SEMANTICS.items()
            )
        },
    )
    return {
        "candidate_count": len(tasks),
        "policy_metric_rows": int(len(metrics)),
        "daily_return_rows": int(len(daily)),
        "pareto_rows": int(len(frontier)),
        "promotion_eligible_rows": int(
            metrics["promotion_eligible"].sum()
        ),
        "recommended_candidates": int(len(best)),
    }


def run_execution_frequency_campaign(
    *,
    signals_root: str | Path,
    horizon_manifest: str | Path,
    candidate_manifest: str | Path,
    output_root: str | Path,
    workers: int = 6,
    resource_budget: ResourceBudget = ResourceBudget(
        64.0,
        12,
        None,
    ),
    min_theme_size: int = 5,
    min_theme_cross_section: int = 5,
    min_direction_train_dates: int = 20,
    direction_rolling_dates: int = 60,
    control_columns: Iterable[str] = ("own_score",),
    quantiles: int = 5,
    carry_overnight: bool = False,
) -> Path:
    if workers <= 0:
        raise ValueError("workers must be positive")
    resource_budget.validate()
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)
    tasks = build_frequency_tasks(
        signals_root=signals_root,
        horizon_manifest=horizon_manifest,
        candidate_manifest=candidate_manifest,
        output_root=output,
        resource_budget=resource_budget,
        workers=workers,
        min_theme_size=min_theme_size,
        min_theme_cross_section=min_theme_cross_section,
        min_direction_train_dates=min_direction_train_dates,
        direction_rolling_dates=direction_rolling_dates,
        control_columns=tuple(control_columns),
        quantiles=quantiles,
        carry_overnight=carry_overnight,
    )
    states: dict[int, dict[str, object]] = {}
    pending: deque[FrequencyTask] = deque()
    for task in tasks:
        if checkpoint_valid(
            task.checkpoint_dir,
            _task_spec(task),
            required_files=CHECKPOINT_FILES,
        ):
            states[task.ordinal] = {
                "status": "reused",
                "detail": str(task.checkpoint_dir),
            }
        else:
            pending.append(task)
    _write_execution_progress(
        output,
        tasks,
        states,
        status="running",
        workers=workers,
    )

    max_inflight = max(1, workers * 2)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        inflight: dict[object, FrequencyTask] = {}
        while pending or inflight:
            while pending and len(inflight) < max_inflight:
                task = pending.popleft()
                states[task.ordinal] = {
                    "status": "running",
                    "detail": "submitted",
                }
                inflight[
                    executor.submit(_run_frequency_task, task)
                ] = task
            _write_execution_progress(
                output,
                tasks,
                states,
                status="running",
                workers=workers,
                current=(
                    next(iter(inflight.values())).unit
                    if inflight
                    else None
                ),
            )
            if not inflight:
                continue
            done, _ = wait(
                inflight,
                return_when=FIRST_COMPLETED,
            )
            for future in done:
                task = inflight.pop(future)
                try:
                    result = future.result()
                    states[task.ordinal] = {
                        "status": str(result["status"]),
                        "detail": str(result["checkpoint"]),
                    }
                except Exception as exc:
                    states[task.ordinal] = {
                        "status": "failed",
                        "detail": repr(exc),
                    }
                    for other in inflight:
                        other.cancel()
                    _write_execution_progress(
                        output,
                        tasks,
                        states,
                        status="failed",
                        workers=workers,
                        current=task.unit,
                    )
                    raise RuntimeError(
                        "Execution frequency task failed: "
                        f"{task.unit}"
                    ) from exc

    aggregate = _aggregate_frequency_results(tasks, output)
    summary = {
        "version": EXECUTION_CAMPAIGN_VERSION,
        "status": "pass",
        "scheduler": (
            "global_dag_candidate_horizon_no_barrier"
        ),
        "workers": int(workers),
        "horizon_barrier": False,
        "scope_barrier": False,
        "carry_overnight": bool(carry_overnight),
        "candidate_manifest": str(
            Path(candidate_manifest).expanduser().resolve()
        ),
        "signals_root": str(
            Path(signals_root).expanduser().resolve()
        ),
        "horizon_manifest": str(
            Path(horizon_manifest).expanduser().resolve()
        ),
        "resource_budget": resource_budget.as_dict(),
        "policy_grid": [
            row.as_dict()
            for row in governed_execution_policy_grid()
        ],
        **aggregate,
    }
    atomic_write_json(output / "summary.json", summary)
    atomic_write_text(
        output / "REPORT.md",
        "\n".join(
            [
                "# GraphAlphaLab execution-frequency report",
                "",
                f"- Status: **{summary['status']}**",
                (
                    "- Candidate × horizon units: "
                    f"{summary['candidate_count']}"
                ),
                (
                    "- Policy rows: "
                    f"{summary['policy_metric_rows']}"
                ),
                f"- Workers: {workers}",
                (
                    "- Scheduler: one global ready queue; "
                    "no horizon or scope barriers."
                ),
                (
                    "- Graph semantics: GFF is not recomputed; "
                    "Within uses induced Global edges and GAL "
                    "local ranking."
                ),
                (
                    "- Direction: walk-forward using prior "
                    "dates only."
                ),
                (
                    "- Overnight positions: disabled by "
                    "default; state resets each trading session."
                ),
                "",
                (
                    "Promotion requires positive 5 bps net "
                    "diagnostic, lower turnover than fixed 5m, "
                    "at least 20 dates, and return-turnover "
                    "Pareto membership."
                ),
                "",
            ]
        ),
    )
    atomic_write_json(
        output / "_SUCCESS",
        {
            "complete": True,
            "version": EXECUTION_CAMPAIGN_VERSION,
        },
    )
    _write_execution_progress(
        output,
        tasks,
        states,
        status="complete",
        workers=workers,
    )
    return output
