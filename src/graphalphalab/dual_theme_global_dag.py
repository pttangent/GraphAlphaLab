from __future__ import annotations

from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from functools import lru_cache
import json
import os
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from .alpha import AlphaResult, evaluate_alpha
from .batch import get_batch
from .checkpoint import CheckpointSpec, checkpoint_valid, commit_frames, safe_key
from .contracts import LabelContract
from .dual_theme_common import (
    DUAL_THEME_ALPHA_VERSION,
    DUAL_THEME_BATCH_ID,
    DUAL_THEME_EXPORT_VERSION,
    load_horizon_manifest,
)
from .dual_theme_reporting import _annotate_result
from .dual_theme_resumable import (
    _FRAME_FILES,
    _checkpoint_contract_hash,
    _checkpoint_frame,
    _legacy_horizon_compatible,
    _result_frames,
    _result_from_checkpoint,
    _scope_path,
    _score_correlation,
    _strict_horizon_compatible,
    _write_horizon_checkpoint,
    run_dual_theme_alpha_campaign as _finalize_campaign,
    stable_export_manifest_record,
)
from .dual_theme_scope_alpha import (
    SCOPE_ALPHA_SEMANTICS,
    _insufficient_factor_result,
    _prepare_inter_factor,
    _prepare_within_factor,
    combine_alpha_results,
)
from .governance import (
    ResourceBudget,
    atomic_write_json,
    atomic_write_text,
    configure_duckdb,
    directory_parquet_records,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
    sha256_json,
)
from .metadata import normalize_metadata
from .reports import write_report_bundle
from .streaming import (
    _audit_pit,
    _columns,
    _factor_keys,
    _factor_where,
    _sql_literal,
    _table_expression,
)


@dataclass(frozen=True)
class FactorTask:
    horizon: str
    horizon_minutes: int
    scope: str
    ordinal: int
    signal_path: str
    labels_path: str
    label_contract_path: str
    checkpoint_root: str
    checkpoint_contract_hash: str
    factor_keys: tuple[str, ...]
    factor_identity: dict[str, object]
    pit_audit: dict[str, object]
    signal_columns: tuple[str, ...]
    join_keys: tuple[str, ...]
    metadata_path: str | None
    metadata_id: str
    metadata_signal_id: str
    slice_columns: tuple[str, ...]
    score_column: str
    symbol_column: str
    quantiles: int
    min_cross_section: int
    min_theme_size: int
    min_theme_cross_section: int
    direction_column: str | None
    default_direction: str
    control_columns: tuple[str, ...]
    annualization_factor: float | None
    allow_legacy_signals: bool
    worker_memory_limit_gb: float
    worker_threads: int
    temp_directory: str | None

    @property
    def factor_unit_name(self) -> str:
        return "|".join(
            f"{key}={self.factor_identity.get(key)}" for key in self.factor_keys
        )

    @property
    def unit_name(self) -> str:
        return (
            f"horizon={self.horizon}|scope={self.scope}|"
            f"{self.factor_unit_name}"
        )

    @property
    def checkpoint_dir(self) -> Path:
        return (
            Path(self.checkpoint_root)
            / f"horizon={self.horizon}"
            / f"scope={self.scope}"
            / safe_key(self.factor_identity, prefix="factor")
        )


@dataclass
class HorizonPlan:
    name: str
    horizon_minutes: int
    labels_path: Path
    label_contract_path: Path
    contract: LabelContract
    run_manifest: dict[str, object]
    checkpoint_contract_hash: str
    manifest_inputs: list[dict[str, object]]
    label_records: list[dict[str, object]]
    horizon_output: Path
    tasks: list[FactorTask]
    reusable_bundle: bool = False
    finalized: bool = False


def split_worker_budget(
    resource_budget: ResourceBudget,
    factor_workers: int,
) -> ResourceBudget:
    workers = max(1, int(factor_workers))
    memory = max(1.0, float(resource_budget.memory_limit_gb) / workers)
    threads = max(1, int(resource_budget.threads) // workers)
    return ResourceBudget(memory, threads, resource_budget.temp_directory)


def round_robin_tasks(tasks: Iterable[FactorTask]) -> list[FactorTask]:
    buckets: dict[tuple[str, str], deque[FactorTask]] = defaultdict(deque)
    for task in tasks:
        buckets[(task.horizon, task.scope)].append(task)
    ordered: list[FactorTask] = []
    keys = sorted(buckets)
    while keys:
        next_keys: list[tuple[str, str]] = []
        for key in keys:
            bucket = buckets[key]
            if bucket:
                ordered.append(bucket.popleft())
            if bucket:
                next_keys.append(key)
        keys = next_keys
    return ordered


def _task_checkpoint_spec(task: FactorTask) -> CheckpointSpec:
    source_hash = sha256_json(
        {
            "contract_hash": task.checkpoint_contract_hash,
            "scope": task.scope,
            "identity": task.factor_identity,
        }
    )
    return CheckpointSpec(
        stage=f"dual-theme-alpha-{task.scope}-factor",
        unit=task.factor_unit_name,
        contract_hash=task.checkpoint_contract_hash,
        source_hash=source_hash,
    )


@lru_cache(maxsize=16)
def _load_metadata_cached(
    path_text: str,
    metadata_id: str,
    slice_columns: tuple[str, ...],
) -> tuple[pd.DataFrame, str]:
    path = Path(path_text)
    raw = (
        pd.read_parquet(path)
        if path.suffix.lower() in {".parquet", ".pq"}
        else pd.read_csv(path)
    )
    normalized, profile = normalize_metadata(
        raw,
        id_column=metadata_id,
        dimensions=list(slice_columns) or None,
    )
    return normalized, profile.id_column


def _empty_result(governance: dict[str, object]) -> AlphaResult:
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


def _run_factor_task(task: FactorTask) -> dict[str, object]:
    checkpoint_dir = task.checkpoint_dir
    checkpoint_spec = _task_checkpoint_spec(task)
    if checkpoint_valid(
        checkpoint_dir,
        checkpoint_spec,
        required_files=_FRAME_FILES,
    ):
        return {
            "unit": task.unit_name,
            "horizon": task.horizon,
            "scope": task.scope,
            "status": "reused",
            "checkpoint": str(checkpoint_dir),
        }

    contract = LabelContract.from_json(task.label_contract_path)
    worker_temp = None
    if task.temp_directory:
        worker_temp = str(
            Path(task.temp_directory).expanduser().resolve()
            / f"factor-worker-{os.getpid()}"
        )
    budget = ResourceBudget(
        task.worker_memory_limit_gb,
        task.worker_threads,
        worker_temp,
    )
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, budget)
        connection.execute(
            f"CREATE VIEW signals AS SELECT * FROM {_table_expression(task.signal_path)}"
        )
        connection.execute(
            f"CREATE VIEW labels AS SELECT * FROM {_table_expression(task.labels_path)}"
        )
        signal_columns = set(task.signal_columns)
        factor_keys = list(task.factor_keys)
        join_keys = list(task.join_keys)
        control_columns = list(task.control_columns)
        select_signal = [
            *factor_keys,
            *join_keys,
            task.symbol_column,
            task.score_column,
        ]
        if task.scope in SCOPE_ALPHA_SEMANTICS:
            select_signal.extend(["context_theme_id", "membership_weight"])
        select_signal.extend(
            column
            for column in (
                "signal_available_time",
                task.direction_column,
                *control_columns,
            )
            if column and column in signal_columns
        )
        select_signal = list(dict.fromkeys(select_signal))
        # Memory diet: factor-key columns are constant within one task and
        # label bookkeeping columns are never consumed downstream. Excluding
        # them from the projection avoids materialising multi-GB constant
        # string/timestamp arrays for large horizons; factor keys are
        # re-attached as cheap categoricals after fetch.
        constant_identity = {
            column: task.factor_identity.get(column) for column in factor_keys
        }
        select_signal = [
            column for column in select_signal if column not in constant_identity
        ]
        signal_projection = ", ".join(
            f's."{column}"' for column in select_signal
        )
        join_expression = " AND ".join(
            f's."{key}"=l."{key}"' for key in join_keys
        )
        label_filter = (
            f"CAST(l.label_id AS VARCHAR)={_sql_literal(contract.label_id)}"
        )
        factor_row = pd.Series(task.factor_identity)
        where = _factor_where(factor_keys, factor_row)
        factor = connection.execute(
            f"""
            SELECT {signal_projection},
                   l."{contract.target_column}" AS target_return
            FROM signals s
            JOIN labels l ON {join_expression}
            WHERE {where} AND {label_filter}
            ORDER BY s.trade_date, s.decision_time, s."{task.symbol_column}"
            """
        ).fetch_df()
        for column, value in constant_identity.items():
            if value is None or (not isinstance(value, str) and pd.isna(value)):
                factor[column] = pd.Series([None] * len(factor), dtype=object)
            else:
                factor[column] = pd.Categorical([value] * len(factor))
        input_rows = int(len(factor))
        symbol_count = (
            int(factor[task.symbol_column].nunique())
            if task.symbol_column in factor.columns and not factor.empty
            else 0
        )
        governance = {
            "pit_audit": dict(task.pit_audit),
            "label_contract": contract.as_dict(),
            "scope": task.scope,
            "scope_semantics": SCOPE_ALPHA_SEMANTICS.get(
                task.scope, "stock_global_cross_section"
            ),
            "checkpoint_contract_hash": task.checkpoint_contract_hash,
            "checkpoint_granularity": "global_dag_scope_factor",
        }

        if factor.empty and task.scope in SCOPE_ALPHA_SEMANTICS:
            result = _insufficient_factor_result(
                factor_row,
                factor_keys=factor_keys,
                scope=task.scope,
                pit_audit=dict(task.pit_audit),
                label_contract=contract,
                reason="no_label_overlap",
                input_rows=0,
                evaluable_rows=0,
                symbol_count=0,
            )
            evaluable_rows = 0
        elif factor.empty:
            result = _empty_result(governance)
            evaluable_rows = 0
        else:
            if (
                task.scope in {"global", "within_theme"}
                and task.metadata_path is not None
            ):
                metadata, resolved_metadata_id = _load_metadata_cached(
                    task.metadata_path,
                    task.metadata_id,
                    task.slice_columns,
                )
                if (
                    task.metadata_signal_id not in factor.columns
                    or resolved_metadata_id not in metadata.columns
                ):
                    raise ValueError(
                        "Metadata join columns missing: "
                        f"signals={task.metadata_signal_id!r}, "
                        f"metadata={resolved_metadata_id!r}"
                    )
                factor = factor.merge(
                    metadata,
                    left_on=task.metadata_signal_id,
                    right_on=resolved_metadata_id,
                    how="left",
                    validate="many_to_one",
                )
            if task.scope == "within_theme":
                prepared = _prepare_within_factor(
                    factor,
                    score_column=task.score_column,
                    control_columns=control_columns,
                    min_theme_size=task.min_theme_size,
                )
                eval_score = "scope_score"
                eval_target = "scope_target_return"
                eval_symbol = task.symbol_column
                eval_min_cross_section = int(task.min_cross_section)
                eval_slices = list(task.slice_columns)
                eval_controls: Iterable[str] = ()
            elif task.scope == "inter_theme":
                prepared = _prepare_inter_factor(
                    factor,
                    score_column=task.score_column,
                    control_columns=control_columns,
                    min_theme_cross_section=task.min_theme_cross_section,
                    min_theme_size=task.min_theme_size,
                )
                eval_score = "scope_score"
                eval_target = "scope_target_return"
                eval_symbol = "symbol_id"
                eval_min_cross_section = int(task.min_theme_cross_section)
                eval_slices = []
                eval_controls = ()
            else:
                prepared = factor
                eval_score = task.score_column
                eval_target = "target_return"
                eval_symbol = task.symbol_column
                eval_min_cross_section = int(task.min_cross_section)
                eval_slices = list(task.slice_columns)
                eval_controls = control_columns
            evaluable_rows = int(len(prepared))
            if prepared.empty and task.scope in SCOPE_ALPHA_SEMANTICS:
                result = _insufficient_factor_result(
                    factor_row,
                    factor_keys=factor_keys,
                    scope=task.scope,
                    pit_audit=dict(task.pit_audit),
                    label_contract=contract,
                    reason=(
                        "insufficient_within_theme_members"
                        if task.scope == "within_theme"
                        else "insufficient_inter_theme_cross_section"
                    ),
                    input_rows=input_rows,
                    evaluable_rows=0,
                    symbol_count=symbol_count,
                )
            elif prepared.empty:
                result = _empty_result(governance)
            else:
                result = evaluate_alpha(
                    prepared,
                    score_column=eval_score,
                    label_column=eval_target,
                    time_column="decision_time",
                    symbol_column=eval_symbol,
                    trade_date_column="trade_date",
                    quantiles=task.quantiles,
                    annualization_factor=task.annualization_factor,
                    min_cross_section=eval_min_cross_section,
                    slice_columns=eval_slices,
                    direction_column=task.direction_column,
                    default_direction=task.default_direction,
                    control_columns=eval_controls,
                    label_overlapping=contract.overlapping,
                    governance=governance,
                )
                if task.scope in SCOPE_ALPHA_SEMANTICS:
                    for frame in (
                        result.metrics,
                        result.ic_series,
                        result.daily_ic,
                        result.quantile_returns,
                        result.portfolio_returns,
                        result.stability,
                    ):
                        if not frame.empty:
                            frame["scope_alpha_unit"] = SCOPE_ALPHA_SEMANTICS[
                                task.scope
                            ]
                    if not result.metrics.empty:
                        result.metrics["scope"] = task.scope
                        result.metrics["scope_input_rows"] = input_rows
                        result.metrics["scope_evaluable_rows"] = evaluable_rows
                        result.metrics["scope_drop_reason"] = None
                        result.metrics["inter_theme_cost_semantics"] = (
                            "theme_portfolio_notional"
                            if task.scope == "inter_theme"
                            else "stock_turnover"
                        )

        commit_frames(
            checkpoint_dir,
            checkpoint_spec,
            {
                name: _checkpoint_frame(frame)
                for name, frame in _result_frames(result).items()
            },
            metadata={
                "horizon": task.horizon,
                "horizon_minutes": task.horizon_minutes,
                "scope": task.scope,
                "ordinal": task.ordinal,
                "factor_identity": task.factor_identity,
                "input_rows": input_rows,
                "evaluable_rows": evaluable_rows,
                "worker_pid": os.getpid(),
                "worker_memory_limit_gb": task.worker_memory_limit_gb,
                "worker_threads": task.worker_threads,
            },
        )
        return {
            "unit": task.unit_name,
            "horizon": task.horizon,
            "scope": task.scope,
            "status": "complete",
            "checkpoint": str(checkpoint_dir),
            "input_rows": input_rows,
            "evaluable_rows": evaluable_rows,
        }
    finally:
        connection.close()


def _discover_scope_inventory(
    signal_path: Path,
    resource_budget: ResourceBudget,
) -> tuple[list[str], list[dict[str, object]], set[str]]:
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, resource_budget)
        connection.execute(
            f"CREATE VIEW signals AS SELECT * FROM {_table_expression(signal_path)}"
        )
        factor_keys, factors = _factor_keys(connection)
        identities: list[dict[str, object]] = []
        for _, row in factors.iterrows():
            identity: dict[str, object] = {}
            for key in factor_keys:
                value = row[key]
                if pd.isna(value):
                    identity[key] = None
                elif hasattr(value, "item"):
                    identity[key] = value.item()
                else:
                    identity[key] = value
            identities.append(identity)
        return factor_keys, identities, _columns(connection, "signals")
    finally:
        connection.close()


def _audit_scope_horizon(
    signal_path: Path,
    labels_path: Path,
    contract: LabelContract,
    join_keys: list[str],
    allow_legacy_signals: bool,
    resource_budget: ResourceBudget,
) -> dict[str, object]:
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, resource_budget)
        connection.execute(
            f"CREATE VIEW signals AS SELECT * FROM {_table_expression(signal_path)}"
        )
        connection.execute(
            f"CREATE VIEW labels AS SELECT * FROM {_table_expression(labels_path)}"
        )
        return _audit_pit(
            connection,
            contract,
            join_keys=join_keys,
            allow_legacy_signals=allow_legacy_signals,
        ).as_dict()
    finally:
        connection.close()


def _write_global_progress(
    root: Path,
    *,
    status: str,
    total: int,
    completed: int,
    reused: int,
    failed: int,
    factor_workers: int,
    active: Iterable[str],
    counts_by_horizon: dict[str, dict[str, int]],
    last_completed: str | None = None,
    error: str | None = None,
) -> None:
    payload = {
        "stage": "dual-theme-global-factor-dag",
        "status": status,
        "scheduler": "bounded_process_pool_global_ready_queue",
        "barriers": {
            "scope": False,
            "horizon": False,
            "factor_completion": True,
            "horizon_reduce_dependency": "all factor nodes for that horizon",
            "final_reduce_dependency": "all horizon reducers",
        },
        "factor_workers": int(factor_workers),
        "total_tasks": int(total),
        "completed_tasks": int(completed),
        "reused_tasks": int(reused),
        "failed_tasks": int(failed),
        "remaining_tasks": max(0, int(total) - int(completed)),
        "progress_pct": round(100.0 * completed / total, 3) if total else 100.0,
        "active_tasks": list(active),
        "last_completed": last_completed,
        "counts_by_horizon": counts_by_horizon,
        "error": error,
    }
    atomic_write_json(root / "global_dag_progress.json", payload)
    lines = [
        "# Dual-theme global factor DAG",
        "",
        f"- Status: **{status}**",
        f"- Progress: **{completed}/{total} ({payload['progress_pct']}%)**",
        f"- Reused factor checkpoints: {reused}",
        f"- Failed tasks: {failed}",
        f"- Factor workers: {factor_workers}",
        "- Scope barrier: none",
        "- Horizon barrier: none",
        "",
        "## Active",
        "",
    ]
    active_rows = list(active)
    lines.extend(f"- `{unit}`" for unit in active_rows)
    if not active_rows:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Horizon progress",
            "",
            "| Horizon | Total | Complete | Reused | Remaining |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for horizon, counts in sorted(counts_by_horizon.items()):
        lines.append(
            f"| {horizon} | {counts['total']} | {counts['completed']} | "
            f"{counts['reused']} | {counts['total'] - counts['completed']} |"
        )
    atomic_write_text(root / "GLOBAL_DAG.md", "\n".join(lines) + "\n")


def _finalize_horizon_from_checkpoints(
    plan: HorizonPlan,
    *,
    expected_factors: int,
    output: Path,
    batch_id: str,
    signal_success: Path,
    export_manifest_path: Path,
    metadata_file: Path | None,
    correlation_sample_modulus: int,
    symbol_column: str,
    score_column: str,
    resource_budget: ResourceBudget,
    allow_partial: bool,
) -> None:
    results: list[AlphaResult] = []
    for task in plan.tasks:
        checkpoint_spec = _task_checkpoint_spec(task)
        if not checkpoint_valid(
            task.checkpoint_dir,
            checkpoint_spec,
            required_files=_FRAME_FILES,
        ):
            raise RuntimeError(f"Missing valid factor checkpoint: {task.unit_name}")
        governance = {
            "pit_audit": dict(task.pit_audit),
            "label_contract": plan.contract.as_dict(),
            "scope": task.scope,
            "scope_semantics": SCOPE_ALPHA_SEMANTICS.get(
                task.scope, "stock_global_cross_section"
            ),
            "checkpoint_contract_hash": plan.checkpoint_contract_hash,
            "checkpoint_granularity": "global_dag_scope_factor",
        }
        results.append(
            _result_from_checkpoint(task.checkpoint_dir, governance=governance)
        )
    result = combine_alpha_results(
        results,
        governance={
            "scope_alpha_contract": {
                "global": "stock_global_cross_section",
                **SCOPE_ALPHA_SEMANTICS,
            },
            "component_count": len(results),
            "checkpoint_granularity": "global_dag_scope_factor",
            "checkpoint_contract_hash": plan.checkpoint_contract_hash,
            "scheduler": "global_ready_queue_no_scope_or_horizon_barrier",
        },
    )
    if correlation_sample_modulus > 0:
        global_tasks = [task for task in plan.tasks if task.scope == "global"]
        if global_tasks:
            global_path = Path(global_tasks[0].signal_path)
            connection = duckdb.connect()
            try:
                configure_duckdb(connection, resource_budget)
                connection.execute(
                    f"CREATE VIEW signals AS SELECT * FROM {_table_expression(global_path)}"
                )
                factor_keys, _ = _factor_keys(connection)
                result.score_correlation = _score_correlation(
                    connection,
                    factor_keys=factor_keys,
                    symbol_column=symbol_column,
                    score_column=score_column,
                    modulus=correlation_sample_modulus,
                )
            finally:
                connection.close()
    result = _annotate_result(result, plan.name, plan.contract)
    identity_columns = [
        column
        for column in ("factor_id", "layer_id", "scale_minutes", "variant_id")
        if column in result.metrics.columns
    ]
    observed = (
        int(result.metrics[identity_columns].drop_duplicates().shape[0])
        if identity_columns and not result.metrics.empty
        else 0
    )
    complete = observed == expected_factors
    if not complete and not allow_partial:
        raise ValueError(
            f"Horizon {plan.name} expected {expected_factors} factors, "
            f"observed {observed}"
        )
    batch_status = {
        "batch_id": DUAL_THEME_BATCH_ID,
        "expected_contracts": expected_factors,
        "observed_contracts": observed,
        "complete": complete,
        "partial": not complete,
        "report_scope": list(get_batch(DUAL_THEME_BATCH_ID).report_scope),
    }
    inputs = {
        "signal_manifest": file_record(export_manifest_path),
        "signal_success": file_record(signal_success),
        "label_files": plan.label_records,
        "label_contract": file_record(plan.label_contract_path),
        **({"metadata": file_record(metadata_file)} if metadata_file else {}),
    }
    write_report_bundle(
        plan.horizon_output,
        batch_id=DUAL_THEME_BATCH_ID,
        batch_status=batch_status,
        alpha=result,
        lineage=inputs,
        run_manifest=plan.run_manifest,
    )
    _write_horizon_checkpoint(
        plan.horizon_output,
        checkpoint_contract_hash=plan.checkpoint_contract_hash,
        observed=observed,
        expected=expected_factors,
    )
    plan.finalized = True


def _build_run_manifest(
    *,
    horizon_name: str,
    contract: LabelContract,
    expected_factors: int,
    checkpoint_contract_hash: str,
    factor_workers: int,
    join_keys: list[str],
    score_column: str,
    symbol_column: str,
    quantiles: int,
    min_cross_section: int,
    control_columns: list[str],
    min_theme_size: int,
    min_theme_cross_section: int,
    direction_column: str | None,
    default_direction: str,
    annualization_factor: float | None,
    correlation_sample_modulus: int,
    manifest_inputs: list[dict[str, object]],
    resource_budget: ResourceBudget,
) -> dict[str, object]:
    return implementation_manifest(
        operation="dual_theme_alpha_report",
        parameters={
            "version": DUAL_THEME_ALPHA_VERSION,
            "horizon": horizon_name,
            "label_contract": contract.as_dict(),
            "join_keys": join_keys,
            "score_column": score_column,
            "symbol_column": symbol_column,
            "quantiles": int(quantiles),
            "min_cross_section": int(min_cross_section),
            "control_columns": control_columns,
            "expected_factors": expected_factors,
            "scope_alpha_contract": {
                "global": "stock_global_cross_section",
                **SCOPE_ALPHA_SEMANTICS,
            },
            "financial_semantics_policy": {
                "direct_alpha": "only layer target=return joined to return labels",
                "risk_layers": "diagnostic regime candidates, never direct Alpha promotion",
            },
            "min_theme_size": int(min_theme_size),
            "min_theme_cross_section": int(min_theme_cross_section),
            "direction_column": direction_column,
            "default_direction": default_direction,
            "annualization_factor": annualization_factor,
            "correlation_sample_modulus": int(correlation_sample_modulus),
            "checkpoint_granularity": "horizon_and_scope_factor",
            "checkpoint_contract_hash": checkpoint_contract_hash,
            "factor_workers": int(factor_workers),
        },
        inputs=manifest_inputs,
        resource_budget=resource_budget,
    )


def run_dual_theme_alpha_campaign(
    signals_root: str | Path,
    horizon_manifest: str | Path,
    output_root: str | Path,
    *,
    metadata_path: str | Path | None = None,
    metadata_id: str = "symbol_id",
    metadata_signal_id: str = "symbol_id",
    slice_dimensions: Iterable[str] = (),
    join_keys: Iterable[str] = ("trade_date", "decision_time", "symbol_id"),
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
    expected_git_commit: str | None = None,
    require_clean: bool = False,
    allow_legacy_signals: bool = False,
    correlation_sample_modulus: int = 1000,
    allow_partial: bool = False,
    factor_workers: int = 6,
) -> Path:
    workers = int(factor_workers)
    if workers <= 0:
        raise ValueError("factor_workers must be positive")
    signals = Path(signals_root).expanduser().resolve()
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)
    (output / "_PARTIAL").unlink(missing_ok=True)
    signal_success = signals / "_SUCCESS"
    export_manifest_path = signals / "export_manifest.json"
    if not signal_success.exists() or not export_manifest_path.exists():
        raise FileNotFoundError(f"Dual-theme signals are incomplete: {signals}")
    export_manifest = json.loads(export_manifest_path.read_text(encoding="utf-8"))
    export_version = str(
        export_manifest.get("export_version")
        or export_manifest.get("parameters", {}).get("version")
        or ""
    )
    if export_version != DUAL_THEME_EXPORT_VERSION and not allow_legacy_signals:
        raise ValueError(
            f"Dual-theme signals use export version {export_version!r}; "
            f"expected {DUAL_THEME_EXPORT_VERSION!r}"
        )
    if export_version != DUAL_THEME_EXPORT_VERSION:
        raise ValueError(
            "Global factor DAG requires governed V2 scope semantics"
        )
    expected_factors = int(export_manifest.get("factor_count") or 0)
    if expected_factors <= 0:
        raise ValueError("Export manifest has no governed factors")
    batch_id = str(
        export_manifest.get("parameters", {}).get("batch_id")
        or DUAL_THEME_BATCH_ID
    )
    horizon_specs = load_horizon_manifest(horizon_manifest)
    join_key_list = list(join_keys)
    control_column_list = list(control_columns)
    dimensions = list(slice_dimensions)
    metadata_file = (
        Path(metadata_path).expanduser().resolve()
        if metadata_path is not None
        else None
    )
    if metadata_file is not None:
        raw = (
            pd.read_parquet(metadata_file)
            if metadata_file.suffix.lower() in {".parquet", ".pq"}
            else pd.read_csv(metadata_file)
        )
        _, metadata_profile = normalize_metadata(
            raw,
            id_column=metadata_id,
            dimensions=dimensions or None,
        )
        dimensions = list(metadata_profile.dimensions)
    worker_budget = split_worker_budget(resource_budget, workers)
    preflight_budget = ResourceBudget(
        max(1.0, worker_budget.memory_limit_gb),
        max(1, worker_budget.threads),
        resource_budget.temp_directory,
    )

    scope_inventory: dict[
        str, tuple[Path, list[str], list[dict[str, object]], set[str]]
    ] = {}
    for scope in ("global", "within_theme", "inter_theme"):
        scope_root = _scope_path(signals, batch_id, scope)
        if not scope_root.exists():
            continue
        factor_keys, identities, signal_columns = _discover_scope_inventory(
            scope_root, preflight_budget
        )
        scope_inventory[scope] = (
            scope_root,
            factor_keys,
            identities,
            signal_columns,
        )
    observed_inventory = sum(len(row[2]) for row in scope_inventory.values())
    if observed_inventory != expected_factors:
        raise ValueError(
            f"Global DAG inventory expected {expected_factors} factors per horizon, "
            f"observed {observed_inventory} across scopes"
        )

    plans: dict[str, HorizonPlan] = {}
    checkpoint_root = output / "_checkpoints" / "dual_theme_alpha"
    checkpoint_root.mkdir(parents=True, exist_ok=True)

    for horizon_spec in horizon_specs:
        contract = LabelContract.from_json(horizon_spec.label_contract)
        label_records = directory_parquet_records(horizon_spec.labels)
        manifest_inputs = [
            stable_export_manifest_record(export_manifest_path),
            file_record(horizon_spec.label_contract),
            *label_records,
        ]
        if metadata_file is not None:
            manifest_inputs.append(file_record(metadata_file))
        checkpoint_contract_hash = _checkpoint_contract_hash(
            horizon_name=horizon_spec.name,
            contract=contract,
            expected_factors=expected_factors,
            manifest_inputs=manifest_inputs,
            join_keys=join_key_list,
            score_column=score_column,
            symbol_column=symbol_column,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            control_columns=control_column_list,
            min_theme_size=min_theme_size,
            min_theme_cross_section=min_theme_cross_section,
            direction_column=direction_column,
            default_direction=default_direction,
            annualization_factor=annualization_factor,
            correlation_sample_modulus=correlation_sample_modulus,
        )
        run_manifest = _build_run_manifest(
            horizon_name=horizon_spec.name,
            contract=contract,
            expected_factors=expected_factors,
            checkpoint_contract_hash=checkpoint_contract_hash,
            factor_workers=workers,
            join_keys=join_key_list,
            score_column=score_column,
            symbol_column=symbol_column,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            control_columns=control_column_list,
            min_theme_size=min_theme_size,
            min_theme_cross_section=min_theme_cross_section,
            direction_column=direction_column,
            default_direction=default_direction,
            annualization_factor=annualization_factor,
            correlation_sample_modulus=correlation_sample_modulus,
            manifest_inputs=manifest_inputs,
            resource_budget=resource_budget,
        )
        enforce_git_lineage(
            run_manifest,
            expected_commit=expected_git_commit,
            require_clean=require_clean,
        )
        contract_hash = checkpoint_contract_hash
        horizon_output = output / f"horizon={horizon_spec.name}"
        reusable = _strict_horizon_compatible(
            horizon_output,
            checkpoint_contract_hash=contract_hash,
        ) or _legacy_horizon_compatible(
            horizon_output,
            horizon_name=horizon_spec.name,
            contract=contract,
            expected_factors=expected_factors,
            manifest_inputs=manifest_inputs,
            join_keys=join_key_list,
            control_columns=control_column_list,
            min_theme_size=min_theme_size,
            min_theme_cross_section=min_theme_cross_section,
            quantiles=quantiles,
            min_cross_section=min_cross_section,
            score_column=score_column,
            symbol_column=symbol_column,
            direction_column=direction_column,
            default_direction=default_direction,
            annualization_factor=annualization_factor,
            correlation_sample_modulus=correlation_sample_modulus,
        )
        plan = HorizonPlan(
            name=horizon_spec.name,
            horizon_minutes=int(contract.horizon_minutes),
            labels_path=Path(horizon_spec.labels),
            label_contract_path=Path(horizon_spec.label_contract),
            contract=contract,
            run_manifest=run_manifest,
            checkpoint_contract_hash=contract_hash,
            manifest_inputs=manifest_inputs,
            label_records=label_records,
            horizon_output=horizon_output,
            tasks=[],
            reusable_bundle=reusable,
            finalized=reusable,
        )
        plans[plan.name] = plan
        if reusable:
            continue
        ordinal = 0
        for scope, (
            scope_root,
            factor_keys,
            identities,
            signal_columns,
        ) in scope_inventory.items():
            pit_audit = _audit_scope_horizon(
                scope_root,
                Path(horizon_spec.labels),
                contract,
                join_key_list,
                allow_legacy_signals,
                preflight_budget,
            )
            for identity in identities:
                ordinal += 1
                task = FactorTask(
                    horizon=plan.name,
                    horizon_minutes=plan.horizon_minutes,
                    scope=scope,
                    ordinal=ordinal,
                    signal_path=str(scope_root),
                    labels_path=str(Path(horizon_spec.labels)),
                    label_contract_path=str(Path(horizon_spec.label_contract)),
                    checkpoint_root=str(checkpoint_root),
                    checkpoint_contract_hash=contract_hash,
                    factor_keys=tuple(factor_keys),
                    factor_identity=dict(identity),
                    pit_audit=pit_audit,
                    signal_columns=tuple(sorted(signal_columns)),
                    join_keys=tuple(join_key_list),
                    metadata_path=str(metadata_file) if metadata_file else None,
                    metadata_id=metadata_id,
                    metadata_signal_id=metadata_signal_id,
                    slice_columns=tuple(dimensions),
                    score_column=score_column,
                    symbol_column=symbol_column,
                    quantiles=int(quantiles),
                    min_cross_section=int(min_cross_section),
                    min_theme_size=int(min_theme_size),
                    min_theme_cross_section=int(min_theme_cross_section),
                    direction_column=direction_column,
                    default_direction=default_direction,
                    control_columns=tuple(control_column_list),
                    annualization_factor=annualization_factor,
                    allow_legacy_signals=allow_legacy_signals,
                    worker_memory_limit_gb=worker_budget.memory_limit_gb,
                    worker_threads=worker_budget.threads,
                    temp_directory=worker_budget.temp_directory,
                )
                plan.tasks.append(task)

    counts_by_horizon: dict[str, dict[str, int]] = {}
    pending_tasks: list[FactorTask] = []
    reused = 0
    for plan in plans.values():
        total = len(plan.tasks)
        completed = 0
        plan_reused = 0
        if plan.reusable_bundle:
            total = expected_factors
            completed = expected_factors
            plan_reused = expected_factors
        else:
            for task in plan.tasks:
                if checkpoint_valid(
                    task.checkpoint_dir,
                    _task_checkpoint_spec(task),
                    required_files=_FRAME_FILES,
                ):
                    completed += 1
                    plan_reused += 1
                else:
                    pending_tasks.append(task)
        counts_by_horizon[plan.name] = {
            "total": total,
            "completed": completed,
            "reused": plan_reused,
        }
        reused += plan_reused

    total_tasks = sum(row["total"] for row in counts_by_horizon.values())
    completed = sum(row["completed"] for row in counts_by_horizon.values())
    ordered_pending = deque(round_robin_tasks(pending_tasks))
    active: dict[object, FactorTask] = {}
    last_completed: str | None = None
    failed = 0
    failures: list[str] = []
    _write_global_progress(
        checkpoint_root,
        status="running",
        total=total_tasks,
        completed=completed,
        reused=reused,
        failed=failed,
        factor_workers=workers,
        active=(),
        counts_by_horizon=counts_by_horizon,
    )

    def maybe_finalize(horizon_name: str) -> None:
        plan = plans[horizon_name]
        counts = counts_by_horizon[horizon_name]
        if plan.finalized or counts["completed"] != counts["total"]:
            return
        _finalize_horizon_from_checkpoints(
            plan,
            expected_factors=expected_factors,
            output=output,
            batch_id=batch_id,
            signal_success=signal_success,
            export_manifest_path=export_manifest_path,
            metadata_file=metadata_file,
            correlation_sample_modulus=correlation_sample_modulus,
            symbol_column=symbol_column,
            score_column=score_column,
            resource_budget=resource_budget,
            allow_partial=allow_partial,
        )

    for horizon_name in plans:
        maybe_finalize(horizon_name)

    try:
        if ordered_pending:
            inflight_limit = max(workers, workers * 2)
            with ProcessPoolExecutor(max_workers=workers) as executor:
                while ordered_pending and len(active) < inflight_limit:
                    task = ordered_pending.popleft()
                    active[executor.submit(_run_factor_task, task)] = task
                while active:
                    done, _ = wait(active, return_when=FIRST_COMPLETED)
                    for future in done:
                        task = active.pop(future)
                        try:
                            result = future.result()
                        except Exception as exc:
                            # Record the failure and keep scheduling; horizon and
                            # campaign reducers enforce factor completeness, so an
                            # incomplete run can never publish _SUCCESS.
                            failed += 1
                            failures.append(f"{task.unit_name}: {exc!r}")
                            if ordered_pending:
                                replacement = ordered_pending.popleft()
                                active[
                                    executor.submit(_run_factor_task, replacement)
                                ] = replacement
                            continue
                        last_completed = str(result["unit"])
                        completed += 1
                        horizon_counts = counts_by_horizon[task.horizon]
                        horizon_counts["completed"] += 1
                        if result.get("status") == "reused":
                            reused += 1
                            horizon_counts["reused"] += 1
                        maybe_finalize(task.horizon)
                        if ordered_pending:
                            next_task = ordered_pending.popleft()
                            active[
                                executor.submit(_run_factor_task, next_task)
                            ] = next_task
                    _write_global_progress(
                        checkpoint_root,
                        status="running",
                        total=total_tasks,
                        completed=completed,
                        reused=reused,
                        failed=failed,
                        factor_workers=workers,
                        active=(task.unit_name for task in active.values()),
                        counts_by_horizon=counts_by_horizon,
                        last_completed=last_completed,
                        error="; ".join(failures) if failures else None,
                    )
        for horizon_name in plans:
            maybe_finalize(horizon_name)
        if failures:
            raise RuntimeError(
                f"{len(failures)} factor task(s) failed: " + "; ".join(failures[:20])
            )
    except Exception as exc:
        failed += 1
        _write_global_progress(
            checkpoint_root,
            status="failed",
            total=total_tasks,
            completed=completed,
            reused=reused,
            failed=failed,
            factor_workers=workers,
            active=(task.unit_name for task in active.values()),
            counts_by_horizon=counts_by_horizon,
            last_completed=last_completed,
            error=repr(exc),
        )
        raise

    _write_global_progress(
        checkpoint_root,
        status="reducing",
        total=total_tasks,
        completed=completed,
        reused=reused,
        failed=failed,
        factor_workers=workers,
        active=(),
        counts_by_horizon=counts_by_horizon,
        last_completed=last_completed,
    )
    result_path = _finalize_campaign(
        signals,
        horizon_manifest,
        output,
        metadata_path=metadata_file,
        metadata_id=metadata_id,
        metadata_signal_id=metadata_signal_id,
        slice_dimensions=dimensions,
        join_keys=join_key_list,
        score_column=score_column,
        symbol_column=symbol_column,
        quantiles=quantiles,
        min_cross_section=min_cross_section,
        min_theme_size=min_theme_size,
        min_theme_cross_section=min_theme_cross_section,
        direction_column=direction_column,
        default_direction=default_direction,
        control_columns=control_column_list,
        annualization_factor=annualization_factor,
        resource_budget=resource_budget,
        expected_git_commit=expected_git_commit,
        require_clean=require_clean,
        allow_legacy_signals=allow_legacy_signals,
        correlation_sample_modulus=correlation_sample_modulus,
        allow_partial=allow_partial,
        factor_workers=workers,
    )
    _write_global_progress(
        checkpoint_root,
        status="complete",
        total=total_tasks,
        completed=total_tasks,
        reused=reused,
        failed=failed,
        factor_workers=workers,
        active=(),
        counts_by_horizon=counts_by_horizon,
        last_completed=last_completed,
    )
    summary_path = output / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["scheduler"] = {
            "mode": "global_factor_dag",
            "factor_workers": workers,
            "scope_barrier": False,
            "horizon_barrier": False,
            "worker_budget": worker_budget.as_dict(),
            "total_budget": resource_budget.as_dict(),
        }
        atomic_write_json(summary_path, summary)
    return result_path
