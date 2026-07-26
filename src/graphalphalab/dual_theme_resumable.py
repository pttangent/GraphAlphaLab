from __future__ import annotations

import gc
import json
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from .alpha import AlphaResult, evaluate_alpha
from .batch import get_batch
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
from .dual_theme_common import (
    DUAL_THEME_ALPHA_VERSION,
    DUAL_THEME_BATCH_ID,
    DUAL_THEME_EXPORT_VERSION,
    load_horizon_manifest,
)
from .dual_theme_reporting import (
    _annotate_result,
    _cross_scope_comparison,
    _label_target_semantics,
    _matched_variant_comparison,
    _scope_summary,
    _semantic_summary,
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
    atomic_write_frame,
    atomic_write_json,
    atomic_write_text,
    configure_duckdb,
    directory_parquet_records,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
    sha256_file,
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


_FRAME_FILES = {
    "metrics.parquet": "metrics",
    "ic_series.parquet": "ic_series",
    "daily_ic.parquet": "daily_ic",
    "quantile_returns.parquet": "quantile_returns",
    "portfolio_returns.parquet": "portfolio_returns",
    "stability.parquet": "stability",
}


def _checkpoint_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.shape[1] == 0:
        return pd.DataFrame({"_checkpoint_empty": pd.Series(dtype="int8")})
    return frame


def _restore_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if list(frame.columns) == ["_checkpoint_empty"]:
        return pd.DataFrame()
    return frame


def _factor_identity(keys: list[str], row: pd.Series) -> dict[str, object]:
    result: dict[str, object] = {}
    for key in keys:
        value = row[key]
        if pd.isna(value):
            result[key] = None
        elif hasattr(value, "item"):
            result[key] = value.item()
        else:
            result[key] = value
    return result


def _result_frames(result: AlphaResult) -> dict[str, pd.DataFrame]:
    return {
        name: getattr(result, attribute)
        for name, attribute in _FRAME_FILES.items()
    }


def _result_from_checkpoint(
    root: Path,
    *,
    governance: dict[str, object],
) -> AlphaResult:
    loaded = load_checkpoint_frames(root, _FRAME_FILES)
    restored = {
        _FRAME_FILES[name]: _restore_frame(frame)
        for name, frame in loaded.items()
    }
    return AlphaResult(
        metrics=restored["metrics"],
        ic_series=restored["ic_series"],
        daily_ic=restored["daily_ic"],
        quantile_returns=restored["quantile_returns"],
        portfolio_returns=restored["portfolio_returns"],
        stability=restored["stability"],
        score_correlation=pd.DataFrame(),
        governance=governance,
    )


def _scope_path(signals: Path, batch_id: str, scope: str) -> Path:
    return signals / f"batch_id={batch_id}" / f"scope={scope}"


def _score_correlation(
    connection: duckdb.DuckDBPyConnection,
    *,
    factor_keys: list[str],
    symbol_column: str,
    score_column: str,
    modulus: int,
) -> pd.DataFrame:
    if modulus <= 0:
        return pd.DataFrame()
    identity_sql = " || '|' || ".join(
        f"coalesce(CAST(\"{column}\" AS VARCHAR), '')" for column in factor_keys
    )
    sample = connection.execute(
        f"""
        SELECT {identity_sql} AS factor_key,
               decision_time,
               "{symbol_column}" AS symbol_id,
               "{score_column}" AS score
        FROM signals
        WHERE abs(hash(CAST(decision_time AS VARCHAR) || '|' || CAST("{symbol_column}" AS VARCHAR)))
              % {int(modulus)} = 0
        """
    ).fetch_df()
    if sample.empty:
        return pd.DataFrame()
    pivot = sample.pivot_table(
        index=["decision_time", "symbol_id"],
        columns="factor_key",
        values="score",
        aggfunc="mean",
    )
    if pivot.shape[1] < 2:
        return pd.DataFrame()
    matrix = pivot.corr(method="spearman")
    correlation = (
        matrix.rename_axis(index="factor_a", columns="factor_b")
        .reset_index()
        .melt(
            id_vars="factor_a",
            var_name="factor_b",
            value_name="score_spearman_correlation",
        )
    )
    correlation["sample_rows"] = int(len(sample))
    correlation["sample_modulus"] = int(modulus)
    return correlation


def _evaluate_scope_checkpointed(
    signals_path: Path,
    labels_path: Path,
    *,
    scope: str,
    checkpoint_root: Path,
    checkpoint_contract_hash: str,
    label_contract: LabelContract,
    join_keys: list[str],
    metadata: pd.DataFrame | None,
    metadata_signal_id: str,
    metadata_id: str,
    slice_columns: list[str],
    score_column: str,
    symbol_column: str,
    quantiles: int,
    min_cross_section: int,
    min_theme_size: int,
    min_theme_cross_section: int,
    direction_column: str | None,
    default_direction: str,
    control_columns: list[str],
    annualization_factor: float | None,
    resource_budget: ResourceBudget,
    allow_legacy_signals: bool,
    correlation_sample_modulus: int,
) -> AlphaResult:
    label_contract.validate()
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
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
            join_keys=join_keys,
            allow_legacy_signals=allow_legacy_signals,
        )
        signal_columns = _columns(connection, "signals")
        label_columns = _columns(connection, "labels")
        required_signal = set(join_keys)
        if scope in SCOPE_ALPHA_SEMANTICS:
            required_signal.update({"context_theme_id", "membership_weight"})
        missing_signal = sorted(required_signal - signal_columns)
        missing_label = sorted(set(join_keys) - label_columns)
        if missing_signal or missing_label:
            raise ValueError(
                f"{scope} scope contract columns missing; "
                f"signal_missing={missing_signal}, label_missing={missing_label}"
            )
        factor_keys, factors = _factor_keys(connection)
        if factors.empty:
            return combine_alpha_results(
                [],
                governance={
                    "pit_audit": pit_audit.as_dict(),
                    "label_contract": label_contract.as_dict(),
                    "scope": scope,
                    "checkpoint_granularity": "factor",
                    "factor_count": 0,
                },
            )
        select_signal = [
            *factor_keys,
            *join_keys,
            symbol_column,
            score_column,
        ]
        if scope in SCOPE_ALPHA_SEMANTICS:
            select_signal.extend(["context_theme_id", "membership_weight"])
        select_signal.extend(
            column
            for column in (
                "signal_available_time",
                direction_column,
                *control_columns,
            )
            if column and column in signal_columns
        )
        select_signal = list(dict.fromkeys(select_signal))
        signal_projection = ", ".join(
            f's."{column}"' for column in select_signal
        )
        join_expression = " AND ".join(
            f's."{key}"=l."{key}"' for key in join_keys
        )
        label_filter = (
            f"CAST(l.label_id AS VARCHAR)={_sql_literal(label_contract.label_id)}"
        )
        results: list[AlphaResult] = []
        unit_states: list[dict[str, object]] = []
        reused = 0
        completed = 0
        total = int(len(factors))
        write_progress(
            checkpoint_root,
            stage=f"dual-theme-{scope}-factor-checkpoints",
            total=total,
            completed=0,
            units=(),
        )
        governance = {
            "pit_audit": pit_audit.as_dict(),
            "label_contract": label_contract.as_dict(),
            "scope": scope,
            "scope_semantics": (
                SCOPE_ALPHA_SEMANTICS.get(scope, "stock_global_cross_section")
            ),
            "checkpoint_contract_hash": checkpoint_contract_hash,
            "checkpoint_granularity": "factor",
        }
        for ordinal, (_, factor_row) in enumerate(factors.iterrows(), start=1):
            identity = _factor_identity(factor_keys, factor_row)
            unit_name = "|".join(
                f"{key}={identity.get(key)}" for key in factor_keys
            )
            unit_dir = checkpoint_root / safe_key(identity, prefix="factor")
            source_hash = sha256_json(
                {
                    "contract_hash": checkpoint_contract_hash,
                    "scope": scope,
                    "identity": identity,
                }
            )
            checkpoint_spec = CheckpointSpec(
                stage=f"dual-theme-alpha-{scope}-factor",
                unit=unit_name,
                contract_hash=checkpoint_contract_hash,
                source_hash=source_hash,
            )
            state = {
                "unit": unit_name,
                "status": "pending",
                "attempt": 0,
                "detail": str(unit_dir),
            }
            unit_states.append(state)
            if checkpoint_valid(
                unit_dir,
                checkpoint_spec,
                required_files=_FRAME_FILES,
            ):
                results.append(
                    _result_from_checkpoint(unit_dir, governance=governance)
                )
                reused += 1
                completed += 1
                state["status"] = "reused"
                write_progress(
                    checkpoint_root,
                    stage=f"dual-theme-{scope}-factor-checkpoints",
                    total=total,
                    completed=completed,
                    reused=reused,
                    current=unit_name,
                    units=unit_states,
                )
                continue
            state["status"] = "running"
            state["attempt"] = 1
            write_progress(
                checkpoint_root,
                stage=f"dual-theme-{scope}-factor-checkpoints",
                total=total,
                completed=completed,
                reused=reused,
                current=unit_name,
                units=unit_states,
            )
            where = _factor_where(factor_keys, factor_row)
            factor = connection.execute(
                f"""
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
            ).fetch_df()
            input_rows = int(len(factor))
            symbol_count = (
                int(factor[symbol_column].nunique())
                if symbol_column in factor.columns and not factor.empty
                else 0
            )
            if factor.empty and scope in SCOPE_ALPHA_SEMANTICS:
                result = _insufficient_factor_result(
                    factor_row,
                    factor_keys=factor_keys,
                    scope=scope,
                    pit_audit=pit_audit.as_dict(),
                    label_contract=label_contract,
                    reason="no_label_overlap",
                    input_rows=0,
                    evaluable_rows=0,
                    symbol_count=0,
                )
            elif factor.empty:
                result = AlphaResult(
                    metrics=pd.DataFrame(),
                    ic_series=pd.DataFrame(),
                    daily_ic=pd.DataFrame(),
                    quantile_returns=pd.DataFrame(),
                    portfolio_returns=pd.DataFrame(),
                    stability=pd.DataFrame(),
                    score_correlation=pd.DataFrame(),
                    governance=governance,
                )
            else:
                if scope in {"global", "within_theme"} and metadata is not None:
                    if (
                        metadata_signal_id not in factor.columns
                        or metadata_id not in metadata.columns
                    ):
                        raise ValueError(
                            "Metadata join columns missing: "
                            f"signals={metadata_signal_id!r}, metadata={metadata_id!r}"
                        )
                    factor = factor.merge(
                        metadata,
                        left_on=metadata_signal_id,
                        right_on=metadata_id,
                        how="left",
                        validate="many_to_one",
                    )
                if scope == "within_theme":
                    prepared = _prepare_within_factor(
                        factor,
                        score_column=score_column,
                        control_columns=control_columns,
                        min_theme_size=min_theme_size,
                    )
                    eval_score = "scope_score"
                    eval_target = "scope_target_return"
                    eval_symbol = symbol_column
                    eval_min_cross_section = int(min_cross_section)
                    eval_slices = slice_columns
                    eval_controls: Iterable[str] = ()
                elif scope == "inter_theme":
                    prepared = _prepare_inter_factor(
                        factor,
                        score_column=score_column,
                        control_columns=control_columns,
                        min_theme_cross_section=min_theme_cross_section,
                        min_theme_size=min_theme_size,
                    )
                    eval_score = "scope_score"
                    eval_target = "scope_target_return"
                    eval_symbol = "symbol_id"
                    eval_min_cross_section = int(min_theme_cross_section)
                    eval_slices = []
                    eval_controls = ()
                else:
                    prepared = factor
                    eval_score = score_column
                    eval_target = "target_return"
                    eval_symbol = symbol_column
                    eval_min_cross_section = int(min_cross_section)
                    eval_slices = slice_columns
                    eval_controls = control_columns
                evaluable_rows = int(len(prepared))
                if prepared.empty and scope in SCOPE_ALPHA_SEMANTICS:
                    result = _insufficient_factor_result(
                        factor_row,
                        factor_keys=factor_keys,
                        scope=scope,
                        pit_audit=pit_audit.as_dict(),
                        label_contract=label_contract,
                        reason=(
                            "insufficient_within_theme_members"
                            if scope == "within_theme"
                            else "insufficient_inter_theme_cross_section"
                        ),
                        input_rows=input_rows,
                        evaluable_rows=0,
                        symbol_count=symbol_count,
                    )
                elif prepared.empty:
                    result = AlphaResult(
                        metrics=pd.DataFrame(),
                        ic_series=pd.DataFrame(),
                        daily_ic=pd.DataFrame(),
                        quantile_returns=pd.DataFrame(),
                        portfolio_returns=pd.DataFrame(),
                        stability=pd.DataFrame(),
                        score_correlation=pd.DataFrame(),
                        governance=governance,
                    )
                else:
                    result = evaluate_alpha(
                        prepared,
                        score_column=eval_score,
                        label_column=eval_target,
                        time_column="decision_time",
                        symbol_column=eval_symbol,
                        trade_date_column="trade_date",
                        quantiles=quantiles,
                        annualization_factor=annualization_factor,
                        min_cross_section=eval_min_cross_section,
                        slice_columns=eval_slices,
                        direction_column=direction_column,
                        default_direction=default_direction,
                        control_columns=eval_controls,
                        label_overlapping=label_contract.overlapping,
                        governance=governance,
                    )
                    if scope in SCOPE_ALPHA_SEMANTICS:
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
                            result.metrics["scope_input_rows"] = input_rows
                            result.metrics["scope_evaluable_rows"] = evaluable_rows
                            result.metrics["scope_drop_reason"] = None
                            result.metrics["inter_theme_cost_semantics"] = (
                                "theme_portfolio_notional"
                                if scope == "inter_theme"
                                else "stock_turnover"
                            )
            frames = _result_frames(result)
            commit_frames(
                unit_dir,
                checkpoint_spec,
                {
                    name: _checkpoint_frame(frame)
                    for name, frame in frames.items()
                },
                metadata={
                    "ordinal": ordinal,
                    "scope": scope,
                    "factor_identity": identity,
                    "input_rows": input_rows,
                },
            )
            results.append(result)
            completed += 1
            state["status"] = "complete"
            write_progress(
                checkpoint_root,
                stage=f"dual-theme-{scope}-factor-checkpoints",
                total=total,
                completed=completed,
                reused=reused,
                current=unit_name,
                units=unit_states,
            )
            del factor, result
            if "prepared" in locals():
                del prepared
            gc.collect()
        combined = combine_alpha_results(
            results,
            governance={
                **governance,
                "factor_count": total,
                "factor_checkpoint_count": total,
                "factor_checkpoint_reused": reused,
                "streaming_mode": "factor_sequential_duckdb_checkpointed",
                "resource_budget": resource_budget.as_dict(),
            },
        )
        if scope == "global":
            combined.score_correlation = _score_correlation(
                connection,
                factor_keys=factor_keys,
                symbol_column=symbol_column,
                score_column=score_column,
                modulus=correlation_sample_modulus,
            )
        write_progress(
            checkpoint_root,
            stage=f"dual-theme-{scope}-factor-checkpoints",
            total=total,
            completed=total,
            reused=reused,
            current=None,
            status="complete",
            units=unit_states,
            extra={"checkpoint_contract_hash": checkpoint_contract_hash},
        )
        return combined
    except Exception as exc:
        write_progress(
            checkpoint_root,
            stage=f"dual-theme-{scope}-factor-checkpoints",
            total=int(len(locals().get("factors", []))),
            completed=int(locals().get("completed", 0)),
            reused=int(locals().get("reused", 0)),
            failed=1,
            current=locals().get("unit_name"),
            status="failed",
            units=locals().get("unit_states", []),
            extra={"error": repr(exc)},
        )
        raise
    finally:
        connection.close()


def _legacy_horizon_compatible(
    horizon_output: Path,
    *,
    horizon_name: str,
    contract: LabelContract,
    expected_factors: int,
    manifest_inputs: list[dict[str, object]],
    join_keys: list[str],
    control_columns: list[str],
    min_theme_size: int,
    min_theme_cross_section: int,
    quantiles: int,
    min_cross_section: int,
    score_column: str,
    symbol_column: str,
    direction_column: str | None,
    default_direction: str,
    annualization_factor: float | None,
    correlation_sample_modulus: int,
) -> bool:
    required = (
        horizon_output / "_SUCCESS",
        horizon_output / "alpha_metrics.csv",
        horizon_output / "summary.json",
        horizon_output / "run_manifest.json",
    )
    if not all(path.exists() for path in required):
        return False
    try:
        summary = json.loads((horizon_output / "summary.json").read_text(encoding="utf-8"))
        run_manifest = json.loads((horizon_output / "run_manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    status = summary.get("batch_status", {})
    if not bool(status.get("complete")) or int(status.get("observed_contracts", -1)) != expected_factors:
        return False
    parameters = run_manifest.get("parameters", {})
    expected_parameters = {
        "version": DUAL_THEME_ALPHA_VERSION,
        "horizon": horizon_name,
        "label_contract": contract.as_dict(),
        "join_keys": join_keys,
        "control_columns": control_columns,
        "expected_factors": expected_factors,
        "min_theme_size": int(min_theme_size),
        "min_theme_cross_section": int(min_theme_cross_section),
    }
    if any(parameters.get(key) != value for key, value in expected_parameters.items()):
        return False
    # Old pinned bundles did not record these parameters. Reuse them only for the
    # exact pinned/default execution contract used by the July campaign.
    if not (
        quantiles == 5
        and min_cross_section == 100
        and score_column == "score"
        and symbol_column == "symbol_id"
        and direction_column == "expected_direction"
        and default_direction == "auto"
        and annualization_factor is None
        and correlation_sample_modulus == 0
    ):
        return False
    def normalized(records: Iterable[dict[str, object]]) -> set[tuple[str, int, str]]:
        return {
            (
                str(record.get("path")),
                int(record.get("size_bytes", -1)),
                str(record.get("sha256")),
            )
            for record in records
            if isinstance(record, dict)
        }
    if normalized(run_manifest.get("inputs", [])) != normalized(manifest_inputs):
        return False
    try:
        metrics = pd.read_csv(horizon_output / "alpha_metrics.csv")
    except Exception:
        return False
    identity_columns = [
        column
        for column in ("factor_id", "layer_id", "scale_minutes", "variant_id")
        if column in metrics.columns
    ]
    observed = (
        int(metrics[identity_columns].drop_duplicates().shape[0])
        if identity_columns and not metrics.empty
        else 0
    )
    return observed == expected_factors


def _strict_horizon_compatible(
    horizon_output: Path,
    *,
    checkpoint_contract_hash: str,
) -> bool:
    marker = horizon_output / "horizon_checkpoint.json"
    metrics_path = horizon_output / "alpha_metrics.csv"
    success = horizon_output / "_SUCCESS"
    if not marker.exists() or not metrics_path.exists() or not success.exists():
        return False
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if payload.get("status") != "complete":
        return False
    if str(payload.get("contract_hash")) != checkpoint_contract_hash:
        return False
    record = payload.get("alpha_metrics", {})
    stat = metrics_path.stat()
    return (
        int(record.get("size_bytes", -1)) == int(stat.st_size)
        and str(record.get("sha256")) == sha256_file(metrics_path)
    )


def _write_horizon_checkpoint(
    horizon_output: Path,
    *,
    checkpoint_contract_hash: str,
    observed: int,
    expected: int,
) -> None:
    metrics_record = file_signature(horizon_output / "alpha_metrics.csv")
    metrics_record["path"] = "alpha_metrics.csv"
    atomic_write_json(
        horizon_output / "horizon_checkpoint.json",
        {
            "status": "complete",
            "checkpoint_granularity": "horizon_and_scope_factor",
            "contract_hash": checkpoint_contract_hash,
            "observed_factors": int(observed),
            "expected_factors": int(expected),
            "alpha_metrics": metrics_record,
        },
    )
    atomic_write_json(
        horizon_output / "_SUCCESS",
        {
            "complete": observed == expected,
            "checkpoint_contract_hash": checkpoint_contract_hash,
        },
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
) -> Path:
    signals = Path(signals_root).expanduser().resolve()
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)
    (output / "_PARTIAL").unlink(missing_ok=True)
    success = signals / "_SUCCESS"
    export_manifest_path = signals / "export_manifest.json"
    if not success.exists() or not export_manifest_path.exists():
        raise FileNotFoundError(f"Dual-theme signals are incomplete: {signals}")
    export_manifest = json.loads(export_manifest_path.read_text(encoding="utf-8"))
    export_version = str(
        export_manifest.get("export_version")
        or export_manifest.get("parameters", {}).get("version")
        or ""
    )
    governed_scope_semantics = export_version == DUAL_THEME_EXPORT_VERSION
    if not governed_scope_semantics and not allow_legacy_signals:
        raise ValueError(
            f"Dual-theme signals use export version {export_version!r}; "
            f"expected {DUAL_THEME_EXPORT_VERSION!r}"
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
    metadata = None
    metadata_profile = None
    metadata_file: Path | None = None
    if metadata_path is not None:
        metadata_file = Path(metadata_path).expanduser().resolve()
        raw = (
            pd.read_parquet(metadata_file)
            if metadata_file.suffix.lower() in {".parquet", ".pq"}
            else pd.read_csv(metadata_file)
        )
        metadata, metadata_profile = normalize_metadata(
            raw,
            id_column=metadata_id,
            dimensions=dimensions or None,
        )
        dimensions = list(metadata_profile.dimensions)
    resolved_metadata_id = (
        metadata_profile.id_column if metadata_profile else metadata_id
    )
    all_metrics: list[pd.DataFrame] = []
    horizon_summaries: list[dict[str, object]] = []
    for spec in horizon_specs:
        contract = LabelContract.from_json(spec.label_contract)
        label_records = directory_parquet_records(spec.labels)
        manifest_inputs = [
            file_record(export_manifest_path),
            file_record(spec.label_contract),
            *label_records,
        ]
        if metadata_file is not None:
            manifest_inputs.append(file_record(metadata_file))
        run_manifest = implementation_manifest(
            operation="dual_theme_alpha_report",
            parameters={
                "version": DUAL_THEME_ALPHA_VERSION,
                "horizon": spec.name,
                "label_contract": contract.as_dict(),
                "join_keys": join_key_list,
                "score_column": score_column,
                "symbol_column": symbol_column,
                "quantiles": int(quantiles),
                "min_cross_section": int(min_cross_section),
                "control_columns": control_column_list,
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
            },
            inputs=manifest_inputs,
            resource_budget=resource_budget,
        )
        enforce_git_lineage(
            run_manifest,
            expected_commit=expected_git_commit,
            require_clean=require_clean,
        )
        checkpoint_contract_hash = str(run_manifest["contract_hash"])
        horizon_output = output / f"horizon={spec.name}"
        reusable = _strict_horizon_compatible(
            horizon_output,
            checkpoint_contract_hash=checkpoint_contract_hash,
        ) or _legacy_horizon_compatible(
            horizon_output,
            horizon_name=spec.name,
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
        if reusable:
            metrics = pd.read_csv(horizon_output / "alpha_metrics.csv")
            if not metrics.empty:
                metrics["source_report"] = str(horizon_output)
                all_metrics.append(metrics)
            horizon_summaries.append(
                {
                    "horizon": spec.name,
                    "horizon_minutes": contract.horizon_minutes,
                    "label_target_semantics": _label_target_semantics(contract),
                    "report": str(horizon_output),
                    "observed_factors": expected_factors,
                    "expected_factors": expected_factors,
                    "complete": True,
                    "checkpoint_reused": True,
                }
            )
            continue
        if not governed_scope_semantics:
            raise ValueError(
                "Factor checkpointing is implemented for governed V2 scope semantics only"
            )
        components: list[AlphaResult] = []
        for scope in ("global", "within_theme", "inter_theme"):
            scope_root = _scope_path(signals, batch_id, scope)
            if not scope_root.exists():
                continue
            components.append(
                _evaluate_scope_checkpointed(
                    scope_root,
                    spec.labels,
                    scope=scope,
                    checkpoint_root=(
                        output
                        / "_checkpoints"
                        / "dual_theme_alpha"
                        / f"horizon={spec.name}"
                        / f"scope={scope}"
                    ),
                    checkpoint_contract_hash=checkpoint_contract_hash,
                    label_contract=contract,
                    join_keys=join_key_list,
                    metadata=metadata,
                    metadata_signal_id=metadata_signal_id,
                    metadata_id=resolved_metadata_id,
                    slice_columns=dimensions,
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
                    allow_legacy_signals=allow_legacy_signals,
                    correlation_sample_modulus=(
                        correlation_sample_modulus if scope == "global" else 0
                    ),
                )
            )
        if not components:
            raise FileNotFoundError(
                f"No governed Global/Within/Inter signal roots below {signals}"
            )
        result = combine_alpha_results(
            components,
            governance={
                "scope_alpha_contract": {
                    "global": "stock_global_cross_section",
                    **SCOPE_ALPHA_SEMANTICS,
                },
                "component_count": len(components),
                "checkpoint_granularity": "horizon_and_scope_factor",
                "checkpoint_contract_hash": checkpoint_contract_hash,
            },
        )
        result = _annotate_result(result, spec.name, contract)
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
                f"Horizon {spec.name} expected {expected_factors} factors, "
                f"observed {observed}; use --allow-partial only for an explicit diagnostic"
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
            "signal_success": file_record(success),
            "label_files": label_records,
            "label_contract": file_record(spec.label_contract),
            **({"metadata": file_record(metadata_file)} if metadata_file else {}),
        }
        write_report_bundle(
            horizon_output,
            batch_id=DUAL_THEME_BATCH_ID,
            batch_status=batch_status,
            alpha=result,
            lineage=inputs,
            run_manifest=run_manifest,
        )
        _write_horizon_checkpoint(
            horizon_output,
            checkpoint_contract_hash=checkpoint_contract_hash,
            observed=observed,
            expected=expected_factors,
        )
        if not result.metrics.empty:
            result.metrics["source_report"] = str(horizon_output)
            all_metrics.append(result.metrics)
        horizon_summaries.append(
            {
                "horizon": spec.name,
                "horizon_minutes": contract.horizon_minutes,
                "label_target_semantics": _label_target_semantics(contract),
                "report": str(horizon_output),
                "observed_factors": observed,
                "expected_factors": expected_factors,
                "complete": complete,
                "checkpoint_reused": False,
            }
        )
    combined = (
        pd.concat(all_metrics, ignore_index=True)
        if all_metrics
        else pd.DataFrame()
    )
    matched = _matched_variant_comparison(combined)
    scope_summary = _scope_summary(combined, matched)
    cross_scope = _cross_scope_comparison(combined)
    semantic_summary = _semantic_summary(combined)
    atomic_write_frame(combined, output / "all_horizons_alpha_metrics.csv")
    atomic_write_frame(matched, output / "matched_variant_comparison.csv")
    atomic_write_frame(scope_summary, output / "scope_family_horizon_summary.csv")
    atomic_write_frame(cross_scope, output / "cross_scope_comparison.csv")
    atomic_write_frame(semantic_summary, output / "financial_semantics_summary.csv")
    for scope in ("global", "within_theme", "inter_theme"):
        scoped = (
            combined[combined["scope"] == scope].copy()
            if "scope" in combined.columns
            else pd.DataFrame()
        )
        atomic_write_frame(scoped, output / f"{scope}_alpha_metrics.csv")
    direct_return = (
        combined[combined["semantic_promotion_eligible"].fillna(False)].copy()
        if "semantic_promotion_eligible" in combined.columns
        else pd.DataFrame()
    )
    regime_candidates = (
        combined[~combined["semantic_promotion_eligible"].fillna(False)].copy()
        if "semantic_promotion_eligible" in combined.columns
        else pd.DataFrame()
    )
    atomic_write_frame(direct_return, output / "direct_return_alpha_metrics.csv")
    atomic_write_frame(regime_candidates, output / "regime_candidate_metrics.csv")
    ranking = matched.copy()
    ranking_columns = [
        column
        for column in (
            "semantic_promotion_eligible",
            "abs_ic_increment_vs_node",
            "abs_ic_increment_vs_reverse_placebo",
            "net_5bps_increment_vs_node",
            "net_5bps_increment_vs_reverse_placebo",
        )
        if column in ranking
    ]
    if ranking_columns:
        ranking = ranking.sort_values(
            ranking_columns,
            ascending=[False] * len(ranking_columns),
            na_position="last",
        )
    atomic_write_frame(ranking, output / "ranking.csv")
    complete_all = bool(horizon_summaries) and all(
        bool(row["complete"]) for row in horizon_summaries
    )
    summary = {
        "version": DUAL_THEME_ALPHA_VERSION,
        "batch_id": DUAL_THEME_BATCH_ID,
        "signals_root": str(signals),
        "export_version": export_version,
        "gff_campaign_version": export_manifest.get("gff_campaign_version"),
        "export_manifest_sha256": sha256_file(export_manifest_path),
        "expected_factor_count_per_horizon": expected_factors,
        "horizons": horizon_summaries,
        "metric_rows": int(len(combined)),
        "matched_rows": int(len(matched)),
        "scope_summary_rows": int(len(scope_summary)),
        "cross_scope_rows": int(len(cross_scope)),
        "direct_return_metric_rows": int(len(direct_return)),
        "regime_candidate_metric_rows": int(len(regime_candidates)),
        "checkpoint_granularity": "horizon_and_scope_factor",
        "scope_alpha_contract": {
            "global": "stock_global_cross_section",
            **SCOPE_ALPHA_SEMANTICS,
        },
        "financial_semantics_policy": {
            "direct_return_alpha": "only *_to_return layers evaluated on return labels",
            "risk_or_liquidity_layers": "reported as regime/cross-domain diagnostics and blocked from direct Alpha promotion",
        },
        "complete": complete_all,
        "inter_theme_cost_warning": (
            "Inter-Theme turnover and transaction costs are measured at the "
            "theme-portfolio notional level. Constituent migration costs require "
            "a separate execution overlay before promotion."
        ),
    }
    atomic_write_json(output / "summary.json", summary)
    lines = [
        "# GraphAlphaLab dual-theme multi-horizon Alpha report",
        "",
        f"- Signals: `{signals}`",
        f"- Export contract: `{export_version}`",
        f"- Expected factor identities per horizon: {expected_factors}",
        f"- Horizons: {', '.join(spec.name for spec in horizon_specs)}",
        "- Checkpoint granularity: completed horizon bundle, then scope × factor.",
        "- A restart reuses validated factor Parquet checkpoints and recomputes pooled FDR/governance gates.",
        "- Global Alpha: stock-level market-wide cross-section.",
        "- Within-Theme Alpha: theme-neutral stock selection.",
        "- Inter-Theme Alpha: membership-weighted theme portfolio allocation.",
        "",
        "Key files: `direct_return_alpha_metrics.csv`, `regime_candidate_metrics.csv`, "
        "`within_theme_alpha_metrics.csv`, `inter_theme_alpha_metrics.csv`, "
        "`cross_scope_comparison.csv`, `matched_variant_comparison.csv`, `ranking.csv`.",
    ]
    atomic_write_text(output / "REPORT.md", "\n".join(lines) + "\n")
    marker = output / ("_SUCCESS" if complete_all else "_PARTIAL")
    atomic_write_json(marker, {"complete": complete_all})
    return output
