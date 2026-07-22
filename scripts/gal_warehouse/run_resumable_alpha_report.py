from __future__ import annotations

import argparse
import gc
import json
from pathlib import Path
import shutil

import duckdb
import numpy as np
import pandas as pd

from graphalphalab.alpha import AlphaResult, _bh_fdr, evaluate_alpha
from graphalphalab.batch import validate_batch_contracts
from graphalphalab.checkpoint import (
    CheckpointSpec,
    checkpoint_valid,
    commit_frames,
    load_checkpoint_frames,
    safe_key,
    write_progress,
)
from graphalphalab.contracts import LabelContract
from graphalphalab.governance import (
    ResourceBudget,
    directory_parquet_records,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
    sha256_json,
)
from graphalphalab.io import read_frame
from graphalphalab.metadata import normalize_metadata
from graphalphalab.reports import write_report_bundle
from graphalphalab.streaming import (
    _audit_pit,
    _columns,
    _factor_keys,
    _factor_where,
    _sql_literal,
    _table_expression,
)


FRAME_FILES = {
    "metrics.parquet": "metrics",
    "ic_series.parquet": "ic_series",
    "daily_ic.parquet": "daily_ic",
    "quantile_returns.parquet": "quantile_returns",
    "portfolio_returns.parquet": "portfolio_returns",
    "stability.parquet": "stability",
}


def csv_list(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def checkpoint_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.shape[1] == 0:
        return pd.DataFrame({"_checkpoint_empty": pd.Series(dtype="int8")})
    return frame


def restore_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if list(frame.columns) == ["_checkpoint_empty"]:
        return pd.DataFrame()
    return frame


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    nonempty = [frame for frame in frames if frame.shape[1] > 0]
    return pd.concat(nonempty, ignore_index=True) if nonempty else pd.DataFrame()


def factor_identity(keys: list[str], row: pd.Series) -> dict[str, object]:
    result: dict[str, object] = {}
    for key in keys:
        value = row[key]
        result[key] = None if pd.isna(value) else value.item() if hasattr(value, "item") else value
    return result


def recompute_global_gates(metrics: pd.DataFrame, pit_passed: bool) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    result = metrics.copy()
    result["fdr_qvalue"], result["fdr_pass"] = _bh_fdr(result["spearman_ic_pvalue_daily"])
    result["governance_ready"] = (
        result["direction_predeclared"].fillna(False)
        & result["annualization_valid"].fillna(False)
        & bool(pit_passed)
    )
    result["research_status"] = np.select(
        [
            result["sample_sufficient"].fillna(False)
            & result["fdr_pass"].fillna(False)
            & result["cost_survives_5bps"].fillna(False)
            & result["direction_consistent"].fillna(False)
            & result["governance_ready"].fillna(False),
            result["sample_sufficient"].fillna(False)
            & (pd.to_numeric(result["mean_spearman_ic"], errors="coerce").abs() >= 0.01),
        ],
        ["candidate", "needs_falsification"],
        default="insufficient_or_rejected",
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Factor-checkpointed GraphAlphaLab alpha report")
    parser.add_argument("--batch-id", required=True, choices=("implemented27", "remaining14", "all41"))
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--labels", type=Path, required=True)
    parser.add_argument("--label-contract", type=Path, required=True)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--metadata-id", default="symbol_id")
    parser.add_argument("--metadata-signal-id", default="symbol_id")
    parser.add_argument("--slice-dimensions")
    parser.add_argument("--join-keys", default="trade_date,decision_time,symbol_id")
    parser.add_argument("--score-column", default="score")
    parser.add_argument("--symbol-column", default="symbol_id")
    parser.add_argument("--quantiles", type=int, default=5)
    parser.add_argument("--annualization-factor", type=float)
    parser.add_argument("--min-cross-section", type=int, default=100)
    parser.add_argument("--direction-column", default="expected_direction")
    parser.add_argument("--default-direction", choices=("auto", "positive", "negative"), default="auto")
    parser.add_argument("--control-columns", default="own_score")
    parser.add_argument("--correlation-sample-modulus", type=int, default=1000)
    parser.add_argument("--allow-legacy-signals", action="store_true")
    parser.add_argument("--allow-partial", action="store_true")
    parser.add_argument("--memory-limit-gb", type=float, default=24.0)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-git-commit")
    parser.add_argument("--require-clean", action="store_true")
    parser.add_argument("--reset-checkpoints", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    checkpoints = output / "_checkpoints" / "alpha"
    if args.reset_checkpoints and checkpoints.exists():
        shutil.rmtree(checkpoints)
    checkpoints.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)

    contract = LabelContract.from_json(args.label_contract)
    contract.validate()
    budget = ResourceBudget(args.memory_limit_gb, args.threads, str(args.temp_directory) if args.temp_directory else None)
    budget.validate()
    join_keys = csv_list(args.join_keys)
    controls = csv_list(args.control_columns)
    slice_dimensions = csv_list(args.slice_dimensions)

    input_records = [
        *directory_parquet_records(args.signals),
        *directory_parquet_records(args.labels),
        file_record(args.label_contract),
    ]
    metadata_frame = None
    metadata_profile = None
    if args.metadata:
        raw_metadata = read_frame(args.metadata)
        metadata_frame, metadata_profile = normalize_metadata(
            raw_metadata,
            id_column=args.metadata_id,
            dimensions=slice_dimensions or None,
        )
        slice_dimensions = list(metadata_profile.dimensions)
        input_records.append(file_record(args.metadata))

    parameters = {
        "batch_id": args.batch_id,
        "join_keys": join_keys,
        "score_column": args.score_column,
        "symbol_column": args.symbol_column,
        "quantiles": args.quantiles,
        "annualization_factor": args.annualization_factor,
        "min_cross_section": args.min_cross_section,
        "direction_column": args.direction_column,
        "default_direction": args.default_direction,
        "control_columns": controls,
        "correlation_sample_modulus": args.correlation_sample_modulus,
        "allow_legacy_signals": args.allow_legacy_signals,
        "label_contract": contract.as_dict(),
        "checkpoint_granularity": "factor",
    }
    manifest = implementation_manifest(
        operation="alpha_report_resumable",
        parameters=parameters,
        inputs=input_records,
        resource_budget=budget,
    )
    enforce_git_lineage(manifest, expected_commit=args.expected_git_commit, require_clean=args.require_clean)
    run_contract_hash = str(manifest["contract_hash"])

    connection = duckdb.connect()
    try:
        from graphalphalab.governance import configure_duckdb

        configure_duckdb(connection, budget)
        connection.execute(f"CREATE VIEW signals AS SELECT * FROM {_table_expression(args.signals)}")
        connection.execute(f"CREATE VIEW labels AS SELECT * FROM {_table_expression(args.labels)}")
        pit_audit = _audit_pit(
            connection,
            contract,
            join_keys=join_keys,
            allow_legacy_signals=args.allow_legacy_signals,
        )
        signal_columns = _columns(connection, "signals")
        label_columns = _columns(connection, "labels")
        missing_signal = sorted(set(join_keys) - signal_columns)
        missing_label = sorted(set(join_keys) - label_columns)
        if missing_signal or missing_label:
            raise ValueError(f"Join keys missing; signal_missing={missing_signal}, label_missing={missing_label}")
        factor_keys, factors = _factor_keys(connection)
        select_signal = [
            *factor_keys,
            *join_keys,
            args.symbol_column,
            args.score_column,
            *[
                column
                for column in ("signal_available_time", args.direction_column, *controls)
                if column and column in signal_columns
            ],
        ]
        select_signal = list(dict.fromkeys(select_signal))
        join_expression = " AND ".join(f's."{key}"=l."{key}"' for key in join_keys)
        label_filter = f"CAST(l.label_id AS VARCHAR)={_sql_literal(contract.label_id)}"

        frames_by_name: dict[str, list[pd.DataFrame]] = {name: [] for name in FRAME_FILES}
        unit_states: list[dict[str, object]] = []
        reused = 0
        completed = 0
        total = int(len(factors))
        write_progress(output, stage="alpha-factor-checkpoints", total=total, completed=0, units=[])

        for ordinal, (_, factor_row) in enumerate(factors.iterrows(), start=1):
            identity = factor_identity(factor_keys, factor_row)
            unit_name = "|".join(f"{key}={identity.get(key)}" for key in factor_keys)
            unit_dir = checkpoints / safe_key(identity, prefix="factor")
            source_hash = sha256_json({"run_contract_hash": run_contract_hash, "identity": identity})
            spec = CheckpointSpec("alpha-factor", unit_name, run_contract_hash, source_hash)
            required = list(FRAME_FILES)
            state = {"unit": unit_name, "status": "pending", "attempt": 0, "detail": str(unit_dir)}
            unit_states.append(state)
            if checkpoint_valid(unit_dir, spec, required_files=required):
                loaded = load_checkpoint_frames(unit_dir, required)
                for filename in FRAME_FILES:
                    frames_by_name[filename].append(restore_frame(loaded[filename]))
                state["status"] = "reused"
                reused += 1
                completed += 1
                write_progress(
                    output,
                    stage="alpha-factor-checkpoints",
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
                output,
                stage="alpha-factor-checkpoints",
                total=total,
                completed=completed,
                reused=reused,
                current=unit_name,
                units=unit_states,
            )
            where = _factor_where(factor_keys, factor_row)
            signal_projection = ", ".join(f's."{column}"' for column in select_signal)
            query = f"""
            SELECT {signal_projection},
                   l."{contract.target_column}" AS target_return,
                   l.label_id,
                   l."{contract.entry_time_column}" AS entry_time,
                   l."{contract.exit_time_column}" AS exit_time,
                   l."{contract.available_time_column}" AS label_available_time
            FROM signals s
            JOIN labels l ON {join_expression}
            WHERE {where} AND {label_filter}
            ORDER BY s.trade_date, s.decision_time, s."{args.symbol_column}"
            """
            factor = connection.execute(query).fetch_df()
            if metadata_frame is not None:
                if args.metadata_signal_id not in factor.columns or metadata_profile.id_column not in metadata_frame.columns:
                    raise ValueError(
                        f"Metadata join columns missing: signals={args.metadata_signal_id!r}, metadata={metadata_profile.id_column!r}"
                    )
                factor = factor.merge(
                    metadata_frame,
                    left_on=args.metadata_signal_id,
                    right_on=metadata_profile.id_column,
                    how="left",
                    validate="many_to_one",
                )
            result = evaluate_alpha(
                factor,
                score_column=args.score_column,
                label_column="target_return",
                time_column="decision_time",
                symbol_column=args.symbol_column,
                trade_date_column="trade_date",
                quantiles=args.quantiles,
                annualization_factor=args.annualization_factor,
                min_cross_section=args.min_cross_section,
                slice_columns=slice_dimensions,
                direction_column=args.direction_column or None,
                default_direction=args.default_direction,
                control_columns=controls,
                label_overlapping=contract.overlapping,
                governance={"pit_audit": pit_audit.as_dict(), "label_contract": contract.as_dict()},
            )
            result_frames = {
                "metrics.parquet": result.metrics,
                "ic_series.parquet": result.ic_series,
                "daily_ic.parquet": result.daily_ic,
                "quantile_returns.parquet": result.quantile_returns,
                "portfolio_returns.parquet": result.portfolio_returns,
                "stability.parquet": result.stability,
            }
            commit_frames(
                unit_dir,
                spec,
                {name: checkpoint_frame(frame) for name, frame in result_frames.items()},
                metadata={"ordinal": ordinal, "factor_identity": identity, "observations": int(len(factor))},
            )
            for filename, frame in result_frames.items():
                frames_by_name[filename].append(frame)
            state["status"] = "complete"
            completed += 1
            write_progress(
                output,
                stage="alpha-factor-checkpoints",
                total=total,
                completed=completed,
                reused=reused,
                current=unit_name,
                units=unit_states,
            )
            del factor, result
            gc.collect()

        metrics = recompute_global_gates(concat_frames(frames_by_name["metrics.parquet"]), pit_audit.passed)
        ic_series = concat_frames(frames_by_name["ic_series.parquet"])
        daily_ic = concat_frames(frames_by_name["daily_ic.parquet"])
        quantile_returns = concat_frames(frames_by_name["quantile_returns.parquet"])
        portfolio_returns = concat_frames(frames_by_name["portfolio_returns.parquet"])
        stability = concat_frames(frames_by_name["stability.parquet"])

        score_correlation = pd.DataFrame()
        if args.correlation_sample_modulus > 0 and not factors.empty:
            identity_sql = " || '|' || ".join(
                f"coalesce(CAST({column} AS VARCHAR), '')" for column in factor_keys
            )
            sample = connection.execute(
                f"""
                SELECT {identity_sql} AS factor_key,
                       decision_time,
                       "{args.symbol_column}" AS symbol_id,
                       "{args.score_column}" AS score
                FROM signals
                WHERE abs(hash(CAST(decision_time AS VARCHAR) || '|' || CAST("{args.symbol_column}" AS VARCHAR)))
                      % {int(args.correlation_sample_modulus)} = 0
                """
            ).fetch_df()
            if not sample.empty:
                pivot = sample.pivot_table(
                    index=["decision_time", "symbol_id"], columns="factor_key", values="score", aggfunc="mean"
                )
                if pivot.shape[1] >= 2:
                    score_correlation = (
                        pivot.corr(method="spearman")
                        .stack(dropna=False)
                        .rename("score_spearman_correlation")
                        .reset_index()
                    )
                    score_correlation.columns = ["factor_a", "factor_b", "score_spearman_correlation"]
                    score_correlation["sample_rows"] = int(len(sample))
                    score_correlation["sample_modulus"] = int(args.correlation_sample_modulus)

        alpha = AlphaResult(
            metrics=metrics,
            ic_series=ic_series,
            daily_ic=daily_ic,
            quantile_returns=quantile_returns,
            portfolio_returns=portfolio_returns,
            stability=stability,
            score_correlation=score_correlation,
            governance={
                "pit_audit": pit_audit.as_dict(),
                "label_contract": contract.as_dict(),
                "checkpoint_contract_hash": run_contract_hash,
                "checkpoint_granularity": "factor",
                "factor_checkpoint_count": total,
                "factor_checkpoint_reused": reused,
            },
        )
        status = validate_batch_contracts(metrics, args.batch_id, allow_partial=args.allow_partial)
        lineage = {
            "signal_files": directory_parquet_records(args.signals),
            "label_files": directory_parquet_records(args.labels),
            "label_contract": file_record(args.label_contract),
            **({"metadata": file_record(args.metadata)} if args.metadata else {}),
        }
        write_report_bundle(
            output,
            batch_id=args.batch_id,
            batch_status=status,
            alpha=alpha,
            lineage=lineage,
            run_manifest=manifest,
        )
        write_progress(
            output,
            stage="alpha-factor-checkpoints",
            total=total,
            completed=total,
            reused=reused,
            current=None,
            status="complete",
            units=unit_states,
            extra={"report_success": True, "run_contract_hash": run_contract_hash},
        )
        print(json.dumps({"output": str(output), "factors": total, "reused": reused, "contract_hash": run_contract_hash}, indent=2))
    except Exception as exc:
        write_progress(
            output,
            stage="alpha-factor-checkpoints",
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


if __name__ == "__main__":
    main()
