from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil

import duckdb
import pandas as pd

from graphalphalab.batch import validate_batch_contracts
from graphalphalab.checkpoint import (
    CheckpointSpec,
    checkpoint_valid,
    commit_frames,
    load_checkpoint_frames,
    safe_key,
    write_progress,
)
from graphalphalab.governance import (
    ResourceBudget,
    directory_parquet_records,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
    sha256_json,
)
from graphalphalab.io import read_frame
from graphalphalab.p1_reporting import _aggregate_purity
from graphalphalab.purity import evaluate_theme_purity
from graphalphalab.reports import write_report_bundle


FRAME_NAMES = ("theme_purity.parquet", "snapshot_agreement.parquet")


def csv_list(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def parquet_expression(path: Path) -> str:
    resolved = path.expanduser().resolve()
    if resolved.is_file():
        target = resolved
    else:
        memberships = list(resolved.rglob("memberships.parquet"))
        if memberships:
            target = resolved / "**" / "memberships.parquet"
        else:
            target = resolved / "**" / "*.parquet"
    return f"read_parquet('{target.as_posix().replace(chr(39), chr(39) * 2)}', union_by_name=true, hive_partitioning=false)"


def empty_safe(frame: pd.DataFrame) -> pd.DataFrame:
    return frame if frame.shape[1] else pd.DataFrame({"_checkpoint_empty": pd.Series(dtype="int8")})


def restore(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame() if list(frame.columns) == ["_checkpoint_empty"] else frame


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Date-checkpointed legacy theme purity report")
    result.add_argument("--batch-id", default="theme_discovery")
    result.add_argument("--memberships", type=Path, required=True)
    result.add_argument("--metadata", type=Path, required=True)
    result.add_argument("--dimensions")
    result.add_argument("--membership-id")
    result.add_argument("--metadata-id")
    result.add_argument("--start-date")
    result.add_argument("--end-date")
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--memory-limit-gb", type=float, default=24.0)
    result.add_argument("--threads", type=int, default=8)
    result.add_argument("--temp-directory", type=Path)
    result.add_argument("--expected-git-commit")
    result.add_argument("--require-clean", action="store_true")
    result.add_argument("--allow-partial", action="store_true")
    result.add_argument("--reset-checkpoints", action="store_true")
    return result


def main() -> None:
    args = parser().parse_args()
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    checkpoints = output / "_checkpoints" / "theme"
    if args.reset_checkpoints and checkpoints.exists():
        shutil.rmtree(checkpoints)
    checkpoints.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)

    budget = ResourceBudget(args.memory_limit_gb, args.threads, str(args.temp_directory) if args.temp_directory else None)
    metadata = read_frame(args.metadata)
    membership_records = directory_parquet_records(args.memberships)
    input_records = [*membership_records, file_record(args.metadata)]
    parameters = {
        "batch_id": args.batch_id,
        "dimensions": csv_list(args.dimensions),
        "membership_id": args.membership_id,
        "metadata_id": args.metadata_id,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "checkpoint_granularity": "trade_date",
    }
    manifest = implementation_manifest(
        operation="theme_report_resumable",
        parameters=parameters,
        inputs=input_records,
        resource_budget=budget,
    )
    enforce_git_lineage(manifest, expected_commit=args.expected_git_commit, require_clean=args.require_clean)
    contract_hash = str(manifest["contract_hash"])
    source_hash = sha256_json(membership_records)

    connection = duckdb.connect()
    try:
        connection.execute(f"PRAGMA threads={max(1, args.threads)}")
        connection.execute(f"SET memory_limit='{args.memory_limit_gb:g}GB'")
        connection.execute("PRAGMA preserve_insertion_order=false")
        if args.temp_directory:
            args.temp_directory.mkdir(parents=True, exist_ok=True)
            connection.execute(f"SET temp_directory='{args.temp_directory.resolve().as_posix().replace(chr(39), chr(39) * 2)}'")
        expression = parquet_expression(args.memberships)
        connection.execute(f"CREATE VIEW memberships AS SELECT * FROM {expression}")
        columns = {str(row[0]) for row in connection.execute("DESCRIBE memberships").fetchall()}
        if "trade_date" not in columns:
            raise ValueError("Resumable theme report requires trade_date in memberships")
        conditions = []
        if args.start_date:
            conditions.append(f"CAST(trade_date AS VARCHAR)>='{args.start_date}'")
        if args.end_date:
            conditions.append(f"CAST(trade_date AS VARCHAR)<='{args.end_date}'")
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        dates = [
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM memberships{where} ORDER BY 1"
            ).fetchall()
        ]
        if not dates:
            raise FileNotFoundError("No membership dates matched the requested range")

        detail_frames: list[pd.DataFrame] = []
        agreement_frames: list[pd.DataFrame] = []
        states: list[dict[str, object]] = []
        completed = 0
        reused = 0
        write_progress(output, stage="theme-date-checkpoints", total=len(dates), completed=0, units=[])
        metadata_profile: dict[str, object] | None = None

        for trade_date in dates:
            unit = trade_date
            unit_root = checkpoints / safe_key({"trade_date": trade_date}, prefix="date")
            spec = CheckpointSpec(
                "theme-date",
                unit,
                contract_hash,
                sha256_json({"global_source_hash": source_hash, "trade_date": trade_date}),
            )
            state = {"unit": unit, "status": "pending", "attempt": 0, "detail": str(unit_root)}
            states.append(state)
            if checkpoint_valid(unit_root, spec, required_files=FRAME_NAMES):
                loaded = load_checkpoint_frames(unit_root, FRAME_NAMES)
                detail_frames.append(restore(loaded["theme_purity.parquet"]))
                agreement_frames.append(restore(loaded["snapshot_agreement.parquet"]))
                checkpoint_payload = json.loads((unit_root / "checkpoint.json").read_text(encoding="utf-8"))
                metadata_profile = metadata_profile or checkpoint_payload.get("metadata", {}).get("metadata_profile")
                state["status"] = "reused"
                completed += 1
                reused += 1
                write_progress(
                    output,
                    stage="theme-date-checkpoints",
                    total=len(dates),
                    completed=completed,
                    reused=reused,
                    current=unit,
                    units=states,
                )
                continue

            state["status"] = "running"
            state["attempt"] = 1
            write_progress(
                output,
                stage="theme-date-checkpoints",
                total=len(dates),
                completed=completed,
                reused=reused,
                current=unit,
                units=states,
            )
            memberships = connection.execute(
                f"SELECT * FROM memberships WHERE CAST(trade_date AS VARCHAR)='{trade_date}'"
            ).fetch_df()
            purity = evaluate_theme_purity(
                memberships,
                metadata,
                dimensions=csv_list(args.dimensions) or None,
                membership_id=args.membership_id,
                metadata_id=args.metadata_id,
            )
            metadata_profile = purity.metadata_profile
            commit_frames(
                unit_root,
                spec,
                {
                    "theme_purity.parquet": empty_safe(purity.theme_dimension),
                    "snapshot_agreement.parquet": empty_safe(purity.snapshot_agreement),
                },
                metadata={"trade_date": trade_date, "metadata_profile": purity.metadata_profile},
            )
            detail_frames.append(purity.theme_dimension)
            agreement_frames.append(purity.snapshot_agreement)
            state["status"] = "complete"
            completed += 1
            write_progress(
                output,
                stage="theme-date-checkpoints",
                total=len(dates),
                completed=completed,
                reused=reused,
                current=unit,
                units=states,
            )

        detail = pd.concat([frame for frame in detail_frames if frame.shape[1]], ignore_index=True) if any(frame.shape[1] for frame in detail_frames) else pd.DataFrame()
        agreement = pd.concat([frame for frame in agreement_frames if frame.shape[1]], ignore_index=True) if any(frame.shape[1] for frame in agreement_frames) else pd.DataFrame()
        purity = _aggregate_purity(detail, agreement, metadata_profile or {})
        status_source = detail if not detail.empty else pd.DataFrame()
        status = validate_batch_contracts(status_source, args.batch_id, allow_partial=args.allow_partial)
        lineage = {
            "memberships": membership_records,
            "metadata": file_record(args.metadata),
            "checkpoint_contract_hash": contract_hash,
        }
        write_report_bundle(
            output,
            batch_id=args.batch_id,
            batch_status=status,
            purity=purity,
            lineage=lineage,
            run_manifest=manifest,
        )
        write_progress(
            output,
            stage="theme-date-checkpoints",
            total=len(dates),
            completed=len(dates),
            reused=reused,
            status="complete",
            units=states,
            extra={"report_success": True, "contract_hash": contract_hash},
        )
        print(json.dumps({"output": str(output), "dates": len(dates), "reused": reused}, indent=2))
    except Exception as exc:
        write_progress(
            output,
            stage="theme-date-checkpoints",
            total=len(locals().get("dates", [])),
            completed=int(locals().get("completed", 0)),
            reused=int(locals().get("reused", 0)),
            failed=1,
            current=locals().get("unit"),
            status="failed",
            units=locals().get("states", []),
            extra={"error": repr(exc), "contract_hash": contract_hash},
        )
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
