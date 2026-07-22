from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess

import pandas as pd
import psutil

from graphalphalab.batch import get_batch
from graphalphalab.checkpoint import CheckpointSpec, checkpoint_valid, file_signature, utc_now, write_progress
from graphalphalab.governance import (
    ResourceBudget,
    atomic_write_json,
    directory_parquet_records,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
    sha256_file,
    sha256_json,
)
from graphalphalab.p1_reporting import (
    P1ReportResult,
    _event_summary,
    _read_columns,
    _summarize_purity_shards,
    write_p1_report_bundle,
)


REQUIRED_CHILD_FILES = (
    "p1_partition_summary.csv",
    "p1_snapshot_structure.csv",
    "p1_layer_summary.csv",
    "p1_temporal_summary.csv",
    "p1_relation_summary.csv",
    "summary.json",
    "run_manifest.json",
    "_SUCCESS",
)


def csv_list(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def discover_date_partitions(root: Path, start_date: str, end_date: str) -> dict[str, list[Path]]:
    result: dict[str, list[Path]] = {}
    for memberships in sorted(root.expanduser().resolve().rglob("memberships.parquet")):
        partition = memberships.parent
        manifest_path = partition / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        trade_date = str(manifest.get("trade_date") or manifest.get("date") or "")
        if trade_date and start_date <= trade_date <= end_date:
            result.setdefault(trade_date, []).append(partition)
    return result


def checkpoint_files(root: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for path in sorted(file for file in root.rglob("*") if file.is_file() and file.name != "checkpoint.json"):
        record = file_signature(path)
        record["path"] = path.relative_to(root).as_posix()
        records.append(record)
    return records


def child_checkpoint_valid(root: Path, spec: CheckpointSpec) -> bool:
    if not checkpoint_valid(root, spec, required_files=REQUIRED_CHILD_FILES):
        return False
    try:
        payload = json.loads((root / "checkpoint.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    for record in payload.get("files", []):
        relative = str(record.get("path", ""))
        if not relative:
            return False
        path = root / relative
        if not path.exists() or int(record.get("size_bytes", -1)) != path.stat().st_size:
            return False
        if str(record.get("sha256")) != sha256_file(path):
            return False
    return True


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.stat().st_size:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def concat_child_frames(children: list[Path], filename: str) -> pd.DataFrame:
    frames = [read_csv_if_exists(child / filename) for child in children]
    frames = [frame for frame in frames if frame.shape[1] > 0]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_layer_summary(structure: pd.DataFrame) -> pd.DataFrame:
    if structure.empty:
        return pd.DataFrame()
    return structure.groupby(["layer_id", "scale_minutes"], observed=True, dropna=False).agg(
        date_count=("trade_date", "nunique"),
        snapshot_count=("decision_time", "count"),
        mean_theme_count=("theme_count", "mean"),
        median_theme_count=("theme_count", "median"),
        mean_theme_size=("mean_theme_size", "mean"),
        p90_max_theme_size=("max_theme_size", lambda values: values.quantile(0.90)),
        mean_unique_symbols=("unique_symbols", "mean"),
        noncanonical_membership_rows=("noncanonical_membership_rows", "sum"),
        forced_chunk_memberships=("forced_chunk_memberships", "sum"),
        max_tree_depth=("max_tree_depth", "max"),
    ).reset_index()


def summarize_range(range_root: Path | None) -> pd.DataFrame:
    if range_root is None:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for path in sorted(range_root.expanduser().resolve().rglob("temporal_links.parquet")):
        frame = _read_columns(path, [
            "src_trade_date", "dst_trade_date", "src_decision_time", "dst_decision_time", "layer_id",
            "scale_minutes", "src_theme_id", "dst_theme_id", "overlap", "jaccard", "containment", "event_type",
        ])
        if not frame.empty:
            summary = _event_summary(frame, source="range")
            summary["path"] = str(path)
            frames.append(summary)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Date-checkpointed GraphAlphaLab P1 report")
    parser.add_argument("--batch-id", required=True, choices=("implemented27", "remaining14", "similarity10"))
    parser.add_argument("--p1-root", type=Path, required=True)
    parser.add_argument("--range-root", type=Path)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--dimensions")
    parser.add_argument("--membership-id")
    parser.add_argument("--metadata-id")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--expected-date-count", type=int, default=33)
    parser.add_argument("--expected-contracts", type=int)
    parser.add_argument("--require-consensus", action="store_true")
    parser.add_argument("--allow-partial", action="store_true")
    parser.add_argument("--memory-limit-gb", type=float, default=24.0)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--gal-exe", default="gal")
    parser.add_argument("--expected-git-commit")
    parser.add_argument("--require-clean", action="store_true")
    parser.add_argument("--reset-checkpoints", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    checkpoint_root = output / "_checkpoints" / "p1"
    if args.reset_checkpoints and checkpoint_root.exists():
        shutil.rmtree(checkpoint_root)
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)

    budget = ResourceBudget(args.memory_limit_gb, args.threads, str(args.temp_directory) if args.temp_directory else None)
    budget.validate()
    spec = get_batch(args.batch_id)
    expected_contracts = args.expected_contracts if args.expected_contracts is not None else spec.expected_contracts
    require_consensus = args.require_consensus or args.batch_id == "similarity10"
    date_partitions = discover_date_partitions(args.p1_root, args.start_date, args.end_date)
    dates = sorted(date_partitions)
    if not dates:
        raise FileNotFoundError(f"No P1 partitions found below {args.p1_root}")
    if len(dates) != args.expected_date_count and not args.allow_partial:
        raise ValueError(f"Expected {args.expected_date_count} dates, discovered {len(dates)}")

    source_manifest_records = [
        file_record(partition / "manifest.json")
        for trade_date in dates
        for partition in date_partitions[trade_date]
    ]
    input_records = list(source_manifest_records)
    if args.metadata:
        input_records.append(file_record(args.metadata))
    if args.range_root:
        input_records.extend(directory_parquet_records(args.range_root))
    parameters = {
        "batch_id": args.batch_id,
        "p1_root": str(args.p1_root.expanduser().resolve()),
        "range_root": str(args.range_root.expanduser().resolve()) if args.range_root else None,
        "dimensions": csv_list(args.dimensions),
        "membership_id": args.membership_id,
        "metadata_id": args.metadata_id,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "expected_date_count": args.expected_date_count,
        "expected_contracts": expected_contracts,
        "require_consensus": require_consensus,
        "checkpoint_granularity": "trade_date",
    }
    run_manifest = implementation_manifest(
        operation="p1_report_resumable",
        parameters=parameters,
        inputs=input_records,
        resource_budget=budget,
    )
    enforce_git_lineage(run_manifest, expected_commit=args.expected_git_commit, require_clean=args.require_clean)
    contract_hash = str(run_manifest["contract_hash"])

    states: list[dict[str, object]] = []
    children: list[Path] = []
    completed = 0
    reused = 0
    peak_rss = int(psutil.Process().memory_info().rss)
    write_progress(output, stage="p1-date-checkpoints", total=len(dates), completed=0, units=[])

    try:
        for trade_date in dates:
            unit_name = trade_date
            child = checkpoint_root / f"date={trade_date}"
            date_source_hash = sha256_json([
                file_record(partition / "manifest.json") for partition in date_partitions[trade_date]
            ])
            checkpoint_spec = CheckpointSpec("p1-date", unit_name, contract_hash, date_source_hash)
            attempts_path = child.parent / f".{child.name}.attempts.json"
            attempts = 0
            if attempts_path.exists():
                try:
                    attempts = int(json.loads(attempts_path.read_text(encoding="utf-8")).get("attempts", 0))
                except (OSError, json.JSONDecodeError, TypeError, ValueError):
                    attempts = 0
            state = {"unit": unit_name, "status": "pending", "attempt": attempts, "detail": str(child)}
            states.append(state)
            if child_checkpoint_valid(child, checkpoint_spec):
                state["status"] = "reused"
                reused += 1
                completed += 1
                children.append(child)
                write_progress(
                    output,
                    stage="p1-date-checkpoints",
                    total=len(dates),
                    completed=completed,
                    reused=reused,
                    current=unit_name,
                    units=states,
                )
                continue

            if child.exists():
                shutil.rmtree(child, ignore_errors=True)
            attempts += 1
            atomic_write_json(attempts_path, {"attempts": attempts, "updated_at": utc_now()})
            state["status"] = "running"
            state["attempt"] = attempts
            write_progress(
                output,
                stage="p1-date-checkpoints",
                total=len(dates),
                completed=completed,
                reused=reused,
                current=unit_name,
                units=states,
            )
            child_temp = (args.temp_directory / f"p1_{args.batch_id}_{trade_date}") if args.temp_directory else None
            command = [
                str(args.gal_exe),
                "p1-report",
                "--batch-id", args.batch_id,
                "--p1-root", str(args.p1_root),
                "--start-date", trade_date,
                "--end-date", trade_date,
                "--expected-date-count", "1",
                "--expected-contracts", str(expected_contracts),
                "--memory-limit-gb", str(args.memory_limit_gb),
                "--threads", str(args.threads),
                "--output", str(child),
            ]
            if args.metadata:
                command += ["--metadata", str(args.metadata)]
            if args.dimensions:
                command += ["--dimensions", args.dimensions]
            if args.membership_id:
                command += ["--membership-id", args.membership_id]
            if args.metadata_id:
                command += ["--metadata-id", args.metadata_id]
            if child_temp:
                command += ["--temp-directory", str(child_temp)]
            if require_consensus:
                command.append("--require-consensus")
            if args.expected_git_commit:
                command += ["--expected-git-commit", args.expected_git_commit]
            if args.require_clean:
                command.append("--require-clean")
            if args.allow_partial:
                command.append("--allow-partial")
            subprocess.run(command, check=True)
            payload = {
                **checkpoint_spec.as_dict(),
                "status": "complete",
                "attempt": attempts,
                "completed_at": utc_now(),
                "files": checkpoint_files(child),
            }
            atomic_write_json(child / "checkpoint.json", payload)
            attempts_path.unlink(missing_ok=True)
            state["status"] = "complete"
            completed += 1
            children.append(child)
            peak_rss = max(peak_rss, int(psutil.Process().memory_info().rss))
            write_progress(
                output,
                stage="p1-date-checkpoints",
                total=len(dates),
                completed=completed,
                reused=reused,
                current=unit_name,
                units=states,
            )

        partition_summary = concat_child_frames(children, "p1_partition_summary.csv")
        snapshot_structure = concat_child_frames(children, "p1_snapshot_structure.csv")
        temporal_summary = concat_child_frames(children, "p1_temporal_summary.csv")
        relation_summary = concat_child_frames(children, "p1_relation_summary.csv")
        layer_summary = build_layer_summary(snapshot_structure)
        range_summary = summarize_range(args.range_root)

        purity_detail_paths = [
            path for child in children for path in sorted((child / "theme_purity_parts").glob("*.parquet"))
        ]
        purity_agreement_paths = [
            path for child in children for path in sorted((child / "purity_snapshot_agreement_parts").glob("*.parquet"))
        ]
        metadata_profile = None
        for child in children:
            profile_path = child / "metadata_profile.json"
            if profile_path.exists():
                metadata_profile = json.loads(profile_path.read_text(encoding="utf-8"))
                break
        purity_result = (
            _summarize_purity_shards(
                purity_detail_paths,
                purity_agreement_paths,
                profile=metadata_profile or {},
                budget=budget,
            )
            if metadata_profile is not None
            else None
        )

        observed_dates = sorted(partition_summary["trade_date"].astype(str).unique().tolist()) if not partition_summary.empty else []
        observed_contracts = (
            partition_summary[["layer_id", "scale_minutes"]].drop_duplicates()
            if not partition_summary.empty
            else pd.DataFrame(columns=["layer_id", "scale_minutes"])
        )
        consensus_seen = bool(
            not partition_summary.empty
            and (partition_summary["layer_id"].astype(str) == "similarity_consensus").any()
        )
        errors: list[str] = []
        if len(observed_dates) != args.expected_date_count:
            errors.append(f"expected {args.expected_date_count} dates, observed {len(observed_dates)}")
        if expected_contracts is not None and len(observed_contracts) != expected_contracts:
            errors.append(f"expected {expected_contracts} contracts, observed {len(observed_contracts)}")
        if require_consensus and not consensus_seen:
            errors.append("similarity_consensus P1 was not observed")
        if not partition_summary.empty and int(partition_summary["pit_violations"].fillna(0).sum()) != 0:
            errors.append("one or more P1 partitions reported PIT violations")
        complete = not errors
        if not complete and not args.allow_partial:
            raise ValueError("P1 report contract failed: " + "; ".join(errors))

        governance = {
            "batch_id": args.batch_id,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "expected_dates": args.expected_date_count,
            "observed_dates": observed_dates,
            "observed_date_count": len(observed_dates),
            "expected_contracts": expected_contracts,
            "observed_contracts": [
                f"{row.layer_id}@{int(row.scale_minutes)}m" for row in observed_contracts.itertuples(index=False)
            ],
            "observed_contract_count": len(observed_contracts),
            "consensus_seen": consensus_seen,
            "complete": complete,
            "partial": not complete,
            "errors": errors,
            "partition_count": len(partition_summary),
            "peak_rss_bytes": peak_rss,
            "purity_detail_shard_count": len(purity_detail_paths),
            "purity_agreement_shard_count": len(purity_agreement_paths),
            "resource_budget": budget.as_dict(),
            "checkpoint_granularity": "trade_date",
            "checkpoint_count": len(children),
            "checkpoint_reused": reused,
            "checkpoint_contract_hash": contract_hash,
        }
        result = P1ReportResult(
            partition_summary,
            snapshot_structure,
            layer_summary,
            temporal_summary,
            relation_summary,
            range_summary,
            purity_result,
            governance,
            tuple(purity_detail_paths),
            tuple(purity_agreement_paths),
            None,
        )
        final_inputs = [file_record(child / "checkpoint.json") for child in children]
        if args.metadata:
            final_inputs.append(file_record(args.metadata))
        if args.range_root:
            final_inputs.extend(directory_parquet_records(args.range_root))
        final_manifest = implementation_manifest(
            operation="p1_report_resumable_final",
            parameters=governance,
            inputs=final_inputs,
            resource_budget=budget,
        )
        enforce_git_lineage(final_manifest, expected_commit=args.expected_git_commit, require_clean=args.require_clean)
        write_p1_report_bundle(
            output,
            batch_id=args.batch_id,
            result=result,
            inputs=final_inputs,
            resource_budget=budget,
            run_manifest=final_manifest,
        )
        write_progress(
            output,
            stage="p1-date-checkpoints",
            total=len(dates),
            completed=len(dates),
            reused=reused,
            status="complete",
            units=states,
            extra={"report_success": True, "checkpoint_contract_hash": contract_hash},
        )
        print(json.dumps({"output": str(output), "dates": len(dates), "reused": reused, "contract_hash": contract_hash}, indent=2))
    except Exception as exc:
        if states and states[-1].get("status") == "running":
            states[-1]["status"] = "failed"
            states[-1]["detail"] = repr(exc)
        write_progress(
            output,
            stage="p1-date-checkpoints",
            total=len(dates),
            completed=completed,
            reused=reused,
            failed=1,
            current=states[-1]["unit"] if states else None,
            status="failed",
            units=states,
            extra={"error": repr(exc), "checkpoint_contract_hash": contract_hash},
        )
        raise


if __name__ == "__main__":
    main()
