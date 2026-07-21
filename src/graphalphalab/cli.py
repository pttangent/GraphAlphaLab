from __future__ import annotations

import argparse
import json
from pathlib import Path

from .batch import BATCH_REGISTRY, merge_compact_reports, validate_batch_contracts
from .contracts import LabelContract
from .gff_export import export_gff_signals
from .governance import (
    ResourceBudget,
    directory_parquet_records,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
)
from .io import read_frame
from .metadata import normalize_metadata
from .purity import evaluate_theme_purity
from .reports import write_report_bundle
from .streaming import evaluate_alpha_streaming


def _csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _resource_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--memory-limit-gb", type=float, default=24.0)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--temp-directory", type=Path)


def _lineage_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--expected-git-commit")
    parser.add_argument("--require-clean", action="store_true")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Governed, OOM-bounded GraphAlphaLab research pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    export = sub.add_parser("export-gff-signals", help="Create node and graph score variants from GFF P0 partitions")
    export.add_argument("--batch-id", required=True, choices=("implemented27", "remaining14", "all41"))
    export.add_argument("--p0-root", type=Path, required=True)
    export.add_argument("--variants", default="node_baseline,graph_forward,graph_reverse_placebo")
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--force", action="store_true")
    _resource_args(export)
    _lineage_args(export)

    profile = sub.add_parser("profile-metadata")
    profile.add_argument("--metadata", type=Path, required=True)
    profile.add_argument("--dimensions")
    profile.add_argument("--id-column")
    profile.add_argument("--output", type=Path, required=True)

    theme = sub.add_parser("theme-report")
    theme.add_argument("--batch-id", default="theme_discovery")
    theme.add_argument("--memberships", type=Path, required=True)
    theme.add_argument("--metadata", type=Path, required=True)
    theme.add_argument("--dimensions")
    theme.add_argument("--membership-id")
    theme.add_argument("--metadata-id")
    theme.add_argument("--output", type=Path, required=True)
    theme.add_argument("--allow-partial", action="store_true")

    alpha = sub.add_parser("alpha-report")
    alpha.add_argument("--batch-id", required=True, choices=sorted(BATCH_REGISTRY))
    alpha.add_argument("--signals", type=Path, required=True)
    alpha.add_argument("--labels", type=Path, required=True)
    alpha.add_argument("--label-contract", type=Path, required=True)
    alpha.add_argument("--metadata", type=Path)
    alpha.add_argument("--metadata-id", default="symbol_id")
    alpha.add_argument("--metadata-signal-id", default="symbol_id")
    alpha.add_argument("--slice-dimensions")
    alpha.add_argument("--join-keys", default="trade_date,decision_time,symbol_id")
    alpha.add_argument("--score-column", default="score")
    alpha.add_argument("--symbol-column", default="symbol_id")
    alpha.add_argument("--quantiles", type=int, default=5)
    alpha.add_argument("--annualization-factor", type=float)
    alpha.add_argument("--min-cross-section", type=int, default=100)
    alpha.add_argument("--direction-column", default="expected_direction")
    alpha.add_argument("--default-direction", choices=("auto", "positive", "negative"), default="auto")
    alpha.add_argument("--control-columns", default="own_score")
    alpha.add_argument("--correlation-sample-modulus", type=int, default=1000)
    alpha.add_argument("--allow-legacy-signals", action="store_true")
    alpha.add_argument("--output", type=Path, required=True)
    alpha.add_argument("--allow-partial", action="store_true")
    _resource_args(alpha)
    _lineage_args(alpha)

    merge = sub.add_parser("merge-reports")
    merge.add_argument("--inputs", nargs="+", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "profile-metadata":
        frame = read_frame(args.metadata)
        _, profile = normalize_metadata(frame, id_column=args.id_column, dimensions=_csv_list(args.dimensions) or None)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(profile.as_dict(), indent=2, default=str), encoding="utf-8")
        print(json.dumps(profile.as_dict(), indent=2, default=str))
        return
    if args.command == "merge-reports":
        print(merge_compact_reports(args.inputs, args.output))
        return
    if args.command == "export-gff-signals":
        budget = ResourceBudget(args.memory_limit_gb, args.threads, str(args.temp_directory) if args.temp_directory else None)
        result = export_gff_signals(
            args.p0_root,
            args.output,
            batch_id=args.batch_id,
            variants=_csv_list(args.variants),
            resource_budget=budget,
            expected_git_commit=args.expected_git_commit,
            require_clean=args.require_clean,
            force=args.force,
        )
        print(json.dumps(result.as_dict(), indent=2))
        return
    if args.command == "theme-report":
        memberships = read_frame(args.memberships)
        metadata = read_frame(args.metadata)
        status = validate_batch_contracts(memberships, args.batch_id, allow_partial=args.allow_partial)
        purity = evaluate_theme_purity(
            memberships,
            metadata,
            dimensions=_csv_list(args.dimensions) or None,
            membership_id=args.membership_id,
            metadata_id=args.metadata_id,
        )
        lineage = {
            "memberships": file_record(args.memberships),
            "metadata": file_record(args.metadata),
        }
        print(write_report_bundle(args.output, batch_id=args.batch_id, batch_status=status, purity=purity, lineage=lineage))
        return

    contract = LabelContract.from_json(args.label_contract)
    budget = ResourceBudget(args.memory_limit_gb, args.threads, str(args.temp_directory) if args.temp_directory else None)
    metadata_frame = None
    metadata_profile = None
    slice_dimensions = _csv_list(args.slice_dimensions)
    inputs = [
        *directory_parquet_records(args.signals),
        *directory_parquet_records(args.labels),
        file_record(args.label_contract),
    ]
    if args.metadata:
        raw_metadata = read_frame(args.metadata)
        metadata_frame, metadata_profile = normalize_metadata(
            raw_metadata,
            id_column=args.metadata_id,
            dimensions=slice_dimensions or None,
        )
        slice_dimensions = list(metadata_profile.dimensions)
        inputs.append(file_record(args.metadata))
    parameters = {
        "batch_id": args.batch_id,
        "join_keys": _csv_list(args.join_keys),
        "score_column": args.score_column,
        "symbol_column": args.symbol_column,
        "quantiles": args.quantiles,
        "annualization_factor": args.annualization_factor,
        "min_cross_section": args.min_cross_section,
        "direction_column": args.direction_column,
        "default_direction": args.default_direction,
        "control_columns": _csv_list(args.control_columns),
        "correlation_sample_modulus": args.correlation_sample_modulus,
        "allow_legacy_signals": args.allow_legacy_signals,
        "label_contract": contract.as_dict(),
    }
    manifest = implementation_manifest(
        operation="alpha_report",
        parameters=parameters,
        inputs=inputs,
        resource_budget=budget,
    )
    enforce_git_lineage(manifest, expected_commit=args.expected_git_commit, require_clean=args.require_clean)
    result = evaluate_alpha_streaming(
        args.signals,
        args.labels,
        label_contract=contract,
        join_keys=_csv_list(args.join_keys),
        metadata=metadata_frame,
        metadata_signal_id=args.metadata_signal_id,
        metadata_id=metadata_profile.id_column if metadata_profile else args.metadata_id,
        slice_columns=slice_dimensions,
        score_column=args.score_column,
        symbol_column=args.symbol_column,
        quantiles=args.quantiles,
        min_cross_section=args.min_cross_section,
        direction_column=args.direction_column or None,
        default_direction=args.default_direction,
        control_columns=_csv_list(args.control_columns),
        annualization_factor=args.annualization_factor,
        resource_budget=budget,
        allow_legacy_signals=args.allow_legacy_signals,
        correlation_sample_modulus=args.correlation_sample_modulus,
    )
    status = validate_batch_contracts(result.metrics, args.batch_id, allow_partial=args.allow_partial)
    lineage = {
        "signal_files": directory_parquet_records(args.signals),
        "label_files": directory_parquet_records(args.labels),
        "label_contract": file_record(args.label_contract),
        **({"metadata": file_record(args.metadata)} if args.metadata else {}),
    }
    print(
        write_report_bundle(
            args.output,
            batch_id=args.batch_id,
            batch_status=status,
            alpha=result,
            lineage=lineage,
            run_manifest=manifest,
        )
    )


if __name__ == "__main__":
    main()
