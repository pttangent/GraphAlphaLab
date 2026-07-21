from __future__ import annotations

import argparse
import json
from pathlib import Path

from .batch import BATCH_REGISTRY, get_batch, merge_compact_reports, validate_batch_contracts
from .campaign import CampaignSource, build_campaign_report
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
from .p1_reporting import evaluate_p1_streaming, write_p1_report_bundle
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

    p1 = sub.add_parser(
        "p1-report",
        help="Evaluate governed GFF P1 memberships, theme trees, relations and temporal links partition by partition",
    )
    p1.add_argument("--batch-id", required=True, choices=("implemented27", "remaining14", "similarity10"))
    p1.add_argument("--p1-root", type=Path, required=True)
    p1.add_argument("--range-root", type=Path)
    p1.add_argument("--metadata", type=Path)
    p1.add_argument("--dimensions")
    p1.add_argument("--membership-id")
    p1.add_argument("--metadata-id")
    p1.add_argument("--start-date", required=True)
    p1.add_argument("--end-date", required=True)
    p1.add_argument("--expected-date-count", type=int, default=33)
    p1.add_argument("--expected-contracts", type=int)
    p1.add_argument("--require-consensus", action="store_true")
    p1.add_argument("--allow-partial", action="store_true")
    p1.add_argument("--output", type=Path, required=True)
    _resource_args(p1)
    _lineage_args(p1)

    alpha = sub.add_parser("alpha-report")
    alpha.add_argument("--batch-id", required=True, choices=("implemented27", "remaining14", "all41"))
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

    campaign = sub.add_parser(
        "campaign-report",
        help="Merge governed compact IG27, RM14 and Similarity10 reports into a 33-session campaign bundle",
    )
    campaign.add_argument("--implemented27-alpha", nargs="+", type=Path, required=True)
    campaign.add_argument("--implemented27-p1", type=Path, required=True)
    campaign.add_argument("--remaining14-alpha", nargs="+", type=Path, required=True)
    campaign.add_argument("--remaining14-p1", type=Path, required=True)
    campaign.add_argument("--similarity-p1", type=Path, required=True)
    campaign.add_argument("--all41-alpha", nargs="*", type=Path, default=[])
    campaign.add_argument("--start-date", required=True)
    campaign.add_argument("--end-date", required=True)
    campaign.add_argument("--expected-date-count", type=int, default=33)
    campaign.add_argument("--output", type=Path, required=True)
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
    if args.command == "campaign-report":
        sources = [
            *[CampaignSource("implemented27", "alpha", path) for path in args.implemented27_alpha],
            CampaignSource("implemented27", "p1", args.implemented27_p1),
            *[CampaignSource("remaining14", "alpha", path) for path in args.remaining14_alpha],
            CampaignSource("remaining14", "p1", args.remaining14_p1),
            CampaignSource("similarity10", "p1", args.similarity_p1),
            *[CampaignSource("all41", "alpha_merged", path) for path in args.all41_alpha],
        ]
        print(
            build_campaign_report(
                sources,
                args.output,
                start_date=args.start_date,
                end_date=args.end_date,
                expected_date_count=args.expected_date_count,
            )
        )
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
    if args.command == "p1-report":
        budget = ResourceBudget(args.memory_limit_gb, args.threads, str(args.temp_directory) if args.temp_directory else None)
        metadata = read_frame(args.metadata) if args.metadata else None
        spec = get_batch(args.batch_id)
        expected_contracts = args.expected_contracts if args.expected_contracts is not None else spec.expected_contracts
        result = evaluate_p1_streaming(
            args.p1_root,
            batch_id=args.batch_id,
            metadata=metadata,
            dimensions=_csv_list(args.dimensions) or None,
            metadata_id=args.metadata_id,
            membership_id=args.membership_id,
            range_root=args.range_root,
            start_date=args.start_date,
            end_date=args.end_date,
            expected_contracts=expected_contracts,
            expected_dates=args.expected_date_count,
            require_consensus=args.require_consensus or args.batch_id == "similarity10",
            allow_partial=args.allow_partial,
            resource_budget=budget,
        )
        inputs: list[dict[str, object]] = [
            {
                "partition": row["partition"],
                "manifest_sha256": row["manifest_sha256"],
                "contract_hash": row.get("contract_hash"),
            }
            for row in result.partition_summary.to_dict("records")
        ]
        if args.metadata:
            inputs.append(file_record(args.metadata))
        if args.range_root:
            inputs.extend(directory_parquet_records(args.range_root))
        manifest = implementation_manifest(
            operation="p1_report",
            parameters=result.governance,
            inputs=inputs,
            resource_budget=budget,
        )
        enforce_git_lineage(manifest, expected_commit=args.expected_git_commit, require_clean=args.require_clean)
        print(
            write_p1_report_bundle(
                args.output,
                batch_id=args.batch_id,
                result=result,
                inputs=inputs,
                resource_budget=budget,
                run_manifest=manifest,
            )
        )
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
