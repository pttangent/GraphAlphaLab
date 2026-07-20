from __future__ import annotations

import argparse
from pathlib import Path
import json

from .alpha import evaluate_alpha
from .batch import BATCH_REGISTRY, merge_compact_reports, validate_batch_contracts
from .io import read_frame, sha256_file
from .metadata import normalize_metadata, resolve_id_column
from .purity import evaluate_theme_purity
from .reports import write_report_bundle


def _csv_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GraphAlphaLab batch alpha and theme-purity reporter")
    sub = parser.add_subparsers(dest="command", required=True)

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
    alpha.add_argument("--labels", type=Path)
    alpha.add_argument("--metadata", type=Path)
    alpha.add_argument("--metadata-id")
    alpha.add_argument("--slice-dimensions")
    alpha.add_argument("--join-keys", default="trade_date,decision_time,symbol")
    alpha.add_argument("--score-column", default="score")
    alpha.add_argument("--label-column", default="target_return")
    alpha.add_argument("--time-column", default="decision_time")
    alpha.add_argument("--symbol-column", default="symbol")
    alpha.add_argument("--quantiles", type=int, default=5)
    alpha.add_argument("--annualization-factor", type=float, default=252.0)
    alpha.add_argument("--min-cross-section", type=int, default=10)
    alpha.add_argument("--output", type=Path, required=True)
    alpha.add_argument("--allow-partial", action="store_true")

    merge = sub.add_parser("merge-reports")
    merge.add_argument("--inputs", nargs="+", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "profile-metadata":
        frame = read_frame(args.metadata)
        _, profile = normalize_metadata(frame, id_column=args.id_column, dimensions=_csv_list(args.dimensions))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(profile.as_dict(), indent=2, default=str), encoding="utf-8")
        print(json.dumps(profile.as_dict(), indent=2, default=str))
        return
    if args.command == "merge-reports":
        print(merge_compact_reports(args.inputs, args.output))
        return
    if args.command == "theme-report":
        memberships = read_frame(args.memberships)
        metadata = read_frame(args.metadata)
        status = validate_batch_contracts(memberships, args.batch_id, allow_partial=args.allow_partial)
        purity = evaluate_theme_purity(
            memberships,
            metadata,
            dimensions=_csv_list(args.dimensions),
            membership_id=args.membership_id,
            metadata_id=args.metadata_id,
        )
        lineage = {"memberships_sha256": sha256_file(args.memberships), "metadata_sha256": sha256_file(args.metadata)}
        print(write_report_bundle(args.output, batch_id=args.batch_id, batch_status=status, purity=purity, lineage=lineage))
        return
    signals = read_frame(args.signals)
    lineage = {"signals_sha256": sha256_file(args.signals)}
    if args.labels:
        labels = read_frame(args.labels)
        join_keys = _csv_list(args.join_keys) or []
        missing = [key for key in join_keys if key not in signals.columns or key not in labels.columns]
        if missing:
            raise ValueError(f"Join keys are missing from signal or label frames: {missing}")
        signals = signals.merge(labels, on=join_keys, how="inner", validate="many_to_one")
        lineage["labels_sha256"] = sha256_file(args.labels)
    slice_dimensions = _csv_list(args.slice_dimensions) or []
    if args.metadata:
        metadata = read_frame(args.metadata)
        metadata_norm, metadata_profile = normalize_metadata(
            metadata, id_column=args.metadata_id, dimensions=slice_dimensions or None
        )
        signal_id = metadata_profile.id_column if metadata_profile.id_column in signals.columns else resolve_id_column(signals)
        selected_dimensions = list(metadata_profile.dimensions if not slice_dimensions else slice_dimensions)
        signals = signals.merge(
            metadata_norm[[metadata_profile.id_column, *selected_dimensions]],
            left_on=signal_id,
            right_on=metadata_profile.id_column,
            how="left",
            validate="many_to_one",
        )
        slice_dimensions = selected_dimensions
        lineage["metadata_sha256"] = sha256_file(args.metadata)
    status = validate_batch_contracts(signals, args.batch_id, allow_partial=args.allow_partial)
    result = evaluate_alpha(
        signals,
        score_column=args.score_column,
        label_column=args.label_column,
        time_column=args.time_column,
        symbol_column=args.symbol_column,
        quantiles=args.quantiles,
        annualization_factor=args.annualization_factor,
        min_cross_section=args.min_cross_section,
        slice_columns=slice_dimensions,
    )
    print(write_report_bundle(args.output, batch_id=args.batch_id, batch_status=status, alpha=result, lineage=lineage))


if __name__ == "__main__":
    main()
