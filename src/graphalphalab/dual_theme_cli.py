from __future__ import annotations

import argparse
import json
from pathlib import Path

from .dual_theme import (
    DEFAULT_SCOPES,
    DEFAULT_THEME_FAMILIES,
    DUAL_THEME_BATCH_ID,
    SUPPORTED_VARIANTS,
    export_dual_theme_signals,
    run_dual_theme_alpha_campaign,
)
from .governance import ResourceBudget


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
    parser = argparse.ArgumentParser(
        description="Export and evaluate dual-theme, three-scope GraphFactorFactory outputs"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    export = sub.add_parser(
        "export",
        help="Export Global/Within/Inter graph scores from a dual-theme GFF campaign",
    )
    export.add_argument("--gff-campaign-root", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--batch-id", default=DUAL_THEME_BATCH_ID)
    export.add_argument("--theme-families", default=",".join(DEFAULT_THEME_FAMILIES))
    export.add_argument("--scopes", default=",".join(DEFAULT_SCOPES))
    export.add_argument("--variants", default=",".join(SUPPORTED_VARIANTS))
    export.add_argument("--force", action="store_true")
    export.add_argument("--allow-incomplete-campaign", action="store_true")
    _resource_args(export)
    _lineage_args(export)

    alpha = sub.add_parser(
        "alpha",
        help="Evaluate exported dual-theme signals across every horizon in a manifest",
    )
    alpha.add_argument("--signals", type=Path, required=True)
    alpha.add_argument("--horizon-manifest", type=Path, required=True)
    alpha.add_argument("--output", type=Path, required=True)
    alpha.add_argument("--metadata", type=Path)
    alpha.add_argument("--metadata-id", default="symbol_id")
    alpha.add_argument("--metadata-signal-id", default="symbol_id")
    alpha.add_argument("--slice-dimensions")
    alpha.add_argument("--join-keys", default="trade_date,decision_time,symbol_id")
    alpha.add_argument("--score-column", default="score")
    alpha.add_argument("--symbol-column", default="symbol_id")
    alpha.add_argument("--quantiles", type=int, default=5)
    alpha.add_argument("--min-cross-section", type=int, default=100)
    alpha.add_argument("--direction-column", default="expected_direction")
    alpha.add_argument(
        "--default-direction",
        choices=("auto", "positive", "negative"),
        default="auto",
    )
    alpha.add_argument("--control-columns", default="own_score")
    alpha.add_argument("--annualization-factor", type=float)
    alpha.add_argument("--correlation-sample-modulus", type=int, default=1000)
    alpha.add_argument("--allow-legacy-signals", action="store_true")
    alpha.add_argument("--allow-partial", action="store_true")
    _resource_args(alpha)
    _lineage_args(alpha)

    full = sub.add_parser(
        "full",
        help="Export signals and run all horizon reports in one governed workflow",
    )
    full.add_argument("--gff-campaign-root", type=Path, required=True)
    full.add_argument("--signals-output", type=Path, required=True)
    full.add_argument("--horizon-manifest", type=Path, required=True)
    full.add_argument("--output", type=Path, required=True)
    full.add_argument("--batch-id", default=DUAL_THEME_BATCH_ID)
    full.add_argument("--theme-families", default=",".join(DEFAULT_THEME_FAMILIES))
    full.add_argument("--scopes", default=",".join(DEFAULT_SCOPES))
    full.add_argument("--variants", default=",".join(SUPPORTED_VARIANTS))
    full.add_argument("--metadata", type=Path)
    full.add_argument("--metadata-id", default="symbol_id")
    full.add_argument("--metadata-signal-id", default="symbol_id")
    full.add_argument("--slice-dimensions")
    full.add_argument("--join-keys", default="trade_date,decision_time,symbol_id")
    full.add_argument("--score-column", default="score")
    full.add_argument("--symbol-column", default="symbol_id")
    full.add_argument("--quantiles", type=int, default=5)
    full.add_argument("--min-cross-section", type=int, default=100)
    full.add_argument("--direction-column", default="expected_direction")
    full.add_argument(
        "--default-direction",
        choices=("auto", "positive", "negative"),
        default="auto",
    )
    full.add_argument("--control-columns", default="own_score")
    full.add_argument("--annualization-factor", type=float)
    full.add_argument("--correlation-sample-modulus", type=int, default=1000)
    full.add_argument("--allow-legacy-signals", action="store_true")
    full.add_argument("--allow-partial", action="store_true")
    full.add_argument("--force-export", action="store_true")
    full.add_argument("--allow-incomplete-campaign", action="store_true")
    _resource_args(full)
    _lineage_args(full)
    return parser


def main() -> None:
    args = _parser().parse_args()
    budget = ResourceBudget(
        args.memory_limit_gb,
        args.threads,
        str(args.temp_directory) if args.temp_directory else None,
    )
    if args.command in {"export", "full"}:
        summary = export_dual_theme_signals(
            args.gff_campaign_root,
            args.output if args.command == "export" else args.signals_output,
            batch_id=args.batch_id,
            theme_families=_csv_list(args.theme_families),
            scopes=_csv_list(args.scopes),
            variants=_csv_list(args.variants),
            resource_budget=budget,
            expected_git_commit=args.expected_git_commit,
            require_clean=args.require_clean,
            require_campaign_success=not args.allow_incomplete_campaign,
            force=args.force if args.command == "export" else args.force_export,
        )
        print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
        if args.command == "export":
            return
    signals = args.signals if args.command == "alpha" else args.signals_output
    output = run_dual_theme_alpha_campaign(
        signals,
        args.horizon_manifest,
        args.output,
        metadata_path=args.metadata,
        metadata_id=args.metadata_id,
        metadata_signal_id=args.metadata_signal_id,
        slice_dimensions=_csv_list(args.slice_dimensions),
        join_keys=_csv_list(args.join_keys),
        score_column=args.score_column,
        symbol_column=args.symbol_column,
        quantiles=args.quantiles,
        min_cross_section=args.min_cross_section,
        direction_column=args.direction_column or None,
        default_direction=args.default_direction,
        control_columns=_csv_list(args.control_columns),
        annualization_factor=args.annualization_factor,
        resource_budget=budget,
        expected_git_commit=args.expected_git_commit,
        require_clean=args.require_clean,
        allow_legacy_signals=args.allow_legacy_signals,
        correlation_sample_modulus=args.correlation_sample_modulus,
        allow_partial=args.allow_partial,
    )
    print(output)


if __name__ == "__main__":
    main()
