from __future__ import annotations

import argparse
from pathlib import Path

from graphalphalab.daily_alpha import build_daily_labels, run_rolling_alpha
from graphalphalab.dual_theme import export_dual_theme_signals, run_dual_theme_alpha_campaign
from graphalphalab.governance import ResourceBudget


def csv_ints(value: str) -> tuple[int, ...]:
    return tuple(int(x.strip()) for x in value.split(",") if x.strip())


def main() -> None:
    p = argparse.ArgumentParser(description="Run governed 70-session dual-theme daily and rolling Alpha")
    p.add_argument("--gff-campaign-root", type=Path, required=True)
    p.add_argument("--bars-root", type=Path, required=True)
    p.add_argument("--signals-output", type=Path, required=True)
    p.add_argument("--label-output", type=Path, required=True)
    p.add_argument("--report-output", type=Path, required=True)
    p.add_argument("--rolling-output", type=Path)
    p.add_argument("--metadata", type=Path)
    p.add_argument("--date-count", type=int, default=70)
    p.add_argument("--end-date")
    p.add_argument("--label-profile", choices=("core", "extended"), default="core")
    p.add_argument("--rolling-windows", default="20,40,60")
    p.add_argument("--rolling-step", type=int, default=5)
    p.add_argument("--factor-workers", type=int, default=6)
    p.add_argument("--memory-limit-gb", type=float, default=64)
    p.add_argument("--threads", type=int, default=12)
    p.add_argument("--temp-directory", type=Path)
    p.add_argument("--min-cross-section", type=int, default=100)
    p.add_argument("--min-theme-size", type=int, default=5)
    p.add_argument("--min-theme-cross-section", type=int, default=5)
    p.add_argument("--expected-git-commit")
    p.add_argument("--require-clean", action="store_true")
    p.add_argument("--force-export", action="store_true")
    args = p.parse_args()

    budget = ResourceBudget(args.memory_limit_gb, args.threads, str(args.temp_directory) if args.temp_directory else None)
    export_dual_theme_signals(
        args.gff_campaign_root,
        args.signals_output,
        resource_budget=budget,
        expected_git_commit=args.expected_git_commit,
        require_clean=args.require_clean,
        require_campaign_success=True,
        force=args.force_export,
    )
    horizon_manifest = build_daily_labels(
        gff_campaign_root=args.gff_campaign_root,
        signals_root=args.signals_output,
        bars_root=args.bars_root,
        output_root=args.label_output,
        date_count=args.date_count,
        end_date=args.end_date,
        profile=args.label_profile,
        threads=max(1, args.threads),
        memory_limit_gb=max(4, args.memory_limit_gb / 2),
        temp_directory=(args.temp_directory / "daily_labels") if args.temp_directory else None,
    )
    run_dual_theme_alpha_campaign(
        args.signals_output,
        horizon_manifest,
        args.report_output,
        metadata_path=args.metadata,
        slice_dimensions=("sector_code", "industry_code", "country", "market_cap_bucket") if args.metadata else (),
        min_cross_section=args.min_cross_section,
        min_theme_size=args.min_theme_size,
        min_theme_cross_section=args.min_theme_cross_section,
        resource_budget=budget,
        expected_git_commit=args.expected_git_commit,
        require_clean=args.require_clean,
        correlation_sample_modulus=0,
        factor_workers=args.factor_workers,
    )
    rolling = args.rolling_output or (args.report_output / "rolling_alpha")
    run_rolling_alpha(
        args.report_output,
        rolling,
        label_manifest=args.label_output / "diagnostics" / "daily_label_manifest.json",
        windows=csv_ints(args.rolling_windows),
        step=args.rolling_step,
    )
    print(rolling)


if __name__ == "__main__":
    main()
