from __future__ import annotations

import argparse
from pathlib import Path

from graphalphalab.daily_labels_v2 import build_daily_labels_v2
from graphalphalab.discussion_pack import write_discussion_pack
from graphalphalab.dual_theme import export_dual_theme_signals, run_dual_theme_alpha_campaign
from graphalphalab.governance import ResourceBudget
from graphalphalab.intraday_labels_v2 import build_intraday_labels_v2, merge_horizon_manifests
from graphalphalab.rolling_alpha_v2 import run_rolling_alpha_v2


def parse_schedule(value: str) -> dict[int, int]:
    result: dict[int, int] = {}
    for item in value.split(","):
        if not item.strip():
            continue
        window_text, step_text = item.split(":", 1)
        window, step = int(window_text), int(step_text)
        if window <= 0 or step <= 0:
            raise ValueError("rolling windows and steps must be positive")
        result[window] = step
    if not result:
        raise ValueError("rolling schedule is empty")
    return result


def parse_ints(value: str) -> tuple[int, ...]:
    result = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    if not result:
        raise ValueError("integer list is empty")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one 24-worker global DAG across intraday and next-open daily horizons")
    parser.add_argument("--gff-campaign-root", type=Path, default=Path(r"D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=c4_260105_260722_induced_v2"))
    parser.add_argument("--bars-root", type=Path, default=Path(r"D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1"))
    parser.add_argument("--output-root", type=Path, default=Path(r"C:\GAL"))
    parser.add_argument("--start-date", default="2026-04-01")
    parser.add_argument("--end-date", default="2026-07-22")
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--daily-label-profile", choices=("core", "extended"), default="core")
    parser.add_argument("--rolling-schedule", default="5:1,10:1,15:2,20:2,30:5")
    parser.add_argument("--promotion-windows", default="15,20,30")
    parser.add_argument("--min-direction-train-dates", type=int, default=15)
    parser.add_argument("--direction-rolling-dates", type=int, default=30)
    parser.add_argument("--min-coverage-ratio", type=float, default=0.80)
    parser.add_argument("--factor-workers", type=int, default=24)
    parser.add_argument("--memory-limit-gb", type=float, default=120)
    parser.add_argument("--threads", type=int, default=24)
    parser.add_argument("--min-cross-section", type=int, default=100)
    parser.add_argument("--min-theme-size", type=int, default=5)
    parser.add_argument("--min-theme-cross-section", type=int, default=5)
    parser.add_argument("--expected-git-commit")
    parser.add_argument("--require-clean", action="store_true")
    parser.add_argument("--force-export", action="store_true")
    parser.add_argument("--export-workers", type=int, default=1)
    parser.add_argument("--label-workers", type=int, default=1)
    args = parser.parse_args()

    if args.factor_workers < 1:
        raise ValueError("factor-workers must be positive")
    if args.threads < args.factor_workers:
        raise ValueError("threads must be at least factor-workers")
    if args.memory_limit_gb / args.factor_workers < 4:
        raise ValueError("memory budget must provide at least 4GB per factor worker")

    schedule = parse_schedule(args.rolling_schedule)
    promotion_windows = parse_ints(args.promotion_windows)
    output_root = args.output_root.resolve()
    tag = "c4_260401_260722_induced_v2"
    signals = output_root / "signals" / tag
    labels = output_root / "labels" / tag
    intraday_labels = labels / "intraday"
    daily_labels = labels / "daily_next_open"
    reports = output_root / "reports" / tag
    rolling = reports / "rolling_alpha"
    discussion = reports / "discussion_pack"
    temp = output_root / "tmp" / tag
    for path in (signals, labels, reports, temp):
        path.mkdir(parents=True, exist_ok=True)

    budget = ResourceBudget(args.memory_limit_gb, args.threads, str(temp / "duckdb"))
    export_dual_theme_signals(
        args.gff_campaign_root,
        signals,
        resource_budget=budget,
        expected_git_commit=args.expected_git_commit,
        require_clean=args.require_clean,
        require_campaign_success=True,
        force=args.force_export,
        workers=args.export_workers,
    )
    intraday_manifest = build_intraday_labels_v2(
        gff_campaign_root=args.gff_campaign_root,
        signals_root=signals,
        bars_root=args.bars_root,
        output_root=intraday_labels,
        start_date=args.start_date,
        end_date=args.end_date,
        threads=args.threads,
        memory_limit_gb=max(8.0, args.memory_limit_gb / 2),
        temp_directory=temp / "intraday_labels",
        workers=args.label_workers,
    )
    daily_manifest = build_daily_labels_v2(
        gff_campaign_root=args.gff_campaign_root,
        signals_root=signals,
        bars_root=args.bars_root,
        output_root=daily_labels,
        start_date=args.start_date,
        end_date=args.end_date,
        profile=args.daily_label_profile,
        threads=args.threads,
        memory_limit_gb=max(8.0, args.memory_limit_gb / 2),
        temp_directory=temp / "daily_labels",
        workers=args.label_workers,
    )
    combined_manifest, combined_label_manifest = merge_horizon_manifests(
        (intraday_manifest, daily_manifest),
        labels / "combined_horizon_manifest.json",
        label_manifests=(
            intraday_labels / "diagnostics" / "intraday_label_manifest.json",
            daily_labels / "diagnostics" / "daily_label_manifest.json",
        ),
    )

    # All intraday and daily horizons/scopes/factors share this one global ready queue.
    run_dual_theme_alpha_campaign(
        signals,
        combined_manifest,
        reports,
        metadata_path=args.metadata,
        slice_dimensions=(("sector_code", "industry_code", "country", "market_cap_bucket") if args.metadata else ()),
        min_cross_section=args.min_cross_section,
        min_theme_size=args.min_theme_size,
        min_theme_cross_section=args.min_theme_cross_section,
        resource_budget=budget,
        expected_git_commit=args.expected_git_commit,
        require_clean=args.require_clean,
        correlation_sample_modulus=0,
        factor_workers=args.factor_workers,
    )
    run_rolling_alpha_v2(
        reports,
        rolling,
        label_manifest=combined_label_manifest,
        windows=tuple(schedule),
        window_steps=schedule,
        promotion_windows=promotion_windows,
        min_coverage_ratio=args.min_coverage_ratio,
        min_direction_train_dates=args.min_direction_train_dates,
        direction_rolling_dates=args.direction_rolling_dates,
    )
    write_discussion_pack(reports, rolling, discussion)
    print(f"signals={signals}")
    print(f"combined_horizon_manifest={combined_manifest}")
    print(f"reports={reports}")
    print(f"rolling={rolling}")
    print(f"discussion_pack={discussion}")


if __name__ == "__main__":
    main()
