from __future__ import annotations

import argparse
from pathlib import Path

from graphalphalab.execution_frequency_campaign import (
    run_execution_frequency_campaign,
)
from graphalphalab.governance import ResourceBudget


def _csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run candidate × horizon execution-frequency research in one "
            "six-worker global DAG without recomputing GraphFactorFactory outputs."
        )
    )
    parser.add_argument("--signals-root", type=Path, required=True)
    parser.add_argument("--horizon-manifest", type=Path, required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--memory-limit-gb", type=float, default=64.0)
    parser.add_argument("--threads", type=int, default=12)
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--min-theme-size", type=int, default=5)
    parser.add_argument("--min-theme-cross-section", type=int, default=5)
    parser.add_argument("--min-direction-train-dates", type=int, default=20)
    parser.add_argument("--direction-rolling-dates", type=int, default=60)
    parser.add_argument("--control-columns", default="own_score")
    parser.add_argument("--quantiles", type=int, default=5)
    parser.add_argument(
        "--carry-overnight",
        action="store_true",
        help=(
            "Explicit diagnostic only. Default execution state resets at each "
            "trading session and does not carry intraday books overnight."
        ),
    )
    args = parser.parse_args()

    output = run_execution_frequency_campaign(
        signals_root=args.signals_root,
        horizon_manifest=args.horizon_manifest,
        candidate_manifest=args.candidate_manifest,
        output_root=args.output,
        workers=args.workers,
        resource_budget=ResourceBudget(
            args.memory_limit_gb,
            args.threads,
            str(args.temp_directory) if args.temp_directory else None,
        ),
        min_theme_size=args.min_theme_size,
        min_theme_cross_section=args.min_theme_cross_section,
        min_direction_train_dates=args.min_direction_train_dates,
        direction_rolling_dates=args.direction_rolling_dates,
        control_columns=_csv_list(args.control_columns),
        quantiles=args.quantiles,
        carry_overnight=args.carry_overnight,
    )
    print(output)


if __name__ == "__main__":
    main()
