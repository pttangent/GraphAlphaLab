from __future__ import annotations

import argparse
import json
from pathlib import Path

from graphalphalab.forward_volatility_hardening import install as install_hardening

install_hardening()

from graphalphalab.forward_volatility_report_pack import install as install_report_pack  # noqa: E402

install_report_pack()

from graphalphalab.forward_volatility_report_streaming import install as install_report_streaming  # noqa: E402

install_report_streaming()

from graphalphalab.forward_volatility_report_checkpointed import install as install_report_preflight  # noqa: E402

install_report_preflight()

from graphalphalab.forward_volatility import (  # noqa: E402
    DEFAULT_HORIZONS,
    DEFAULT_TARGETS,
    run_forward_volatility_patch,
)
from graphalphalab.governance import ResourceBudget  # noqa: E402


def _csv_ints(value: str) -> tuple[int, ...]:
    values = tuple(sorted({int(token.strip()) for token in value.split(",") if token.strip()}))
    if not values:
        raise argparse.ArgumentTypeError("At least one horizon is required")
    return values


def _csv_strings(value: str) -> tuple[str, ...]:
    values = tuple(dict.fromkeys(token.strip() for token in value.split(",") if token.strip()))
    if not values:
        raise argparse.ArgumentTypeError("At least one target is required")
    return values


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build governed forward-volatility labels from NFF gff_core and evaluate "
            "all existing GAL graph layers with resumable, preflight-cached, self-healing DAGs."
        )
    )
    parser.add_argument("--signals-root", type=Path, required=True)
    parser.add_argument(
        "--gff-core-template",
        required=True,
        help=(
            "Daily NFF gff_core template. Supported fields: {date}, {yyyymmdd}, "
            "{year}, {month}. Example: D:/NFF/features/gff_core/schema=v1/date={date}/data.parquet"
        ),
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument(
        "--horizons",
        type=_csv_ints,
        default=DEFAULT_HORIZONS,
        help="Comma-separated minute horizons; default: 30,60,120,180",
    )
    parser.add_argument(
        "--targets",
        type=_csv_strings,
        default=DEFAULT_TARGETS,
        help="Comma-separated forward-volatility target columns",
    )
    parser.add_argument("--label-workers", type=int, default=4)
    parser.add_argument("--prediction-workers", type=int, default=6)
    parser.add_argument("--min-workers", type=int, default=2)
    parser.add_argument("--memory-limit-gb", type=float, default=96.0)
    parser.add_argument("--threads", type=int, default=18)
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--expected-git-commit", required=True)
    parser.add_argument("--require-clean", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.label_workers < 1 or args.prediction_workers < 1 or args.min_workers < 1:
        raise ValueError("worker counts must be positive")
    output = run_forward_volatility_patch(
        signals_root=args.signals_root,
        gff_core_template=args.gff_core_template,
        output_root=args.output_root,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=args.horizons,
        targets=args.targets,
        label_workers=args.label_workers,
        prediction_workers=args.prediction_workers,
        min_workers=args.min_workers,
        resource_budget=ResourceBudget(
            memory_limit_gb=args.memory_limit_gb,
            threads=args.threads,
            temp_directory=str(args.temp_directory.resolve()) if args.temp_directory else None,
        ),
        expected_git_commit=args.expected_git_commit,
        require_clean=args.require_clean,
    )
    print(json.dumps({"status": "complete", "report": str(output)}, indent=2))


if __name__ == "__main__":
    main()
