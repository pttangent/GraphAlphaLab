from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from graphalphalab.execution_candidates import (
    select_execution_candidates,
    write_candidate_manifest,
)


def _csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Select matched graph-forward direct-return candidates for the "
            "governed execution-frequency DAG."
        )
    )
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--table-output", type=Path)
    parser.add_argument("--min-date-count", type=int, default=20)
    parser.add_argument("--max-per-horizon", type=int, default=12)
    parser.add_argument("--min-per-horizon", type=int, default=2)
    parser.add_argument(
        "--scopes",
        default="global,within_theme,inter_theme",
    )
    args = parser.parse_args()

    metrics = pd.read_csv(args.metrics)
    candidates = select_execution_candidates(
        metrics,
        min_date_count=args.min_date_count,
        max_per_horizon=args.max_per_horizon,
        min_per_horizon=args.min_per_horizon,
        scopes=_csv_list(args.scopes),
    )
    if candidates.empty:
        raise SystemExit(
            "No execution candidates passed the configured date and semantic gates"
        )
    parameters = {
        "min_date_count": args.min_date_count,
        "max_per_horizon": args.max_per_horizon,
        "min_per_horizon": args.min_per_horizon,
        "scopes": _csv_list(args.scopes),
        "variant": "graph_forward",
        "financial_role": "direct_return_alpha",
    }
    write_candidate_manifest(
        candidates,
        source_metrics=args.metrics,
        output=args.output,
        parameters=parameters,
    )
    table_output = args.table_output or args.output.with_suffix(".csv")
    table_output.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(table_output, index=False)
    print(args.output)


if __name__ == "__main__":
    main()
