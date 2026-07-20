from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from graph_alpha_lab.semantics import SemanticThresholds, build_theme_semantics, load_mapping


def main() -> int:
    parser = argparse.ArgumentParser(description="Build governed sector semantics for P1 theme instances")
    parser.add_argument("--p1-root", type=Path, required=True)
    parser.add_argument("--sector-mapping", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-theme-size", type=int, default=5)
    parser.add_argument("--min-mapping-coverage", type=float, default=0.70)
    parser.add_argument("--min-total-purity", type=float, default=0.50)
    args = parser.parse_args()

    files = sorted(args.p1_root.glob("**/memberships.parquet"))
    if not files:
        raise SystemExit("No memberships.parquet files found")

    frames = []
    for path in files:
        frame = pd.read_parquet(path)
        parts = {part.split("=", 1)[0]: part.split("=", 1)[1] for part in path.parts if "=" in part}
        if "trade_date" not in frame and "date" not in frame:
            frame["trade_date"] = parts.get("date")
        if "layer" not in frame:
            frame["layer"] = parts.get("layer")
        if "scale" not in frame:
            frame["scale"] = int(parts["scale"]) if parts.get("scale") else pd.NA
        frames.append(frame)

    memberships = pd.concat(frames, ignore_index=True)
    mapping = load_mapping(args.sector_mapping)
    thresholds = SemanticThresholds(
        min_theme_size=args.min_theme_size,
        min_mapping_coverage=args.min_mapping_coverage,
        min_total_purity=args.min_total_purity,
    )
    result = build_theme_semantics(memberships, mapping, thresholds)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(args.output, index=False)
    summary = (
        result.groupby(["layer", "scale", "semantic_label"], dropna=False)
        .size()
        .rename("theme_instances")
        .reset_index()
    )
    summary.to_csv(args.output.with_suffix(".summary.csv"), index=False)
    print(f"wrote {len(result):,} theme instances to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
