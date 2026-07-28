from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from graphalphalab.core4_execution_semantics import (
    CORE4_SCOPE_EXECUTION_SEMANTICS,
    annotate_scope_frame,
    load_core4_campaign_contract,
)


ROOT_TABLES = (
    "all_horizons_alpha_metrics.csv",
    "global_alpha_metrics.csv",
    "within_theme_alpha_metrics.csv",
    "inter_theme_alpha_metrics.csv",
    "direct_return_alpha_metrics.csv",
    "regime_candidate_metrics.csv",
    "ranking.csv",
    "scope_family_horizon_summary.csv",
    "cross_scope_comparison.csv",
    "matched_variant_comparison.csv",
)


def _clean_comparison(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "horizon" in result and "horizon_minutes" in result:
        parsed = pd.to_numeric(
            result["horizon"].astype(str).str.replace("m", "", regex=False),
            errors="coerce",
        )
        minutes = pd.to_numeric(result["horizon_minutes"], errors="coerce")
        result = result[parsed.isna() | minutes.isna() | (parsed == minutes)].copy()
    metric_columns = [
        column
        for column in result.columns
        if column.startswith(("mean_", "net_", "cost_", "abs_ic_"))
    ]
    if metric_columns:
        result = result.dropna(subset=metric_columns, how="all")
    return result.reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write corrected induced-global semantic views of GAL report tables."
    )
    parser.add_argument("--gff-campaign-root", type=Path, required=True)
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    audit = load_core4_campaign_contract(args.gff_campaign_root)
    output = args.output or args.report_root / "execution_semantics"
    output.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    for name in ROOT_TABLES:
        source = args.report_root / name
        if not source.exists():
            continue
        frame = pd.read_csv(source)
        if name in {"cross_scope_comparison.csv", "matched_variant_comparison.csv"}:
            frame = _clean_comparison(frame)
        if "scope" in frame.columns:
            frame = annotate_scope_frame(frame)
        destination = output / name
        frame.to_csv(destination, index=False)
        written.append(str(destination))

    semantics = {
        "campaign": {
            key: value for key, value in audit.items() if key != "contract_payload"
        },
        "scope_semantics": {
            scope: semantics.as_dict()
            for scope, semantics in CORE4_SCOPE_EXECUTION_SEMANTICS.items()
        },
        "written_tables": written,
    }
    (output / "scope_semantics.json").write_text(
        json.dumps(semantics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Core4 induced-global execution semantics",
        "",
        "- Global: Global graph estimate + Global stock rank.",
        "- Within-Theme: Global graph estimate + same-theme induced edge selection + Local stock rank inside `decision_time × context_theme_id`.",
        "- Inter-Theme: Global graph estimate + theme aggregation + rank across theme portfolios.",
        "- `within_theme` is not a locally re-estimated graph and must not be described as Local graph Alpha.",
        "",
        f"Campaign: `{audit['campaign_root']}`",
        f"Dates: {audit['start_date']} .. {audit['end_date']} ({audit['date_count']})",
        f"Expected governed factors per horizon: {audit['factor_count_per_horizon']}",
        "",
    ]
    if audit.get("interface_warning"):
        lines.extend(["## Interface warning", "", str(audit["interface_warning"]), ""])
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
