from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import json
from typing import Iterable

import pandas as pd

from .governance import atomic_write_frame, atomic_write_json, atomic_write_text, sha256_file


@dataclass(frozen=True)
class BatchSpec:
    batch_id: str
    expected_contracts: int | None
    expected_upstream_contracts: int | None
    report_scope: tuple[str, ...]
    parents: tuple[str, ...] = ()
    description: str = ""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


BATCH_REGISTRY: dict[str, BatchSpec] = {
    "implemented27": BatchSpec(
        "implemented27", 27, 27, ("alpha", "performance", "robustness", "p1_structure", "temporal"),
        description="The original 27 executable Interaction layer-scale contracts.",
    ),
    "remaining14": BatchSpec(
        "remaining14", 14, 14, ("alpha", "performance", "robustness", "p1_structure", "temporal"),
        description="The 14 specialized Interaction layer-scale contracts.",
    ),
    "similarity10": BatchSpec(
        "similarity10", 11, 10, ("p1_structure", "theme_purity", "temporal", "consensus"),
        description="Ten layer-local Similarity P1 contracts plus the recursive consensus P1.",
    ),
    "theme_discovery": BatchSpec(
        "theme_discovery", 1, 10, ("theme_purity", "structure", "optional_alpha"),
        description="Legacy alias for a single consensus membership export. Prefer similarity10 P1 reporting.",
    ),
    "all41": BatchSpec(
        "all41", 41, 41, ("alpha", "performance", "robustness", "cross_batch"),
        parents=("implemented27", "remaining14"),
        description="Combined Interaction registry; built from compact batch reports.",
    ),
    "three_batch_33day": BatchSpec(
        "three_batch_33day", None, 51, ("campaign", "alpha", "p1_structure", "theme_purity", "temporal"),
        parents=("implemented27", "remaining14", "similarity10"),
        description="Governed compact campaign report for IG27, RM14 and Similarity10 over 33 sessions.",
    ),
}


def get_batch(batch_id: str) -> BatchSpec:
    key = str(batch_id).strip().lower()
    if key not in BATCH_REGISTRY:
        raise KeyError(f"Unknown batch {batch_id!r}; available={sorted(BATCH_REGISTRY)}")
    return BATCH_REGISTRY[key]


def validate_batch_contracts(frame: pd.DataFrame, batch_id: str, *, allow_partial: bool = False) -> dict[str, object]:
    spec = get_batch(batch_id)
    keys = [column for column in ("layer_id", "scale_minutes") if column in frame.columns]
    actual = int(frame[keys].drop_duplicates().shape[0]) if keys else None
    complete = spec.expected_contracts is None or actual == spec.expected_contracts
    if not complete and not allow_partial:
        raise ValueError(
            f"Batch {batch_id} expected {spec.expected_contracts} layer-scale contracts, got {actual}; "
            "use --allow-partial only for an explicitly partial report"
        )
    return {
        "batch_id": spec.batch_id,
        "expected_contracts": spec.expected_contracts,
        "observed_contracts": actual,
        "complete": complete,
        "partial": not complete,
        "report_scope": list(spec.report_scope),
    }


def merge_compact_reports(inputs: Iterable[str | Path], output_root: str | Path) -> Path:
    roots = [Path(raw).expanduser().resolve() for raw in inputs]
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, object]] = []
    metrics: list[pd.DataFrame] = []
    source_hashes: list[dict[str, object]] = []
    for root in roots:
        summary_path = root / "summary.json"
        success_path = root / "_SUCCESS"
        metrics_path = root / "alpha_metrics.csv"
        if not summary_path.exists() or not success_path.exists():
            raise FileNotFoundError(f"Compact report is not governed/complete: {root}")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if bool(summary.get("batch_status", {}).get("partial")):
            raise ValueError(f"Cannot merge partial report without an explicit upstream completion: {root}")
        summaries.append(summary)
        source_hashes.append({"root": str(root), "summary_sha256": sha256_file(summary_path), "success_sha256": sha256_file(success_path)})
        if metrics_path.exists():
            frame = pd.read_csv(metrics_path)
            frame["source_report"] = str(root)
            metrics.append(frame)
    combined = pd.concat(metrics, ignore_index=True) if metrics else pd.DataFrame()
    if not combined.empty:
        atomic_write_frame(combined, output_root / "alpha_metrics.csv")
        ranking_columns = [column for column in ("research_status", "fdr_pass", "net_mean_5bps", "mean_spearman_ic") if column in combined.columns]
        ranking = combined.copy()
        if "research_status" in ranking:
            ranking["_status_rank"] = ranking["research_status"].map({"candidate": 0, "needs_falsification": 1, "insufficient_or_rejected": 2}).fillna(3)
            ranking_columns = ["_status_rank", *[c for c in ranking_columns if c != "research_status"]]
        if ranking_columns:
            ranking = ranking.sort_values(ranking_columns, ascending=[True] + [False] * (len(ranking_columns) - 1), na_position="last")
        atomic_write_frame(ranking.drop(columns=["_status_rank"], errors="ignore"), output_root / "ranking.csv")
    payload = {
        "batch_id": "all41",
        "source_reports": [str(root) for root in roots],
        "source_summaries": summaries,
        "source_hashes": source_hashes,
        "factor_count": int(len(combined)),
        "complete": True,
    }
    atomic_write_json(output_root / "summary.json", payload)
    atomic_write_text(
        output_root / "REPORT.md",
        "\n".join(
            [
                "# GraphAlphaLab all41 compact merge",
                "",
                f"- Source reports: {len(summaries)}",
                f"- Factor rows: {len(combined)}",
                "- Complete: True",
                "",
                "This report was merged from hashed compact report bundles and did not reread large GFF partitions.",
            ]
        ) + "\n",
    )
    atomic_write_json(output_root / "_SUCCESS", {"source_count": len(roots), "factor_count": len(combined)})
    return output_root
