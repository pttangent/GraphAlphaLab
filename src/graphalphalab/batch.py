from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import json
from typing import Iterable

import pandas as pd

from .io import atomic_write_json, atomic_write_text


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
        "implemented27", 27, 27, ("alpha", "performance", "robustness"),
        description="The original 27 executable Interaction layer-scale contracts.",
    ),
    "remaining14": BatchSpec(
        "remaining14", 14, 14, ("alpha", "performance", "robustness"),
        description="The 14 specialized Interaction layer-scale contracts.",
    ),
    "theme_discovery": BatchSpec(
        "theme_discovery", 1, 12, ("theme_purity", "structure", "optional_alpha"),
        description="Ten core and two diagnostic similarity inputs feeding the Theme Base Graph.",
    ),
    "all41": BatchSpec(
        "all41", 41, 41, ("alpha", "performance", "robustness", "cross_batch"),
        parents=("implemented27", "remaining14"),
        description="Combined Interaction registry; built from compact batch reports.",
    ),
}


def get_batch(batch_id: str) -> BatchSpec:
    key = str(batch_id).strip().lower()
    if key not in BATCH_REGISTRY:
        raise KeyError(f"Unknown batch {batch_id!r}; available={sorted(BATCH_REGISTRY)}")
    return BATCH_REGISTRY[key]


def validate_batch_contracts(
    frame: pd.DataFrame,
    batch_id: str,
    *,
    allow_partial: bool = False,
) -> dict[str, object]:
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
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, object]] = []
    metrics: list[pd.DataFrame] = []
    for raw in inputs:
        root = Path(raw).expanduser().resolve()
        summary_path = root / "summary.json"
        metrics_path = root / "alpha_metrics.csv"
        if not summary_path.exists():
            raise FileNotFoundError(summary_path)
        summaries.append(json.loads(summary_path.read_text(encoding="utf-8")))
        if metrics_path.exists():
            frame = pd.read_csv(metrics_path)
            frame["source_report"] = str(root)
            metrics.append(frame)
    combined = pd.concat(metrics, ignore_index=True) if metrics else pd.DataFrame()
    if not combined.empty:
        combined.to_csv(output_root / "alpha_metrics.csv", index=False)
        ranking_columns = [
            column
            for column in ("fdr_pass", "net_sharpe_5bps", "mean_spearman_ic")
            if column in combined.columns
        ]
        ranking = (
            combined.sort_values(ranking_columns, ascending=[False] * len(ranking_columns))
            if ranking_columns
            else combined.copy()
        )
        ranking.to_csv(output_root / "ranking.csv", index=False)
    payload = {
        "batch_id": "all41",
        "source_reports": [str(Path(value).resolve()) for value in inputs],
        "source_summaries": summaries,
        "factor_count": int(len(combined)),
        "complete": all(not bool(item.get("batch_status", {}).get("partial")) for item in summaries),
    }
    atomic_write_json(output_root / "summary.json", payload)
    lines = [
        "# GraphAlphaLab all41 compact merge",
        "",
        f"- Source reports: {len(summaries)}",
        f"- Factor rows: {len(combined)}",
        f"- Complete: {payload['complete']}",
        "",
        "This report was merged from compact batch summaries and did not reread large GFF partitions.",
    ]
    atomic_write_text(output_root / "REPORT.md", "\n".join(lines) + "\n")
    return output_root
