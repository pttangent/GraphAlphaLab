from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from .governance import atomic_write_frame, atomic_write_json, atomic_write_text, sha256_file


@dataclass(frozen=True)
class CampaignSource:
    batch_id: str
    report_type: str
    path: Path


def _validate_bundle(path: Path) -> dict[str, object]:
    summary_path = path / "summary.json"
    success_path = path / "_SUCCESS"
    if not summary_path.exists() or not success_path.exists():
        raise FileNotFoundError(f"Incomplete governed report bundle: {path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    success = json.loads(success_path.read_text(encoding="utf-8")) if success_path.stat().st_size else {}
    complete = success.get("complete", True)
    if complete is False:
        raise ValueError(f"Upstream report is explicitly partial: {path}")
    if bool(summary.get("batch_status", {}).get("partial")):
        raise ValueError(f"Upstream alpha report is partial: {path}")
    if bool(summary.get("governance", {}).get("partial")):
        raise ValueError(f"Upstream P1 report is partial: {path}")
    return summary


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _dates_from_report(path: Path) -> set[str]:
    dates: set[str] = set()
    daily_ic = path / "daily_ic.csv"
    if daily_ic.exists():
        frame = _read_csv(daily_ic)
        if "trade_date" in frame:
            dates.update(frame["trade_date"].dropna().astype(str))
    partition_summary = path / "p1_partition_summary.csv"
    if partition_summary.exists():
        frame = _read_csv(partition_summary)
        if "trade_date" in frame:
            dates.update(frame["trade_date"].dropna().astype(str))
    return dates


def build_campaign_report(
    sources: Iterable[CampaignSource],
    output_root: str | Path,
    *,
    start_date: str,
    end_date: str,
    expected_date_count: int = 33,
) -> Path:
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    success = output / "_SUCCESS"
    success.unlink(missing_ok=True)

    source_rows: list[dict[str, object]] = []
    all_dates: set[str] = set()
    alpha_frames: list[pd.DataFrame] = []
    p1_layer_frames: list[pd.DataFrame] = []
    temporal_frames: list[pd.DataFrame] = []
    purity_frames: list[pd.DataFrame] = []

    source_list = list(sources)
    required = {
        ("implemented27", "alpha"),
        ("implemented27", "p1"),
        ("remaining14", "alpha"),
        ("remaining14", "p1"),
        ("similarity10", "p1"),
    }
    observed = {(source.batch_id, source.report_type) for source in source_list}
    missing = required - observed
    if missing:
        raise ValueError(f"Campaign report is missing required sources: {sorted(missing)}")

    for source in source_list:
        root = source.path.expanduser().resolve()
        summary = _validate_bundle(root)
        dates = _dates_from_report(root)
        if source.report_type != "alpha_merged":
            if not dates:
                raise ValueError(f"Report has no auditable trade_date evidence: {root}")
            if min(dates) < start_date or max(dates) > end_date:
                raise ValueError(f"Report contains dates outside campaign range: {root}")
            if len(dates) != expected_date_count:
                raise ValueError(
                    f"Report {root} expected {expected_date_count} dates, observed {len(dates)}: {sorted(dates)}"
                )
            all_dates.update(dates)
        source_rows.append({
            "batch_id": source.batch_id,
            "report_type": source.report_type,
            "path": str(root),
            "summary_sha256": sha256_file(root / "summary.json"),
            "success_sha256": sha256_file(root / "_SUCCESS"),
            "date_count": len(dates),
            "complete": True,
            "run_contract_hash": summary.get("run_contract_hash"),
        })
        alpha_path = root / "alpha_metrics.csv"
        if alpha_path.exists():
            frame = _read_csv(alpha_path)
            frame["campaign_batch_id"] = source.batch_id
            frame["source_report"] = str(root)
            alpha_frames.append(frame)
        p1_path = root / "p1_layer_summary.csv"
        if p1_path.exists():
            frame = _read_csv(p1_path)
            frame["campaign_batch_id"] = source.batch_id
            frame["source_report"] = str(root)
            p1_layer_frames.append(frame)
        for filename in ("p1_temporal_summary.csv", "p1_range_temporal_summary.csv"):
            temporal_path = root / filename
            if temporal_path.exists():
                frame = _read_csv(temporal_path)
                frame["campaign_batch_id"] = source.batch_id
                frame["temporal_scope"] = filename.removeprefix("p1_").removesuffix("_summary.csv")
                frame["source_report"] = str(root)
                temporal_frames.append(frame)
        purity_path = root / "purity_dimension_summary.csv"
        if purity_path.exists():
            frame = _read_csv(purity_path)
            frame["campaign_batch_id"] = source.batch_id
            frame["source_report"] = str(root)
            purity_frames.append(frame)

    if len(all_dates) != expected_date_count:
        raise ValueError(
            f"Campaign expected {expected_date_count} distinct trading dates in {start_date}..{end_date}, "
            f"observed {len(all_dates)}: {sorted(all_dates)}"
        )

    source_frame = pd.DataFrame(source_rows)
    alpha = pd.concat(alpha_frames, ignore_index=True) if alpha_frames else pd.DataFrame()
    p1_layers = pd.concat(p1_layer_frames, ignore_index=True) if p1_layer_frames else pd.DataFrame()
    temporal = pd.concat(temporal_frames, ignore_index=True) if temporal_frames else pd.DataFrame()
    purity = pd.concat(purity_frames, ignore_index=True) if purity_frames else pd.DataFrame()

    atomic_write_frame(source_frame, output / "campaign_sources.csv")
    atomic_write_frame(alpha, output / "campaign_alpha_metrics.csv")
    atomic_write_frame(p1_layers, output / "campaign_p1_layers.csv")
    atomic_write_frame(temporal, output / "campaign_temporal.csv")
    atomic_write_frame(purity, output / "campaign_purity.csv")

    batch_rows: list[dict[str, object]] = []
    for batch_id in ("implemented27", "remaining14", "similarity10"):
        alpha_batch = alpha[alpha["campaign_batch_id"] == batch_id] if not alpha.empty else pd.DataFrame()
        p1_batch = p1_layers[p1_layers["campaign_batch_id"] == batch_id] if not p1_layers.empty else pd.DataFrame()
        temporal_batch = temporal[temporal["campaign_batch_id"] == batch_id] if not temporal.empty else pd.DataFrame()
        purity_batch = purity[purity["campaign_batch_id"] == batch_id] if not purity.empty else pd.DataFrame()
        batch_rows.append({
            "batch_id": batch_id,
            "alpha_factor_rows": int(len(alpha_batch)),
            "alpha_candidate_count": int((alpha_batch.get("research_status") == "candidate").sum()) if "research_status" in alpha_batch else 0,
            "alpha_needs_falsification_count": int((alpha_batch.get("research_status") == "needs_falsification").sum()) if "research_status" in alpha_batch else 0,
            "alpha_5bps_survivors": int(alpha_batch.get("cost_survives_5bps", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not alpha_batch.empty else 0,
            "p1_contract_rows": int(len(p1_batch)),
            "p1_mean_theme_count": float(p1_batch["mean_theme_count"].mean()) if "mean_theme_count" in p1_batch else None,
            "p1_forced_chunk_memberships": int(p1_batch.get("forced_chunk_memberships", pd.Series(dtype=float)).fillna(0).sum()) if not p1_batch.empty else 0,
            "temporal_summary_rows": int(len(temporal_batch)),
            "temporal_edge_count": int(temporal_batch.get("edge_count", pd.Series(dtype=float)).fillna(0).sum()) if not temporal_batch.empty else 0,
            "mean_weighted_purity": float(purity_batch["weighted_purity"].mean()) if "weighted_purity" in purity_batch else None,
        })
    batch_summary = pd.DataFrame(batch_rows)
    atomic_write_frame(batch_summary, output / "campaign_batch_summary.csv")

    payload = {
        "campaign_id": "three_batch_33day",
        "start_date": start_date,
        "end_date": end_date,
        "expected_date_count": expected_date_count,
        "observed_dates": sorted(all_dates),
        "observed_date_count": len(all_dates),
        "source_count": len(source_rows),
        "sources": source_rows,
        "complete": True,
        "batch_summary": batch_rows,
    }
    atomic_write_json(output / "summary.json", payload)
    lines = [
        "# GraphAlphaLab three-batch 33-day campaign",
        "",
        f"- Date range: {start_date} to {end_date}",
        f"- Trading dates: {len(all_dates)} / {expected_date_count}",
        f"- Governed source bundles: {len(source_rows)}",
        "",
        "## Batch overview",
        "",
    ]
    for row in batch_rows:
        lines.extend([
            f"### {row['batch_id']}",
            f"- Alpha factor rows: {row['alpha_factor_rows']}",
            f"- Candidates / needs falsification: {row['alpha_candidate_count']} / {row['alpha_needs_falsification_count']}",
            f"- 5 bps survivors: {row['alpha_5bps_survivors']}",
            f"- P1 layer rows: {row['p1_contract_rows']}",
            f"- Mean P1 theme count: {row['p1_mean_theme_count']}",
            f"- Temporal rows / edges: {row['temporal_summary_rows']} / {row['temporal_edge_count']}",
            f"- Mean weighted purity: {row['mean_weighted_purity']}",
            "",
        ])
    lines.extend([
        "## Interpretation boundary",
        "",
        "Interaction Alpha and P1 structure are reported separately. Similarity P1 purity and temporal stability are diagnostics, not future-return labels and not clustering targets.",
        "",
        "The campaign bundle was built only from compact governed child reports and did not reread the large GFF partitions.",
        "",
    ])
    atomic_write_text(output / "REPORT.md", "\n".join(lines))
    atomic_write_json(success, {"campaign_id": "three_batch_33day", "complete": True})
    return output
