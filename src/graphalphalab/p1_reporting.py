from __future__ import annotations

from dataclasses import dataclass
import gc
import json
from pathlib import Path
from typing import Iterable, Any

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from .governance import (
    ResourceBudget,
    atomic_write_frame,
    atomic_write_json,
    atomic_write_text,
    file_record,
    implementation_manifest,
    sha256_file,
    sha256_json,
)
from .metadata import normalize_metadata
from .purity import PurityResult, evaluate_theme_purity


@dataclass
class P1ReportResult:
    partition_summary: pd.DataFrame
    snapshot_structure: pd.DataFrame
    layer_summary: pd.DataFrame
    temporal_summary: pd.DataFrame
    relation_summary: pd.DataFrame
    range_temporal_summary: pd.DataFrame
    purity: PurityResult | None
    governance: dict[str, object]


def _schema_hash(schema: pa.Schema) -> str:
    return sha256_json([(field.name, str(field.type), field.nullable) for field in schema])


def validate_gff_partition(path: str | Path) -> dict[str, Any]:
    root = Path(path).expanduser().resolve()
    manifest_path = root / "manifest.json"
    success_path = root / "_SUCCESS"
    if not manifest_path.exists() or not success_path.exists():
        raise ValueError(f"Incomplete GFF partition: {root}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for record in manifest.get("files", []):
        file_path = root / str(record["path"])
        if not file_path.exists():
            raise ValueError(f"Missing GFF output file: {file_path}")
        if int(file_path.stat().st_size) != int(record["bytes"]):
            raise ValueError(f"GFF bytes mismatch: {file_path}")
        if sha256_file(file_path) != str(record["sha256"]):
            raise ValueError(f"GFF SHA mismatch: {file_path}")
        parquet = pq.ParquetFile(file_path)
        if int(parquet.metadata.num_rows) != int(record["rows"]):
            raise ValueError(f"GFF row-count mismatch: {file_path}")
        if _schema_hash(parquet.schema_arrow) != str(record["schema_hash"]):
            raise ValueError(f"GFF schema mismatch: {file_path}")
    return manifest


def _read_columns(path: Path, columns: Iterable[str]) -> pd.DataFrame:
    parquet = pq.ParquetFile(path)
    selected = [column for column in columns if column in parquet.schema_arrow.names]
    if not selected:
        return pd.DataFrame()
    return parquet.read(columns=selected).to_pandas()


def _date_in_range(value: str, start_date: str | None, end_date: str | None) -> bool:
    return (start_date is None or value >= start_date) and (end_date is None or value <= end_date)


def discover_p1_partitions(
    root: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[Path]:
    base = Path(root).expanduser().resolve()
    partitions: list[Path] = []
    for memberships in sorted(base.rglob("memberships.parquet")):
        partition = memberships.parent
        trade_date = ""
        manifest_path = partition / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                trade_date = str(manifest.get("trade_date") or manifest.get("date") or "")
            except Exception:
                trade_date = ""
        if not trade_date:
            try:
                parquet = pq.ParquetFile(memberships)
                if "trade_date" not in parquet.schema_arrow.names or parquet.metadata.num_rows == 0:
                    continue
                dates = parquet.read_row_group(0, columns=["trade_date"]).column(0).to_pylist()
                trade_date = str(next((item for item in dates if item is not None), ""))
            except Exception:
                continue
        if trade_date and _date_in_range(trade_date, start_date, end_date):
            partitions.append(partition)
    return partitions


def _aggregate_purity(detail: pd.DataFrame, agreement: pd.DataFrame, profile: dict[str, object]) -> PurityResult:
    if detail.empty:
        summary = pd.DataFrame()
    else:
        rows: list[dict[str, object]] = []
        for dimension, group in detail.groupby("dimension", observed=True):
            weights = group["covered_member_count"].fillna(0).to_numpy(dtype=float)
            values = group["purity_weighted"].to_numpy(dtype=float)
            valid = np.isfinite(values) & (weights > 0)
            rows.append({
                "dimension": dimension,
                "theme_count": int(len(group)),
                "mean_coverage": float(group["coverage"].mean()),
                "weighted_purity": float(np.average(values[valid], weights=weights[valid])) if valid.any() else np.nan,
                "median_purity": float(group["purity_weighted"].median()),
                "p10_purity": float(group["purity_weighted"].quantile(0.10)),
                "p90_purity": float(group["purity_weighted"].quantile(0.90)),
                "mean_normalized_entropy": float(group["normalized_entropy"].mean()),
                "mean_dominant_lift": float(group["dominant_lift"].replace([np.inf, -np.inf], np.nan).mean()),
            })
        summary = pd.DataFrame(rows)
    return PurityResult(detail, summary, agreement, profile)


def _snapshot_structure(memberships: pd.DataFrame) -> pd.DataFrame:
    if memberships.empty:
        return pd.DataFrame()
    required = ["trade_date", "decision_time", "layer_id", "scale_minutes"]
    keys = [column for column in required if column in memberships.columns]
    rows: list[dict[str, object]] = []
    for values, group in memberships.groupby(keys, observed=True, dropna=False):
        base = dict(zip(keys, values if isinstance(values, tuple) else (values,)))
        canonical = group
        if "canonical_eligible" in group.columns:
            canonical = group[group["canonical_eligible"].fillna(False)]
        sizes = canonical.groupby("theme_id", observed=True)["symbol_id"].nunique() if not canonical.empty else pd.Series(dtype=float)
        split = group.get("split_origin", pd.Series(index=group.index, dtype=str)).fillna("").astype(str)
        rows.append({
            **base,
            "membership_rows": int(len(group)),
            "canonical_membership_rows": int(len(canonical)),
            "noncanonical_membership_rows": int(len(group) - len(canonical)),
            "unique_symbols": int(canonical["symbol_id"].nunique()) if not canonical.empty else 0,
            "theme_count": int(len(sizes)),
            "mean_theme_size": float(sizes.mean()) if len(sizes) else np.nan,
            "median_theme_size": float(sizes.median()) if len(sizes) else np.nan,
            "p90_theme_size": float(sizes.quantile(0.90)) if len(sizes) else np.nan,
            "max_theme_size": int(sizes.max()) if len(sizes) else 0,
            "forced_chunk_memberships": int(split.str.contains("forced", case=False, regex=False).sum()),
            "max_tree_depth": int(pd.to_numeric(group.get("tree_depth"), errors="coerce").max()) if "tree_depth" in group and group["tree_depth"].notna().any() else 0,
        })
    return pd.DataFrame(rows)


def _tree_snapshot_summary(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = _read_columns(path, [
        "trade_date", "decision_time", "layer_id", "scale_minutes", "theme_id", "is_leaf",
        "is_micro_theme", "is_oversized", "split_origin", "canonical_eligible", "depth",
    ])
    if frame.empty:
        return frame
    keys = [column for column in ("trade_date", "decision_time", "layer_id", "scale_minutes") if column in frame]
    rows: list[dict[str, object]] = []
    for values, group in frame.groupby(keys, observed=True, dropna=False):
        base = dict(zip(keys, values if isinstance(values, tuple) else (values,)))
        split = group.get("split_origin", pd.Series(index=group.index, dtype=str)).fillna("").astype(str)
        rows.append({
            **base,
            "tree_node_count": int(len(group)),
            "leaf_count": int(group.get("is_leaf", False).fillna(False).sum()) if "is_leaf" in group else 0,
            "micro_theme_count": int(group.get("is_micro_theme", False).fillna(False).sum()) if "is_micro_theme" in group else 0,
            "oversized_theme_count": int(group.get("is_oversized", False).fillna(False).sum()) if "is_oversized" in group else 0,
            "canonical_tree_node_count": int(group.get("canonical_eligible", False).fillna(False).sum()) if "canonical_eligible" in group else 0,
            "forced_chunk_count": int(split.str.contains("forced", case=False, regex=False).sum()),
            "tree_max_depth": int(pd.to_numeric(group.get("depth"), errors="coerce").max()) if "depth" in group and group["depth"].notna().any() else 0,
        })
    return pd.DataFrame(rows)


def _event_summary(frame: pd.DataFrame, *, source: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    keys = [column for column in ("trade_date", "src_trade_date", "dst_trade_date", "layer_id", "scale_minutes", "event_type") if column in frame]
    if not keys:
        return pd.DataFrame()
    result = frame.groupby(keys, observed=True, dropna=False).size().rename("edge_count").reset_index()
    result["source"] = source
    for metric in ("overlap", "jaccard", "containment"):
        if metric in frame.columns:
            means = frame.groupby(keys, observed=True, dropna=False)[metric].mean().rename(f"mean_{metric}").reset_index()
            result = result.merge(means, on=keys, how="left")
    return result


def evaluate_p1_streaming(
    p1_root: str | Path,
    *,
    batch_id: str,
    metadata: pd.DataFrame | None = None,
    dimensions: Iterable[str] | None = None,
    metadata_id: str | None = None,
    membership_id: str | None = None,
    range_root: str | Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    expected_contracts: int | None = None,
    expected_dates: int | None = None,
    require_consensus: bool = False,
    allow_partial: bool = False,
    resource_budget: ResourceBudget | None = None,
) -> P1ReportResult:
    budget = resource_budget or ResourceBudget()
    budget.validate()
    partitions = discover_p1_partitions(p1_root, start_date=start_date, end_date=end_date)
    if not partitions:
        raise FileNotFoundError(f"No governed P1 memberships found under {Path(p1_root).resolve()}")

    metadata_norm = None
    metadata_profile: dict[str, object] | None = None
    if metadata is not None:
        metadata_norm, profile = normalize_metadata(metadata, id_column=metadata_id, dimensions=dimensions)
        metadata_profile = profile.as_dict()

    partition_rows: list[dict[str, object]] = []
    structure_frames: list[pd.DataFrame] = []
    temporal_frames: list[pd.DataFrame] = []
    relation_frames: list[pd.DataFrame] = []
    purity_detail: list[pd.DataFrame] = []
    purity_agreement: list[pd.DataFrame] = []
    observed_dates: set[str] = set()
    observed_contracts: set[tuple[str, int]] = set()
    consensus_seen = False
    process = psutil.Process()
    peak_rss = int(process.memory_info().rss)

    for partition in partitions:
        manifest = validate_gff_partition(partition)
        memberships_path = partition / "memberships.parquet"
        memberships = _read_columns(memberships_path, [
            "trade_date", "decision_time", "layer_id", "scale_minutes", "theme_id", "symbol_id",
            "membership_weight", "canonical_eligible", "theme_size", "tree_depth", "split_origin",
        ])
        trade_date = str(manifest.get("trade_date") or manifest.get("date") or (memberships["trade_date"].iloc[0] if not memberships.empty else ""))
        layer_id = str(manifest.get("layer_id") or manifest.get("layer") or (memberships["layer_id"].iloc[0] if not memberships.empty else ""))
        scale_minutes = int(manifest.get("scale_minutes") if manifest.get("scale_minutes") is not None else manifest.get("scale") if manifest.get("scale") is not None else (memberships["scale_minutes"].iloc[0] if not memberships.empty else 0))
        observed_dates.add(trade_date)
        observed_contracts.add((layer_id, scale_minutes))
        consensus_seen = consensus_seen or layer_id == "similarity_consensus"
        if not memberships.empty:
            memberships["batch_id"] = batch_id

        snapshot = _snapshot_structure(memberships)
        tree = _tree_snapshot_summary(partition / "theme_tree_nodes.parquet")
        if not tree.empty:
            merge_keys = [column for column in ("trade_date", "decision_time", "layer_id", "scale_minutes") if column in snapshot and column in tree]
            snapshot = snapshot.merge(tree, on=merge_keys, how="left")
        structure_frames.append(snapshot)

        temporal = _read_columns(partition / "temporal_edges.parquet", [
            "trade_date", "decision_time", "layer_id", "scale_minutes", "src_theme_id", "dst_theme_id",
            "overlap", "jaccard", "containment", "event_type",
        ]) if (partition / "temporal_edges.parquet").exists() else pd.DataFrame()
        event = _event_summary(temporal, source="within_day")
        if not event.empty:
            temporal_frames.append(event)

        relations = _read_columns(partition / "theme_relations.parquet", [
            "trade_date", "decision_time", "layer_id", "scale_minutes", "relation_edge_count",
            "relation_weight_sum", "relation_abs_weight_sum",
        ]) if (partition / "theme_relations.parquet").exists() else pd.DataFrame()
        if not relations.empty:
            keys = [column for column in ("trade_date", "layer_id", "scale_minutes") if column in relations]
            relation_frames.append(relations.groupby(keys, observed=True, dropna=False).agg(
                relation_rows=("relation_edge_count", "size"),
                relation_edge_count=("relation_edge_count", "sum"),
                relation_weight_sum=("relation_weight_sum", "sum"),
                relation_abs_weight_sum=("relation_abs_weight_sum", "sum"),
            ).reset_index())

        if metadata_norm is not None and metadata_profile is not None:
            canonical = memberships
            if "canonical_eligible" in canonical:
                canonical = canonical[canonical["canonical_eligible"].fillna(False)].copy()
            if not canonical.empty:
                purity = evaluate_theme_purity(
                    canonical,
                    metadata_norm,
                    dimensions=metadata_profile["dimensions"],
                    membership_id=membership_id,
                    metadata_id=metadata_profile["id_column"],
                )
                purity_detail.append(purity.theme_dimension)
                purity_agreement.append(purity.snapshot_agreement)

        qa_path = partition / "qa.json"
        qa = json.loads(qa_path.read_text(encoding="utf-8")) if qa_path.exists() else {}
        partition_rows.append({
            "trade_date": trade_date,
            "layer_id": layer_id,
            "scale_minutes": scale_minutes,
            "partition": str(partition),
            "manifest_sha256": sha256_file(partition / "manifest.json"),
            "contract_hash": manifest.get("contract_hash"),
            "research_only": bool(manifest.get("research_only", True)),
            "production_identity_ready": bool(manifest.get("production_identity_ready", False)),
            "membership_rows": int(len(memberships)),
            "qa_status": qa.get("status"),
            "pit_violations": int(qa.get("pit_violations", 0) or 0),
        })
        peak_rss = max(peak_rss, int(process.memory_info().rss))
        del memberships, snapshot, tree, temporal, relations
        gc.collect()

    partition_summary = pd.DataFrame(partition_rows)
    structure = pd.concat(structure_frames, ignore_index=True) if structure_frames else pd.DataFrame()
    if structure.empty:
        layer_summary = pd.DataFrame()
    else:
        layer_summary = structure.groupby(["layer_id", "scale_minutes"], observed=True, dropna=False).agg(
            date_count=("trade_date", "nunique"),
            snapshot_count=("decision_time", "count"),
            mean_theme_count=("theme_count", "mean"),
            median_theme_count=("theme_count", "median"),
            mean_theme_size=("mean_theme_size", "mean"),
            p90_max_theme_size=("max_theme_size", lambda values: values.quantile(0.90)),
            mean_unique_symbols=("unique_symbols", "mean"),
            noncanonical_membership_rows=("noncanonical_membership_rows", "sum"),
            forced_chunk_memberships=("forced_chunk_memberships", "sum"),
            max_tree_depth=("max_tree_depth", "max"),
        ).reset_index()
    temporal_summary = pd.concat(temporal_frames, ignore_index=True) if temporal_frames else pd.DataFrame()
    relation_summary = pd.concat(relation_frames, ignore_index=True) if relation_frames else pd.DataFrame()

    range_frames: list[pd.DataFrame] = []
    if range_root is not None:
        for path in sorted(Path(range_root).expanduser().resolve().rglob("temporal_links.parquet")):
            frame = _read_columns(path, [
                "src_trade_date", "dst_trade_date", "src_decision_time", "dst_decision_time", "layer_id",
                "scale_minutes", "src_theme_id", "dst_theme_id", "overlap", "jaccard", "containment", "event_type",
            ])
            if not frame.empty:
                summary = _event_summary(frame, source="range")
                summary["path"] = str(path)
                range_frames.append(summary)
    range_temporal = pd.concat(range_frames, ignore_index=True) if range_frames else pd.DataFrame()

    detail = pd.concat(purity_detail, ignore_index=True) if purity_detail else pd.DataFrame()
    agreement = pd.concat(purity_agreement, ignore_index=True) if purity_agreement else pd.DataFrame()
    purity_result = _aggregate_purity(detail, agreement, metadata_profile or {}) if metadata_profile is not None else None

    complete = True
    errors: list[str] = []
    if expected_contracts is not None and len(observed_contracts) != expected_contracts:
        complete = False
        errors.append(f"expected {expected_contracts} contracts, observed {len(observed_contracts)}")
    if expected_dates is not None and len(observed_dates) != expected_dates:
        complete = False
        errors.append(f"expected {expected_dates} dates, observed {len(observed_dates)}")
    if require_consensus and not consensus_seen:
        complete = False
        errors.append("similarity_consensus P1 was not observed")
    if int(partition_summary.get("pit_violations", pd.Series(dtype=int)).sum()) != 0:
        complete = False
        errors.append("one or more P1 partitions reported PIT violations")
    if not complete and not allow_partial:
        raise ValueError("P1 report contract failed: " + "; ".join(errors))

    governance = {
        "batch_id": batch_id,
        "start_date": start_date,
        "end_date": end_date,
        "expected_dates": expected_dates,
        "observed_dates": sorted(observed_dates),
        "observed_date_count": len(observed_dates),
        "expected_contracts": expected_contracts,
        "observed_contracts": [f"{layer}@{scale}m" for layer, scale in sorted(observed_contracts)],
        "observed_contract_count": len(observed_contracts),
        "consensus_seen": consensus_seen,
        "complete": complete,
        "partial": not complete,
        "errors": errors,
        "partition_count": len(partition_summary),
        "peak_rss_bytes": peak_rss,
        "resource_budget": budget.as_dict(),
    }
    return P1ReportResult(
        partition_summary,
        structure,
        layer_summary,
        temporal_summary,
        relation_summary,
        range_temporal,
        purity_result,
        governance,
    )


def write_p1_report_bundle(
    output_root: str | Path,
    *,
    batch_id: str,
    result: P1ReportResult,
    inputs: Iterable[dict[str, object]],
    resource_budget: ResourceBudget,
    run_manifest: dict[str, object] | None = None,
) -> Path:
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    success = output / "_SUCCESS"
    success.unlink(missing_ok=True)
    atomic_write_frame(result.partition_summary, output / "p1_partition_summary.csv")
    atomic_write_frame(result.snapshot_structure, output / "p1_snapshot_structure.csv")
    atomic_write_frame(result.layer_summary, output / "p1_layer_summary.csv")
    atomic_write_frame(result.temporal_summary, output / "p1_temporal_summary.csv")
    atomic_write_frame(result.relation_summary, output / "p1_relation_summary.csv")
    atomic_write_frame(result.range_temporal_summary, output / "p1_range_temporal_summary.csv")
    if result.purity is not None:
        atomic_write_frame(result.purity.theme_dimension, output / "theme_purity.csv")
        atomic_write_frame(result.purity.dimension_summary, output / "purity_dimension_summary.csv")
        atomic_write_frame(result.purity.snapshot_agreement, output / "purity_snapshot_agreement.csv")
        atomic_write_json(output / "metadata_profile.json", result.purity.metadata_profile)

    manifest = run_manifest or implementation_manifest(
        operation="p1_report",
        parameters=result.governance,
        inputs=inputs,
        resource_budget=resource_budget,
    )
    atomic_write_json(output / "run_manifest.json", manifest)
    summary = {
        "batch_id": batch_id,
        "report_type": "p1_structure_temporal_purity",
        "governance": result.governance,
        "run_contract_hash": manifest["contract_hash"],
        "layer_summary": result.layer_summary.to_dict("records"),
        "purity_dimensions": result.purity.dimension_summary.to_dict("records") if result.purity is not None else [],
    }
    atomic_write_json(output / "summary.json", summary)

    lines = [
        f"# GraphAlphaLab P1 report: {batch_id}",
        "",
        "## Completion and governance",
        "",
        f"- Dates: {result.governance['observed_date_count']} / {result.governance['expected_dates']}",
        f"- Contracts: {result.governance['observed_contract_count']} / {result.governance['expected_contracts']}",
        f"- Consensus P1 observed: {result.governance['consensus_seen']}",
        f"- Governed partitions: {result.governance['partition_count']}",
        f"- Peak RSS: {result.governance['peak_rss_bytes']}",
        f"- Complete: {result.governance['complete']}",
        "",
        "## Layer-local and consensus structure",
        "",
    ]
    for row in result.layer_summary.to_dict("records"):
        lines.extend([
            f"### {row.get('layer_id')}@{row.get('scale_minutes')}m",
            f"- Dates / snapshots: {row.get('date_count')} / {row.get('snapshot_count')}",
            f"- Mean themes / mean size: {row.get('mean_theme_count')} / {row.get('mean_theme_size')}",
            f"- P90 maximum theme size: {row.get('p90_max_theme_size')}",
            f"- Forced/non-canonical memberships: {row.get('forced_chunk_memberships')} / {row.get('noncanonical_membership_rows')}",
            "",
        ])
    lines.extend([
        "## Temporal evidence",
        "",
        f"- Within-day temporal summary rows: {len(result.temporal_summary)}",
        f"- Range temporal summary rows: {len(result.range_temporal_summary)}",
        "",
        "P1 memberships remain GFF outputs. Metadata purity is evaluation-only and never changes clustering.",
        "",
    ])
    atomic_write_text(output / "REPORT.md", "\n".join(lines))
    atomic_write_json(success, {"batch_id": batch_id, "complete": bool(result.governance["complete"])})
    return output
