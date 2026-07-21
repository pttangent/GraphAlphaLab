from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from graphalphalab.campaign import CampaignSource, build_campaign_report
from graphalphalab.governance import ResourceBudget, sha256_file
from graphalphalab.p1_reporting import (
    _schema_hash,
    evaluate_p1_streaming,
    write_p1_report_bundle,
)


def _write_governed_p1(root: Path, *, trade_date: str, layer_id: str, scale: int) -> Path:
    partition = root / "p1" / f"date={trade_date}" / f"layer={layer_id}" / f"scale={scale}"
    partition.mkdir(parents=True)
    decision0 = pd.Timestamp(f"{trade_date} 14:30:00", tz="UTC")
    decision1 = pd.Timestamp(f"{trade_date} 14:35:00", tz="UTC")
    memberships = pd.DataFrame([
        {"trade_date": trade_date, "decision_time": decision0, "layer_id": layer_id, "scale_minutes": scale, "theme_id": "t0", "symbol_id": 1, "membership_weight": 1.0, "canonical_eligible": True, "theme_size": 2, "tree_depth": 0, "split_origin": "leiden"},
        {"trade_date": trade_date, "decision_time": decision0, "layer_id": layer_id, "scale_minutes": scale, "theme_id": "t0", "symbol_id": 2, "membership_weight": 1.0, "canonical_eligible": True, "theme_size": 2, "tree_depth": 0, "split_origin": "leiden"},
        {"trade_date": trade_date, "decision_time": decision1, "layer_id": layer_id, "scale_minutes": scale, "theme_id": "t1", "symbol_id": 1, "membership_weight": 1.0, "canonical_eligible": True, "theme_size": 2, "tree_depth": 0, "split_origin": "leiden"},
        {"trade_date": trade_date, "decision_time": decision1, "layer_id": layer_id, "scale_minutes": scale, "theme_id": "t1", "symbol_id": 2, "membership_weight": 1.0, "canonical_eligible": True, "theme_size": 2, "tree_depth": 0, "split_origin": "leiden"},
    ])
    temporal = pd.DataFrame([{
        "trade_date": trade_date, "decision_time": decision1, "layer_id": layer_id, "scale_minutes": scale,
        "src_theme_id": "t0", "dst_theme_id": "t1", "overlap": 2, "jaccard": 1.0,
        "containment": 1.0, "event_type": "continue",
    }])
    tree = memberships[["trade_date", "decision_time", "layer_id", "scale_minutes", "theme_id"]].drop_duplicates()
    tree["is_leaf"] = True
    tree["is_micro_theme"] = False
    tree["is_oversized"] = False
    tree["split_origin"] = "leiden"
    tree["canonical_eligible"] = True
    tree["depth"] = 0
    relations = pd.DataFrame(columns=["trade_date", "decision_time", "layer_id", "scale_minutes", "relation_edge_count", "relation_weight_sum", "relation_abs_weight_sum"])
    frames = {
        "memberships.parquet": memberships,
        "temporal_edges.parquet": temporal,
        "theme_tree_nodes.parquet": tree,
        "theme_relations.parquet": relations,
    }
    records = []
    for name, frame in frames.items():
        path = partition / name
        frame.to_parquet(path, index=False)
        parquet = pq.ParquetFile(path)
        records.append({
            "path": name,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "rows": parquet.metadata.num_rows,
            "row_groups": parquet.metadata.num_row_groups,
            "schema_hash": _schema_hash(parquet.schema_arrow),
            "columns": parquet.schema_arrow.names,
        })
    (partition / "manifest.json").write_text(json.dumps({
        "trade_date": trade_date,
        "layer_id": layer_id,
        "scale_minutes": scale,
        "contract_hash": "contract",
        "research_only": True,
        "production_identity_ready": False,
        "files": records,
    }), encoding="utf-8")
    (partition / "qa.json").write_text(json.dumps({"status": "pass", "pit_violations": 0}), encoding="utf-8")
    (partition / "_SUCCESS").write_text("", encoding="utf-8")
    return partition


def test_p1_report_streams_governed_partition_and_publishes_bundle(tmp_path: Path) -> None:
    p1_root = tmp_path / "gff"
    _write_governed_p1(p1_root, trade_date="2026-06-01", layer_id="layer_a", scale=30)
    metadata = pd.DataFrame({"symbol_id": [1, 2], "sector_code": ["A", "A"]})
    result = evaluate_p1_streaming(
        p1_root,
        batch_id="implemented27",
        metadata=metadata,
        dimensions=["sector_code"],
        start_date="2026-06-01",
        end_date="2026-06-01",
        expected_contracts=1,
        expected_dates=1,
        resource_budget=ResourceBudget(memory_limit_gb=1, threads=1),
    )
    assert result.governance["complete"] is True
    assert result.layer_summary.iloc[0]["snapshot_count"] == 2
    assert result.temporal_summary.iloc[0]["edge_count"] == 1
    output = write_p1_report_bundle(
        tmp_path / "report",
        batch_id="implemented27",
        result=result,
        inputs=[],
        resource_budget=ResourceBudget(memory_limit_gb=1, threads=1),
    )
    assert (output / "_SUCCESS").exists()
    assert (output / "theme_purity.csv").exists()


def _fake_report(root: Path, *, report_type: str, batch_id: str, trade_date: str) -> Path:
    root.mkdir(parents=True)
    (root / "summary.json").write_text(json.dumps({"batch_id": batch_id, "run_contract_hash": "hash"}), encoding="utf-8")
    (root / "_SUCCESS").write_text(json.dumps({"complete": True}), encoding="utf-8")
    if report_type == "alpha":
        pd.DataFrame({"trade_date": [trade_date], "mean_spearman_ic": [0.01]}).to_csv(root / "daily_ic.csv", index=False)
        pd.DataFrame({"research_status": ["needs_falsification"], "cost_survives_5bps": [False]}).to_csv(root / "alpha_metrics.csv", index=False)
    else:
        pd.DataFrame({"trade_date": [trade_date], "layer_id": ["x"], "scale_minutes": [30]}).to_csv(root / "p1_partition_summary.csv", index=False)
        pd.DataFrame({"layer_id": ["x"], "scale_minutes": [30], "mean_theme_count": [2.0]}).to_csv(root / "p1_layer_summary.csv", index=False)
        pd.DataFrame({"event_type": ["continue"], "edge_count": [1]}).to_csv(root / "p1_temporal_summary.csv", index=False)
    return root


def test_campaign_requires_and_merges_all_three_batches(tmp_path: Path) -> None:
    date = "2026-06-01"
    sources = [
        CampaignSource("implemented27", "alpha", _fake_report(tmp_path / "ig-a", report_type="alpha", batch_id="implemented27", trade_date=date)),
        CampaignSource("implemented27", "p1", _fake_report(tmp_path / "ig-p1", report_type="p1", batch_id="implemented27", trade_date=date)),
        CampaignSource("remaining14", "alpha", _fake_report(tmp_path / "rm-a", report_type="alpha", batch_id="remaining14", trade_date=date)),
        CampaignSource("remaining14", "p1", _fake_report(tmp_path / "rm-p1", report_type="p1", batch_id="remaining14", trade_date=date)),
        CampaignSource("similarity10", "p1", _fake_report(tmp_path / "sim-p1", report_type="p1", batch_id="similarity10", trade_date=date)),
    ]
    output = build_campaign_report(
        sources,
        tmp_path / "campaign",
        start_date=date,
        end_date=date,
        expected_date_count=1,
    )
    assert (output / "_SUCCESS").exists()
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["observed_date_count"] == 1
    assert len(summary["batch_summary"]) == 3
