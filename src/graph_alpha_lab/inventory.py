from __future__ import annotations

from pathlib import Path
import hashlib
import pandas as pd
import pyarrow.parquet as pq


def _rows(path: Path) -> int | None:
    return pq.ParquetFile(path).metadata.num_rows if path.exists() else None


def _sha(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def discover_partitions(warehouse_root: Path, dates: set[str], variant: str) -> pd.DataFrame:
    rows: list[dict] = []
    for success in (warehouse_root / "p0").glob("date=*/layer=*/scale=*/variant=*/_SUCCESS"):
        p0 = success.parent
        bits = {x.split("=", 1)[0]: x.split("=", 1)[1] for x in p0.parts if "=" in x}
        if bits.get("date") not in dates or bits.get("variant") != variant:
            continue
        day, layer, scale = bits["date"], bits["layer"], int(bits["scale"])
        p1 = warehouse_root / "p1" / f"date={day}" / f"layer={layer}" / f"scale={scale}" / f"variant={variant}"
        node, edges, memberships = p0 / "node_projection.parquet", p0 / "edges.parquet", p1 / "memberships.parquet"
        node_rows, edge_rows, membership_rows = _rows(node), _rows(edges), _rows(memberships)
        findings: list[str] = []
        if not node.exists() or not edges.exists(): findings.append("p0_success_missing_core_file")
        if node_rows and edge_rows == 0: findings.append("eligible_nodes_but_empty_graph")
        if (p1 / "_SUCCESS").exists() and not memberships.exists(): findings.append("p1_success_missing_memberships")
        rows.append({
            "trade_date": day, "layer": layer, "scale": scale, "variant": variant,
            "p0_success": True, "p1_success": (p1 / "_SUCCESS").exists(),
            "node_projection_exists": node.exists(), "edges_exists": edges.exists(), "memberships_exists": memberships.exists(),
            "node_rows": node_rows, "edge_rows": edge_rows, "membership_rows": membership_rows,
            "p0_manifest_hash": _sha(p0 / "manifest.json"), "p1_manifest_hash": _sha(p1 / "manifest.json"),
            "governance_status": "OK" if not findings else "|".join(findings),
            "p0_path": str(p0), "p1_path": str(p1),
        })
    out = pd.DataFrame(rows)
    return out.sort_values(["trade_date", "layer", "scale"]).reset_index(drop=True) if not out.empty else out
