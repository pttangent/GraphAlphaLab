from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from graph_alpha_lab.inventory import discover_partitions


def test_empty_graph_is_governance_failure(tmp_path: Path):
    part = tmp_path / "p0/date=2026-07-01/layer=x/scale=15/variant=v"
    p1 = tmp_path / "p1/date=2026-07-01/layer=x/scale=15/variant=v"
    part.mkdir(parents=True)
    p1.mkdir(parents=True)
    (part / "_SUCCESS").write_text("")
    (p1 / "_SUCCESS").write_text("")
    pq.write_table(
        pa.table({"decision_time": [1], "symbol_id": [1], "node_score": [1.0]}),
        part / "node_projection.parquet",
    )
    pq.write_table(
        pa.table({
            "decision_time": pa.array([], type=pa.int64()),
            "src_symbol_id": pa.array([], type=pa.int64()),
            "dst_symbol_id": pa.array([], type=pa.int64()),
            "edge_weight": pa.array([], type=pa.float64()),
        }),
        part / "edges.parquet",
    )
    pq.write_table(
        pa.table({"decision_time": [1], "symbol_id": [1], "theme_id": ["t"], "core_score": [1.0]}),
        p1 / "memberships.parquet",
    )
    out = discover_partitions(tmp_path, {"2026-07-01"}, "v")
    assert "eligible_nodes_but_empty_graph" in out.iloc[0].governance_status
