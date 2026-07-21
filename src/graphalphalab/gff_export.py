from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

import duckdb

from .governance import (
    ResourceBudget,
    atomic_write_json,
    configure_duckdb,
    file_record,
    implementation_manifest,
    sha256_file,
)


@dataclass(frozen=True)
class ExportSummary:
    partition_count: int
    output_file_count: int
    output_rows: int
    edge_pit_violations: int
    variants: tuple[str, ...]
    output_root: str

    def as_dict(self) -> dict[str, object]:
        return {
            "partition_count": self.partition_count,
            "output_file_count": self.output_file_count,
            "output_rows": self.output_rows,
            "edge_pit_violations": self.edge_pit_violations,
            "variants": list(self.variants),
            "output_root": self.output_root,
        }


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _discover_partitions(root: Path) -> list[tuple[Path, Path]]:
    rows: list[tuple[Path, Path]] = []
    for edges in sorted(root.rglob("edges.parquet")):
        node = edges.with_name("node_projection.parquet")
        if node.exists():
            rows.append((edges, node))
    if not rows:
        raise FileNotFoundError(f"No GFF P0 edges/node_projection pairs found below {root}")
    return rows


def _partition_metadata(connection: duckdb.DuckDBPyConnection, edges: Path) -> dict[str, object]:
    row = connection.execute(
        f"""
        SELECT
          any_value(CAST(trade_date AS VARCHAR)) AS trade_date,
          any_value(CAST(layer_id AS VARCHAR)) AS layer_id,
          any_value(CAST(scale_minutes AS INTEGER)) AS scale_minutes,
          count(*) AS edge_rows,
          sum(CASE WHEN edge_available_time > decision_time THEN 1 ELSE 0 END) AS pit_violations
        FROM read_parquet('{_sql_path(edges)}', union_by_name=true)
        """
    ).fetchone()
    return {
        "trade_date": str(row[0]),
        "layer_id": str(row[1]),
        "scale_minutes": int(row[2]),
        "edge_rows": int(row[3]),
        "pit_violations": int(row[4] or 0),
    }


def export_gff_signals(
    p0_root: str | Path,
    output_root: str | Path,
    *,
    batch_id: str,
    variants: Iterable[str] = ("node_baseline", "graph_forward", "graph_reverse_placebo"),
    resource_budget: ResourceBudget = ResourceBudget(),
    expected_git_commit: str | None = None,
    require_clean: bool = False,
    force: bool = False,
) -> ExportSummary:
    root = Path(p0_root).expanduser().resolve()
    out_root = Path(output_root).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    requested = tuple(dict.fromkeys(str(value).strip() for value in variants if str(value).strip()))
    supported = {"node_baseline", "graph_forward", "graph_reverse_placebo"}
    unknown = sorted(set(requested) - supported)
    if unknown:
        raise ValueError(f"Unsupported signal variants: {unknown}; supported={sorted(supported)}")
    partitions = _discover_partitions(root)
    con = duckdb.connect()
    configure_duckdb(con, resource_budget)
    output_files: list[dict[str, object]] = []
    source_files: list[dict[str, object]] = []
    total_rows = 0
    total_pit = 0
    for edges, node in partitions:
        meta = _partition_metadata(con, edges)
        if meta["pit_violations"]:
            raise ValueError(
                f"GFF edge PIT violation in {edges}: {meta['pit_violations']} rows have edge_available_time > decision_time"
            )
        total_pit += int(meta["pit_violations"])
        trade_date = str(meta["trade_date"])
        layer = str(meta["layer_id"])
        scale = int(meta["scale_minutes"])
        source_files.extend([file_record(edges), file_record(node)])
        e = _sql_path(edges)
        n = _sql_path(node)
        for variant in requested:
            destination = (
                out_root
                / f"batch_id={batch_id}"
                / f"layer_id={layer}"
                / f"scale_minutes={scale}"
                / f"variant_id={variant}"
                / f"trade_date={trade_date}"
                / "data.parquet"
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists() and not force:
                output_files.append(file_record(destination))
                total_rows += int(
                    con.execute(f"SELECT count(*) FROM read_parquet('{_sql_path(destination)}')").fetchone()[0]
                )
                continue
            d = _sql_path(destination)
            if variant == "node_baseline":
                query = f"""
                SELECT
                  '{batch_id}'::VARCHAR AS batch_id,
                  layer_id || '__node_baseline' AS factor_id,
                  layer_id,
                  CAST(scale_minutes AS INTEGER) AS scale_minutes,
                  'node_baseline'::VARCHAR AS variant_id,
                  trade_date,
                  decision_time,
                  p0_snapshot_id,
                  symbol,
                  symbol_id,
                  security_entity_id,
                  node_score::DOUBLE AS score,
                  node_score::DOUBLE AS own_score,
                  0::BIGINT AS edge_count,
                  decision_time AS signal_available_time
                FROM read_parquet('{n}', union_by_name=true)
                WHERE node_score IS NOT NULL
                """
            elif variant == "graph_forward":
                query = f"""
                WITH e AS (
                  SELECT * FROM read_parquet('{e}', union_by_name=true)
                  WHERE edge_available_time <= decision_time AND edge_weight IS NOT NULL
                ), nodes AS (
                  SELECT * FROM read_parquet('{n}', union_by_name=true)
                )
                SELECT
                  '{batch_id}'::VARCHAR AS batch_id,
                  any_value(e.layer_id) || '__graph_forward' AS factor_id,
                  any_value(e.layer_id) AS layer_id,
                  CAST(any_value(e.scale_minutes) AS INTEGER) AS scale_minutes,
                  'graph_forward'::VARCHAR AS variant_id,
                  any_value(e.trade_date) AS trade_date,
                  e.decision_time,
                  any_value(e.p0_snapshot_id) AS p0_snapshot_id,
                  any_value(dst.symbol) AS symbol,
                  e.dst_symbol_id AS symbol_id,
                  any_value(dst.security_entity_id) AS security_entity_id,
                  sum(e.edge_weight * src.node_score) / nullif(sum(abs(e.edge_weight)), 0) AS score,
                  any_value(dst.node_score) AS own_score,
                  count(*)::BIGINT AS edge_count,
                  max(e.edge_available_time) AS signal_available_time
                FROM e
                JOIN nodes src
                  ON e.decision_time=src.decision_time AND e.src_symbol_id=src.symbol_id
                JOIN nodes dst
                  ON e.decision_time=dst.decision_time AND e.dst_symbol_id=dst.symbol_id
                WHERE src.node_score IS NOT NULL
                GROUP BY e.decision_time, e.dst_symbol_id
                """
            else:
                query = f"""
                WITH e AS (
                  SELECT * FROM read_parquet('{e}', union_by_name=true)
                  WHERE edge_available_time <= decision_time AND edge_weight IS NOT NULL
                ), nodes AS (
                  SELECT * FROM read_parquet('{n}', union_by_name=true)
                )
                SELECT
                  '{batch_id}'::VARCHAR AS batch_id,
                  any_value(e.layer_id) || '__graph_reverse_placebo' AS factor_id,
                  any_value(e.layer_id) AS layer_id,
                  CAST(any_value(e.scale_minutes) AS INTEGER) AS scale_minutes,
                  'graph_reverse_placebo'::VARCHAR AS variant_id,
                  any_value(e.trade_date) AS trade_date,
                  e.decision_time,
                  any_value(e.p0_snapshot_id) AS p0_snapshot_id,
                  any_value(src.symbol) AS symbol,
                  e.src_symbol_id AS symbol_id,
                  any_value(src.security_entity_id) AS security_entity_id,
                  sum(e.edge_weight * dst.node_score) / nullif(sum(abs(e.edge_weight)), 0) AS score,
                  any_value(src.node_score) AS own_score,
                  count(*)::BIGINT AS edge_count,
                  max(e.edge_available_time) AS signal_available_time
                FROM e
                JOIN nodes src
                  ON e.decision_time=src.decision_time AND e.src_symbol_id=src.symbol_id
                JOIN nodes dst
                  ON e.decision_time=dst.decision_time AND e.dst_symbol_id=dst.symbol_id
                WHERE dst.node_score IS NOT NULL
                GROUP BY e.decision_time, e.src_symbol_id
                """
            temporary = destination.with_name(f".{destination.name}.part")
            if temporary.exists():
                temporary.unlink()
            con.execute(
                f"COPY ({query}) TO '{_sql_path(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
            )
            temporary.replace(destination)
            rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{d}')").fetchone()[0])
            total_rows += rows
            record = file_record(destination)
            record.update(
                {
                    "batch_id": batch_id,
                    "layer_id": layer,
                    "scale_minutes": scale,
                    "trade_date": trade_date,
                    "variant_id": variant,
                    "rows": rows,
                }
            )
            output_files.append(record)
    parameters = {
        "batch_id": batch_id,
        "variants": list(requested),
        "p0_root": str(root),
        "output_root": str(out_root),
        "partition_count": len(partitions),
    }
    manifest = implementation_manifest(
        operation="export_gff_signals",
        parameters=parameters,
        inputs=source_files,
        resource_budget=resource_budget,
    )
    if expected_git_commit and manifest.get("git_commit") != expected_git_commit:
        raise ValueError(
            f"GAL Git commit mismatch: expected {expected_git_commit}, got {manifest.get('git_commit')}"
        )
    if require_clean and manifest.get("git_dirty") is not False:
        raise ValueError(f"Governed export requires clean checkout; git_dirty={manifest.get('git_dirty')!r}")
    manifest.update(
        {
            "output_files": output_files,
            "output_rows": total_rows,
            "edge_pit_violations": total_pit,
            "graph_aggregated_variants": [value for value in requested if value != "node_baseline"],
            "node_baseline_is_network_alpha": False,
        }
    )
    atomic_write_json(out_root / "export_manifest.json", manifest)
    atomic_write_json(out_root / "_SUCCESS", {"contract_hash": manifest["contract_hash"]})
    return ExportSummary(len(partitions), len(output_files), total_rows, total_pit, requested, str(out_root))
