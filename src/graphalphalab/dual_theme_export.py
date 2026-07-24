from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb

from .dual_theme_common import (
    DEFAULT_SCOPES,
    DEFAULT_THEME_FAMILIES,
    DUAL_THEME_BATCH_ID,
    DUAL_THEME_EXPORT_VERSION,
    SUPPORTED_VARIANTS,
    DualThemeExportSummary,
    _sql_path,
    discover_dual_theme_partitions,
)
from .dual_theme_sql import _inter_query, _partition_metadata, _stock_query
from .governance import (
    ResourceBudget,
    atomic_write_json,
    configure_duckdb,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
)


def export_dual_theme_signals(
    campaign_root: str | Path,
    output_root: str | Path,
    *,
    batch_id: str = DUAL_THEME_BATCH_ID,
    theme_families: Iterable[str] = DEFAULT_THEME_FAMILIES,
    scopes: Iterable[str] = DEFAULT_SCOPES,
    variants: Iterable[str] = SUPPORTED_VARIANTS,
    resource_budget: ResourceBudget = ResourceBudget(),
    expected_git_commit: str | None = None,
    require_clean: bool = False,
    force: bool = False,
) -> DualThemeExportSummary:
    campaign = Path(campaign_root).expanduser().resolve()
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    selected_families = tuple(dict.fromkeys(str(value).strip() for value in theme_families if str(value).strip()))
    selected_scopes = tuple(dict.fromkeys(str(value).strip() for value in scopes if str(value).strip()))
    requested_variants = tuple(dict.fromkeys(str(value).strip() for value in variants if str(value).strip()))
    unknown = sorted(set(requested_variants) - set(SUPPORTED_VARIANTS))
    if unknown:
        raise ValueError(f"Unsupported variants: {unknown}")
    partitions = discover_dual_theme_partitions(
        campaign,
        theme_families=selected_families,
        scopes=selected_scopes,
    )
    connection = duckdb.connect()
    configure_duckdb(connection, resource_budget)
    output_files: list[dict[str, object]] = []
    inputs: list[dict[str, object]] = []
    total_rows = 0
    total_pit = 0
    factor_keys: set[tuple[str, str, str, int, str]] = set()
    counts: dict[str, int] = {}
    for partition in partitions:
        meta = _partition_metadata(connection, partition)
        if int(meta["pit_violations"]):
            raise ValueError(f"GFF edge PIT violation in {partition.edges}: {meta['pit_violations']}")
        total_pit += int(meta["pit_violations"])
        inputs.extend([file_record(partition.edges), file_record(partition.nodes)])
        if partition.scope_memberships is not None:
            inputs.append(file_record(partition.scope_memberships))
        trade_date = str(meta["trade_date"])
        layer = str(meta["layer_id"])
        scale = int(meta["scale_minutes"])
        for variant in requested_variants:
            destination = (
                output
                / f"batch_id={batch_id}"
                / f"scope={partition.scope}"
                / f"theme_family={partition.theme_family}"
                / f"layer_id={layer}"
                / f"scale_minutes={scale}"
                / f"variant_id={variant}"
                / f"trade_date={trade_date}"
                / "data.parquet"
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists() and not force:
                rows = int(
                    connection.execute(
                        f"SELECT count(*) FROM read_parquet('{_sql_path(destination)}', union_by_name=true)"
                    ).fetchone()[0]
                )
            else:
                query = (
                    _inter_query(batch_id=batch_id, partition=partition, meta=meta, variant=variant)
                    if partition.scope == "inter_theme"
                    else _stock_query(batch_id=batch_id, partition=partition, meta=meta, variant=variant)
                )
                temporary = destination.with_name(f".{destination.name}.part")
                temporary.unlink(missing_ok=True)
                connection.execute(
                    f"COPY ({query}) TO '{_sql_path(temporary)}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
                )
                temporary.replace(destination)
                rows = int(
                    connection.execute(
                        f"SELECT count(*) FROM read_parquet('{_sql_path(destination)}', union_by_name=true)"
                    ).fetchone()[0]
                )
            total_rows += rows
            if rows > 0:
                factor_keys.add((partition.scope, partition.theme_family, layer, scale, variant))
                key = f"{partition.scope}|{partition.theme_family}"
                counts[key] = counts.get(key, 0) + 1
            record = file_record(destination)
            record.update(
                {
                    "scope": partition.scope,
                    "theme_family": partition.theme_family,
                    "layer_id": layer,
                    "scale_minutes": scale,
                    "variant_id": variant,
                    "trade_date": trade_date,
                    "rows": rows,
                }
            )
            output_files.append(record)
    campaign_contract = campaign / "runs" / "campaign_contract.json"
    campaign_summary = campaign / "runs" / "campaign_summary.json"
    if campaign_contract.exists():
        inputs.append(file_record(campaign_contract))
    if campaign_summary.exists():
        inputs.append(file_record(campaign_summary))
    parameters = {
        "version": DUAL_THEME_EXPORT_VERSION,
        "batch_id": batch_id,
        "campaign_root": str(campaign),
        "theme_families": list(selected_families),
        "scopes": list(selected_scopes),
        "variants": list(requested_variants),
        "factor_count": len(factor_keys),
    }
    manifest = implementation_manifest(
        operation="export_dual_theme_gff_signals",
        parameters=parameters,
        inputs=inputs,
        resource_budget=resource_budget,
    )
    enforce_git_lineage(
        manifest,
        expected_commit=expected_git_commit,
        require_clean=require_clean,
    )
    manifest.update(
        {
            "export_version": DUAL_THEME_EXPORT_VERSION,
            "output_files": output_files,
            "output_rows": total_rows,
            "factor_count": len(factor_keys),
            "empty_output_file_count": sum(int(row.get("rows", 0)) == 0 for row in output_files),
            "edge_pit_violations": total_pit,
            "counts_by_scope_family": counts,
            "inter_theme_projection": "theme_node_signal_broadcast_to_canonical_scope_members",
            "global_computation": "shared_once_across_theme_families",
        }
    )
    atomic_write_json(output / "export_manifest.json", manifest)
    atomic_write_json(
        output / "_SUCCESS",
        {"contract_hash": manifest["contract_hash"], "factor_count": len(factor_keys)},
    )
    return DualThemeExportSummary(
        len(partitions),
        len(output_files),
        total_rows,
        len(factor_keys),
        total_pit,
        str(output),
        counts,
    )
