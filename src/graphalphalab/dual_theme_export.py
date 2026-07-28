from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
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
    P0Partition,
    _sql_path,
    discover_dual_theme_partitions,
    load_gff_campaign_contract,
    validate_partition_inventory,
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


def _export_partition_worker(payload: dict) -> dict[str, object]:
    # One GFF P0 partition x all requested variants. Each worker owns a DuckDB
    # connection with a split resource budget; checkpoint-style file reuse is
    # identical to the sequential path (existing data.parquet kept unless force).
    partition = P0Partition(
        payload["scope"],
        payload["theme_family"],
        Path(payload["edges"]),
        Path(payload["nodes"]),
        Path(payload["scope_memberships"]) if payload["scope_memberships"] else None,
    )
    output = Path(payload["output_root"])
    batch_id = str(payload["batch_id"])
    requested_variants = tuple(payload["variants"])
    force = bool(payload["force"])
    budget = ResourceBudget(
        memory_limit_gb=float(payload["memory_limit_gb"]),
        threads=int(payload["threads"]),
        temp_directory=payload["temp_directory"],
    )
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, budget)
        meta = _partition_metadata(connection, partition)
        if int(meta["pit_violations"]):
            raise ValueError(
                f"GFF edge PIT violation in {partition.edges}: {meta['pit_violations']}"
            )
        inputs = [file_record(partition.edges), file_record(partition.nodes)]
        if partition.scope_memberships is not None:
            inputs.append(file_record(partition.scope_memberships))
        trade_date = str(meta["trade_date"])
        layer = str(meta["layer_id"])
        scale = int(meta["scale_minutes"])
        output_files: list[dict[str, object]] = []
        factor_keys: list[tuple[str, str, str, int, str]] = []
        total_rows = 0
        nonempty = 0
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
                        f"SELECT count(*) FROM read_parquet('{_sql_path(destination)}', "
                        "union_by_name=true)"
                    ).fetchone()[0]
                )
            else:
                query = (
                    _inter_query(
                        batch_id=batch_id,
                        partition=partition,
                        meta=meta,
                        variant=variant,
                    )
                    if partition.scope == "inter_theme"
                    else _stock_query(
                        batch_id=batch_id,
                        partition=partition,
                        meta=meta,
                        variant=variant,
                    )
                )
                temporary = destination.with_name(f".{destination.name}.{payload['worker_tag']}.part")
                temporary.unlink(missing_ok=True)
                connection.execute(
                    f"COPY ({query}) TO '{_sql_path(temporary)}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
                )
                temporary.replace(destination)
                rows = int(
                    connection.execute(
                        f"SELECT count(*) FROM read_parquet('{_sql_path(destination)}', "
                        "union_by_name=true)"
                    ).fetchone()[0]
                )
            total_rows += rows
            if rows > 0:
                factor_keys.append(
                    (partition.scope, partition.theme_family, layer, scale, variant)
                )
                nonempty += 1
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
        return {
            "inputs": inputs,
            "output_files": output_files,
            "rows": total_rows,
            "factor_keys": factor_keys,
            "pit_violations": int(meta["pit_violations"]),
            "nonempty_files": nonempty,
        }
    finally:
        connection.close()


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
    require_campaign_success: bool = True,
    force: bool = False,
    workers: int = 1,
) -> DualThemeExportSummary:
    campaign = Path(campaign_root).expanduser().resolve()
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)
    campaign_success = campaign / "_SUCCESS"
    if require_campaign_success and not campaign_success.exists():
        raise FileNotFoundError(f"GFF campaign is not complete: {campaign_success}")
    campaign_contract = load_gff_campaign_contract(campaign)
    selected_families = tuple(
        dict.fromkeys(
            str(value).strip() for value in theme_families if str(value).strip()
        )
    )
    selected_scopes = tuple(
        dict.fromkeys(str(value).strip() for value in scopes if str(value).strip())
    )
    requested_variants = tuple(
        dict.fromkeys(str(value).strip() for value in variants if str(value).strip())
    )
    unknown = sorted(set(requested_variants) - set(SUPPORTED_VARIANTS))
    if unknown:
        raise ValueError(f"Unsupported variants: {unknown}")
    partitions = discover_dual_theme_partitions(
        campaign,
        theme_families=selected_families,
        scopes=selected_scopes,
    )
    inventory = validate_partition_inventory(
        partitions,
        campaign_contract,
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
    if workers > 1 and len(partitions) > 1:
        connection.close()
        payloads = [
            {
                "scope": partition.scope,
                "theme_family": partition.theme_family,
                "edges": str(partition.edges),
                "nodes": str(partition.nodes),
                "scope_memberships": str(partition.scope_memberships) if partition.scope_memberships else None,
                "output_root": str(output),
                "batch_id": batch_id,
                "variants": list(requested_variants),
                "force": force,
                "memory_limit_gb": resource_budget.memory_limit_gb / workers,
                "threads": max(1, resource_budget.threads // workers),
                "temp_directory": resource_budget.temp_directory,
                "worker_tag": f"w{index % workers}",
            }
            for index, partition in enumerate(partitions)
        ]
        pending = list(payloads)
        inflight: dict[object, dict] = {}
        with ProcessPoolExecutor(max_workers=workers) as pool:
            while pending and len(inflight) < workers * 2:
                inflight[pool.submit(_export_partition_worker, pending.pop(0))] = payloads[0]
            while inflight:
                done, _ = wait(inflight, return_when=FIRST_COMPLETED)
                for future in done:
                    inflight.pop(future)
                    result = future.result()
                    total_pit += int(result["pit_violations"])
                    inputs.extend(result["inputs"])
                    output_files.extend(result["output_files"])
                    total_rows += int(result["rows"])
                    for key in result["factor_keys"]:
                        factor_keys.add(tuple(key))
                    for record in result["output_files"]:
                        if int(record.get("rows", 0)) > 0:
                            key = f"{record['scope']}|{record['theme_family']}"
                            counts[key] = counts.get(key, 0) + 1
                    if pending:
                        inflight[pool.submit(_export_partition_worker, pending.pop(0))] = payloads[0]
        output_files.sort(key=lambda record: str(record["path"]))
        inputs.sort(key=lambda record: str(record["path"]))
        return _finalize_export(
            campaign,
            output,
            batch_id=batch_id,
            campaign_contract=campaign_contract,
            inventory=inventory,
            selected_families=selected_families,
            selected_scopes=selected_scopes,
            requested_variants=requested_variants,
            resource_budget=resource_budget,
            expected_git_commit=expected_git_commit,
            require_clean=require_clean,
            require_campaign_success=require_campaign_success,
            output_files=output_files,
            inputs=inputs,
            total_rows=total_rows,
            total_pit=total_pit,
            factor_keys=factor_keys,
            counts=counts,
            partitions=partitions,
        )
    for partition in partitions:
        meta = _partition_metadata(connection, partition)
        if int(meta["pit_violations"]):
            raise ValueError(
                f"GFF edge PIT violation in {partition.edges}: {meta['pit_violations']}"
            )
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
                        f"SELECT count(*) FROM read_parquet('{_sql_path(destination)}', "
                        "union_by_name=true)"
                    ).fetchone()[0]
                )
            else:
                query = (
                    _inter_query(
                        batch_id=batch_id,
                        partition=partition,
                        meta=meta,
                        variant=variant,
                    )
                    if partition.scope == "inter_theme"
                    else _stock_query(
                        batch_id=batch_id,
                        partition=partition,
                        meta=meta,
                        variant=variant,
                    )
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
                        f"SELECT count(*) FROM read_parquet('{_sql_path(destination)}', "
                        "union_by_name=true)"
                    ).fetchone()[0]
                )
            total_rows += rows
            if rows > 0:
                factor_keys.add(
                    (partition.scope, partition.theme_family, layer, scale, variant)
                )
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
    return _finalize_export(
        campaign,
        output,
        batch_id=batch_id,
        campaign_contract=campaign_contract,
        inventory=inventory,
        selected_families=selected_families,
        selected_scopes=selected_scopes,
        requested_variants=requested_variants,
        resource_budget=resource_budget,
        expected_git_commit=expected_git_commit,
        require_clean=require_clean,
        require_campaign_success=require_campaign_success,
        output_files=output_files,
        inputs=inputs,
        total_rows=total_rows,
        total_pit=total_pit,
        factor_keys=factor_keys,
        counts=counts,
        partitions=partitions,
    )


def _finalize_export(
    campaign: Path,
    output: Path,
    *,
    batch_id: str,
    campaign_contract: dict[str, object],
    inventory: dict[str, object],
    selected_families: tuple[str, ...],
    selected_scopes: tuple[str, ...],
    requested_variants: tuple[str, ...],
    resource_budget: ResourceBudget,
    expected_git_commit: str | None,
    require_clean: bool,
    require_campaign_success: bool,
    output_files: list[dict[str, object]],
    inputs: list[dict[str, object]],
    total_rows: int,
    total_pit: int,
    factor_keys: set[tuple[str, str, str, int, str]],
    counts: dict[str, int],
    partitions: tuple[P0Partition, ...],
) -> DualThemeExportSummary:
    campaign_success = campaign / "_SUCCESS"
    campaign_contract_path = Path(campaign_contract["path"])
    campaign_summary = campaign / "runs" / "campaign_summary.json"
    inputs.append(file_record(campaign_contract_path))
    if campaign_success.exists():
        inputs.append(file_record(campaign_success))
    if campaign_summary.exists():
        inputs.append(file_record(campaign_summary))
    parameters = {
        "version": DUAL_THEME_EXPORT_VERSION,
        "batch_id": batch_id,
        "campaign_root": str(campaign),
        "gff_campaign_version": campaign_contract["campaign_version"],
        "theme_families": list(selected_families),
        "scopes": list(selected_scopes),
        "variants": list(requested_variants),
        "factor_count": len(factor_keys),
        "require_campaign_success": require_campaign_success,
        "partition_inventory": inventory,
        "scope_semantics": {
            "global": "stock_cross_section",
            "within_theme": "canonical_theme_membership_attached_for_theme_neutral_stock_alpha",
            "inter_theme": "theme_node_signal_broadcast_only_for_weighted_theme_portfolio_label_aggregation",
        },
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
            "gff_campaign_version": campaign_contract["campaign_version"],
            "output_files": output_files,
            "output_rows": total_rows,
            "factor_count": len(factor_keys),
            "empty_output_file_count": sum(
                int(row.get("rows", 0)) == 0 for row in output_files
            ),
            "edge_pit_violations": total_pit,
            "counts_by_scope_family": counts,
            "inter_theme_projection": (
                "theme_node_signal_broadcast_to_canonical_scope_members_for_weighted_theme_return_aggregation"
            ),
            "within_theme_projection": (
                "canonical_context_theme_id_attached_to_each_stock_for_within_theme_neutralization"
            ),
            "global_computation": "shared_once_across_theme_families",
            "partition_inventory": inventory,
        }
    )
    atomic_write_json(output / "export_manifest.json", manifest)
    atomic_write_json(
        output / "_SUCCESS",
        {
            "contract_hash": manifest["contract_hash"],
            "factor_count": len(factor_keys),
            "export_version": DUAL_THEME_EXPORT_VERSION,
            "gff_campaign_version": campaign_contract["campaign_version"],
        },
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
