from __future__ import annotations

import json
import math
import os
from pathlib import Path
import shutil
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from . import forward_volatility as core
from .checkpoint import checkpoint_valid, commit_frames
from .contracts import LabelContract
from .governance import (
    ResourceBudget,
    atomic_write_json,
    atomic_write_text,
    configure_duckdb,
    file_record,
    sha256_file,
    sha256_json,
)
from .streaming import _table_expression


_HARDENING_VERSION = "GAL_FORWARD_VOLATILITY_HARDENING_V2"
_ORIGINAL_FINALIZE = core._finalize_volatility_report


def _file_matches(path: str | Path, record: object) -> bool:
    file_path = Path(path)
    if not file_path.exists() or not isinstance(record, dict):
        return False
    try:
        if str(record.get("sha256")) != sha256_file(file_path):
            return False
        expected_rows = record.get("rows")
        if expected_rows is not None and file_path.suffix.lower() in {".parquet", ".pq"}:
            return int(expected_rows) == int(pq.ParquetFile(file_path).metadata.num_rows)
        return True
    except Exception:
        return False


def _records(root: Path, names: tuple[str, ...]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for name in names:
        path = root / name
        record = file_record(path)
        record["path"] = name
        if path.suffix.lower() in {".parquet", ".pq"}:
            record["rows"] = int(pq.ParquetFile(path).metadata.num_rows)
        rows.append(record)
    return rows


def _records_match(root: Path, records: object) -> bool:
    if not isinstance(records, list):
        return False
    return all(
        isinstance(record, dict)
        and bool(record.get("path"))
        and _file_matches(root / str(record["path"]), record)
        for record in records
    )


def _decision_inventory(
    signals_root: Path,
    batch_id: str,
    output_root: Path,
    *,
    start_date: str | None,
    end_date: str | None,
    export_anchor: dict[str, object],
    resource_budget: ResourceBudget,
) -> list[dict[str, str]]:
    preflight = output_root / "_checkpoints" / "forward_volatility" / "preflight"
    cache = preflight / "decision_inventory.json"
    key = sha256_json(
        {
            "kind": "forward-volatility-decision-inventory-v2",
            "signals": export_anchor,
            "start_date": start_date,
            "end_date": end_date,
        }
    )
    cached = core._cache_read(cache, key)
    if isinstance(cached, list) and all(
        isinstance(row, dict)
        and _file_matches(str(row.get("decisions", "")), row)
        for row in cached
    ):
        return [dict(row) for row in cached]

    source_root = core._global_signal_root(signals_root, batch_id)
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, resource_budget)
        source = _table_expression(source_root)
        clauses: list[str] = []
        if start_date:
            clauses.append(f"CAST(trade_date AS DATE) >= DATE '{start_date}'")
        if end_date:
            clauses.append(f"CAST(trade_date AS DATE) <= DATE '{end_date}'")
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        dates = [
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM {source} {where} ORDER BY 1"
            ).fetchall()
        ]
        if not dates:
            raise ValueError("No governed signal dates were discovered for the requested range")
        result: list[dict[str, str]] = []
        for trade_date in dates:
            destination = (
                preflight / "decisions" / f"trade_date={trade_date}" / core.LABEL_DATA_FILE
            )
            marker = destination.parent / "cache.json"
            day_key = sha256_json(
                {
                    "kind": "forward-volatility-decisions-date-v2",
                    "signals": export_anchor,
                    "trade_date": trade_date,
                }
            )
            day_cached = core._cache_read(marker, day_key)
            if not _file_matches(destination, day_cached):
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(f".{destination.name}.{os.getpid()}.part")
                temporary.unlink(missing_ok=True)
                connection.execute(
                    f"""
                    COPY (
                      SELECT DISTINCT
                        CAST(trade_date AS VARCHAR) AS trade_date,
                        decision_time,
                        CAST(symbol_id AS BIGINT) AS symbol_id
                      FROM {source}
                      WHERE CAST(trade_date AS VARCHAR)='{trade_date}'
                      ORDER BY decision_time, symbol_id
                    ) TO '{core._sql_path(temporary)}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)
                    """
                )
                os.replace(temporary, destination)
                day_cached = {
                    "path": str(destination),
                    "rows": int(pq.ParquetFile(destination).metadata.num_rows),
                    "sha256": sha256_file(destination),
                }
                core._cache_write(marker, day_key, day_cached)
            result.append(
                {
                    "trade_date": trade_date,
                    "decisions": str(destination),
                    "rows": int(day_cached["rows"]),
                    "sha256": str(day_cached["sha256"]),
                }
            )
    finally:
        connection.close()
    core._cache_write(cache, key, result)
    return result


def _build_symbol_rows(
    frame: pd.DataFrame,
    decisions: np.ndarray,
    config: core.ForwardVolatilityConfig,
    *,
    trade_date: str,
    symbol_id: int,
) -> list[dict[str, object]]:
    frame = frame.sort_values("timestamp")
    timestamps = pd.to_datetime(frame["timestamp"], utc=True).astype("int64").to_numpy()
    available_ns = (
        pd.to_datetime(frame["available_time_1m"], utc=True, errors="coerce")
        .astype("int64")
        .to_numpy()
    )
    raw = pd.to_numeric(frame["ret_1m"], errors="coerce").to_numpy(float)
    valid = np.isfinite(raw) & (raw > -1.0)
    returns = np.zeros(len(raw), dtype=float)
    returns[valid] = np.log1p(raw[valid])
    squared = returns * returns
    downside = np.where(returns < 0, squared, 0.0)
    bipower = np.zeros(len(returns), dtype=float)
    if len(returns) > 1:
        pair_valid = valid[1:] & valid[:-1]
        bipower[1:] = np.where(
            pair_valid,
            (math.pi / 2.0) * np.abs(returns[1:]) * np.abs(returns[:-1]),
            0.0,
        )
    cumulative_rv = np.r_[0.0, np.cumsum(squared)]
    cumulative_down = np.r_[0.0, np.cumsum(downside)]
    cumulative_bv = np.r_[0.0, np.cumsum(bipower)]
    cumulative_count = np.r_[0, np.cumsum(valid.astype(np.int64))]
    minute_ns = int(pd.Timedelta(minutes=1).value)
    rows: list[dict[str, object]] = []
    for raw_decision in decisions:
        decision_ns = int(raw_decision)
        decision_time = pd.Timestamp(decision_ns, tz="UTC")
        for horizon_value in config.horizons:
            horizon = int(horizon_value)
            entry_ns = decision_ns + int(config.entry_lag_minutes) * minute_ns
            exit_ns = entry_ns + horizon * minute_ns
            future_left = int(np.searchsorted(timestamps, entry_ns, side="right"))
            future_right = int(np.searchsorted(timestamps, exit_ns, side="right"))
            past_left = int(
                np.searchsorted(timestamps, decision_ns - horizon * minute_ns, side="right")
            )
            past_right = int(np.searchsorted(timestamps, decision_ns, side="right"))
            future_count = int(cumulative_count[future_right] - cumulative_count[future_left])
            past_count = int(cumulative_count[past_right] - cumulative_count[past_left])
            required = int(math.ceil(horizon * float(config.min_coverage)))
            if future_count < required:
                continue
            if future_right <= future_left or timestamps[future_right - 1] < exit_ns:
                continue
            future_rv = core._interval_sum(cumulative_rv, future_left, future_right)
            future_down = core._interval_sum(cumulative_down, future_left, future_right)
            future_bv = core._interval_sum(cumulative_bv, future_left, future_right)
            future_jump = max(future_rv - future_bv, 0.0)
            past_rv = (
                core._interval_sum(cumulative_rv, past_left, past_right)
                if past_count >= required
                else np.nan
            )
            expansion = (
                math.log((future_rv + config.epsilon) / (past_rv + config.epsilon))
                if np.isfinite(past_rv)
                else np.nan
            )
            path = np.cumsum(returns[future_left:future_right])
            wealth = np.exp(path)
            peak = np.maximum.accumulate(np.r_[1.0, wealth])
            drawdown = np.r_[1.0, wealth] / peak - 1.0
            max_source_available = int(np.max(available_ns[future_left:future_right]))
            label_available_ns = max(exit_ns, max_source_available)
            rows.append(
                {
                    "trade_date": trade_date,
                    "decision_time": decision_time,
                    "symbol_id": int(symbol_id),
                    "label_id": f"forward_volatility_{horizon}m_v1",
                    "entry_time": pd.Timestamp(entry_ns, tz="UTC"),
                    "exit_time": pd.Timestamp(exit_ns, tz="UTC"),
                    "label_available_time": pd.Timestamp(label_available_ns, tz="UTC"),
                    "horizon_minutes": horizon,
                    "entry_lag_minutes": int(config.entry_lag_minutes),
                    "future_observation_count": future_count,
                    "past_observation_count": past_count,
                    "past_window_complete": bool(past_count >= required),
                    "past_rv": past_rv,
                    "future_rv": future_rv,
                    "forward_log_rv": math.log(future_rv + config.epsilon),
                    "forward_vol_expansion": expansion,
                    "forward_downside_semivar": future_down,
                    "forward_downside_share": future_down / (future_rv + config.epsilon),
                    "forward_jump_var": future_jump,
                    "forward_jump_share": future_jump / (future_rv + config.epsilon),
                    "forward_max_abs_return": float(np.max(np.abs(path))),
                    "forward_max_drawdown": float(max(0.0, -np.min(drawdown))),
                    "label_status": "complete",
                }
            )
    return rows


def _run_label_task(task: core.LabelDateTask) -> dict[str, object]:
    spec = core._label_spec(task)
    if checkpoint_valid(task.checkpoint_dir, spec, required_files=(core.LABEL_DATA_FILE,)):
        return {"unit": task.trade_date, "status": "reused"}
    config = core.ForwardVolatilityConfig(**task.config)
    config.validate()
    decisions = pd.read_parquet(task.decisions)
    available = set(pq.ParquetFile(task.gff_core).schema_arrow.names)
    required = {"timestamp", "symbol_id", "ret_1m", "available_time_1m"}
    missing = sorted(required - available)
    if missing:
        raise ValueError(f"NFF gff_core missing required columns: {missing}")
    market = pq.read_table(task.gff_core, columns=sorted(required)).to_pandas()
    market["timestamp"] = pd.to_datetime(market["timestamp"], utc=True)
    market["available_time_1m"] = pd.to_datetime(
        market["available_time_1m"], utc=True, errors="coerce"
    )
    decisions["decision_time"] = pd.to_datetime(decisions["decision_time"], utc=True)
    decision_map = {
        int(symbol_id): group["decision_time"].astype("int64").sort_values().unique()
        for symbol_id, group in decisions.groupby("symbol_id", observed=True)
    }
    market = market[market["symbol_id"].isin(decision_map)].copy()
    rows: list[dict[str, object]] = []
    for symbol_id, group in market.groupby("symbol_id", observed=True, sort=False):
        rows.extend(
            _build_symbol_rows(
                group,
                decision_map[int(symbol_id)],
                config,
                trade_date=task.trade_date,
                symbol_id=int(symbol_id),
            )
        )
    labels = pd.DataFrame(rows)
    if labels.empty:
        raise ValueError(f"No forward-volatility labels produced for {task.trade_date}")
    labels["forward_tail_event"] = 0.0
    for _, indices in labels.groupby(
        ["decision_time", "horizon_minutes"], observed=True, sort=False
    ).groups.items():
        rank = labels.loc[indices, "forward_log_rv"].rank(method="average", pct=True)
        labels.loc[indices, "forward_tail_event"] = (
            rank >= float(config.tail_quantile)
        ).astype(float)
    duplicate = int(
        labels.duplicated(
            ["label_id", "trade_date", "decision_time", "symbol_id"], keep=False
        ).sum()
    )
    entry = pd.to_datetime(labels["entry_time"], utc=True)
    decision = pd.to_datetime(labels["decision_time"], utc=True)
    exit_time = pd.to_datetime(labels["exit_time"], utc=True)
    available_time = pd.to_datetime(labels["label_available_time"], utc=True)
    invalid_time = int(
        ((entry <= decision) | (exit_time <= entry) | (available_time < exit_time)).sum()
    )
    if duplicate or invalid_time:
        raise ValueError(
            f"Label QA failed for {task.trade_date}: duplicate={duplicate}, invalid_time={invalid_time}"
        )
    labels = labels.sort_values(
        ["decision_time", "symbol_id", "horizon_minutes"]
    ).reset_index(drop=True)
    commit_frames(
        task.checkpoint_dir,
        spec,
        {core.LABEL_DATA_FILE: labels},
        metadata={
            "version": core.FORWARD_VOLATILITY_VERSION,
            "hardening_version": _HARDENING_VERSION,
            "trade_date": task.trade_date,
            "rows": int(len(labels)),
            "symbols": int(labels["symbol_id"].nunique()),
            "decisions": int(labels["decision_time"].nunique()),
            "label_available_time_policy": "max_exit_and_future_source_available_time",
        },
    )
    return {"unit": task.trade_date, "status": "built", "rows": int(len(labels))}


def _write_contracts(
    output_root: Path,
    labels_root: Path,
    config: core.ForwardVolatilityConfig,
    *,
    labels_anchor: list[dict[str, object]],
) -> Path:
    root = output_root / "contracts"
    contract_hash = sha256_json(
        {
            "version": core.FORWARD_VOLATILITY_VERSION,
            "hardening_version": _HARDENING_VERSION,
            "config": config.as_dict(),
            "labels": labels_anchor,
        }
    )
    manifest = root / "horizon_manifest.json"
    marker = root / "checkpoint.json"
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    if (
        payload.get("status") == "complete"
        and payload.get("contract_hash") == contract_hash
        and manifest.exists()
        and _records_match(root, payload.get("artifacts"))
    ):
        return manifest

    pending = root.with_name(f".{root.name}.{os.getpid()}.pending")
    shutil.rmtree(pending, ignore_errors=True)
    pending.mkdir(parents=True, exist_ok=False)
    horizons: list[dict[str, str]] = []
    rows: list[dict[str, object]] = []
    names: list[str] = []
    for horizon_value in config.horizons:
        horizon = int(horizon_value)
        for target in config.targets:
            name = f"{target}_{horizon}m"
            filename = f"{name}.json"
            contract = LabelContract(
                label_id=f"forward_volatility_{horizon}m_v1",
                horizon_minutes=horizon,
                entry_lag_minutes=int(config.entry_lag_minutes),
                target_column=target,
                overlapping=True,
                rebalance_minutes=5,
                require_entry_after_decision=True,
            )
            contract.validate()
            (pending / filename).write_text(
                json.dumps(contract.as_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            names.append(filename)
            horizons.append(
                {
                    "name": name,
                    "labels": str(labels_root.resolve()),
                    "label_contract": str((root / filename).resolve()),
                }
            )
            rows.append(
                {
                    "name": name,
                    "horizon_minutes": horizon,
                    "target_kind": target,
                    "label_id": contract.label_id,
                    "contract_file": filename,
                }
            )
    (pending / "horizon_manifest.json").write_text(
        json.dumps(
            {"version": core.FORWARD_VOLATILITY_VERSION, "horizons": horizons},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(rows).to_parquet(pending / "contract_index.parquet", index=False)
    names.extend(["horizon_manifest.json", "contract_index.parquet"])
    artifacts = _records(pending, tuple(names))
    atomic_write_json(
        pending / "checkpoint.json",
        {
            "status": "complete",
            "contract_hash": contract_hash,
            "hardening_version": _HARDENING_VERSION,
            "contracts": len(rows),
            "artifacts": artifacts,
        },
    )
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    os.replace(pending, root)
    return root / "horizon_manifest.json"


def _report_sources(raw_root: Path, config: core.ForwardVolatilityConfig) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for horizon_value in config.horizons:
        horizon = int(horizon_value)
        for target in config.targets:
            name = f"{target}_{horizon}m"
            root = raw_root / f"horizon={name}"
            for filename in (
                "_SUCCESS",
                "alpha_metrics.csv",
                "quantile_returns.csv",
                "daily_ic.csv",
            ):
                record = file_record(root / filename)
                record["path"] = f"horizon={name}/{filename}"
                records.append(record)
    return records


def _finalize_volatility_report(
    output_root: Path,
    raw_root: Path,
    config: core.ForwardVolatilityConfig,
) -> Path:
    report_root = output_root / "prediction_report"
    sources = _report_sources(raw_root, config)
    contract_hash = sha256_json(
        {
            "version": core.FORWARD_VOLATILITY_VERSION,
            "hardening_version": _HARDENING_VERSION,
            "config": config.as_dict(),
            "sources": sources,
        }
    )
    marker = report_root / "checkpoint.json"
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    if (
        payload.get("status") == "complete"
        and payload.get("contract_hash") == contract_hash
        and (report_root / "_SUCCESS").exists()
        and _records_match(report_root, payload.get("artifacts"))
    ):
        return report_root

    report = _ORIGINAL_FINALIZE(output_root, raw_root, config)
    success = report / "_SUCCESS"
    success.unlink(missing_ok=True)
    names = (
        "prediction_metrics.parquet",
        "variant_comparison.parquet",
        "layer_target_summary.parquet",
        "quantile_targets.parquet",
        "daily_ic.parquet",
        "REPORT.md",
        "summary.json",
    )
    artifacts = _records(report, names)
    atomic_write_json(
        report / "checkpoint.json",
        {
            "status": "complete",
            "contract_hash": contract_hash,
            "hardening_version": _HARDENING_VERSION,
            "sources": sources,
            "artifacts": artifacts,
        },
    )
    atomic_write_text(success, core._utc_now() + "\n")
    return report


def install() -> None:
    """Install V2 hardening before invoking the forward-volatility workflow."""
    core._decision_inventory = _decision_inventory
    core._build_symbol_rows = _build_symbol_rows
    core._run_label_task = _run_label_task
    core._write_contracts = _write_contracts
    core._finalize_volatility_report = _finalize_volatility_report


__all__ = ["install"]
