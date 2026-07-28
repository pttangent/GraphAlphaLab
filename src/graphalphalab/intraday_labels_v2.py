from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import os
import shutil
from typing import Iterable

import duckdb
import pandas as pd

from .checkpoint import CheckpointSpec, checkpoint_valid, commit_frames, write_progress
from .contracts import LabelContract
from .daily_labels_v2 import bar_files, selected_dates, sql_literal, sql_path
from .dual_theme_common import DUAL_THEME_BATCH_ID, load_gff_campaign_contract
from .governance import atomic_write_frame, atomic_write_json, file_record, sha256_file, sha256_json


@dataclass(frozen=True)
class IntradayLabelSpec:
    name: str
    horizon_minutes: int

    @property
    def label_id(self) -> str:
        return f"intraday_{self.horizon_minutes}m_next_bar_open"


DEFAULT_INTRADAY_SPECS = tuple(IntradayLabelSpec(f"{minutes}m", minutes) for minutes in (5, 15, 30, 60, 120, 180))


def _contract(spec: IntradayLabelSpec) -> LabelContract:
    return LabelContract(
        label_id=spec.label_id,
        horizon_minutes=spec.horizon_minutes,
        entry_lag_minutes=1,
        rebalance_minutes=spec.horizon_minutes,
        horizon_tolerance_seconds=60,
        require_entry_after_decision=True,
    )


def build_intraday_labels_v2(
    *,
    gff_campaign_root: str | Path,
    signals_root: str | Path,
    bars_root: str | Path,
    output_root: str | Path,
    start_date: str = "2026-04-01",
    end_date: str = "2026-07-22",
    specs: Iterable[IntradayLabelSpec] = DEFAULT_INTRADAY_SPECS,
    threads: int = 24,
    memory_limit_gb: float = 60,
    temp_directory: str | Path | None = None,
) -> Path:
    campaign = load_gff_campaign_contract(gff_campaign_root)
    dates = selected_dates(campaign["dates"], start_date, end_date)
    specs = tuple(specs)
    if not specs:
        raise ValueError("At least one intraday horizon is required")

    bars = Path(bars_root).resolve()
    signals = Path(signals_root).resolve()
    output = Path(output_root).resolve()
    parts = output / "_checkpoints" / "intraday_labels_v2"
    labels_root = output / "labels"
    contracts_root = output / "contracts"
    diagnostics = output / "diagnostics"
    for path in (parts, labels_root, contracts_root, diagnostics):
        path.mkdir(parents=True, exist_ok=True)

    files = bar_files(bars)
    missing = [value for value in dates if value not in files]
    if missing:
        raise FileNotFoundError(f"Missing intraday bars: {missing}")
    global_signals = signals / f"batch_id={DUAL_THEME_BATCH_ID}" / "scope=global"
    export_manifest = signals / "export_manifest.json"
    if not global_signals.exists() or not export_manifest.exists() or not (signals / "_SUCCESS").exists():
        raise FileNotFoundError(f"Incomplete governed signal export: {signals}")

    contract_hash = sha256_json({
        "operation": "dual_theme_intraday_labels_v1_next_bar_open",
        "code_sha256": sha256_file(Path(__file__)),
        "campaign": file_record(campaign["path"]),
        "signals": file_record(export_manifest),
        "dates": dates,
        "specs": [{"name": spec.name, "minutes": spec.horizon_minutes} for spec in specs],
        "entry": "next exact 1-minute bar open",
        "exit": "exact horizon bar close",
        "rth": "America/New_York 09:30-16:00",
    })

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={max(1, threads)}")
    con.execute(f"SET memory_limit='{memory_limit_gb:g}GB'")
    if temp_directory:
        Path(temp_directory).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{sql_path(Path(temp_directory))}'")
    dates_sql = ",".join(sql_literal(value) for value in dates)
    signal_pattern = sql_path(global_signals / "**" / "data.parquet")
    con.execute(f"""
      CREATE VIEW decisions AS
      SELECT DISTINCT CAST(trade_date AS VARCHAR) trade_date,
             CAST(decision_time AS TIMESTAMPTZ) decision_time, CAST(symbol_id AS BIGINT) symbol_id
      FROM read_parquet('{signal_pattern}', union_by_name=true, hive_partitioning=false)
      WHERE CAST(trade_date AS VARCHAR) IN ({dates_sql})
    """)
    con.execute(f"""
      CREATE VIEW bars AS
      SELECT CAST(trade_date AS VARCHAR) trade_date, CAST(symbol_id AS BIGINT) symbol_id,
             CAST(timestamp AS TIMESTAMPTZ) "timestamp", CAST(available_time AS TIMESTAMPTZ) available_time,
             CAST(open AS DOUBLE) "open", CAST(close AS DOUBLE) "close"
      FROM read_parquet('{sql_path(bars / 'date=*' / '*.parquet')}', union_by_name=true, hive_partitioning=false)
      WHERE CAST(trade_date AS VARCHAR) IN ({dates_sql})
        AND symbol_id IS NOT NULL AND open IS NOT NULL AND close IS NOT NULL
        AND CAST(timezone('America/New_York', timestamp) AS TIME) BETWEEN TIME '09:30:00' AND TIME '15:59:00'
    """)

    states: list[dict[str, object]] = []
    completed = reused = 0
    write_progress(diagnostics, stage="intraday-label-v2", total=len(dates), completed=0, units=[])
    try:
        for trade_date in dates:
            records = [file_record(path) for path in files[trade_date]]
            checkpoint = CheckpointSpec("intraday-label-v2", trade_date, contract_hash, sha256_json(records))
            target = parts / f"date={trade_date}"
            state = {"unit": trade_date, "status": "pending", "attempt": 0, "detail": str(target)}
            states.append(state)
            if checkpoint_valid(target, checkpoint, required_files=("data.parquet",)):
                state["status"] = "reused"; completed += 1; reused += 1
                write_progress(diagnostics, stage="intraday-label-v2", total=len(dates), completed=completed, reused=reused, current=trade_date, units=states)
                continue
            queries: list[str] = []
            for spec in specs:
                queries.append(f"""
                  SELECT d.trade_date,d.decision_time,d.symbol_id,{sql_literal(spec.label_id)} label_id,
                         entry.timestamp entry_time,exit.timestamp + INTERVAL '1 minute' exit_time,
                         greatest(entry.available_time,exit.available_time,exit.timestamp + INTERVAL '1 minute') label_available_time,
                         exit.close/nullif(entry.open,0)-1 target_return
                  FROM decisions d
                  JOIN bars entry ON entry.trade_date=d.trade_date AND entry.symbol_id=d.symbol_id
                                 AND entry.timestamp=d.decision_time + INTERVAL '1 minute'
                  JOIN bars exit ON exit.trade_date=d.trade_date AND exit.symbol_id=d.symbol_id
                                AND exit.timestamp=d.decision_time + INTERVAL '{spec.horizon_minutes} minute'
                  WHERE d.trade_date={sql_literal(trade_date)}
                    AND entry.timestamp>d.decision_time
                    AND exit.timestamp + INTERVAL '1 minute'>entry.timestamp
                """)
            temporary = target.parent / f".{target.name}.part.parquet"
            temporary.unlink(missing_ok=True)
            con.execute("COPY (" + " UNION ALL ".join(queries) + f") TO '{sql_path(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            frame = con.execute(f"SELECT * FROM read_parquet('{sql_path(temporary)}')").fetch_df()
            temporary.unlink(missing_ok=True)
            commit_frames(target, checkpoint, {"data.parquet": frame}, metadata={"trade_date": trade_date, "sources": records})
            state["status"] = "complete"; state["detail"] = f"{len(frame)} rows"; completed += 1
            write_progress(diagnostics, stage="intraday-label-v2", total=len(dates), completed=completed, reused=reused, current=trade_date, units=states)

        parts_pattern = sql_path(parts / "date=*" / "data.parquet")
        coverage: list[dict[str, object]] = []
        horizons: list[dict[str, str]] = []
        for spec in specs:
            target = labels_root / f"label_id={spec.label_id}"
            pending = labels_root / f".{target.name}.{os.getpid()}.pending"
            shutil.rmtree(pending, ignore_errors=True); pending.mkdir(parents=True)
            destination = pending / "data.parquet"
            con.execute(f"COPY (SELECT * FROM read_parquet('{parts_pattern}', union_by_name=true, hive_partitioning=false) WHERE label_id={sql_literal(spec.label_id)}) TO '{sql_path(destination)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            stats_row = con.execute(f"SELECT count(*),count(DISTINCT trade_date),min(trade_date),max(trade_date),count(DISTINCT decision_time),count(DISTINCT symbol_id) FROM read_parquet('{sql_path(destination)}')").fetchone()
            if target.exists(): shutil.rmtree(target)
            os.replace(pending, target)
            contract_path = contracts_root / f"{spec.label_id}.json"
            atomic_write_json(contract_path, _contract(spec).as_dict())
            coverage.append({"label_id": spec.name, "contract_label_id": spec.label_id, "horizon_type": "intraday", "horizon_minutes": spec.horizon_minutes, "rows": int(stats_row[0]), "available_date_count": int(stats_row[1]), "first_available_date": stats_row[2], "last_available_date": stats_row[3], "decision_count": int(stats_row[4]), "symbol_count": int(stats_row[5]), "tail_missing_date_count": 0, "tail_missing_dates": "", "entry_policy": "next_1m_bar_open", "exit_policy": f"exact_{spec.horizon_minutes}m_bar_close"})
            horizons.append({"name": spec.name, "labels": str(target), "label_contract": str(contract_path)})

        atomic_write_frame(pd.DataFrame(coverage), diagnostics / "intraday_label_coverage.csv")
        atomic_write_json(diagnostics / "intraday_label_manifest.json", {"version": "GAL_INTRADAY_LABELS_V1_NEXT_BAR_OPEN", "analysis_dates": dates, "date_count": len(dates), "start_date": dates[0], "end_date": dates[-1], "labels": coverage})
        atomic_write_json(output / "intraday_horizon_manifest.json", {"horizons": horizons})
        atomic_write_json(output / "_SUCCESS", {"status": "complete", "contract_hash": contract_hash})
        write_progress(diagnostics, stage="intraday-label-v2", total=len(dates), completed=len(dates), reused=reused, status="complete", units=states)
        return output / "intraday_horizon_manifest.json"
    finally:
        con.close()


def merge_horizon_manifests(
    manifests: Iterable[str | Path],
    output_path: str | Path,
    *,
    label_manifests: Iterable[str | Path] = (),
) -> tuple[Path, Path]:
    horizons: list[dict[str, str]] = []
    names: set[str] = set()
    for path in manifests:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        for row in payload.get("horizons", []):
            name = str(row["name"])
            if name in names:
                raise ValueError(f"Duplicate horizon name: {name}")
            names.add(name)
            horizons.append({"name": name, "labels": str(Path(row["labels"]).resolve()), "label_contract": str(Path(row["label_contract"]).resolve())})
    profiles: list[dict[str, object]] = []
    dates: list[str] = []
    for path in label_manifests:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not dates:
            dates = [str(value) for value in payload.get("analysis_dates", [])]
        profiles.extend(payload.get("labels", []))
    destination = Path(output_path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(destination, {"horizons": horizons})
    combined_labels = destination.with_name("combined_label_manifest.json")
    atomic_write_json(combined_labels, {"version": "GAL_COMBINED_INTRADAY_DAILY_LABELS_V1", "analysis_dates": dates, "start_date": dates[0] if dates else None, "end_date": dates[-1] if dates else None, "labels": profiles, "horizon_count": len(horizons)})
    return destination, combined_labels
