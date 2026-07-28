from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import shutil

import duckdb
import pandas as pd

from .checkpoint import CheckpointSpec, checkpoint_valid, commit_frames, write_progress
from .contracts import LabelContract
from .dual_theme_common import DUAL_THEME_BATCH_ID, load_gff_campaign_contract
from .governance import atomic_write_frame, atomic_write_json, file_record, sha256_file, sha256_json


@dataclass(frozen=True)
class DailyLabelSpec:
    label_id: str
    entry_offset: int
    entry_point: str
    exit_offset: int
    exit_point: str

    @property
    def nominal_minutes(self) -> int:
        return 390 if self.entry_offset == self.exit_offset else max(1, self.exit_offset - self.entry_offset) * 390


# Every governed daily Alpha buys at the next trading-session open.
CORE_DAILY_LABELS = (
    DailyLabelSpec("daily_n1_open_to_n1_close", 1, "open", 1, "close"),
    DailyLabelSpec("daily_n1_open_to_n2_open", 1, "open", 2, "open"),
    DailyLabelSpec("daily_n1_open_to_n2_close", 1, "open", 2, "close"),
)
EXTENDED_DAILY_LABELS = CORE_DAILY_LABELS + (
    DailyLabelSpec("daily_n1_open_to_n3_close", 1, "open", 3, "close"),
    DailyLabelSpec("daily_n1_open_to_n5_close", 1, "open", 5, "close"),
    DailyLabelSpec("daily_n1_open_to_n7_close", 1, "open", 7, "close"),
)


def sql_path(path: Path) -> str:
    return path.expanduser().resolve().as_posix().replace("'", "''")


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def bar_files(root: Path) -> dict[str, list[Path]]:
    rows: dict[str, list[Path]] = {}
    for date_root in sorted(root.glob("date=*")):
        files = sorted(date_root.rglob("*.parquet"))
        if files:
            rows[date_root.name.split("=", 1)[1]] = files
    if not rows:
        raise FileNotFoundError(f"No bars below {root}")
    return rows


def selected_dates(campaign_dates: list[object], start_date: str | None, end_date: str | None) -> list[str]:
    dates = sorted(str(value) for value in campaign_dates)
    if start_date:
        dates = [value for value in dates if value >= start_date]
    if end_date:
        dates = [value for value in dates if value <= end_date]
    if not dates:
        raise ValueError("No campaign dates remain after filtering")
    return dates


def _specs(profile: str) -> tuple[DailyLabelSpec, ...]:
    if profile == "core":
        return CORE_DAILY_LABELS
    if profile == "extended":
        return EXTENDED_DAILY_LABELS
    raise ValueError("profile must be core or extended")


def _point(alias: str, name: str) -> tuple[str, str, str]:
    prefix = "rth_open" if name == "open" else "rth_close"
    return f"{alias}.{prefix}_time", f"{alias}.{prefix}_price", f"{alias}.{prefix}_available_time"


def _contract(spec: DailyLabelSpec) -> LabelContract:
    return LabelContract(
        label_id=spec.label_id,
        horizon_minutes=spec.nominal_minutes,
        entry_lag_minutes=0,
        rebalance_minutes=390,
        horizon_tolerance_seconds=21 * 24 * 3600,
        horizon_unit="trading_sessions",
        entry_session_offset=spec.entry_offset,
        exit_session_offset=spec.exit_offset,
        entry_point=spec.entry_point,
        exit_point=spec.exit_point,
        tail_policy="allow_truncated_tail",
    )


def build_daily_labels_v2(
    *,
    gff_campaign_root: str | Path,
    signals_root: str | Path,
    bars_root: str | Path,
    output_root: str | Path,
    start_date: str = "2026-04-01",
    end_date: str = "2026-07-22",
    profile: str = "core",
    threads: int = 24,
    memory_limit_gb: float = 60,
    temp_directory: str | Path | None = None,
) -> Path:
    campaign = load_gff_campaign_contract(gff_campaign_root)
    dates = selected_dates(campaign["dates"], start_date, end_date)
    specs = _specs(profile)
    if any(spec.entry_offset < 1 or spec.entry_point != "open" for spec in specs):
        raise ValueError("All daily labels must enter at next-session open")

    bars = Path(bars_root).resolve()
    signals = Path(signals_root).resolve()
    output = Path(output_root).resolve()
    parts = output / "_checkpoints" / "daily_labels_v2"
    labels_root = output / "labels"
    contracts_root = output / "contracts"
    diagnostics = output / "diagnostics"
    for path in (parts, labels_root, contracts_root, diagnostics):
        path.mkdir(parents=True, exist_ok=True)

    files = bar_files(bars)
    missing = [value for value in dates if value not in files]
    if missing:
        raise FileNotFoundError(f"Missing base-date bars: {missing}")
    all_dates = sorted(files)
    first, last = all_dates.index(dates[0]), all_dates.index(dates[-1])
    max_offset = max(spec.exit_offset for spec in specs)
    support = all_dates[first : min(len(all_dates), last + max_offset + 1)]

    global_signals = signals / f"batch_id={DUAL_THEME_BATCH_ID}" / "scope=global"
    export_manifest = signals / "export_manifest.json"
    if not global_signals.exists() or not export_manifest.exists() or not (signals / "_SUCCESS").exists():
        raise FileNotFoundError(f"Incomplete governed signal export: {signals}")

    contract_hash = sha256_json({
        "operation": "dual_theme_daily_labels_v4_next_open",
        "code_sha256": sha256_file(Path(__file__)),
        "campaign": file_record(campaign["path"]),
        "signals": file_record(export_manifest),
        "dates": dates,
        "specs": [spec.__dict__ for spec in specs],
        "rth": "America/New_York 09:30-16:00",
        "tail_policy": "trailing missing future sessions allowed without fabrication",
    })

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={max(1, threads)}")
    con.execute(f"SET memory_limit='{memory_limit_gb:g}GB'")
    if temp_directory:
        Path(temp_directory).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{sql_path(Path(temp_directory))}'")
    support_sql = ",".join(sql_literal(value) for value in support)
    dates_sql = ",".join(sql_literal(value) for value in dates)
    con.execute(f"""
      CREATE VIEW bars AS
      SELECT CAST(trade_date AS VARCHAR) trade_date, CAST(symbol_id AS BIGINT) symbol_id,
             CAST(timestamp AS TIMESTAMPTZ) "timestamp", CAST(available_time AS TIMESTAMPTZ) available_time,
             CAST(open AS DOUBLE) "open", CAST(close AS DOUBLE) "close"
      FROM read_parquet('{sql_path(bars / 'date=*' / '*.parquet')}', union_by_name=true, hive_partitioning=false)
      WHERE CAST(trade_date AS VARCHAR) IN ({support_sql})
        AND symbol_id IS NOT NULL AND open IS NOT NULL AND close IS NOT NULL
    """)
    con.execute("""
      CREATE VIEW rth AS SELECT * FROM bars
      WHERE CAST(timezone('America/New_York', timestamp) AS TIME) BETWEEN TIME '09:30:00' AND TIME '16:00:00'
    """)
    con.execute("""
      CREATE VIEW ohlc AS
      SELECT trade_date, symbol_id,
             min(timestamp) rth_open_time, max(timestamp) rth_close_time,
             arg_min(open,timestamp) rth_open_price, arg_max(close,timestamp) rth_close_price,
             arg_min(available_time,timestamp) rth_open_available_time,
             arg_max(available_time,timestamp) rth_close_available_time
      FROM rth GROUP BY trade_date,symbol_id
    """)
    con.execute("CREATE VIEW calendar AS SELECT trade_date,row_number() OVER(ORDER BY trade_date)-1 session_index FROM (SELECT DISTINCT trade_date FROM ohlc)")
    signal_pattern = sql_path(global_signals / "**" / "data.parquet")
    con.execute(f"""
      CREATE VIEW decisions AS
      SELECT DISTINCT CAST(trade_date AS VARCHAR) trade_date,
             CAST(decision_time AS TIMESTAMPTZ) decision_time, CAST(symbol_id AS BIGINT) symbol_id
      FROM read_parquet('{signal_pattern}', union_by_name=true, hive_partitioning=false)
      WHERE CAST(trade_date AS VARCHAR) IN ({dates_sql})
    """)

    states: list[dict[str, object]] = []
    completed = reused = 0
    write_progress(diagnostics, stage="daily-label-v2", total=len(dates), completed=0, units=[])
    try:
        for base_date in dates:
            base_index = support.index(base_date)
            support_dates = support[base_index : base_index + max_offset + 1]
            records = [file_record(path) for day in support_dates for path in files[day]]
            checkpoint = CheckpointSpec("daily-label-v2", base_date, contract_hash, sha256_json(records))
            target = parts / f"date={base_date}"
            state = {"unit": base_date, "status": "pending", "attempt": 0, "detail": str(target)}
            states.append(state)
            if checkpoint_valid(target, checkpoint, required_files=("data.parquet",)):
                state["status"] = "reused"; completed += 1; reused += 1
                write_progress(diagnostics, stage="daily-label-v2", total=len(dates), completed=completed, reused=reused, current=base_date, units=states)
                continue
            queries: list[str] = []
            for label in specs:
                et, ep, ea = _point("entry", label.entry_point)
                xt, xp, xa = _point("exit", label.exit_point)
                queries.append(f"""
                  SELECT d.trade_date,d.decision_time,d.symbol_id,{sql_literal(label.label_id)} label_id,
                         {et} entry_time,{xt} exit_time,greatest({ea},{xa}) label_available_time,
                         {xp}/nullif({ep},0)-1 target_return,
                         {label.entry_offset} entry_session_offset,{label.exit_offset} exit_session_offset,
                         {sql_literal(label.entry_point)} entry_point,{sql_literal(label.exit_point)} exit_point
                  FROM decisions d JOIN calendar b ON b.trade_date=d.trade_date
                  JOIN calendar ec ON ec.session_index=b.session_index+{label.entry_offset}
                  JOIN calendar xc ON xc.session_index=b.session_index+{label.exit_offset}
                  JOIN ohlc entry ON entry.trade_date=ec.trade_date AND entry.symbol_id=d.symbol_id
                  JOIN ohlc exit ON exit.trade_date=xc.trade_date AND exit.symbol_id=d.symbol_id
                  WHERE d.trade_date={sql_literal(base_date)} AND {et}>d.decision_time AND {xt}>{et}
                """)
            temporary = target.parent / f".{target.name}.part.parquet"
            temporary.unlink(missing_ok=True)
            con.execute("COPY (" + " UNION ALL ".join(queries) + f") TO '{sql_path(temporary)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            frame = con.execute(f"SELECT * FROM read_parquet('{sql_path(temporary)}')").fetch_df()
            temporary.unlink(missing_ok=True)
            commit_frames(target, checkpoint, {"data.parquet": frame}, metadata={"base_date": base_date, "sources": records})
            state["status"] = "complete"; state["detail"] = f"{len(frame)} rows"; completed += 1
            write_progress(diagnostics, stage="daily-label-v2", total=len(dates), completed=completed, reused=reused, current=base_date, units=states)

        parts_pattern = sql_path(parts / "date=*" / "data.parquet")
        coverage: list[dict[str, object]] = []
        horizons: list[dict[str, str]] = []
        for label in specs:
            target = labels_root / f"label_id={label.label_id}"
            pending = labels_root / f".{target.name}.{os.getpid()}.pending"
            shutil.rmtree(pending, ignore_errors=True); pending.mkdir(parents=True)
            destination = pending / "data.parquet"
            con.execute(f"COPY (SELECT * FROM read_parquet('{parts_pattern}', union_by_name=true, hive_partitioning=false) WHERE label_id={sql_literal(label.label_id)}) TO '{sql_path(destination)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            stats_row = con.execute(f"SELECT count(*),count(DISTINCT trade_date),min(trade_date),max(trade_date) FROM read_parquet('{sql_path(destination)}')").fetchone()
            if target.exists(): shutil.rmtree(target)
            os.replace(pending, target)
            contract_path = contracts_root / f"{label.label_id}.json"
            atomic_write_json(contract_path, _contract(label).as_dict())
            available = con.execute(f"SELECT DISTINCT trade_date FROM read_parquet('{sql_path(target / 'data.parquet')}') ORDER BY trade_date").fetch_df()["trade_date"].astype(str).tolist()
            missing_dates = [value for value in dates if value not in set(available)]
            interior = [value for value in missing_dates if available and value < max(available)]
            if interior:
                raise RuntimeError(f"Interior daily-label gaps for {label.label_id}: {interior}")
            coverage.append({"label_id": label.label_id, "horizon_type": "daily_next_open", "rows": int(stats_row[0]), "available_date_count": int(stats_row[1]), "first_available_date": stats_row[2], "last_available_date": stats_row[3], "tail_missing_date_count": len(missing_dates), "tail_missing_dates": ",".join(missing_dates), "entry_policy": "next_session_open", "exit_policy": f"session+{label.exit_offset}_{label.exit_point}"})
            horizons.append({"name": label.label_id, "labels": str(target), "label_contract": str(contract_path)})

        atomic_write_frame(pd.DataFrame(coverage), diagnostics / "daily_label_coverage.csv")
        manifest = {"version": "GAL_DAILY_LABELS_V4_NEXT_OPEN", "analysis_dates": dates, "date_count": len(dates), "start_date": dates[0], "end_date": dates[-1], "profile": profile, "entry_policy": "all daily Alpha enters at next trading-session open", "tail_policy": "allow_truncated_tail", "labels": coverage}
        atomic_write_json(diagnostics / "daily_label_manifest.json", manifest)
        atomic_write_json(output / "daily_horizon_manifest.json", {"horizons": horizons})
        atomic_write_json(output / "_SUCCESS", {"status": "complete", "contract_hash": contract_hash})
        write_progress(diagnostics, stage="daily-label-v2", total=len(dates), completed=len(dates), reused=reused, status="complete", units=states)
        return output / "daily_horizon_manifest.json"
    finally:
        con.close()
