from __future__ import annotations

import json
from pathlib import Path

import duckdb


BASE = Path(r"D:\DEV\AnotherNetworkFactory")
BARS_ROOT = BASE / "warehouses" / "NFF_warehouse" / "canonical" / "bars_1m" / "schema=v1"
OUT = BASE / "warehouses" / "GAL_warehouse"
LABELS = OUT / "labels"
CONTRACTS = OUT / "contracts"
DIAG = OUT / "label_diagnostics"

EXPECTED_DATES = [
    "2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05",
    "2026-06-08", "2026-06-09", "2026-06-10", "2026-06-11", "2026-06-12",
    "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-22",
    "2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26", "2026-06-29",
    "2026-06-30", "2026-07-01", "2026-07-02", "2026-07-06", "2026-07-07",
    "2026-07-08", "2026-07-09", "2026-07-10", "2026-07-13", "2026-07-14",
    "2026-07-15", "2026-07-16", "2026-07-17",
]


def sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def write_daily_contract(label_id: str) -> None:
    payload = {
        "label_id": label_id,
        "horizon_minutes": 1,
        "entry_lag_minutes": 1,
        "target_column": "target_return",
        "decision_time_column": "decision_time",
        "entry_time_column": "entry_time",
        "exit_time_column": "exit_time",
        "available_time_column": "label_available_time",
        "overlapping": True,
        "rebalance_minutes": None,
        "horizon_tolerance_seconds": 21 * 24 * 3600,
        "require_entry_after_decision": True,
    }
    (CONTRACTS / f"{label_id}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    LABELS.mkdir(parents=True, exist_ok=True)
    CONTRACTS.mkdir(parents=True, exist_ok=True)
    DIAG.mkdir(parents=True, exist_ok=True)

    pattern = sql_path(BARS_ROOT / "date=*" / "part-*.parquet")
    date_values = ", ".join(f"'{d}'" for d in EXPECTED_DATES)
    daily_dest = LABELS / "daily_forward.parquet"
    temp_dest = LABELS / "daily_forward.parquet.tmp"
    if temp_dest.exists():
        temp_dest.unlink()

    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='24GB'")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW bars AS
        SELECT
          CAST(trade_date AS VARCHAR) AS trade_date,
          CAST(symbol_id AS BIGINT) AS symbol_id,
          CAST(symbol AS VARCHAR) AS symbol,
          CAST(timestamp AS TIMESTAMPTZ) AS timestamp,
          CAST(available_time AS TIMESTAMPTZ) AS available_time,
          CAST(close AS DOUBLE) AS close,
          CAST(open AS DOUBLE) AS open
        FROM read_parquet('{pattern}', union_by_name=true, hive_partitioning=false)
        WHERE trade_date IN ({date_values})
          AND symbol_id IS NOT NULL
          AND close IS NOT NULL
          AND open IS NOT NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW rth_bars AS
        SELECT *
        FROM bars
        WHERE strftime(timestamp, '%H:%M:%S') >= '21:30:00'
           OR strftime(timestamp, '%H:%M:%S') <= '04:00:00'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW daily_ohlc AS
        SELECT
          trade_date,
          symbol_id,
          any_value(symbol) AS symbol,
          min(timestamp) AS rth_open_time,
          max(timestamp) AS rth_close_time,
          arg_min(open, timestamp) AS rth_open_price,
          arg_max(close, timestamp) AS rth_close_price,
          arg_min(available_time, timestamp) AS open_available_time,
          arg_max(available_time, timestamp) AS close_available_time,
          count(*) AS rth_bar_count
        FROM rth_bars
        GROUP BY trade_date, symbol_id
        HAVING rth_open_price IS NOT NULL AND rth_close_price IS NOT NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW calendar AS
        SELECT trade_date, row_number() OVER (ORDER BY trade_date) - 1 AS session_index
        FROM (SELECT DISTINCT trade_date FROM daily_ohlc)
        """
    )
    con.execute(
        f"""
        COPY (
          WITH entries AS (
            SELECT c.trade_date, c.session_index, d.symbol_id,
                   'n_close' AS entry_mode,
                   d.rth_close_time AS entry_time,
                   d.rth_close_price AS entry_price,
                   d.close_available_time AS entry_available_time
            FROM daily_ohlc d JOIN calendar c USING (trade_date)
            UNION ALL
            SELECT c.trade_date, c.session_index, d.symbol_id,
                   'n1_open' AS entry_mode,
                   n.rth_open_time AS entry_time,
                   n.rth_open_price AS entry_price,
                   n.open_available_time AS entry_available_time
            FROM daily_ohlc d
            JOIN calendar c ON c.trade_date=d.trade_date
            JOIN calendar cn ON cn.session_index=c.session_index+1
            JOIN daily_ohlc n ON n.trade_date=cn.trade_date AND n.symbol_id=d.symbol_id
            UNION ALL
            SELECT c.trade_date, c.session_index, d.symbol_id,
                   'n1_close' AS entry_mode,
                   n.rth_close_time AS entry_time,
                   n.rth_close_price AS entry_price,
                   n.close_available_time AS entry_available_time
            FROM daily_ohlc d
            JOIN calendar c ON c.trade_date=d.trade_date
            JOIN calendar cn ON cn.session_index=c.session_index+1
            JOIN daily_ohlc n ON n.trade_date=cn.trade_date AND n.symbol_id=d.symbol_id
          ),
          exit_points AS (
            SELECT c.trade_date, c.session_index, d.symbol_id,
                   'open' AS exit_point,
                   d.rth_open_time AS exit_time,
                   d.rth_open_price AS exit_price,
                   d.open_available_time AS exit_available_time
            FROM daily_ohlc d JOIN calendar c ON c.trade_date=d.trade_date
            UNION ALL
            SELECT c.trade_date, c.session_index, d.symbol_id,
                   'close' AS exit_point,
                   d.rth_close_time AS exit_time,
                   d.rth_close_price AS exit_price,
                   d.close_available_time AS exit_available_time
            FROM daily_ohlc d JOIN calendar c ON c.trade_date=d.trade_date
          )
          SELECT
            base.trade_date,
            rb.timestamp AS decision_time,
            base.symbol_id,
            'daily_' || e.entry_mode || '_to_n' || CAST(x.session_index - e.session_index AS VARCHAR) || '_' || x.exit_point AS label_id,
            e.entry_time,
            x.exit_time,
            greatest(e.entry_available_time, x.exit_available_time) AS label_available_time,
            e.entry_price,
            x.exit_price,
            x.exit_price / nullif(e.entry_price, 0) - 1.0 AS target_return,
            e.entry_mode,
            x.exit_point,
            CAST(x.session_index - e.session_index AS INTEGER) AS exit_session_offset
          FROM daily_ohlc base
          JOIN rth_bars rb ON rb.trade_date=base.trade_date AND rb.symbol_id=base.symbol_id
          JOIN calendar cb ON cb.trade_date=base.trade_date
          JOIN entries e ON e.trade_date=base.trade_date AND e.symbol_id=base.symbol_id
          JOIN exit_points x
            ON x.symbol_id=base.symbol_id
           AND x.session_index BETWEEN cb.session_index + 1 AND cb.session_index + 7
          WHERE e.entry_time > rb.timestamp
            AND x.exit_time > e.entry_time
            AND x.exit_price IS NOT NULL
            AND e.entry_price IS NOT NULL
        ) TO '{sql_path(temp_dest)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """
    )

    coverage = con.execute(
        f"""
        SELECT
          label_id,
          any_value(entry_mode) AS entry_mode,
          any_value(exit_point) AS exit_point,
          any_value(exit_session_offset) AS exit_session_offset,
          count(*) AS label_rows,
          count(DISTINCT trade_date) AS trade_dates,
          count(DISTINCT decision_time) AS decision_times,
          count(DISTINCT symbol_id) AS symbols
        FROM read_parquet('{sql_path(temp_dest)}')
        GROUP BY label_id
        ORDER BY entry_mode, exit_session_offset, exit_point
        """
    ).df()

    rth = con.execute(
        f"""
        SELECT
          count(*) AS row_count,
          sum(CASE
                WHEN strftime(decision_time, '%H:%M:%S') >= '21:30:00'
                  OR strftime(decision_time, '%H:%M:%S') <= '04:00:00'
                THEN 0 ELSE 1
              END) AS invalid_decision_rows,
          min(strftime(decision_time, '%H:%M:%S')) AS min_decision_time,
          max(strftime(decision_time, '%H:%M:%S')) AS max_decision_time,
          count(DISTINCT label_id) AS label_count,
          count(DISTINCT trade_date) AS trade_date_count
        FROM read_parquet('{sql_path(temp_dest)}')
        """
    ).fetchone()
    rth_check = {
        "row_count": int(rth[0] or 0),
        "invalid_decision_rows": int(rth[1] or 0),
        "min_decision_time": rth[2],
        "max_decision_time": rth[3],
        "label_count": int(rth[4] or 0),
        "trade_date_count": int(rth[5] or 0),
        "rth_window_local_timestamp_rendering": "21:30:00-04:00:00 Asia/Taipei display for stored TIMESTAMPTZ",
    }
    if rth_check["invalid_decision_rows"]:
        temp_dest.unlink(missing_ok=True)
        raise RuntimeError(f"daily_forward.parquet failed RTH decision_time validation: {rth_check}")

    temp_dest.replace(daily_dest)
    coverage.to_csv(DIAG / "daily_forward_coverage.csv", index=False)
    (DIAG / "daily_forward_rth_verified.json").write_text(json.dumps(rth_check, indent=2), encoding="utf-8")
    label_ids = sorted(coverage["label_id"].astype(str).unique().tolist())
    for label_id in label_ids:
        write_daily_contract(label_id)
    manifest = {
        "source": str(BARS_ROOT),
        "expected_dates": EXPECTED_DATES,
        "daily_label_count": len(label_ids),
        "daily_labels": label_ids,
        "rth_check": rth_check,
        "price_source": "bars_1m close/open",
        "tail_missing_policy": "label_id coverage is reported separately; unavailable N+offset exits are absent from that label_id denominator",
    }
    (DIAG / "daily_label_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
