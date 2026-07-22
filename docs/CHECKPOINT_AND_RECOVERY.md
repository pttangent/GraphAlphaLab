# Checkpoint, Recovery and Progress Contract

GraphAlphaLab long-running warehouse jobs must not treat a final `_SUCCESS` marker as a checkpoint. `_SUCCESS` means the complete report contract passed. Recoverable work is stored below `_checkpoints` in independently validated units.

## Checkpoint units

| Operation | Recovery unit | Location |
|---|---|---|
| P1 report | one trade date | `<report>/_checkpoints/p1/date=YYYY-MM-DD/` |
| Global Alpha report | one complete factor identity | `<report>/_checkpoints/alpha/factor-<hash>/` |
| Legacy theme purity | one trade date | `<report>/_checkpoints/theme/date-<hash>/` |
| GFF signal export | one P0 partition and variant | beside `data.parquet` as `data.checkpoint.json` |
| Daily label build | one base trade date | `labels/daily_forward_parts/date=YYYY-MM-DD/` |

A checkpoint is reusable only when all of the following agree:

1. stage and unit identity;
2. current run contract hash, including code lineage and parameters;
3. source hash or source file records;
4. required output file existence;
5. recorded and actual output SHA-256;
6. recorded and actual byte count;
7. recorded and actual row count where applicable.

A directory or Parquet file without a valid checkpoint is not considered complete and is rebuilt automatically.

## P1 recovery

Use the scheduler:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File scripts\gal_warehouse\run_p1_scheduler.ps1 `
  -MaxParallel 3 `
  -MaxAttempts 5
```

The scheduler invokes `run_resumable_p1_report.py`. Each trading date is evaluated through the existing governed `gal p1-report` implementation. A crash while processing one date preserves every previously completed date. The final report is produced only after all required child reports and their hashes validate.

To intentionally discard all P1 checkpoints, run the Python entrypoint with `--reset-checkpoints`. Do not delete a partial report merely to resume it.

## Alpha recovery

`run_alpha_scheduler.ps1` and `run_daily_alpha_scheduler.ps1` invoke `run_resumable_alpha_report.py` for global Alpha reports. PIT auditing is performed before factor execution. Each factor then writes its metrics, IC, quantile returns, portfolio returns and stability outputs atomically.

After recovery, pooled FDR and final governance gates are recomputed across all factors. A q-value stored inside a single-factor checkpoint is never treated as the final pooled q-value.

Within-theme Alpha currently uses report-level scheduler retry. Its output is removed and rebuilt on a failed attempt; global Alpha has the stronger factor-level checkpoint contract.

## Label recovery

`build_daily_labels_only.py` creates one shard per base trade date. The shard uses bars from the base session through the next seven expected sessions. A change to any required source bar file or to label-building code invalidates that shard.

After all date shards validate, the script atomically combines them into `labels/daily_forward.parquet`, reruns RTH validation and regenerates label contracts.

## Progress and dashboards

Every resumable report writes:

- `progress.json`: machine-readable state;
- `DASHBOARD.md`: human-readable unit table;
- per-unit checkpoint metadata;
- final `_SUCCESS` only after complete aggregation.

Schedulers additionally write:

```text
GAL_warehouse/logs/<scheduler>/DASHBOARD.json
GAL_warehouse/logs/<scheduler>/DASHBOARD.md
GAL_warehouse/logs/<scheduler>/attempts.json
GAL_warehouse/logs/<scheduler>/scheduler.log
```

Scheduler states are:

- `waiting_input`: required upstream data is absent;
- `retryable`: inputs exist and no active process or final success is present;
- `running`: a matching process is active;
- `complete`: final `_SUCCESS` exists;
- `exhausted`: maximum attempts reached.

The presence of an incomplete output directory no longer blocks restart.

## Failure policy

Schedulers persist attempt counts and restart inactive incomplete jobs until `MaxAttempts` is reached. When all runnable jobs have exhausted attempts, the scheduler exits with an error rather than waiting forever.

Before manually resetting checkpoints, inspect:

1. the report-level `progress.json`;
2. the scheduler dashboard;
3. the relevant stderr log;
4. the failed checkpoint or child report.

Use `--reset-checkpoints` only after a deliberate contract or source reset. Normal OOM, process termination and machine reboot require no cleanup.

## Current boundary

This branch contains governed recovery for P1 reporting, global intraday and daily Alpha, legacy theme purity, GFF signal export and the checked-in daily label builder. `run_within_theme_alpha.py` still recovers at whole-report granularity. Warehouse-only scripts that are not checked into this repository, including local EOD or intraday label builders, are not silently treated as fixed; they must adopt the same shard and checkpoint contract separately.
