# Dual-Theme Alpha global DAG + factor checkpoint runbook

## Fixed identity

```text
Repository: ptangent/GraphAlphaLab
Branch: agent/dual-theme-factor-checkpoint
Base semantic implementation: 2d6902b74ee694f252ed7cac099e1b31143494b7
GFF campaign: D:\G4C\campaign=c4_260701_260722_induced_v2
Signals: D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\GAL_alpha\campaign=c4_260701_260722_induced_v2\signals
Reports: D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\GAL_alpha\campaign=c4_260701_260722_induced_v2\reports
```

Do not run two Alpha processes against the same report directory concurrently.

## Scheduler contract

The default scheduler is a global factor DAG:

```text
ready node = horizon × scope × factor
default factor workers = 6
scope scheduling barrier = none
horizon scheduling barrier = none
```

All runnable factors from all six horizons and all three scopes enter one bounded ready queue. A completed task immediately releases one worker slot, which is filled by the next globally ready factor. The scheduler does not wait for a scope or horizon to finish before starting work elsewhere.

Dependencies are only:

```text
factor node
  -> horizon reducer, after every factor belonging to that horizon is complete
  -> final campaign reducer, after all horizon reducers are complete
```

The queue is round-robin interleaved across `horizon × scope` buckets so the first worker wave is not monopolized by one scope or one horizon.

## Resource contract

`MemoryLimitGb` and `Threads` are total run budgets, not per-worker budgets.

With:

```text
FactorWorkers = 6
MemoryLimitGb = 64
Threads = 12
```

each factor process receives approximately:

```text
10.67 GB DuckDB memory limit
2 DuckDB threads
```

Each process uses its own DuckDB connection and a PID-specific spill directory below `TempDirectory`. This avoids multiplying the total budget by six.

## Checkpoint contract

```text
L1 horizon:
reports\horizon=<h>\horizon_checkpoint.json
reports\horizon=<h>\_SUCCESS

L3 factor:
reports\_checkpoints\dual_theme_alpha\
  horizon=<h>\scope=<scope>\factor-<hash>\checkpoint.json
```

Each factor checkpoint contains:

```text
metrics.parquet
ic_series.parquet
daily_ic.parquet
quantile_returns.parquet
portfolio_returns.parquet
stability.parquet
checkpoint.json
```

Reuse requires matching:

- mathematical checkpoint contract hash;
- scope and factor identity;
- input file size and SHA-256;
- output file size and SHA-256;
- Parquet row counts;
- complete status.

Worker count, worker memory split, thread split, and spill directory are execution choices. They do not invalidate mathematically equivalent factor checkpoints.

The global DAG keeps compatibility with factor checkpoints written by the earlier scope-sequential factor-checkpoint implementation because it preserves the same checkpoint path, stage, unit identity, and source-hash contract.

## Git

```powershell
cd D:\DEV\AnotherNetworkFactory\GraphAlphaLab

if (git status --porcelain) {
    throw "Working tree is dirty. Do not reset or overwrite local changes."
}

git fetch origin --prune
git switch agent/dual-theme-factor-checkpoint
git pull --ff-only origin agent/dual-theme-factor-checkpoint

$Head = (git rev-parse HEAD).Trim()
Write-Host "GAL checkpoint commit: $Head"
```

Use the existing Python 3.11 environment:

```powershell
& D:\GAL\venv311\Scripts\Activate.ps1
python -m pip install -e ".[test]"
python -m compileall -q src scripts tests
python -m pytest -q --tb=short
```

## July campaign command

Keep `CorrelationSampleModulus=0` to remain compatible with the completed bundles produced by the pinned July attempts.

```powershell
$Campaign = "D:\G4C\campaign=c4_260701_260722_induced_v2"
$Signals = "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\GAL_alpha\campaign=c4_260701_260722_induced_v2\signals"
$Reports = "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\GAL_alpha\campaign=c4_260701_260722_induced_v2\reports"
$Manifest = "D:\GAL\contracts\dual_theme_horizons.c4_260701_260722_induced_v2.json"
$Metadata = "D:\GAL\metadata\symbol_metadata.with_symbol_id.parquet"
$Temp = "D:\GAL\duckdb_tmp\c4_260701_260722_induced_v2"

.\scripts\run_dual_theme_alpha_20260102.ps1 `
  -GffCampaignRoot $Campaign `
  -HorizonManifest $Manifest `
  -SignalsOutput $Signals `
  -ReportOutput $Reports `
  -Metadata $Metadata `
  -MemoryLimitGb 64 `
  -Threads 12 `
  -FactorWorkers 6 `
  -TempDirectory $Temp `
  -MinCrossSection 100 `
  -MinThemeSize 5 `
  -MinThemeCrossSection 5 `
  -CorrelationSampleModulus 0
```

Do not add `-ForceExport`, `-AllowPartial`, or `-SkipCleanCheck` for a governed run.

## Restart behavior

Rerun the exact same command.

The scheduler first validates:

1. complete horizon bundles;
2. factor checkpoints across every horizon and scope;
3. only invalid or missing factor nodes enter the ready queue.

A process failure can lose at most the factor tasks that were actively running. By default that is at most six factors, not a full scope or horizon.

A corrupt checkpoint is rebuilt individually. Do not delete the full `_checkpoints` directory.

## Monitoring

Global DAG:

```text
reports\_checkpoints\dual_theme_alpha\global_dag_progress.json
reports\_checkpoints\dual_theme_alpha\GLOBAL_DAG.md
```

The dashboard reports:

- total, completed, reused, failed, and remaining tasks;
- six active factor nodes;
- progress by horizon;
- explicit `scope_barrier=false`;
- explicit `horizon_barrier=false`;
- last completed factor.

Factor checkpoints remain under their horizon/scope paths for inspection.

## Final acceptance

Required:

```text
reports\_SUCCESS
reports\summary.json: complete=true
reports\summary.json: scheduler.mode=global_factor_dag
reports\summary.json: scheduler.factor_workers=6
reports\summary.json: scheduler.scope_barrier=false
reports\summary.json: scheduler.horizon_barrier=false
all six horizon summaries complete
318 factors per horizon
zero PIT violations
```
