# Dual-Theme Alpha factor checkpoint runbook

## Fixed identity

```text
Repository: ptangent/GraphAlphaLab
Branch: agent/dual-theme-factor-checkpoint
Base/pinned semantic implementation: 2d6902b74ee694f252ed7cac099e1b31143494b7
GFF campaign: D:\G4C\campaign=c4_260701_260722_induced_v2
Signals: D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\GAL_alpha\campaign=c4_260701_260722_induced_v2\signals
Reports: D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\GAL_alpha\campaign=c4_260701_260722_induced_v2\reports
```

Do not run the old and new Alpha processes against the same report directory concurrently. Let the current attempt finish or stop it cleanly before starting this branch.

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

## Resume command for the July campaign

The July pinned attempts used `correlation_sample_modulus=0`. Keep the same value so compatible completed horizon bundles can be reused.

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
  -TempDirectory $Temp `
  -MinCrossSection 100 `
  -MinThemeSize 5 `
  -MinThemeCrossSection 5 `
  -CorrelationSampleModulus 0 `
  -FactorWorkers 4
```

`FactorWorkers` defaults to `4`, so the final line is explicit but optional.

Do not add `-ForceExport`, `-AllowPartial`, or `-SkipCleanCheck` for the governed run.

## Parallel execution contract

Scopes remain sequential:

```text
global -> within_theme -> inter_theme
```

Inside the active scope, factors run with four parallel workers by default. Every worker has:

- an independent DuckDB connection;
- an independent spill subdirectory;
- an independent factor checkpoint directory;
- read-only access to signals, labels and metadata.

The configured `MemoryLimitGb` and `Threads` are total budgets, not per-worker budgets. They are divided across active workers. With the July command:

```text
Total: 64 GB, 12 DuckDB threads, 4 factor workers
Per worker: 16 GB, 3 DuckDB threads
```

The scheduler automatically reduces workers when there are fewer factors, fewer total threads, or insufficient memory. Changing only worker count, total threads, memory or spill path does not invalidate mathematically equivalent factor checkpoints.

## Checkpoint hierarchy

```text
L1: reports\horizon=<h>\_SUCCESS
L2/L3: reports\_checkpoints\dual_theme_alpha\horizon=<h>\scope=<scope>\factor-<hash>\checkpoint.json
```

Each factor directory contains validated Parquet frames:

```text
metrics.parquet
ic_series.parquet
daily_ic.parquet
quantile_returns.parquet
portfolio_returns.parquet
stability.parquet
checkpoint.json
```

A factor is reused only when all of the following match:

- mathematical execution contract hash;
- source hash and factor identity;
- file SHA-256 and byte count;
- Parquet row count;
- complete checkpoint status.

A corrupt or parameter-incompatible checkpoint is rebuilt automatically. Never delete all checkpoints merely because one unit is invalid.

## Current pinned-attempt compatibility

Completed horizon bundles from the pinned `2d6902b` run can be reused without factor checkpoints only when:

- horizon `_SUCCESS` exists;
- report summary is complete;
- exactly 318 factor identities are present;
- export manifest, label contracts, label files and metadata hashes match;
- the original pinned/default parameters match;
- `CorrelationSampleModulus` remains `0`.

An incomplete horizon from the old attempt has no factor checkpoints and must run once under the new branch. After the first new factor completes, every subsequent completed factor is restartable.

## Monitoring

Per-scope progress is written to:

```text
reports\_checkpoints\dual_theme_alpha\horizon=<h>\scope=<scope>\progress.json
reports\_checkpoints\dual_theme_alpha\horizon=<h>\scope=<scope>\DASHBOARD.md
```

After a restart, verify `reused_units` increases and `factor_worker_plan.workers` is `4` for a normal 64 GB / 12-thread run.

Because four factors can be in flight simultaneously, an abrupt process kill can require recomputing at most the four factors that had not yet atomically committed. All previously committed factors are reused.

## Final acceptance

Required:

```text
reports\_SUCCESS
reports\summary.json: complete=true
reports\summary.json: checkpoint_granularity=horizon_and_scope_factor
reports\summary.json: factor_workers=4
all six horizon summaries complete
318 factors per horizon
zero PIT violations
```
