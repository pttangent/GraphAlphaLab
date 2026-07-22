# GAL Warehouse Schedulers

Operational scripts used for the 2026-06-01 through 2026-07-17 GFF-to-GAL run.

Default local roots:

- GraphAlphaLab worktree: `C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67`
- GAL warehouse: `D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse`
- GFF warehouse: `D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse`
- NFF bars: `D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1`

Scripts:

- `run_alpha_scheduler.ps1`: schedules existing global Interaction alpha plus lagged Similarity-Leiden within-theme alpha, with retries and a scheduler dashboard.
- `run_resumable_alpha_report.py`: evaluates global Alpha one factor at a time and atomically checkpoints every complete factor.
- `run_within_theme_alpha.py`: joins each IG27/RM14 signal at time `t` to the latest canonical `similarity_consensus` membership strictly before `t`, ranks graph scores inside each theme, builds theme-neutral long/short portfolios, and writes factor-, decision- and theme-level results.
- `run_p1_scheduler.ps1`: schedules IG27, RM14 and Similarity10 P1 reports with date-level recovery.
- `run_resumable_p1_report.py`: invokes the governed P1 report one trading date at a time, validates each child bundle, then aggregates the final report.
- `run_resumable_theme_report.py`: date-checkpointed compatibility path for the legacy `theme-report` purity workflow.
- `build_daily_labels_only.py`: rebuilds daily labels from canonical `bars_1m` using one validated checkpoint per base trade date and writes RTH verification.
- `run_daily_label_build.ps1`: wrapper for `build_daily_labels_only.py`.
- `run_daily_alpha_scheduler.ps1`: schedules factor-checkpointed daily Alpha reports from verified daily label contracts.
- `run_daily_pipeline.ps1`: builds daily labels, validates RTH, then starts the daily Alpha scheduler.

## Recovery contract

An incomplete output directory is no longer treated as a reason to skip a job. Schedulers restart inactive jobs that do not have final `_SUCCESS`, up to `MaxAttempts`.

Every resumable report writes:

```text
progress.json
DASHBOARD.md
_checkpoints\...
_SUCCESS              # only after complete aggregation
```

Schedulers write combined state under their log directory:

```text
attempts.json
DASHBOARD.json
DASHBOARD.md
scheduler.log
```

Valid checkpoints bind the current run contract, source records and output hashes. A stale, damaged or parameter-incompatible checkpoint is rebuilt automatically. Machine reboot, OOM and process termination require no manual deletion.

See `docs/CHECKPOINT_AND_RECOVERY.md` for the complete contract.

## Intraday scheduling contract

`run_alpha_scheduler.ps1` does not impose a false serial dependency between Similarity and Interaction production:

- global IG27/RM14 Alpha jobs may continue while Similarity P1 is still running;
- within-theme jobs wait until at least one `memberships.parquet` exists below the configured Similarity P1 root;
- once memberships are available, fixed-direction `trade_intensity_to_volatility` tests at 30m/60m/120m have first priority;
- broad graph-forward within-theme discovery for IG27/RM14 then runs at 30m/60m/120m;
- default parallelism is three 8-thread / 24GB jobs, matching the 24-core / 128GB host more safely than ten simultaneous jobs.

Run with the default membership root:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File scripts\gal_warehouse\run_alpha_scheduler.ps1 `
  -MaxParallel 3 `
  -MaxAttempts 5
```

Override the Similarity P1 root when needed:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File scripts\gal_warehouse\run_alpha_scheduler.ps1 `
  -ThemeMemberships D:\GFF\similarity_p1\p1 `
  -MaxParallel 3 `
  -MaxAttempts 5
```

The formal trade-intensity jobs use `factor_id=trade_intensity_to_volatility__graph_forward` with a predeclared negative direction. Broader jobs retain `auto` direction and are discovery-only.

Daily Alpha is guarded by `label_diagnostics\daily_forward_rth_verified.json`; it will not start when labels were generated with the wrong timestamp window.
