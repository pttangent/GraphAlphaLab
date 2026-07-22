# GAL Warehouse Schedulers

Operational scripts used for the 2026-06-01 through 2026-07-17 GFF-to-GAL run.

Default local roots:

- GraphAlphaLab worktree: `C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67`
- GAL warehouse: `D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse`
- GFF warehouse: `D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse`
- NFF bars: `D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1`

Scripts:

- `run_alpha_scheduler.ps1`: schedules existing global Interaction alpha plus lagged Similarity-Leiden within-theme alpha.
- `run_within_theme_alpha.py`: joins each IG27/RM14 signal at time `t` to the latest canonical `similarity_consensus` membership strictly before `t`, ranks graph scores inside each theme, builds theme-neutral long/short portfolios, and writes factor-, decision- and theme-level results.
- `run_p1_scheduler.ps1`: schedules IG27, RM14 and Similarity10 P1/theme discovery reports.
- `build_daily_labels_only.py`: rebuilds daily labels from canonical `bars_1m` only and writes RTH verification.
- `run_daily_label_build.ps1`: wrapper for `build_daily_labels_only.py`.
- `run_daily_alpha_scheduler.ps1`: schedules daily alpha reports from verified daily label contracts.
- `run_daily_pipeline.ps1`: builds daily labels, validates RTH, then starts the daily alpha scheduler.

## Intraday scheduling contract

`run_alpha_scheduler.ps1` no longer imposes a false serial dependency between Similarity and Interaction production:

- global IG27/RM14 alpha jobs may continue while Similarity P1 is still running;
- within-theme jobs wait until at least one `memberships.parquet` exists below the configured Similarity P1 root;
- once memberships are available, fixed-direction `trade_intensity_to_volatility` tests at 30m/60m/120m have first priority;
- broad graph-forward within-theme discovery for IG27/RM14 then runs at 30m/60m/120m;
- the default parallelism is three 8-thread / 24GB jobs, matching the 24-core / 128GB host more safely than ten simultaneous jobs.

Run with the default membership root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\gal_warehouse\run_alpha_scheduler.ps1
```

Override the Similarity P1 root when needed:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\gal_warehouse\run_alpha_scheduler.ps1 `
  -ThemeMemberships D:\GFF\similarity_p1\p1 `
  -MaxParallel 3
```

The formal trade-intensity jobs use `factor_id=trade_intensity_to_volatility__graph_forward` with a predeclared negative direction. The broader jobs retain `auto` direction and are discovery-only.

Daily alpha is guarded by `label_diagnostics\daily_forward_rth_verified.json`; it will not start when labels were generated with the wrong timestamp window.
