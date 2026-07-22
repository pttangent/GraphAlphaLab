# GAL Warehouse Schedulers

Operational scripts used for the 2026-06-01 through 2026-07-17 GFF-to-GAL run.

Default local roots:

- GraphAlphaLab worktree: `C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67`
- GAL warehouse: `D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse`
- GFF warehouse: `D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse`
- NFF bars: `D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1`

Scripts:

- `run_alpha_scheduler.ps1`: keeps intraday Interaction alpha reports filled up to the configured parallelism.
- `run_p1_scheduler.ps1`: schedules IG27, RM14 and Similarity10 P1/theme discovery reports.
- `build_daily_labels_only.py`: rebuilds daily labels from canonical `bars_1m` only and writes RTH verification.
- `run_daily_label_build.ps1`: wrapper for `build_daily_labels_only.py`.
- `run_daily_alpha_scheduler.ps1`: schedules daily alpha reports from verified daily label contracts.
- `run_daily_pipeline.ps1`: builds daily labels, validates RTH, then starts the daily alpha scheduler.

Daily alpha is guarded by `label_diagnostics\daily_forward_rth_verified.json`; it will not start when labels were generated with the wrong timestamp window.
