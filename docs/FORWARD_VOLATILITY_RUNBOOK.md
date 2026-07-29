# Forward-volatility label and prediction patch

This branch adds a governed sidecar workflow above an already completed GAL dual-theme signal export. It does not mutate NFF, GFF, the existing signal export, or the currently running return-label checkpoints.

## Stages

1. **Preflight decision inventory**
   - Anchored to the math-relevant fields in `signals/export_manifest.json`.
   - Caches one distinct `(trade_date, decision_time, symbol_id)` Parquet per date.
   - Validates cached Parquet SHA-256 and row count before reuse.
2. **Forward-volatility label DAG**
   - One atomic checkpoint per trading date.
   - Reads `timestamp`, `symbol_id`, `ret_1m`, and `available_time_1m` from NFF `gff_core`.
   - Builds 30/60/120/180-minute forward labels after a one-minute entry lag.
   - `label_available_time` is the later of the contractual exit and the latest upstream `available_time_1m` used by that label.
   - Requires an exact, non-truncated future window. An incomplete past window only disables `forward_vol_expansion`; it does not discard otherwise valid forward-RV labels.
   - Validates duplicate keys, entry/exit ordering, label availability, exact tails, and minimum future observation coverage.
   - A broken process pool steps down worker count and requeues every uncommitted date.
3. **Contract publication**
   - Publishes one Label Contract per target × horizon and a generated horizon manifest.
   - The contract checkpoint is anchored to all daily label-file fingerprints and validates every emitted contract artifact before reuse.
4. **Layer prediction DAG**
   - Calls the current `dual_theme_global_dag` implementation, so scope/factor inventory preflight caches, PIT audits, factor checkpoints, round-robin scheduling and BrokenProcessPool healing are identical to the existing 77-day workflow.
   - Uses `own_score` as the node control and evaluates Global, Within-theme and Inter-theme scope semantics.
5. **Complete volatility discussion pack**
   - Retains every factor and every variant, including non-significant, insufficient and rejected results.
   - Separates predictive statistics, sample coverage, governance fields, generic evaluator diagnostics, daily IC, quantiles and stability slices into small narrow shards.
   - Compares every available `graph_forward` result against `node_baseline` and `graph_reverse_placebo`.
   - Records explicit non-confirmation reasons instead of reporting only successful candidates.
   - Audits label coverage, missingness, future-window observations, past-window availability and source-availability delay.
   - Catalogues large raw snapshot evidence such as `ic_series.csv` without duplicating it into another large file.
   - The report checkpoint is anchored to all consumed raw reports and label Parquets; `_SUCCESS` is published last.

## Entry point

```powershell
.\scripts\run_forward_volatility_77d.ps1 `
  -SignalsRoot "D:\GAL\signals\77d_dual_theme" `
  -GffCoreTemplate "D:\US-Stock\NFF\warehouse\features\gff_core\schema=v1\date={date}\data.parquet" `
  -OutputRoot "D:\GAL\forward_volatility_77d" `
  -StartDate "2026-01-05" `
  -EndDate "2026-07-22"
```

Supported template variables are `{date}`, `{yyyymmdd}`, `{year}`, and `{month}`.

Re-run the identical command after interruption. Valid decision caches, date-label checkpoints, contract artifacts, horizon preflight caches, factor checkpoints and the final report checkpoint are reused. Only incomplete or fingerprint-mismatched units are recomputed.

## Default targets

```text
forward_log_rv
forward_vol_expansion
forward_downside_semivar
forward_downside_share
forward_jump_var
forward_jump_share
forward_max_abs_return
forward_max_drawdown
forward_tail_event
```

`forward_tail_event` is a cross-sectional label: a stock is marked as a tail event when its future log realized variance is in the top 10% at the same decision time and horizon.

## Report structure

```text
<output>/prediction_report/
├── REPORT.md
├── README_UPLOAD.md
├── DATA_DICTIONARY.md
├── ANALYSIS_GUIDE.md
├── summary.json
├── checkpoint.json
├── _SUCCESS
├── overview/
│   ├── factor_master_compact.csv
│   ├── target_horizon_scope_summary.csv
│   ├── layer_target_horizon_summary.csv
│   ├── factor_status_counts.csv
│   ├── matched_variant_comparison.csv
│   ├── strongest_weakest_graph_increments.csv
│   ├── non_confirmation_reason_counts.csv
│   ├── label_date_coverage.csv
│   ├── label_distribution_by_date.csv
│   └── label_distribution_summary.csv
├── factor_metrics/target_kind=.../horizon_minutes=.../scope=.../predictive_metrics.csv
├── factor_coverage/target_kind=.../horizon_minutes=.../scope=.../coverage_metrics.csv
├── factor_governance/target_kind=.../horizon_minutes=.../scope=.../governance_metrics.csv
├── generic_evaluator_diagnostics/.../generic_evaluator_diagnostics.csv
├── daily_ic/target_kind=.../horizon_minutes=.../scope=.../daily_ic.csv
├── quantiles/target_kind=.../horizon_minutes=.../scope=.../quantile_targets.csv
├── stability/target_kind=.../horizon_minutes=.../scope=.../stability_slices.csv
├── variant_comparisons/target_kind=.../horizon_minutes=.../scope=.../matched_variants.csv
├── target_reports/target=<target>/
│   ├── REPORT.md
│   ├── summary.csv
│   └── matched_variant_comparison.csv
└── indexes/
    ├── predictive_metric_shard_index.csv
    ├── coverage_metric_shard_index.csv
    ├── governance_metric_shard_index.csv
    ├── generic_evaluator_shard_index.csv
    ├── daily_ic_shard_index.csv
    ├── quantile_shard_index.csv
    ├── stability_shard_index.csv
    ├── variant_comparison_shard_index.csv
    ├── target_report_index.csv
    ├── raw_evidence_index.csv
    └── UPLOAD_MANIFEST.csv
```

The pack intentionally avoids a single giant wide table. `factor_master_compact.csv` is a complete narrow navigation table, while detailed metric families are split by target × horizon × scope. `UPLOAD_MANIFEST.csv` records file size, row count, content role and upload priority.

## Recommended upload order for later discussion

1. `REPORT.md`
2. `summary.json`
3. the complete `overview/` directory
4. `indexes/UPLOAD_MANIFEST.csv`
5. only the detailed shards requested during analysis

Large raw evidence remains in `raw_volatility_diagnostic`; `indexes/raw_evidence_index.csv` tells the analyst where each raw file is located and what it is used for.

## Interpretation

A matched layer is marked `graph_incremental_confirmed` only when:

- the graph-forward sample is sufficient;
- graph-forward FDR q-value is at most 0.05;
- daily IC sign consistency is at least 0.55;
- graph-forward absolute IC exceeds reverse-placebo absolute IC by more than 0.002; and
- graph-forward absolute IC exceeds node-baseline absolute IC.

Every failed matched group is still retained and receives one or more explicit reasons such as `sample_insufficient`, `fdr_not_passed`, `graph_not_stronger_than_node`, or `graph_not_clearly_stronger_than_reverse`.

The generic return evaluator still emits portfolio and cost fields internally. They are retained in a separate audit shard but are not interpreted as option P&L. This workflow validates incremental prediction of future stock volatility; it does not yet prove that implied volatility is mispriced or that an option strategy is profitable.
