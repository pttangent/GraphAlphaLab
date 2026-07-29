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
5. **Volatility report**
   - Keeps IC, ICIR, daily sign consistency, FDR, quantile monotonicity and target spreads.
   - Compares `graph_forward` against both `node_baseline` and `graph_reverse_placebo`.
   - Deliberately removes return-P&L, transaction-cost, Sharpe, drawdown and VaR fields from the final volatility report.
   - The final report has its own source-anchored artifact checkpoint and publishes `_SUCCESS` last.

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

## Main outputs

```text
<output>/labels/trade_date=YYYY-MM-DD/data.parquet
<output>/labels/trade_date=YYYY-MM-DD/checkpoint.json
<output>/contracts/horizon_manifest.json
<output>/contracts/checkpoint.json
<output>/_checkpoints/forward_volatility/preflight/
<output>/_checkpoints/forward_volatility/label_build/progress.json
<output>/raw_volatility_diagnostic/_checkpoints/dual_theme_alpha/
<output>/prediction_report/prediction_metrics.parquet
<output>/prediction_report/variant_comparison.parquet
<output>/prediction_report/layer_target_summary.parquet
<output>/prediction_report/quantile_targets.parquet
<output>/prediction_report/daily_ic.parquet
<output>/prediction_report/checkpoint.json
<output>/prediction_report/REPORT.md
<output>/_SUCCESS
```

## Interpretation

A layer is marked `graph_incremental_confirmed` only when:

- the sample is sufficient;
- graph-forward FDR q-value is at most 0.05;
- daily IC sign consistency is at least 0.55;
- graph-forward absolute IC exceeds reverse-placebo absolute IC by more than 0.002; and
- graph-forward absolute IC exceeds node-baseline absolute IC.

This validates incremental prediction of future stock volatility. It does not yet prove that implied volatility is mispriced or that an option strategy is profitable.
