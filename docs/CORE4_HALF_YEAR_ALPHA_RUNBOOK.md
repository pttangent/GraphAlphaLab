# Core4 Half-Year Alpha Runbook

This runbook is for a local agent that must consume the governed GraphFactorFactory v2 Core4 campaign and produce GraphAlphaLab reports for:

```text
GFF branch: agent/core4-batched-halfyear
GFF campaign: c4_260105_260722_induced_v2
GAL branch: agent/dual-theme-alpha-reporting
Expected dates: 2026-01-05 through 2026-07-22
Expected XNYS sessions: 137
Expected GFF DAG tasks: 30,143 = 137 × 220 + 3
Expected IGC scope contracts per date: 106
Expected GAL factor identities per horizon: 318 = 106 × 3 variants
```

## 1. Do not start Alpha until the GFF campaign is truly half-year complete

The GFF script is named `run_core4_20260105_20260722.ps1`, but an earlier checked-in version still contained:

```powershell
$StartDate = "2026-07-01"
$EndDate = "2026-07-22"
$ExpectedSessions = 15
$ExpectedDagTasks = 3303
```

Before running or resuming GFF, inspect the actual file on branch `agent/core4-batched-halfyear`. For the requested half-year campaign it must contain:

```powershell
$StartDate = "2026-01-05"
$EndDate = "2026-07-22"
$ExpectedSessions = 137
$ExpectedDagTasks = 30143
```

Do not trust the filename. Trust `runs/campaign_contract.json`, `runs/campaign_summary.json`, and the final `_SUCCESS` marker.

The GFF command is:

```powershell
powershell -ExecutionPolicy Bypass -File ".\scripts\run_core4_20260105_20260722.ps1" `
  -RepoRoot "D:\DEV\AnotherNetworkFactory\GraphFactorFactory_v2" `
  -GffCoreTemplate "D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\features\gff_core\schema=v1\date={date}\data.parquet" `
  -NffManifestTemplate "D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\manifests\date={date}.json" `
  -OutputRoot "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse" `
  -ResearchCacheRoot "D:\G4C_cache" `
  -CampaignId "c4_260105_260722_induced_v2" 2>&1
```

Normal resume must omit `-Force`. Valid checkpoints should be reused.

## 2. Required GFF contract

The local agent must reject the campaign unless all of the following are true:

```text
campaign_version = SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN
Consensus = disabled
Theme families = momentum_state, residual_return
Global IGC = computed once
Within-Theme mode = induced_global_final_edges
Within-Theme local re-estimation = false
Within-Theme P1 = rebuilt from the induced edge graph
Inter-Theme = aggregate cross-theme edges from shared Global P0
Dates = 137, first 2026-01-05, last 2026-07-22
Per-date scope contracts = 26 Global + 20×4 scoped = 106
```

The GAL half-year runner checks these conditions before doing any export.

## 3. Prepare governed labels

GAL owns labels. GFF outputs must remain graph-only.

Create one label dataset and one label contract per horizon. Recommended research horizons are:

```text
5m, 15m, 30m, 60m, 120m, 180m
```

Every label row must contain at least:

```text
trade_date
decision_time
symbol_id
label_id
target_return
entry_time
exit_time
label_available_time
```

PIT rules:

```text
entry_time > decision_time
exit_time > entry_time
label_available_time >= exit_time
exit_time - entry_time = governed horizon
one unique label row per trade_date × decision_time × symbol_id × label_id
```

A typical return label contract is:

```json
{
  "label_id": "forward_return_30m",
  "horizon_minutes": 30,
  "entry_lag_minutes": 1,
  "target_column": "target_return",
  "decision_time_column": "decision_time",
  "entry_time_column": "entry_time",
  "exit_time_column": "exit_time",
  "available_time_column": "label_available_time",
  "overlapping": true,
  "rebalance_minutes": null,
  "horizon_tolerance_seconds": 60,
  "require_entry_after_decision": true
}
```

`overlapping=true` means annualized Sharpe is not governance-valid. It may still be shown as a diagnostic, but it must not be promoted.

Create a horizon manifest such as:

```json
{
  "horizons": [
    {
      "name": "5m",
      "labels": "D:\\GAL\\labels\\forward_5m.parquet",
      "label_contract": "D:\\GAL\\contracts\\forward_5m.json"
    },
    {
      "name": "15m",
      "labels": "D:\\GAL\\labels\\forward_15m.parquet",
      "label_contract": "D:\\GAL\\contracts\\forward_15m.json"
    },
    {
      "name": "30m",
      "labels": "D:\\GAL\\labels\\forward_30m.parquet",
      "label_contract": "D:\\GAL\\contracts\\forward_30m.json"
    },
    {
      "name": "60m",
      "labels": "D:\\GAL\\labels\\forward_60m.parquet",
      "label_contract": "D:\\GAL\\contracts\\forward_60m.json"
    },
    {
      "name": "120m",
      "labels": "D:\\GAL\\labels\\forward_120m.parquet",
      "label_contract": "D:\\GAL\\contracts\\forward_120m.json"
    },
    {
      "name": "180m",
      "labels": "D:\\GAL\\labels\\forward_180m.parquet",
      "label_contract": "D:\\GAL\\contracts\\forward_180m.json"
    }
  ]
}
```

Do not fill missing late-session labels with future or next-session observations. Missing horizons near the close must remain missing unless the label contract explicitly defines an overnight target.

## 4. The three Alpha units are economically different

### Global Alpha

```text
Unit: stock
Cross-section: all eligible stocks at a decision time
Signal: Global graph score
Target: forward stock return
```

### Within-Theme Alpha

```text
Unit: stock inside its canonical theme
Signal treatment: rank/residualize graph score inside decision_time × theme_id
Control: own_score, so the result tests graph increment beyond the node baseline
Target treatment: subtract the same-theme mean forward return
Interpretation: stock selection within a theme, not sector/theme allocation
```

A theme must contain at least `MinThemeSize` eligible stocks. The default is 5.

### Inter-Theme Alpha

```text
Unit: theme portfolio
Signal: one graph score per theme
Target: membership-weighted forward return of the theme's member stocks
Cross-section: themes at the same decision time
Interpretation: theme allocation / rotation, not stock selection
```

GAL refuses to rank the broadcast stock rows directly. All members of one theme carry the same theme signal, so stock-level tie-breaking would be arbitrary and financially invalid.

Inter-Theme turnover is currently measured at theme-portfolio notional level. Constituent migration and basket execution costs need a separate execution overlay before promotion.

## 5. Financial-semantic governance

The Core4 registry includes different economic targets:

```text
*_to_return
*_to_volatility
*_to_liquidity
*_to_price_impact
```

When the label is a forward return:

```text
*_to_return          -> direct return Alpha candidate
all other targets    -> risk/liquidity regime or cross-domain diagnostic
```

GAL writes them separately:

```text
direct_return_alpha_metrics.csv
regime_candidate_metrics.csv
financial_semantics_summary.csv
```

A statistically strong volatility or liquidity layer must not be described as direct return Alpha. It may be used later as:

```text
regime switch
position-sizing variable
risk-budget modifier
conditional Alpha interaction
execution/liquidity filter
```

## 6. Run GAL

Use the governed runner added to GAL:

```powershell
powershell -ExecutionPolicy Bypass -File ".\scripts\run_core4_alpha_20260105_20260722.ps1" `
  -RepoRoot "D:\DEV\AnotherNetworkFactory\GraphAlphaLab" `
  -GffCampaignRoot "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=c4_260105_260722_induced_v2" `
  -HorizonManifest "D:\GAL\contracts\dual_theme_horizons.json" `
  -SignalsOutput "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\signals\c4_260105_260722_induced_v2" `
  -ReportOutput "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\reports\c4_260105_260722_induced_v2" `
  -Metadata "D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\reference\symbol_metadata.parquet"
```

The script performs:

```text
1. GFF campaign contract/date/version preflight
2. GAL branch and clean-worktree check
3. editable install
4. compileall
5. full pytest suite
6. governed signal export
7. PIT audit
8. Global / Within / Inter multi-horizon Alpha
9. factor-count and output-contract validation
```

Do not use `-AllowPartial` for the final report. It is only for an explicit diagnostic.

Normal resume does not use `-ForceExport`; existing governed signal files are reused. Use `-ForceExport` only after exporter code, scope semantics, or source GFF partitions have changed.

## 7. Required outputs

The run is not complete unless these exist:

```text
signals/_SUCCESS
signals/export_manifest.json
reports/_SUCCESS
reports/summary.json
reports/all_horizons_alpha_metrics.csv
reports/direct_return_alpha_metrics.csv
reports/regime_candidate_metrics.csv
reports/global_alpha_metrics.csv
reports/within_theme_alpha_metrics.csv
reports/inter_theme_alpha_metrics.csv
reports/cross_scope_comparison.csv
reports/scope_family_horizon_summary.csv
reports/financial_semantics_summary.csv
reports/matched_variant_comparison.csv
reports/ranking.csv
reports/REPORT.md
```

Each `horizon=<name>` directory must also contain the normal governed Alpha report bundle.

## 8. Acceptance checks

The local agent must report the following values, not merely say that the command exited successfully:

```text
GFF campaign version
GFF first/last date
GFF date count
GFF contract count per date
GFF failed task count
GAL commit
GAL export version
GAL edge PIT violations
GAL factor count per horizon
Observed factor count for every horizon
Direct-return factor rows
Regime-candidate factor rows
Within/Inter insufficient-factor rates
Number of candidate / needs_falsification / insufficient rows
```

Expected structural values:

```text
GFF dates = 137
GFF per-date scope contracts = 106
GAL factors per horizon = 318
edge PIT violations = 0
all horizons complete = true
```

## 9. Interpretation order

Read results in this order:

```text
1. summary.json and financial_semantics_summary.csv
2. direct_return_alpha_metrics.csv
3. matched_variant_comparison.csv
4. cross_scope_comparison.csv
5. within_theme_alpha_metrics.csv
6. inter_theme_alpha_metrics.csv
7. regime_candidate_metrics.csv
8. stability, daily IC, portfolio returns and cost diagnostics inside each horizon bundle
```

For direct Alpha, prefer evidence that satisfies all of the following:

```text
Graph Forward improves absolute IC versus node baseline
Graph Forward improves versus reverse-edge placebo
Daily IC sign is stable across at least 20 dates
Direction was predeclared rather than chosen from the observed sample
Net result survives realistic transaction costs
Result is not concentrated in one date, hour, sector or theme family
Within/Inter result matches its stated economic unit
```

`default-direction=auto` is exploratory. It cannot satisfy promotion governance because the sign is learned from the same sample. Before a final promotion run, create and freeze a factor-direction policy based on economic logic or an earlier training period, then rerun on a separate validation period.

## 10. Failure handling

```text
Missing GFF _SUCCESS                   -> stop; resume GFF
15 dates instead of 137                -> stop; fix GFF script constants and rerun/resume
Wrong Core4 campaign version           -> stop; do not use legacy override
PIT violation                          -> stop; inspect source availability/label timing
Factor count below 318                  -> inspect empty partitions and export manifest; do not silently allow partial
Within/Inter insufficient factors       -> retain as insufficient_or_rejected; do not call them missing
Non-return layer appears as candidate   -> treat as a bug; semantic promotion eligibility must be false
Dirty GAL checkout                      -> commit/stash before governed run
Out of memory                           -> lower GAL threads or memory limit; do not concatenate all partitions in Pandas
```
