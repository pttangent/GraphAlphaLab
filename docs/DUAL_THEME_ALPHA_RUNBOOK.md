# Dual Theme, Three Scope, Multi-Horizon Alpha

This workflow consumes `GraphFactorFactory_v2` campaign version
`SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V1`.

## Input identity

The two independent theme families are:

```text
momentum_state_similarity@30m -> theme_family=momentum_state
residual_return_similarity@30m -> theme_family=residual_return
```

The Alpha identity is:

```text
theme_family × scope × layer × scale × variant × horizon
```

Global IGC is read once as `theme_family=shared_global`. Within-theme and
Inter-theme are read separately for both theme families.

Before export, GAL requires:

```text
GFF campaign _SUCCESS
runs/campaign_contract.json
dual-theme campaign version
Consensus disabled
complete scope-contract inventory for every campaign date
```

For the current full contract, the P0 inventory per date is:

```text
Global                                  26
Momentum Within-theme                   20
Momentum Inter-theme                    20
Residual Within-theme                   20
Residual Inter-theme                    20
-------------------------------------------
Total                                  106
```

## Inter-theme label alignment

GFF Inter-theme P0 contains synthetic Theme nodes. GAL does not join these
synthetic IDs directly to stock labels. It computes Theme-node scores and then
broadcasts each score to the canonical member stocks from the corresponding
Theme Scope Index at the same decision time. This preserves the economic
meaning of Inter-theme propagation while keeping the governed label join at:

```text
trade_date, decision_time, symbol_id
```

## Signal variants

Every non-empty contract is exported as:

```text
node_baseline
graph_forward
graph_reverse_placebo
```

The maximum one-day factor identity count for the current GFF contract is:

```text
106 scope-contract instances × 3 variants = 318 factors per horizon
```

Empty but valid P0 partitions remain recorded in the export manifest and are
not falsely counted as evaluable factors.

## Horizon manifest

Copy `examples/dual_theme_horizons.example.json` and replace each labels and
label-contract path. Any positive governed horizon is supported. Typical
comparison horizons are 5m, 15m, 30m, 60m, 120m and 180m.

Each label contract continues to enforce entry time, exit time, availability,
horizon duration and duplicate-key PIT rules.

## One-day execution

```powershell
.\scripts\run_dual_theme_alpha_20260102.ps1 `
  -GffCampaignRoot "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=DUAL_THEME_FULL_IGC_COMPARE_20260102" `
  -HorizonManifest "D:\GAL\contracts\dual_theme_horizons.json" `
  -SignalsOutput "D:\GAL\signals\dual_theme_20260102" `
  -ReportOutput "D:\GAL\reports\dual_theme_20260102" `
  -Metadata "D:\NFF\reference\symbol_metadata.parquet"
```

Normal resume does not use `-ForceExport`. Existing signal files are reused.
The GAL checkout is clean-governed independently of whether the temporary GFF
one-day test disabled its own Git commit gate.

## Outputs

```text
signals/export_manifest.json
signals/_SUCCESS
reports/horizon=<name>/alpha_metrics.csv
reports/horizon=<name>/daily_ic.csv
reports/horizon=<name>/portfolio_returns.csv
reports/scope_family_horizon_summary.csv
reports/matched_variant_comparison.csv
reports/ranking.csv
reports/summary.json
reports/_SUCCESS
```

`matched_variant_comparison.csv` reports graph-forward increments against both
node baseline and reverse-edge placebo for absolute IC and 5 bps net return.

`scope_family_horizon_summary.csv` is the compact comparison table across:

```text
shared Global
Momentum Within
Momentum Inter
Residual Within
Residual Inter
all supplied horizons
```

## Governance boundary

A single trading day cannot pass the existing `>=20 dates` inference gate.
The one-day result is for schema validation, mechanism comparison, runtime,
coverage and sign diagnostics. It must not be promoted as production Alpha.
