# GraphAlphaLab next architecture: induced-global graph, local execution, frequency gates

## 1. Correct computation identity

The supported GraphFactorFactory_v2 source is:

```text
repository: pttangent/GraphFactorFactory_v2
branch: agent/core4-batched-halfyear
entrypoint: scripts/run_core4_20260105_20260722.ps1
campaign id: c4_260105_260722_induced_v2
campaign root: D:\G4H\campaign=c4_260105_260722_induced_v2
research cache: D:\G4C
GFF campaign version: SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN
```

The version name contains `INDUCED_WITHIN`, but its actual estimator semantics are:

```text
Global IGC estimator
-> governed Global final edges
-> same-theme edge filter using context_theme_id
-> induced graph partition
-> P1 rebuilt from the induced edge graph
```

It does **not** perform:

```text
local residualization
local candidate generation
local lag selection
local Top-K / degree-cap competition
local CUDA estimation
```

Therefore GAL must use these names:

| GFF scope | Graph estimation | Scope operation | GAL ranking | Canonical GAL name |
|---|---|---|---|---|
| global | Global market | none | Global stock rank | `global_graph_global_rank` |
| within_theme | Global market | same-theme induced Global final edges | Local rank inside `decision_time × context_theme_id` | `induced_global_graph_local_rank` |
| inter_theme | Global market | aggregate Global edges/scores into theme portfolios | Rank across themes | `global_graph_inter_theme_rank` |

`within_theme` must never be described as a locally estimated graph. It is a Global graph with theme-scoped edge selection and Local execution ranking.

## 2. GFF compatibility contract

Before export, GAL runs:

```powershell
python scripts/audit_core4_gff_compatibility.py `
  --gff-campaign-root "D:\G4H\campaign=c4_260105_260722_induced_v2" `
  --output "D:\GAL\audits\c4_260105_260722_induced_v2.json"
```

The audit requires:

```text
campaign _SUCCESS
runs/campaign_contract.json
campaign version exactly SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN
within_theme_mode = induced_global_final_edges
all local re-estimation flags = false
Global scope contracts = 26
Momentum Within / Inter = 20 / 20
Residual Within / Inter = 20 / 20
106 GFF scope contracts per date
318 GAL factor variants per horizon
complete partition inventory
```

The GFF `gal_interface` may still expose the legacy generic label `theme_local`. GAL treats that label as non-authoritative and resolves estimator semantics from `campaign_contract.json`.

## 3. Research architecture

```text
GFF graph outputs
  |
  +-- GAL semantic input audit
  |
  +-- governed signal export
  |     +-- Global graph score
  |     +-- induced-global same-theme score
  |     +-- Inter-Theme aggregate score
  |
  +-- discovery Alpha
  |     +-- IC and falsification
  |     +-- node baseline
  |     +-- reverse-edge placebo
  |     +-- direct-return vs regime semantics
  |
  +-- candidate shortlist
  |
  +-- execution-frequency laboratory
        +-- same factor
        +-- same horizon
        +-- same universe
        +-- PIT-safe walk-forward direction
        +-- different rebalance frequency and gates only
```

Frequency research is deliberately separated from GFF. No graph or theme is recomputed.

## 4. Do not confuse horizon and trading frequency

A 180-minute return label does not imply that the portfolio must rebalance every 180 minutes. The next GAL report must form a matrix:

```text
rows    = signal factor and scale
columns = return horizon × execution policy
```

For every fixed factor and fixed horizon, compare:

```text
5m, 15m, 30m, 60m, 120m, 180m fixed rebalance
rank-change gate
confidence gate
theme-stability gate
combined gate with turnover cap
```

This isolates the effect of trading frequency. Comparing a 5m horizon strategy with a 180m horizon strategy is not a valid frequency experiment because both label and execution change.

## 5. Gate factors that reduce unnecessary trading

### 5.1 Rank-change gate

Rebalance only when the current score ranking has materially changed from the previous executed ranking.

```text
rank_change = 1 - Spearman(previous_rank, current_rank)
```

Default research threshold: `rank_change >= 0.25`.

Use case: signals update every five minutes but the selected long/short names remain nearly identical.

### 5.2 Confidence gate

Use the cross-sectional score dispersion as a real-time confidence proxy:

```text
score_dispersion = score_p90 - score_p10
```

Rebalance only when current dispersion exceeds a quantile calculated from **past observations only**. Default research quantile: 60%.

Use case: avoid paying turnover when graph scores are compressed and do not separate names/themes.

### 5.3 Minimum target-turnover gate

If the newly calculated target differs only slightly from current positions, keep the old book.

Default research threshold:

```text
candidate target turnover >= 0.10
```

This is a no-trade band, not a performance gate.

### 5.4 Theme-stability gate

For Within-Theme and Inter-Theme execution, compare the current `symbol -> context_theme_id` mapping with the prior executed mapping.

```text
retention = unchanged theme assignments / common symbols
```

Default research threshold: 70%.

Use case: do not rebalance aggressively while P1 membership is undergoing a split, merge or unstable remapping.

### 5.5 Turnover cap

When the target book requires a large change, blend gradually from the current book toward the target.

Default research cap:

```text
maximum traded notional per rebalance = 0.50
```

This prevents a single theme transition from producing an almost complete portfolio replacement.

### 5.6 Risk/liquidity regime gates

The strongest non-return GFF layers should not be promoted directly to return Alpha. They become execution gates:

```text
trade_intensity_to_volatility
flow_to_volatility
liquidity_to_volatility
burst_to_liquidity
venue_fragmentation_to_price_impact
```

Next implementation stage:

```text
base direct-return Alpha
× risk/liquidity regime state
-> gross exposure, rebalance permission, turnover budget and holding period
```

These gates must be estimated from information available at the decision time and tested out of sample.

## 6. Direction governance

The old `default_direction=auto` uses the same sample to choose long-high or long-low. It is allowed only as a discovery diagnostic.

Execution-frequency reports use:

```text
predeclared financial direction
or
walk-forward daily direction based only on prior dates
```

Default walk-forward rule:

```text
minimum training dates = 20
rolling training window = 60 dates
```

The first 20 dates are training-only and produce no executed results.

## 7. Candidate-first computation

Do not rerun frequency policies for all 1,908 factor-horizon rows immediately.

Recommended DAG:

```text
Stage A: existing discovery Alpha over all factors
Stage B: shortlist graph-forward direct-return factors
Stage C: candidate × horizon × execution policy
Stage D: policy Pareto frontier and gate attribution
```

Initial shortlist rule:

```text
graph_forward only
direct_return_alpha only
matched node/placebo data available
at least 20 dates
rank by:
  graph abs-IC increment vs node
  graph abs-IC increment vs reverse placebo
  gross spread
  sign consistency
  stability across dates
```

Frequency evaluation is then parallelized at:

```text
candidate factor × horizon
```

Each worker evaluates the full policy grid for one candidate/horizon so prepared data is loaded only once.

## 8. Required output tables

```text
scope_semantics.json
frequency_policy_metrics.csv
frequency_policy_returns.csv
frequency_pareto_frontier.csv
gate_effectiveness.csv
frequency_horizon_matrix.csv
candidate_execution_recommendations.csv
```

Core metrics:

```text
rebalance count and rate
mean and total turnover
turnover reduction vs fixed 5m
Gross return
Net return at 1/2/5/10 bps
Net increment vs fixed 5m
Daily diagnostic Sharpe
Hit rate
Maximum drawdown
Rank change
Score dispersion
Theme retention
Gate pass rate
Average time between executed rebalances
```

## 9. Promotion rules

A policy is not promoted merely because it has the highest backtest return.

Required:

```text
PIT-safe direction
same factor/horizon matched comparison
positive net result under approved cost assumption
lower turnover than native frequency
stable result across months and market regimes
not dominated on the return-turnover Pareto frontier
no dependence on one trading day
```

For a 137-session campaign, use:

```text
first 20 sessions: direction/gate warm-up
remaining sessions: walk-forward evaluation
monthly slices
early/mid/late session slices
high/low volatility slices
high/low liquidity slices
```

## 10. Commands

Audit GFF compatibility:

```powershell
python scripts/audit_core4_gff_compatibility.py `
  --gff-campaign-root "D:\G4H\campaign=c4_260105_260722_induced_v2" `
  --output "D:\GAL\audits\core4_induced_global.json"
```

Create corrected semantic views after the existing Alpha report:

```powershell
python scripts/annotate_induced_global_report.py `
  --gff-campaign-root "D:\G4H\campaign=c4_260105_260722_induced_v2" `
  --report-root "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\reports\c4_260105_260722_induced_v2"
```

Run one shortlisted factor/horizon frequency experiment:

```powershell
python scripts/run_dual_theme_frequency_candidate.py `
  --signals-root "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\signals\c4_260105_260722_induced_v2" `
  --labels "D:\GAL\labels\forward_return_180m.parquet" `
  --label-contract "D:\GAL\contracts\forward_return_180m.json" `
  --output "D:\GAL\frequency\vwap_60m_to_180m" `
  --scope inter_theme `
  --factor-id "inter_theme::momentum_state::vwap_dislocation_to_return::graph_forward" `
  --layer-id "vwap_dislocation_to_return" `
  --scale-minutes 60 `
  --variant-id graph_forward
```

## 11. Current boundary

The new execution-frequency module is a candidate laboratory. The next production step is to place candidate × horizon tasks into the existing six-worker global DAG and add factor-level execution checkpoints. Until that integration is validated, the existing Alpha checkpoint contract remains unchanged.
