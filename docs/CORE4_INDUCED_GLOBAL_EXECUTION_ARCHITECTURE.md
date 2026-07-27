# Core4 induced-global execution architecture

## 1. Governed source identity

```text
GFF repository: pttangent/GraphFactorFactory_v2
GFF branch: agent/core4-batched-halfyear
GFF entrypoint: scripts/run_core4_20260105_20260722.ps1
GFF campaign: D:\G4H\campaign=c4_260105_260722_induced_v2
GFF version: SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN
GAL branch: agent/induced-global-frequency-gates
```

The GFF `within_theme` scope is not a locally re-estimated graph. Its governed computation is:

```text
Global IGC estimator
-> governed Global final edges
-> same-theme edge selection by context_theme_id
-> induced graph partition
-> P1 rebuilt from the induced edge graph
```

It does not perform local residualization, local candidate generation, local lag selection, local Top-K competition, local degree-cap competition, or Local CUDA estimation.

GAL uses the following unambiguous names:

| GFF scope | Graph estimator | GAL ranking | Canonical GAL name |
|---|---|---|---|
| global | Global market | Global stock cross-section | `global_graph_global_rank` |
| within_theme | Global market, same-theme induced edges | Local rank inside `decision_time × context_theme_id` | `induced_global_graph_local_rank` |
| inter_theme | Global market, cross-theme aggregation | Rank across theme portfolios | `global_graph_inter_theme_rank` |

## 2. End-to-end architecture

```text
GFF outputs
  -> Core4 compatibility and scope-semantics audit
  -> governed GAL signal export
  -> all-factor discovery Alpha, six-worker global DAG
  -> matched graph-forward candidate selection
  -> candidate × horizon execution-frequency DAG
  -> frequency, gate, turnover, cost and Pareto reports
```

Frequency research never recomputes GFF. It keeps the factor, return horizon and universe fixed and changes only the execution policy.

## 3. GFF compatibility audit

Before export, GAL requires:

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

The generic GFF interface label `theme_local` is non-authoritative. GAL resolves estimator provenance from `campaign_contract.json`.

## 4. Candidate selection

Execution research is not run over all discovery rows. `execution_candidates.py` matches, within the same factor/scope/horizon identity:

```text
graph_forward
node_baseline
graph_reverse_placebo
```

Eligible inputs are graph-forward direct-return factors with sufficient dates. Ranking combines:

```text
absolute IC increment vs node baseline
absolute IC increment vs reverse-edge placebo
graph absolute IC
daily IC sign consistency
gross return per unit turnover
ICIR
```

Candidates are labelled:

```text
strict_incremental: graph abs-IC exceeds both matched controls
exploratory: retained only to satisfy an explicit per-horizon research floor
```

The default full campaign selects at most 12 candidates per horizon and requires at least 20 dates.

## 5. Execution policy grid

For every fixed candidate and fixed return horizon, GAL compares:

```text
fixed rebalance: 5m, 15m, 30m, 60m, 120m, 180m
rank-change gates: 15m, 30m, 60m, 120m
past-only confidence gates: 15m, 30m, 60m, 120m
combined gates with turnover caps: 15m, 30m, 60m, 120m
theme-stability gates: 30m, 60m, 120m
```

Default gate definitions:

```text
rank_change = 1 - Spearman(previous executed rank, current rank)
score_dispersion = score P90 - score P10
confidence threshold = rolling past-only 60th percentile
theme_retention = unchanged theme assignments / common symbols
minimum target turnover = 0.10
maximum traded notional per rebalance = 0.50
```

The first 20 dates are warm-up. Direction for each later date is inferred only from previous dates, using at most the previous 60 dates. Discovery `default_direction=auto` is not reused as an execution direction.

Execution state resets at each trading session by default. Intraday positions are not silently carried overnight. `--carry-overnight` exists only as an explicit diagnostic override.

## 6. Global DAG and checkpoint contract

The production execution unit is:

```text
candidate factor × return horizon
```

Each worker loads and prepares the candidate once, then evaluates the complete policy grid. All horizons and scopes share one ready queue. There is no horizon barrier and no scope barrier.

Default resources:

```text
workers = 6
memory = 64 GB total, split across workers
threads = 12 total, split across workers
max inflight = 2 × workers
```

Checkpoint path:

```text
<FrequencyOutput>\_checkpoints\execution_frequency\
  horizon=<h>\scope=<scope>\candidate-<contract-hash>\
```

Each candidate checkpoint contains:

```text
metrics.parquet
daily_returns.parquet
decision_returns.parquet
frontier.parquet
gate_effectiveness.parquet
checkpoint.json
```

Checkpoint identity includes candidate identity, label contract, policy grid, preparation thresholds, direction rules, quantiles and overnight policy. Worker count, memory and threads are execution choices and do not change the mathematical identity.

Progress files:

```text
<FrequencyOutput>\_checkpoints\execution_frequency\global_dag_progress.json
<FrequencyOutput>\_checkpoints\execution_frequency\progress.json
<FrequencyOutput>\_checkpoints\execution_frequency\DASHBOARD.md
```

## 7. PIT and schema audit

Every candidate task checks:

```text
signal_available_time exists
signal_available_time <= decision_time
entry_time is after decision_time according to the label contract
exit_time > entry_time
actual horizon matches the label contract
candidate horizon matches the horizon manifest
```

Global, induced-global/local-rank and Inter-Theme preparation reuse the same GAL calculation principles as the discovery Alpha path.

## 8. Output bundle

```text
execution_candidates.json
execution_candidates.csv
scope_semantics.json
frequency_policy_metrics.csv
frequency_policy_returns.csv
frequency_pareto_frontier.csv
gate_effectiveness.csv
frequency_horizon_matrix.csv
candidate_execution_recommendations.csv
decision_returns_catalog.csv
summary.json
REPORT.md
_SUCCESS
```

Raw decision-level returns remain inside candidate checkpoints. `frequency_policy_returns.csv` is the compact daily aggregation.

Promotion requires all of:

```text
PIT-safe walk-forward direction
same factor and horizon comparison
positive net mean at 5 bps
lower turnover than fixed 5m
at least 20 evaluation dates
membership on the return-turnover Pareto frontier
```

Passing this rule means “promote for falsification”, not production approval.

## 9. Canonical run

```powershell
cd D:\DEV\AnotherNetworkFactory\GraphAlphaLab

git fetch origin --prune
git switch agent/induced-global-frequency-gates
git pull --ff-only origin agent/induced-global-frequency-gates

.\scripts\run_core4_induced_global_alpha_20260105_20260722.ps1 `
  -HorizonManifest "D:\GAL\contracts\dual_theme_horizons.c4_260105_260722_induced_v2.json" `
  -Metadata "D:\GAL\metadata\symbol_metadata.with_symbol_id.parquet" `
  -FactorWorkers 6 `
  -FrequencyWorkers 6 `
  -CandidatesPerHorizon 12 `
  -MinCandidateDates 20
```

The runner performs the GFF audit, signal export, discovery Alpha DAG, semantic annotation, candidate selection and execution-frequency DAG. It validates both Alpha and frequency output bundles before reporting success.

Do not use force export, partial reports or dirty-checkout bypasses for the governed run.

## 10. Current boundary

Implemented and CI-covered:

```text
induced-global semantic correction
matched candidate selection
candidate × horizon six-worker global DAG
factor-level atomic execution checkpoints
PIT-safe session-reset execution engine
fixed-frequency and gate comparison
cost analysis and Pareto reports
canonical half-year runner integration
```

Still requires local evidence before merging the Draft PR:

```text
real 137-session GFF directory audit
full local candidate × horizon execution run
coverage waterfall for Global -> induced -> membership -> label -> evaluable rows
monthly and time-of-day stability review
risk/liquidity regime gates as a later execution overlay
```
