# Lagged Similarity-Leiden Within-Theme Alpha Contract

## Dependency graph

```text
NFF / Research Feature Store
        ├── Similarity P0 -> recursive consensus P1 memberships
        └── IG27 / RM14 P0 -> exported graph-forward scores
                              
Similarity memberships + Interaction scores + forward labels
        -> GraphAlphaLab within-theme alpha
```

Similarity and Interaction production are independent and may run concurrently. Only the within-theme evaluation waits for Similarity consensus memberships.

## Point-in-time membership

For an Interaction signal at decision time `t`, the evaluator uses the latest canonical `similarity_consensus` membership satisfying:

```text
theme_decision_time < t
```

The strict inequality is enforced by a DuckDB ASOF join. Same-snapshot memberships are not eligible.

Additional filters:

- `canonical_eligible = true`
- `theme_size >= 20` by default
- forced operational chunks are excluded
- when multiple canonical rows exist for one symbol and snapshot, the deepest tree membership is selected

## Portfolio construction

For each factor and decision time:

1. residualize the graph score against configured controls inside each theme;
2. rank members inside each theme;
3. form top and bottom quantiles inside each theme;
4. orient the spread using either a predeclared direction or discovery-only automatic direction;
5. make each theme locally dollar neutral;
6. equal-weight eligible themes in the aggregate portfolio;
7. calculate turnover from symbol-level position transitions and apply 0/1/2/5/10 bps costs.

Outputs include:

- `metrics.parquet/csv`
- `ic_series.parquet/csv`
- `portfolio_returns.parquet/csv`
- `theme_returns.parquet/csv`
- `run_manifest.json`
- `SUMMARY.md`
- `_SUCCESS`

## Scheduled experiments

Formal falsification:

- `trade_intensity_to_volatility__graph_forward`
- fixed negative direction
- 30m, 60m and 120m labels

Discovery-only screens:

- all graph-forward IG27 factors
- all graph-forward RM14 factors
- 30m, 60m and 120m labels
- automatic direction; these results cannot be promoted directly

## Governance boundary

A result is not governance-ready merely because the theme lag is PIT-safe. Promotion still requires a predeclared direction and a non-overlapping/purged label contract, together with significance, cost survival and falsification tests.
