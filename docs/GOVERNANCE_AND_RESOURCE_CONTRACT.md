# Governance, PIT and resource contract

## Responsibility boundary

GraphFactorFactory produces graphs, node projections and themes. GraphAlphaLab produces graph-derived scores, target labels, metadata interpretation, Alpha and performance reports.

`node_projection.node_score` is a source-state baseline. A network claim requires an edge aggregation variant such as `graph_forward` and a weaker reverse/shuffle placebo.

## PIT gates

A governed Alpha run requires:

- `signal_available_time <= decision_time` for every score row;
- one target per join key and `label_id`;
- `entry_time > decision_time` unless the Label Contract explicitly relaxes it;
- `exit_time > entry_time`;
- `label_available_time >= exit_time`;
- actual clock-time horizon within the declared tolerance;
- a Label Contract stored and hashed with the report.

A file hash without the Label Contract is insufficient lineage.

## Statistical governance

- Snapshot IC is diagnostic.
- Daily mean IC is the default inference unit.
- FDR is applied across factor rows using daily-IC p-values.
- A negative factor is direction-consistent when negative daily IC dominates; positive-only consistency is incorrect.
- If direction is selected from observed IC, the factor is `needs_falsification`, not a governed candidate.
- Overlapping forward labels invalidate direct annualized Sharpe. GAL reports diagnostic daily statistics and sets `annualization_valid=false`.

## Cost governance

Portfolio rows store equal-weight long and short positions. Traded notional is the sum of absolute weight changes at each decision. Cost is charged per decision:

```text
net_return = oriented_return - traded_notional × cost_bps / 10,000
```

## OOM contract

The supported large-run path is:

```text
Parquet dataset → DuckDB bounded join → one factor at a time → compact result tables
```

Controls:

- `--memory-limit-gb` limits DuckDB memory;
- `--threads` caps query parallelism;
- `--temp-directory` enables disk spill;
- factors are evaluated sequentially;
- graph signals are written as partitioned Parquet shards;
- correlation uses deterministic sampling rather than a full 100M-row pivot;
- all report writes are atomic;
- `_SUCCESS` is published last;
- compact batch merge refuses any upstream bundle without `_SUCCESS`.

Do not use the legacy pattern of loading all41 signal and label rows into one Pandas DataFrame.

## Required report lineage

- GAL Git commit and dirty state;
- input file path, size and SHA-256;
- GFF export manifest and source partition hashes;
- Label Contract and contract hash;
- metadata hash and selected dimensions;
- resource budget;
- PIT audit counts;
- batch expected/observed contracts;
- report contract hash and `_SUCCESS`.
