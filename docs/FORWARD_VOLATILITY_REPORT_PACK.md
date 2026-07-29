# Forward-volatility report pack data contract

## Goal

The report folder must be sufficient for a later full discussion without requiring one giant wide table or uploading every raw decision-level file. It must retain evidence about successful, non-significant, insufficient and rejected factors.

## Information retention policy

The pack retains every factor identity and every available variant. No row is removed because FDR, sample sufficiency, direction consistency or graph-incremental gates fail.

Data is separated by semantic role:

- predictive strength;
- sample and evaluability coverage;
- governance and status;
- graph/node/reverse matched comparisons;
- daily IC inference evidence;
- compact quantile curves;
- original and snapshot-derived stability slices;
- label coverage and distribution diagnostics;
- generic return-evaluator fields retained only for audit;
- raw evidence location and fingerprint catalogues.

## Large-file policy

### Quantile evidence

Raw `quantile_returns.csv` is read in chunks. The report stores one compact row per:

```text
factor identity × quantile × target × horizon
```

The compact row contains:

- mean target;
- target standard deviation;
- observation count;
- minimum and maximum target;
- first and last trade date.

Decision-level quantile rows are not copied into the report folder.

### Snapshot IC evidence

Raw `ic_series.csv` is read in chunks and reduced to:

- one all-snapshot summary per factor;
- one hour-of-day summary per factor;
- count, mean IC, IC standard deviation, positive rate and date coverage.

The full snapshot file remains under `raw_volatility_diagnostic` and is referenced by `indexes/raw_evidence_index.csv`.

### Daily IC

Daily IC is retained without significance filtering because it is the main inference-unit evidence for the 77-session campaign. It is split by target × horizon × scope.

## Preflight and checkpoint policy

Before opening large CSV files, report preflight fingerprints:

- every raw horizon report artifact;
- every daily label checkpoint;
- every daily label Parquet;
- the report configuration.

Small files use SHA-256. Large files use a fast size + nanosecond-mtime anchor; their governed upstream checkpoint and report manifest remain part of the source set.

If the source contract and every report artifact fingerprint match, the finalizer returns before reading raw evidence. The status is written to:

```text
<output>/_checkpoints/forward_volatility/report_preflight.json
```

Only a changed source or damaged/missing report artifact triggers reconstruction.

## Mandatory overview files

```text
overview/factor_master_compact.csv
overview/target_horizon_scope_summary.csv
overview/layer_target_horizon_summary.csv
overview/factor_status_counts.csv
overview/matched_variant_comparison.csv
overview/strongest_weakest_graph_increments.csv
overview/non_confirmation_reason_counts.csv
overview/label_date_coverage.csv
overview/label_distribution_by_date.csv
overview/label_distribution_summary.csv
```

`factor_master_compact.csv` is complete but intentionally narrow. Detailed fields are in semantic shards.

## Non-confirmation reasons

Every matched graph-forward group that is not confirmed records all applicable reasons:

```text
graph_forward_missing
node_baseline_missing
reverse_placebo_missing
sample_insufficient
fdr_not_passed
daily_sign_consistency_below_0.55
graph_not_stronger_than_node
graph_not_clearly_stronger_than_reverse
```

This makes negative and ambiguous results directly analyzable rather than silently discarded.

## Upload workflow

Upload in this order:

1. root `REPORT.md`, `summary.json`, `DATA_DICTIONARY.md` and `ANALYSIS_GUIDE.md`;
2. the complete `overview/` folder;
3. `indexes/UPLOAD_MANIFEST.csv` and all shard indexes;
4. target-specific reports;
5. only detailed shards requested during the discussion.

`UPLOAD_MANIFEST.csv` supplies relative path, file size, row count, content role and upload priority.
