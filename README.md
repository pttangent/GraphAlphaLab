# GraphAlphaLab

GraphAlphaLab is the governed research and interpretation layer above GraphFactorFactory v2.

```text
GFF P0 edges + node projections + P1 themes
                    ↓
GAL graph-signal export, labels, PIT audit, P1 structure/temporal/purity,
Alpha, costs and compact campaign reports
```

GFF remains graph-only. GAL owns future labels, metadata interpretation and performance research.

## What changed in 0.3

- `gal p1-report` now scans governed GFF P1 partitions one partition at a time.
- P1 reports include layer-local and consensus Memberships, Theme Tree refinement, relations, within-day Temporal edges, range Temporal links and Metadata Purity.
- The canonical Similarity contract is now `similarity10`: ten layer-local P1 contracts plus `similarity_consensus@0m`.
- `gal campaign-report` combines IG27, RM14 and Similarity10 compact reports for the 33 XNYS sessions from 2026-06-01 through 2026-07-17.
- Large P1 Parquet trees are never concatenated into one all-history Pandas frame; each governed partition is validated, summarized and released before the next partition. Purity detail is disk-sharded and aggregated with bounded DuckDB.
- Campaign merge reads only compact report bundles and refuses missing `_SUCCESS`, partial reports or incomplete date coverage.

The 0.2 graph-alpha controls remain unchanged:

- `node_projection.node_score` is a node baseline, never network Alpha.
- `gal export-gff-signals` builds `graph_forward` and `graph_reverse_placebo` from real GFF edges.
- `alpha-report` requires an explicit Label Contract and uses daily IC as the inference unit.
- Overlapping labels invalidate annualized Sharpe.
- DuckDB memory, threads and spill are bounded.
- `_SUCCESS` is always published last.

## Install

```bash
python -m pip install -e ".[test]"
python -m compileall -q src scripts
python -m pytest -q --tb=short
```

## Export Interaction graph signals

```powershell
$HEAD = (git rev-parse HEAD).Trim()

gal export-gff-signals `
  --batch-id implemented27 `
  --p0-root "D:\GFF\ig27\p0" `
  --variants "node_baseline,graph_forward,graph_reverse_placebo" `
  --output "D:\GAL\signals\implemented27" `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory "D:\GAL\duckdb_tmp" `
  --expected-git-commit $HEAD `
  --require-clean
```

Repeat with `--batch-id remaining14` and its own P0 root.

## Evaluate a 33-session P1 batch

```powershell
gal p1-report `
  --batch-id similarity10 `
  --p1-root "D:\GFF\similarity\p1" `
  --range-root "D:\GFF\similarity\range" `
  --metadata "D:\NFF\reference\symbol_metadata.parquet" `
  --dimensions "sector_code,industry_code,country,market_cap_bucket,semantic_theme" `
  --start-date 2026-06-01 `
  --end-date 2026-07-17 `
  --expected-date-count 33 `
  --require-consensus `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory "D:\GAL\duckdb_tmp" `
  --output "D:\GAL\reports\similarity10_p1" `
  --expected-git-commit $HEAD `
  --require-clean
```

For `implemented27` and `remaining14`, point `--p1-root` to the corresponding isolated P1 root. The expected contract counts are taken from the batch registry: 27, 14 and 11 respectively.

Detailed purity evidence is written as partitioned Parquet shards under the report bundle. Compact `purity_dimension_summary.csv` and `purity_snapshot_agreement_summary.csv` are used for comparison and campaign merging.

## Run Alpha reports

Use one governed report per horizon. Example:

```powershell
gal alpha-report `
  --batch-id implemented27 `
  --signals "D:\GAL\signals\implemented27" `
  --labels "D:\GAL\labels\forward_5m.parquet" `
  --label-contract "D:\GAL\contracts\forward_5m.json" `
  --metadata "D:\NFF\reference\symbol_metadata.parquet" `
  --slice-dimensions "sector_code,industry_code,country,market_cap_bucket" `
  --join-keys "trade_date,decision_time,symbol_id" `
  --control-columns "own_score" `
  --default-direction auto `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory "D:\GAL\duckdb_tmp" `
  --min-cross-section 100 `
  --output "D:\GAL\reports\implemented27_5m" `
  --expected-git-commit $HEAD `
  --require-clean
```

Repeat for 15m and 30m, then for `remaining14`.

## Compact all41 Alpha merge

```powershell
gal merge-reports `
  --inputs "D:\GAL\reports\implemented27_5m" "D:\GAL\reports\remaining14_5m" `
  --output "D:\GAL\reports\all41_5m"
```

## Three-batch 33-day campaign

```powershell
gal campaign-report `
  --implemented27-alpha `
    "D:\GAL\reports\implemented27_5m" `
    "D:\GAL\reports\implemented27_15m" `
    "D:\GAL\reports\implemented27_30m" `
  --implemented27-p1 "D:\GAL\reports\implemented27_p1" `
  --remaining14-alpha `
    "D:\GAL\reports\remaining14_5m" `
    "D:\GAL\reports\remaining14_15m" `
    "D:\GAL\reports\remaining14_30m" `
  --remaining14-p1 "D:\GAL\reports\remaining14_p1" `
  --similarity-p1 "D:\GAL\reports\similarity10_p1" `
  --all41-alpha `
    "D:\GAL\reports\all41_5m" `
    "D:\GAL\reports\all41_15m" `
    "D:\GAL\reports\all41_30m" `
  --start-date 2026-06-01 `
  --end-date 2026-07-17 `
  --expected-date-count 33 `
  --output "D:\GAL\reports\three_batch_33day"
```

See `docs/THREE_BATCH_33DAY_RUNBOOK.md` for the complete governed workflow and output contract.
