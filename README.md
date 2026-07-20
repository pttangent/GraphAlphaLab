# GraphAlphaLab

GraphAlphaLab is the research and interpretation layer for GraphFactorFactory outputs.

```text
GFF P0/P1 + NFF target data + metadata
                ↓
labels, purity, alpha, performance, robustness
                ↓
compact batch reports for discussion
```

GraphAlphaLab owns everything that must not influence graph construction:

- target and future-return labels;
- node and theme Alpha evaluation;
- sector, industry, semantic-theme and arbitrary metadata purity;
- performance, risk, turnover, cost and robustness analysis;
- financial interpretation and research ranking;
- incremental reports for `implemented27`, `remaining14`, `theme_discovery` and compact `all41` consolidation.

## Why batch reports

The 27 original Interaction contracts finish before the specialized remaining14 and Theme Discovery work. GAL therefore never requires all inputs to finish at once.

```text
implemented27 report  -> available first
remaining14 report    -> available later
theme_discovery report-> purity and structure report when themes finish
all41 merge           -> reads compact summaries, not large GFF Parquet again
```

Every report records whether it is complete or explicitly partial.

## Metadata support

Purity is not limited to `sector`. GAL auto-detects and reports any useful categorical dimension, including:

- `sector_code` / sector aliases;
- `industry_code` / industry aliases;
- sub-industry;
- semantic or business theme;
- country and exchange;
- security / quote type;
- ETF status;
- a deterministic market-cap bucket;
- future custom categorical columns supplied with `--dimensions`.

The current supplied metadata sample contains 5,002 symbols and includes sector, industry, country, exchange, security type and market capitalization. Missing metadata is reported as coverage loss, never treated as a category match or purity zero.

## Install

```bash
python -m pip install -e ".[test]"
```

## Profile metadata

```powershell
gal profile-metadata `
  --metadata "D:\NFF\reference\symbol_metadata.parquet" `
  --output "D:\GAL\metadata_profile.json"
```

## Theme Discovery purity report

```powershell
gal theme-report `
  --batch-id theme_discovery `
  --memberships "D:\GFF\similarity\memberships.parquet" `
  --metadata "D:\NFF\reference\symbol_metadata.parquet" `
  --output "D:\GAL\reports\theme_discovery"
```

Outputs include per-theme/per-dimension purity, entropy, coverage, dominant-label lift, snapshot NMI/ARI and compact Markdown interpretation. Metadata never changes theme membership.

## Alpha report by batch

Signals may already contain labels, or labels may be joined from a separate table.

```powershell
gal alpha-report `
  --batch-id implemented27 `
  --signals "D:\GAL\inputs\implemented27_scores.parquet" `
  --labels "D:\GAL\labels\forward_returns.parquet" `
  --join-keys "trade_date,decision_time,symbol" `
  --annualization-factor 19656 `
  --output "D:\GAL\reports\implemented27"
```

The Markdown report lists every factor. Full machine-readable evidence is retained in compact CSV files.

## Merge completed batches without rereading raw data

```powershell
gal merge-reports `
  --inputs `
    "D:\GAL\reports\implemented27" `
    "D:\GAL\reports\remaining14" `
  --output "D:\GAL\reports\all41"
```

See `docs/REPORT_SPEC.md` and `docs/BATCH_CONTRACT.md`.
