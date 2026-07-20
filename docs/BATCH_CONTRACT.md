# Batch reporting contract

## Built-in batches

| Batch | Expected layer-scale contracts | Primary output |
|---|---:|---|
| `implemented27` | 27 | Alpha and performance report |
| `remaining14` | 14 | Alpha and performance report |
| `theme_discovery` | 1 consensus output / 12 upstream inputs | Theme purity and structure report; optional Alpha |
| `all41` | 41 | Compact merge of implemented27 + remaining14 |

A report with fewer observed contracts is rejected unless `--allow-partial` is explicit. Partial status is written into `summary.json` and the report header.

## Report bundle

Each batch output is self-contained and compact:

```text
<output>/
├── REPORT.md
├── summary.json
├── alpha_metrics.csv                 # when Alpha is evaluated
├── ranking.csv
├── ic_series.csv
├── quantile_returns.csv
├── portfolio_returns.csv
├── stability_slices.csv
├── score_correlation.csv
├── theme_purity.csv                  # for Theme Discovery
├── purity_dimension_summary.csv
├── purity_snapshot_agreement.csv
├── metadata_profile.json
├── highest_purity_themes.csv
└── lowest_purity_themes.csv
```

The bundle must record input SHA-256 values and batch completion. A later `all41` merge reads only these compact files.

## Required input identity

Every production research bundle should record:

- GAL Git commit;
- GFF Git commit and run manifest hash;
- GFF P0/P1 partition hashes;
- NFF signal and target manifest hashes;
- label contract and horizon;
- metadata file hash and selected dimensions;
- batch ID, expected and observed contracts;
- partial/completed status.
