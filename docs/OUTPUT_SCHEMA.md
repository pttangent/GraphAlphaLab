# Output schema

Each run creates an immutable run directory keyed by month, variant and configuration fingerprint.

Core files:

- `config_resolved.json`
- `partition_inventory.csv`
- `governance_findings.csv`
- `failures.csv`
- `dashboard.md`
- `progress.json`
- `cache/alpha_base/date=*/data.parquet`
- `shards/date=*/layer=*/scale=*/experiment=hierarchy/result.parquet`
- `shards/.../summary.csv`
- `shards/.../manifest.json`
- `reports/PER_DAY_RESULTS.csv`
- `reports/PARTITION_MATRIX.csv`
- `reports/MONTHLY_ALPHA_REPORT.md`

A shard is reusable only when `_SUCCESS` exists and its manifest fingerprint matches the resolved configuration and upstream P0/P1 manifest hashes.
