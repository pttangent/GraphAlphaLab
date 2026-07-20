# Local usage and operations

## Recommended machine profile

The default full-month command is designed for 24 CPU cores, 128GB RAM, and NVMe storage. GPU is not used by the baseline contract. Lower `--workers` first if RAM or storage throughput becomes limiting; never reduce the market universe merely to avoid OOM.

## Operational sequence

1. Run `python -m pytest -q`.
2. Run the three-day validation command.
3. Inspect `governance_findings.csv` and require zero PIT/schema failures.
4. Stop the process once and rerun with `--resume` to verify shard reuse.
5. Run the full month.
6. Review `MONTHLY_ALPHA_REPORT.md`; treat all automatic discoveries as in-sample until a later month confirms them.

## Resource behavior

- Work is partitioned by date × layer × scale.
- Each task writes an independent shard and `_SUCCESS` marker.
- RAM and disk gates are checked before cache and partition work.
- `dashboard.md` is rewritten after each finished task.
- A failed partition is recorded and does not erase successful shards.

## Troubleshooting

### Disk gate failure

Free space or move `--output-root`; do not lower the gate unless the storage plan is understood.

### RAM gate failure

Reduce `--workers` and optionally `--duckdb-threads`. Keep `--memory-limit-gb` below physical RAM.

### Empty graph marked successful

This becomes `GOVERNANCE_FAILURE` in `governance_findings.csv`. Repair/rebuild the upstream P0 partition instead of silently evaluating it.

### Missing P1

The partition remains in inventory but is not evaluated for hierarchy until governed P1 memberships exist.
