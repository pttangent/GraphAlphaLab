# GraphAlphaLab

GraphAlphaLab is the governed research and interpretation layer above GraphFactorFactory v2.

```text
GFF P0 edges + node projections + P1 themes
                    ↓
GAL graph-signal export, labels, PIT audit, purity, Alpha, costs and compact reports
```

GFF remains graph-only. GAL owns future labels, metadata interpretation and performance research.

## What changed in 0.2

- `node_projection.node_score` is explicitly a **node baseline**, never network Alpha.
- `gal export-gff-signals` builds OOM-bounded `graph_forward` and `graph_reverse_placebo` scores from real GFF edges.
- `alpha-report` requires an explicit Label Contract and audits signal availability, entry/exit timing, duplicate labels and horizon consistency.
- Statistical significance uses **daily IC means**, not hundreds of overlapping intraday snapshots as independent observations.
- Stable negative IC is handled correctly through a predeclared direction contract.
- Transaction costs use per-decision traded notional rather than one average turnover value applied to every observation.
- Overlapping labels invalidate annualized Sharpe; GAL reports diagnostic daily statistics instead of inventing an annualization.
- All41 evaluation is factor-sequential through DuckDB with an explicit memory limit, thread budget and spill directory.
- Output files are atomic and `_SUCCESS` is written only after the governed bundle completes.

## Install

```bash
python -m pip install -e ".[test]"
```

## 1. Export actual graph signals from GFF

```powershell
$HEAD = (git rev-parse HEAD).Trim()

gal export-gff-signals `
  --batch-id implemented27 `
  --p0-root "D:\GFF\interaction\p0" `
  --variants "node_baseline,graph_forward,graph_reverse_placebo" `
  --output "D:\GAL\signals\implemented27" `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory "D:\GAL\duckdb_tmp" `
  --expected-git-commit $HEAD `
  --require-clean
```

The exporter rejects any edge with `edge_available_time > decision_time` and records the SHA-256 of every P0 input and exported shard.

## 2. Create an explicit label contract

Example `forward_5m.json`:

```json
{
  "label_id": "forward_5m_next_bar",
  "horizon_minutes": 5,
  "entry_lag_minutes": 1,
  "target_column": "target_return",
  "decision_time_column": "decision_time",
  "entry_time_column": "entry_time",
  "exit_time_column": "exit_time",
  "available_time_column": "label_available_time",
  "overlapping": true,
  "rebalance_minutes": 5,
  "horizon_tolerance_seconds": 60,
  "require_entry_after_decision": true
}
```

Labels must contain the contract columns and one row per declared join key. GAL refuses same-time entry, duplicate targets, incorrect horizons and labels available before the exit price exists.

## 3. Run an OOM-bounded batch report

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
  --output "D:\GAL\reports\implemented27" `
  --expected-git-commit $HEAD `
  --require-clean
```

`default-direction=auto` is allowed for exploration, but such factors are never governance-ready candidates. Production-grade research should provide a predeclared `expected_direction` column.

## 4. Theme Discovery purity

```powershell
gal theme-report `
  --batch-id theme_discovery `
  --memberships "D:\GFF\similarity\memberships.parquet" `
  --metadata "D:\NFF\reference\symbol_metadata.parquet" `
  --output "D:\GAL\reports\theme_discovery"
```

Metadata evaluates the GFF memberships but never changes them.

## 5. Compact all41 merge

```powershell
gal merge-reports `
  --inputs "D:\GAL\reports\implemented27" "D:\GAL\reports\remaining14" `
  --output "D:\GAL\reports\all41"
```

The merge requires upstream `_SUCCESS` files and hashed summaries; it never rereads large GFF partitions.

See `docs/GOVERNANCE_AND_RESOURCE_CONTRACT.md`, `docs/REPORT_SPEC.md` and `docs/BATCH_CONTRACT.md`.
