# GraphAlphaLab

GraphAlphaLab is a read-only research layer over governed GraphFactorFactory_v2 outputs. It discovers monthly NFF/P0/P1 partitions, builds reusable PIT-safe alpha labels, evaluates Node → Graph → P1 incremental information, records governance failures, supports hash-based resume, and generates auditable monthly reports.

It never modifies NFF, P0, or P1 outputs.

## Install (Windows PowerShell)

```powershell
cd D:\GraphAlphaLab
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e ".[dev]"
```

## Three-day validation

```powershell
python scripts/run_monthly_alpha_research.py `
  --warehouse-root "D:\GraphFactorFactory_v2\warehouse" `
  --dates 2026-07-01,2026-07-02,2026-07-06 `
  --variant candidate_v2_shadow `
  --output-root "D:\GraphAlphaLab\artifacts" `
  --workers 6 `
  --duckdb-threads 2 `
  --memory-limit-gb 48 `
  --min-free-disk-gb 200 `
  --resume
```

## Full month

```powershell
python scripts/run_monthly_alpha_research.py `
  --warehouse-root "D:\GraphFactorFactory_v2\warehouse" `
  --month 2026-07 `
  --variant candidate_v2_shadow `
  --output-root "D:\GraphAlphaLab\artifacts" `
  --workers 16 `
  --duckdb-threads 3 `
  --memory-limit-gb 72 `
  --min-free-disk-gb 200 `
  --resume
```

Use `--only-layer return_lead_lag,price_impact_liquidity` for targeted research. Interrupting and rerunning with `--resume` reuses only shards whose input/config fingerprint still matches.

## Outputs

Each run creates:

- `partition_inventory.csv`
- `governance_findings.csv`
- `dashboard.md` and `progress.json`
- reusable `cache/alpha_base/date=*/data.parquet`
- independent `shards/date=*/layer=*/scale=*/experiment=hierarchy/`
- `reports/PER_DAY_RESULTS.csv`
- `reports/PARTITION_MATRIX.csv`
- `reports/MONTHLY_ALPHA_REPORT.md`

## Research contract

For each decision-time cross section, GraphAlphaLab computes raw and residualized Node, one-hop Graph, and strict leave-one-out P1 theme signals. Direction and absolute-return risk targets are evaluated separately at 5m, 15m, and 30m horizons.