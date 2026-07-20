# CLI examples

Three-day smoke validation:

```powershell
python scripts/run_monthly_alpha_research.py `
  --warehouse-root "D:\GraphFactorFactory_v2\warehouse" `
  --dates 2026-07-01,2026-07-02,2026-07-06 `
  --variant candidate_v2_shadow `
  --output-root "D:\GraphAlphaLab\artifacts" `
  --workers 6 --duckdb-threads 2 --memory-limit-gb 48 `
  --min-free-disk-gb 200 --resume
```

Full month:

```powershell
python scripts/run_monthly_alpha_research.py `
  --warehouse-root "D:\GraphFactorFactory_v2\warehouse" `
  --month 2026-07 `
  --variant candidate_v2_shadow `
  --output-root "D:\GraphAlphaLab\artifacts" `
  --workers 16 --duckdb-threads 3 --memory-limit-gb 72 `
  --min-free-disk-gb 200 --resume
```
