# Theme semantic reference layer

Sector semantics enrich P1; they do not replace or modify graph communities.

## Problems corrected from the original scripts

- yfinance results had no `as_of_date`, effective interval, source, or confidence, so they were not PIT-auditable.
- Manual ticker lists silently overrode vendor classifications and were difficult to maintain.
- Purity was calculated after dropping unmapped members, inflating apparent purity.
- `theme_id` was compared across dates even though identity is only safe at the full theme-instance key unless temporal edges prove continuity.
- Output counted theme snapshots, not stable economic themes, and had no mapping-coverage gate.

## Governed mapping contract

Required columns:

- `symbol`
- `sector`

Recommended columns:

- `industry`
- `source`
- `confidence`
- `effective_from`
- `effective_to`
- `as_of_date`

The module normalizes symbols, rejects conflicting active assignments, selects only records effective on the trade date, and reports both mapped purity and total-theme purity.

## Effective combination with Alpha research

1. Use `theme_semantics.parquet` to measure whether a graph community has recognizable sector meaning.
2. Keep ordinary strict P1 LOO as the baseline.
3. Add same-sector LOO, cross-sector LOO, and their disagreement as separate experimental signals.
4. Residualize semantic signals after traditional controls, Node, Graph, and ordinary P1.
5. Compare sector-aligned and cross-sector cohorts. A semantic label remains explanatory until it demonstrates out-of-sample incremental IC.

The semantic profile includes mapping coverage, total purity, mapped purity, entropy, HHI, source confidence, and a governed label: `HIGH_PURITY`, `SECTOR_ALIGNED`, `CROSS_SECTOR`, `LOW_COVERAGE`, `TOO_SMALL`, or `UNMAPPED`.

## CLI

```powershell
python scripts/run_theme_semantic_research.py `
  --p1-root "D:\GraphFactorFactory_v2\warehouse\p1" `
  --sector-mapping "D:\reference\sector_mapping.parquet" `
  --output "D:\GraphAlphaLab\artifacts\theme_semantics.parquet"
```
