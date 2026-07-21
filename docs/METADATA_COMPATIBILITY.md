# Current metadata compatibility

The supplied `symbol_metadata` sample was inspected with this schema:

```text
symbol, source_symbol, company_name, sector_code, industry_code,
last_price, rank, market_cap, exchange, country, quote_type,
shares_outstanding, enterprise_value, currency, security_type,
is_etf, fetch_status, fetch_error
```

Observed profile:

- 5,002 symbols;
- 4,675 non-null sector values across 17 sectors;
- 4,674 non-null industry values across 236 industries;
- 4,595 non-null country values across 58 countries;
- 312 non-null exchange values across 6 exchanges;
- 4,716 non-null market-cap values;
- 282 rate-limited/error rows.

GAL therefore treats `sector_code` and `industry_code` as separate purity dimensions, adds a deterministic market-cap bucket, and reports country/exchange/security-type purity only where coverage is adequate. Missing exchange or security-type data is reported as low coverage rather than treated as a category.

Future columns such as `sub_industry`, `semantic_theme`, `business_theme`, `upstream_theme` or `downstream_theme` are automatically supported when categorical, or can be selected explicitly with `--dimensions`.
