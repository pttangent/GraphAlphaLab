# Complete GraphAlphaLab report specification

The goal is **information distillation**: preserve enough evidence to discuss results without repeatedly downloading large GFF outputs or asking an external model to recompute metrics.

## 1. Theme Discovery report

Theme Discovery is evaluated independently of Alpha. Metadata never creates or changes communities.

### Metadata completeness

For every selected dimension:

- metadata row and unique-ID count;
- duplicate-resolution count;
- member coverage;
- cardinality;
- alias used, such as `sector -> sector_code`;
- missing-data count;
- identity mode: symbol, symbol_id or security_entity_id.

### Per-theme, per-dimension metrics

- member and weighted member count;
- covered member count and coverage;
- dominant label and member count;
- count-based and weighted purity;
- HHI concentration;
- entropy and normalized entropy;
- effective category count;
- global dominant-label share;
- dominant-label lift over market prevalence.

### Snapshot-level clustering agreement

- NMI and ARI for each metadata dimension;
- theme count and metadata category count;
- covered members;
- warning that NMI/ARI must not be optimized alone.

### Interpretation list

The report must list:

- highest-purity themes;
- lowest-purity themes;
- high-stability, low-sector-purity cross-sector candidates;
- themes with insufficient metadata coverage;
- themes dominated by broad market-cap, country or exchange effects;
- dimensions where purity is informative versus nearly constant.

## 2. Alpha report: every factor

Each factor card is listed, not selectively summarized.

### Identity and lineage

- batch, factor, layer, scale, horizon and variant;
- input hashes and implementation commit;
- observed and expected batch contracts;
- partial/completed status.

### Data sufficiency

- observations, decisions, dates and symbols;
- score and label missingness;
- cross-section size distribution;
- coverage by date/time;
- sample-sufficiency gate.

### Predictive strength

- cross-sectional Spearman and Pearson IC;
- mean, median, standard deviation and ICIR;
- IC positive rate;
- t-stat, p-value and Benjamini-Hochberg FDR q-value;
- sign consistency and horizon decay when multiple horizons exist.

### Portfolio read-through

- quantile returns for every bucket;
- top and bottom leg returns;
- long-short spread;
- quantile monotonicity;
- hit rate, t-stat and annualized Sharpe under an explicit annualization factor;
- cumulative return and max drawdown.

### Risk and tail metrics

- standard deviation;
- skew and excess kurtosis;
- 5% VaR and CVaR;
- worst slices;
- long and short leg asymmetry;
- concentration warnings.

### Tradability and costs

- top/bottom membership turnover;
- gross and net results at 0/1/2/5/10 bps;
- cost-survival gate;
- optional market-cap, liquidity and participation-rate slices when supplied.

### Stability

- by date;
- by time of day;
- by market regime when supplied;
- by sector, industry, semantic theme and market-cap bucket when metadata is supplied;
- sensitivity to score winsorization, neutralization and quantile count when variants are supplied.

### Robustness and incremental value

The complete research framework should reserve fields for:

- reverse-direction, zero-lag and time-shuffle placebos;
- source/target symbol shuffle;
- degree-preserving rewiring;
- own-history and market-only baselines;
- node-only versus P0 graph versus P1 theme increment;
- factor ablation and residual incremental IC;
- factor-score correlation and redundancy clusters;
- multiple-testing correction.

Metrics not computable from the supplied columns must be explicitly marked unavailable, never silently omitted or invented.

## 3. Research decision states

Every factor receives one state:

- `candidate`: sufficient sample, FDR pass, direction consistent and survives 5 bps;
- `needs_falsification`: non-trivial signal but one or more robustness gates remain open;
- `insufficient_or_rejected`: insufficient data or failed core gates.

These states are research triage, not live-trading authorization.

## 4. Compact discussion layer

`REPORT.md` lists every factor and its key metrics. CSV files retain complete metric rows and evidence slices. `summary.json` contains only compact counts, lineage and decision summaries. This is the interface for later discussion.
