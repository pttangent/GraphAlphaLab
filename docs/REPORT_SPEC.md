# GraphAlphaLab report specification

The report is an information-distillation interface: enough evidence for discussion without reopening large GFF partitions.

## Mandatory identity

Every factor card includes batch, factor, layer, scale, variant, label ID, horizon, direction source, input hashes and run contract hash.

## Mandatory PIT section

- signal rows and signal-availability violations;
- label rows and duplicate keys;
- entry/exit/available-time violations;
- clock-time horizon mismatch;
- Label Contract JSON and hash.

## Alpha evidence

- snapshot IC series for diagnostics;
- daily IC series for inference;
- mean/median daily RankIC and ICIR;
- daily sign consistency;
- daily t-stat, p-value and BH-FDR;
- raw top-minus-bottom and direction-oriented spread;
- per-decision turnover and 0/1/2/5/10 bps net results;
- annualization validity and diagnostic daily Sharpe;
- drawdown, VaR, CVaR, skew and kurtosis;
- metadata stability slices;
- deterministic sampled score-correlation matrix;
- governance-ready and research-status gates.

## Candidate gate

A factor cannot be `candidate` unless:

- at least 20 trading dates and 100 valid decisions exist;
- PIT and Label Contract pass;
- direction was predeclared;
- annualization is valid;
- FDR passes;
- daily sign consistency passes;
- 5 bps cost survives.

Graph incremental-value discussion additionally requires comparison against node baseline and reverse/shuffle placebo. Missing evidence is marked unavailable, never inferred.
