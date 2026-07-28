# Dual-theme daily labels and rolling Alpha

## Purpose

This workflow extends the induced-global dual-theme campaign with governed trading-session labels and rolling-window reports. It does not modify GFF or rebuild any graph.

Canonical runner:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_core4_daily_alpha_70d.ps1
```

The default run consumes the last 70 trading sessions from the GFF campaign through `2026-07-22`, evaluates Global, induced-global/Within-Theme and Inter-Theme factors, then writes 20/40/60-session rolling reports every five sessions.

## Daily labels

Core labels:

- `daily_n_close_to_n1_open`
- `daily_n_close_to_n1_close`
- `daily_n1_open_to_n1_close`

The optional `extended` profile also builds N+3, N+5 and N+7 close labels.

The label builder:

1. reads the governed date list from `runs/campaign_contract.json`;
2. takes the requested 70-session analysis slice;
3. reads canonical `bars_1m`;
4. converts timestamps with the `America/New_York` timezone and selects 09:30–16:00 RTH, so January and July are both DST-correct;
5. uses the governed exported Global signal decision grid, preserving the existing join key `trade_date,decision_time,symbol_id`;
6. writes one checkpoint for each base trade date;
7. writes one Parquet dataset and one contract per label.

## Trading-session contract

Daily horizons are governed by:

```text
horizon_unit = trading_sessions
entry_session_offset
exit_session_offset
entry_point = open | close
exit_point = open | close
```

Wall-clock minutes are not the authoritative daily horizon because weekends, holidays and DST make that representation incorrect. Existing minute-level contracts retain their original serialized payload and contract hash, so old intraday checkpoints remain reusable.

## Missing future labels

A base date is valid even when its required future session is not yet available. For example, when bars after 2026-07-22 are absent, an N+1 label for 2026-07-22 is simply unavailable.

The workflow never:

- invents a zero return;
- forward-fills a future price;
- substitutes an earlier exit;
- counts an unavailable tail date as an observed label.

It records the missing dates in `diagnostics/daily_label_coverage.csv`. Missing dates are allowed only at the trailing edge. An interior label gap is a hard error.

When future bars arrive, the base-date checkpoint source hash changes only for the affected tail dates; completed historical date checkpoints are reused.

## Alpha scopes

The generated horizon manifest is passed to the existing six-worker global factor DAG, so every daily label evaluates:

- Global stock Alpha;
- induced-global graph plus Within-Theme local ranking;
- Inter-Theme portfolio Alpha;
- node baseline, graph-forward and reverse-edge placebo variants.

The factor checkpoint granularity remains `horizon × scope × factor`.

## Rolling Alpha

Rolling windows default to 20, 40 and 60 sessions with a five-session step. Each row reports:

- expected and observed sessions;
- coverage ratio;
- mean IC, IC t-stat and p-value;
- sign consistency;
- PIT-oriented gross return;
- net return at 0/1/2/5/10 bps;
- turnover, hit rate and max drawdown;
- label tail coverage.

Direction governance:

- a predeclared direction is used when present;
- otherwise the direction is inferred only from daily IC observations strictly before the rolling window;
- at least 20 prior dates are required, using at most the latest 60;
- same-window direction is not used for the PIT-oriented result.

Outputs:

```text
rolling_alpha/rolling_alpha_metrics.csv
rolling_alpha/rolling_scope_summary.csv
rolling_alpha/rolling_stability_summary.csv
rolling_alpha/summary.json
rolling_alpha/REPORT.md
rolling_alpha/_SUCCESS
```

## Default paths

```text
GFF campaign:
D:\G4H\campaign=c4_260105_260722_induced_v2

NFF bars:
D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1

GAL labels:
D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\labels\c4_260105_260722_daily70

GAL reports:
D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\reports\c4_260105_260722_daily70
```
