# IG27 + RM14 + Similarity10: 33-session GraphAlphaLab runbook

## Campaign contract

Date range:

```text
2026-06-01 through 2026-07-17
33 XNYS trading sessions
```

Upstream GFF batches:

```text
implemented27: 27 P0 + 27 P1 contracts per session
remaining14:   14 P0 + 14 P1 contracts per session
similarity10:  10 P0 + 10 layer-local P1 + 1 consensus P0 + 1 consensus P1 per session
```

GAL outputs:

```text
IG27 graph Alpha by horizon
IG27 P1 structure, relations, temporal and purity
RM14 graph Alpha by horizon
RM14 P1 structure, relations, temporal and purity
Similarity10 layer-local + consensus P1 structure, temporal and purity
All41 compact Alpha merges
One three-batch 33-session campaign report
```

Similarity P1 is not converted into a fabricated future-return factor. Its primary outputs are membership structure, recursive refinement quality, temporal stability, consensus behavior and metadata purity. Optional theme-level Alpha should be a separate governed research contract.

## Directory isolation

Use separate roots for the three batches. Do not point `p1-report` at a mixed 41-layer P1 root.

```text
D:\GFF\ig27\p0
D:\GFF\ig27\p1
D:\GFF\rm14\p0
D:\GFF\rm14\p1
D:\GFF\similarity\p0
D:\GFF\similarity\p1
D:\GFF\similarity\consensus
D:\GFF\similarity\range
```

This keeps contract counting deterministic and prevents an IG27 report from silently absorbing RM14 partitions.

## Git and environment gate

```powershell
cd D:\DEV\GraphAlphaLab

git fetch origin
git switch agent/batch-alpha-reporting
git reset --hard origin/agent/batch-alpha-reporting

$HEAD = (git rev-parse HEAD).Trim()

git status --short
python -m pip install -e ".[test]"
python -m compileall -q src scripts
python -m pytest -q --tb=short

gal export-gff-signals --help
gal alpha-report --help
gal p1-report --help
gal campaign-report --help
```

Requirements:

```text
git status --short is empty
all tests pass
all four CLI help commands return successfully
```

## Resource policy

Recommended initial limits for a 24-core, 128GB machine:

```text
GAL memory limit: 24GB
GAL DuckDB threads: 8
P1 report processing: sequential partition scan
Spill directory: dedicated NVMe path
```

```powershell
$TEMP = "D:\GAL\duckdb_tmp"
New-Item -ItemType Directory -Force $TEMP | Out-Null
```

Do not run the two Interaction signal exporters or Alpha reports simultaneously. Do not concatenate the 33-day P1 Membership Parquets with Pandas. `p1-report` validates and releases one governed partition at a time. High-cardinality purity detail is written as partitioned Parquet shards under the report bundle and summarized with bounded DuckDB; it is never accumulated into one multi-million-row Pandas table.

## Variables

```powershell
$IG27_P0 = "D:\GFF\ig27\p0"
$IG27_P1 = "D:\GFF\ig27\p1"
$RM14_P0 = "D:\GFF\rm14\p0"
$RM14_P1 = "D:\GFF\rm14\p1"
$SIM_P1 = "D:\GFF\similarity\p1"
$SIM_RANGE = "D:\GFF\similarity\range"

$METADATA = "D:\NFF\reference\symbol_metadata.parquet"
$SIGNALS = "D:\GAL\signals"
$LABELS = "D:\GAL\labels"
$CONTRACTS = "D:\GAL\contracts"
$REPORTS = "D:\GAL\reports"
$TEMP = "D:\GAL\duckdb_tmp"
```

Verify all roots before the run:

```powershell
Test-Path $IG27_P0
Test-Path $IG27_P1
Test-Path $RM14_P0
Test-Path $RM14_P1
Test-Path $SIM_P1
Test-Path $SIM_RANGE
Test-Path $METADATA
```

## Stage 1: export real Interaction graph signals

```powershell
gal export-gff-signals `
  --batch-id implemented27 `
  --p0-root $IG27_P0 `
  --variants "node_baseline,graph_forward,graph_reverse_placebo" `
  --output "$SIGNALS\implemented27" `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory $TEMP `
  --expected-git-commit $HEAD `
  --require-clean

gal export-gff-signals `
  --batch-id remaining14 `
  --p0-root $RM14_P0 `
  --variants "node_baseline,graph_forward,graph_reverse_placebo" `
  --output "$SIGNALS\remaining14" `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory $TEMP `
  --expected-git-commit $HEAD `
  --require-clean
```

Each export must end with `_SUCCESS`. The exporter rejects `edge_available_time > decision_time`.

## Stage 2: Interaction Alpha by horizon

Run each horizon independently. Example for 5m:

```powershell
gal alpha-report `
  --batch-id implemented27 `
  --signals "$SIGNALS\implemented27" `
  --labels "$LABELS\forward_5m.parquet" `
  --label-contract "$CONTRACTS\forward_5m.json" `
  --metadata $METADATA `
  --slice-dimensions "sector_code,industry_code,country,market_cap_bucket" `
  --join-keys "trade_date,decision_time,symbol_id" `
  --control-columns "own_score" `
  --default-direction auto `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory $TEMP `
  --min-cross-section 100 `
  --output "$REPORTS\implemented27_5m" `
  --expected-git-commit $HEAD `
  --require-clean

gal alpha-report `
  --batch-id remaining14 `
  --signals "$SIGNALS\remaining14" `
  --labels "$LABELS\forward_5m.parquet" `
  --label-contract "$CONTRACTS\forward_5m.json" `
  --metadata $METADATA `
  --slice-dimensions "sector_code,industry_code,country,market_cap_bucket" `
  --join-keys "trade_date,decision_time,symbol_id" `
  --control-columns "own_score" `
  --default-direction auto `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory $TEMP `
  --min-cross-section 100 `
  --output "$REPORTS\remaining14_5m" `
  --expected-git-commit $HEAD `
  --require-clean
```

Repeat for 15m and 30m using the matching labels and Label Contracts.

Every report must contain exactly 33 distinct dates in `daily_ic.csv`. `default-direction=auto` is exploratory and cannot produce governance-ready candidates.

## Stage 3: P1 reports for all three batches

### IG27

```powershell
gal p1-report `
  --batch-id implemented27 `
  --p1-root $IG27_P1 `
  --metadata $METADATA `
  --dimensions "sector_code,industry_code,country,market_cap_bucket" `
  --start-date 2026-06-01 `
  --end-date 2026-07-17 `
  --expected-date-count 33 `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory $TEMP `
  --output "$REPORTS\implemented27_p1" `
  --expected-git-commit $HEAD `
  --require-clean
```

### RM14

```powershell
gal p1-report `
  --batch-id remaining14 `
  --p1-root $RM14_P1 `
  --metadata $METADATA `
  --dimensions "sector_code,industry_code,country,market_cap_bucket" `
  --start-date 2026-06-01 `
  --end-date 2026-07-17 `
  --expected-date-count 33 `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory $TEMP `
  --output "$REPORTS\remaining14_p1" `
  --expected-git-commit $HEAD `
  --require-clean
```

### Similarity10 + recursive consensus P1

```powershell
gal p1-report `
  --batch-id similarity10 `
  --p1-root $SIM_P1 `
  --range-root $SIM_RANGE `
  --metadata $METADATA `
  --dimensions "sector_code,industry_code,country,market_cap_bucket,semantic_theme" `
  --start-date 2026-06-01 `
  --end-date 2026-07-17 `
  --expected-date-count 33 `
  --require-consensus `
  --memory-limit-gb 24 `
  --threads 8 `
  --temp-directory $TEMP `
  --output "$REPORTS\similarity10_p1" `
  --expected-git-commit $HEAD `
  --require-clean
```

Remove `semantic_theme` if the metadata file does not contain that column. Do not fabricate it.

Similarity completion gate:

```text
33 dates
11 observed P1 contracts
10 layer-local layer-scale contracts
similarity_consensus@0m
0 partition PIT violations
all partitions have valid manifest hashes and _SUCCESS
```

## Stage 4: all41 compact merges

```powershell
gal merge-reports `
  --inputs "$REPORTS\implemented27_5m" "$REPORTS\remaining14_5m" `
  --output "$REPORTS\all41_5m"

gal merge-reports `
  --inputs "$REPORTS\implemented27_15m" "$REPORTS\remaining14_15m" `
  --output "$REPORTS\all41_15m"

gal merge-reports `
  --inputs "$REPORTS\implemented27_30m" "$REPORTS\remaining14_30m" `
  --output "$REPORTS\all41_30m"
```

These commands read compact report bundles only.

## Stage 5: final three-batch campaign report

```powershell
gal campaign-report `
  --implemented27-alpha `
    "$REPORTS\implemented27_5m" `
    "$REPORTS\implemented27_15m" `
    "$REPORTS\implemented27_30m" `
  --implemented27-p1 "$REPORTS\implemented27_p1" `
  --remaining14-alpha `
    "$REPORTS\remaining14_5m" `
    "$REPORTS\remaining14_15m" `
    "$REPORTS\remaining14_30m" `
  --remaining14-p1 "$REPORTS\remaining14_p1" `
  --similarity-p1 "$REPORTS\similarity10_p1" `
  --all41-alpha `
    "$REPORTS\all41_5m" `
    "$REPORTS\all41_15m" `
    "$REPORTS\all41_30m" `
  --start-date 2026-06-01 `
  --end-date 2026-07-17 `
  --expected-date-count 33 `
  --output "$REPORTS\three_batch_33day"
```

The campaign command refuses:

```text
missing _SUCCESS
partial child reports
missing required IG27/RM14/Similarity10 sources
any primary child report with fewer or more than 33 dates
any primary date outside 2026-06-01 through 2026-07-17
```

Derived `all41` compact reports do not repeat `daily_ic.csv`; they are accepted only after their governed summary and `_SUCCESS` are validated.

## P1 report outputs

Each P1 bundle contains:

```text
REPORT.md
summary.json
run_manifest.json
p1_partition_summary.csv
p1_snapshot_structure.csv
p1_layer_summary.csv
p1_temporal_summary.csv
p1_relation_summary.csv
p1_range_temporal_summary.csv
theme_purity_parts/part-*.parquet
theme_purity_parts_manifest.json
purity_dimension_summary.csv
purity_snapshot_agreement_parts/part-*.parquet
purity_snapshot_agreement_parts_manifest.json
purity_snapshot_agreement_summary.csv
metadata_profile.json
_SUCCESS
```

The detailed purity and agreement evidence remains partitioned on disk to prevent OOM. The summary CSV files are compact and suitable for campaign merging.

Range Temporal may be empty for Interaction batches if GFF did not produce range links. Daily `temporal_edges.parquet` remains part of every governed P1 partition.

## Final campaign outputs

```text
REPORT.md
summary.json
campaign_sources.csv
campaign_batch_summary.csv
campaign_alpha_metrics.csv
campaign_p1_layers.csv
campaign_temporal.csv
campaign_purity.csv
_SUCCESS
```

## Final local-AI report

The local AI must report:

```text
GAL branch and commit
GFF commit for each batch
33-session date list
IG27 P0/P1 partition counts
RM14 P0/P1 partition counts
Similarity10 layer-local and consensus P1 counts
PIT violations
invalid or missing manifests
Alpha factor counts by horizon
candidate / needs-falsification / rejected counts
5 bps survivors
P1 mean and maximum theme sizes
forced non-canonical chunk counts
continue / split / merge / split_merge counts
Similarity consensus temporal links
purity by metadata dimension
peak RSS and DuckDB spill usage
all output bundle paths
```

Do not claim that Similarity purity proves Alpha. Do not claim that node baselines prove network Alpha. Do not promote auto-direction factors without a new predeclared out-of-sample run.
