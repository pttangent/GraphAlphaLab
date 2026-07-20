# Implementation status

Branch: `agent/monthly-alpha-research`

Implemented:

- governed monthly partition discovery for NFF/P0/P1
- reusable PIT-safe alpha-base cache
- Node, one-hop Graph, strict leave-one-out P1 and hierarchy residual experiments
- direction and absolute-return risk targets at 5m, 15m and 30m
- partition-level resume with config/input fingerprints
- RAM and disk resource gates
- governance findings for successful-but-empty graphs and missing core files
- per-partition shards, dashboard, progress JSON and monthly report outputs
- Windows/Linux CI across Python 3.11 and 3.13
- governed sector/industry reference mappings with source, confidence and effective-date support
- theme semantic coverage, total purity, mapped purity, entropy and HHI
- strict same-sector LOO, cross-sector LOO and semantic-disagreement signals

Validation:

- baseline suite before semantic extension: 4 tests passed
- focused semantic suite: 3 tests passed
- semantic tests cover purity inflation from unmapped members, PIT effective-date selection and strict LOO exclusion
- CLI `--help`: pass

Known boundaries:

- this branch does not claim a completed full production-month run on the user's local warehouse
- the uploaded 1,264-symbol mapping remains user reference data and is not committed as production truth
- semantic labels are explanatory until monthly and out-of-sample incremental IC proves otherwise
